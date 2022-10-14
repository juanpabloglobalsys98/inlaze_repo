import logging

from api_admin.models import LevelPercentageBase
from api_partner.helpers import (
    DB_USER_PARTNER,
    HasLevel,
    IsActive,
    IsNotBanned,
    IsTerms,
    PartnerAccumStatusCHO,
    fx_conversion_campaign_fixed_income_cases,
)
from api_partner.models import (
    AdditionalInfo,
    Campaign,
    FxPartner,
    Link,
    Partner,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from api_partner.serializers import CampaignPartnerSerializer
from cerberus import Validator
from core.helpers import (
    CurrencyAll,
    to_int,
)
from django.db import models
from django.db.models import (
    Case,
    F,
    Prefetch,
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class CampaignPartnerAPI(APIView):
    """
        Returning campaigns to partner
    """
    permission_classes = [
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        HasLevel,
        IsTerms,
    ]

    def get(self, request):
        """
            Method that return the campaigns

            #Params

           -  status : "int"
                Param to define status filter to return records
        """

        validator = Validator(
            schema={
                "status": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": [
                        1,
                        2,
                        3,
                    ],
                },
            },
        )

        if not validator.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = request.user
        partner = request.user.partner

        level = LevelPercentageBase.objects.order_by("-created_at").first()
        partner = Partner.objects.filter(user_id=request.user.id).first()
        query = Q(cpa_count__gt=0, partner_link_accumulated__partner_id=request.user.id)
        partner_daily = PartnerLinkDailyReport.objects.filter(query)
        cpa = False
        if partner_daily:
            cpa = True

        if partner.campaign_status == Partner.CampaignStatus.NOT_VIEW:
            return Response(
                data={
                    "count": 0,
                    "campaigns": [],
                    "country": partner.additionalinfo.country,
                    "level": partner.level,
                    "cpa_count": cpa,
                },
                status=status.HTTP_200_OK,
            )

        # Force conversion to USD with annotate cases
        fx_conversion_fixed_income_cases = fx_conversion_campaign_fixed_income_cases(
            model_func=F,
            currency_local=CurrencyAll.USD,
        )

        # Filter campaings part 1
        filters = [
            Q(
                partnerlinkaccumulated_to_campaign__partner=partner,
                partnerlinkaccumulated_to_campaign__status=PartnerAccumStatusCHO.ACTIVE,
                partnerlinkaccumulated_to_campaign__is_assigned=False,
            ),
            # ~Q(status=Campaign.Status.NOT_AVALAIBLE)
        ]
        campaign_ids_by_partner_accum_status = set(Campaign.objects.filter(*filters).values_list("pk", flat=True))

        # list pks

        filters = [
            Q(
                partnerlinkaccumulated_to_campaign__partner=partner,
                partnerlinkaccumulated_to_campaign__status=PartnerAccumStatusCHO.BY_CAMPAIGN,
                partnerlinkaccumulated_to_campaign__is_assigned=False,
            ),
            ~Q(status=Campaign.Status.NOT_AVALAIBLE)
        ]
        campaign_ids_by_partner_accum_campaign = set(Campaign.objects.filter(*filters).values_list("pk", flat=True))

        filters = [
            ~Q(partnerlinkaccumulated_to_campaign__partner=partner),
            ~Q(status=Campaign.Status.NOT_AVALAIBLE)
        ]
        campaign_ids_by_campaign = set(Campaign.objects.filter(*filters).values_list("pk", flat=True))

        campaigns_id = set.union(campaign_ids_by_partner_accum_status,
                                 campaign_ids_by_partner_accum_campaign, campaign_ids_by_campaign)

        filters = [
            Q(pk__in=campaigns_id),
        ]

        # TEMP special case
        if(user.pk == 156):
            # pk 43 is Galera bet BR
            # pk 38 is 888Sport ESP
            filters.append(
                Q(
                    pk__in=(43, 38,)
                )
            )
        elif(user.pk == 210):
            filters.append(
                Q(
                    pk=-1
                )
            )
        if "status" in validator.document:
            filters.append(
                Q(status=validator.document.get("status")),
            )
            filters.append(
                ~Q(partnerlinkaccumulated_to_campaign__status=PartnerAccumStatusCHO.INACTIVE),
            )

        # Prefetch links data about the partner of current session
        filters_partner_link_accumulated = [
            Q(partner=partner),
            Q(is_assigned=False),
        ]

        partner_link_accumulated_pref = PartnerLinkAccumulated.objects.filter(*filters_partner_link_accumulated)

        # Get Campaigns
        campaigns = Campaign.objects.prefetch_related(
            Prefetch(
                lookup="partnerlinkaccumulated_to_campaign",
                queryset=partner_link_accumulated_pref,
                to_attr="partner_link_accumulated",
            ),
        ).select_related(
            "bookmaker",
        ).filter(
            *filters,
        ).annotate(
            bookmaker_name=F("bookmaker__name"),
            campaign_name=Concat(
                F("bookmaker__name"),
                Value(" "),
                F("title"),
            ),
            fixed_income_unitary_usd=Case(
                *fx_conversion_fixed_income_cases,
                default=0.0,
                output_field=models.FloatField(),
            ),
        ).order_by("-temperature")

        filters = (
            Q(created_at__gte=timezone.now()),
        )

        fx_partner = FxPartner.objects.using(DB_USER_PARTNER).filter(*filters).order_by("created_at").first()

        if(fx_partner is None):
            # Get just next from supplied date
            filters = (
                Q(created_at__lte=timezone.now()),
            )
            fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        fx_partner_percentage = fx_partner.fx_percentage

        campaign_serializer = CampaignPartnerSerializer(
            campaigns,
            many=True,
            context={
                "user": user,
                "partner": partner,
                "level": level.percentages,
                "fx_partner": fx_partner,
                "fx_partner_percentage": fx_partner_percentage,
            }
        )

        additional_info_partner = AdditionalInfo.objects.filter(
            partner_id=request.user.id
        ).first()

        if hasattr(additional_info_partner, "country") == False:
            logger.warning(
                f"this partner doesnt have country \"{request.user.id}\""
            )
            return Response(
                data={
                    "count": campaigns.count(),
                    "campaigns": campaign_serializer.data,
                    "country": "COL",
                    "level": partner.level,
                    "cpa_count": cpa,

                },
                status=status.HTTP_200_OK,
            )

        return Response(
            data={
                "count": campaigns.count(),
                "campaigns": campaign_serializer.data,
                "country": partner.additionalinfo.country,
                "level": partner.level,
                "cpa_count": cpa,
            },
            status=status.HTTP_200_OK,
        )


class CampaignAssignedAPI(APIView):
    """ Returning campaigns assigned to partner """
    permission_classes = [
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsTerms,
    ]

    def get(self, request):
        user = request.user
        partner = user.partner

        # Prefetch links data about the partner of current session
        filters = [
            Q(partner=partner),
            Q(link_to_partner_link_accumulated__isnull=False),
        ]

        partner_link_accumulated_pref = PartnerLinkAccumulated.objects.filter(*filters)
        partner_link_accumulated_pks = partner_link_accumulated_pref.values_list("pk", flat=True)

        filters = [Q(partner_link_accumulated__pk__in=partner_link_accumulated_pks)]
        links_pref = Link.objects.filter(*filters).select_related("partner_link_accumulated")

        # Force conversion to USD with annotate cases
        fx_conversion_fixed_income_cases = fx_conversion_campaign_fixed_income_cases(
            model_func=F,
            currency_local=CurrencyAll.USD,
        )

        # Get campaings included respective prefetch
        filters = [Q(link_to_campaign__pk__in=links_pref)]
        campaigns = Campaign.objects.prefetch_related(
            Prefetch(
                lookup="link_to_campaign",
                queryset=links_pref,
                to_attr="link",
            ),
            Prefetch(
                lookup="partnerlinkaccumulated_to_campaign",
                queryset=partner_link_accumulated_pref,
                to_attr="partner_link_accumulated",
            ),
        ).select_related(
            "bookmaker",
        ).filter(
            *filters
        ).annotate(
            bookmaker_name=F("bookmaker__name"),
            campaign_name=Concat(
                F("bookmaker__name"),
                Value(" "),
                F("title"),
            ),
            fixed_income_unitary_usd=Case(
                *fx_conversion_fixed_income_cases,
                default=0.0,
                output_field=models.FloatField(),
            ),
        ).order_by("-temperature").distinct()

        filters = (
            Q(created_at__gte=timezone.now()),
        )

        fx_partner = FxPartner.objects.using(DB_USER_PARTNER).filter(*filters).order_by("created_at").first()

        if(fx_partner is None):
            # Get just next from supplied date
            filters = (
                Q(created_at__lte=timezone.now()),
            )
            fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        level = LevelPercentageBase.objects.order_by("-created_at").first()
        partner = Partner.objects.filter(user_id=request.user.id).first()

        fx_partner_percentage = fx_partner.fx_percentage
        campaign_serializer = CampaignPartnerSerializer(
            campaigns,
            many=True,
            context={
                "user": user,
                "partner": partner,
                "level": level.percentages,
                "fx_partner": fx_partner,
                "fx_partner_percentage": fx_partner_percentage,
            }
        )

        additional_info_partner = AdditionalInfo.objects.filter(
            partner_id=request.user.id
        ).first()

        if hasattr(additional_info_partner, "country") == False:
            logger.warning(
                f"this partner doesnt have country \"{request.user.id}\""
            )
            return Response(
                data={
                    "count": campaigns.count(),
                    "campaigns": campaign_serializer.data,
                    "country": "COL",
                },
                status=status.HTTP_200_OK,
            )

        return Response(
            data={
                "count": campaigns.count(),
                "campaigns": campaign_serializer.data,
                "country": partner.additionalinfo.country,
            }, status=status.HTTP_200_OK,
        )
