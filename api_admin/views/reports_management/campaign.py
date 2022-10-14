import logging

from api_admin.helpers import (
    DB_ADMIN,
    calculate_temperature,
    create_history,
)
from api_admin.models import LevelPercentageBase
from api_admin.paginators import GetAllCampaigns
from api_admin.serializers import (
    CampaignBasicSer,
    CampaignManageSer,
    CampaignSer,
    HistoricalCampaignSER,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerAccumUpdateReasonCHO,
    PartnerLevelCHO,
)
from api_partner.models import (
    Bookmaker,
    Campaign,
    HistoricalCampaign,
    PartnerLinkAccumulated,
)
from api_partner.serializers import CampaignPartnerBasicSER
from cerberus import Validator
from core.helpers import (
    CountryCampaign,
    CurrencyAll,
    CurrencyCondition,
    CurrencyFixedIncome,
    HavePermissionBasedView,
    to_int,
)
from django.conf import settings
from django.db import transaction
from django.db.models import (
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


class CampaignAPI(APIView, GetAllCampaigns):
    """
        Class view with CRUD to campaign
    """
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
            Method to get campaign based in filters
        """

        validator = Validator(
            schema={
                "campaign_title": {
                    "required": False,
                    "type": "string",
                },
                "countries": {
                    "required": False,
                    "type": "string",
                },
                "status": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Campaign.Status.values,
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "default": "-campaign_title",
                    "allowed": (
                        CampaignSer.Meta.fields +
                        tuple(["-"+i for i in CampaignSer.Meta.fields])
                    ),
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                }
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = []
        if "campaign_title" in validator.document:
            filters.append(
                Q(
                    campaign_title__icontains=validator.document.get("campaign_title")
                )
            )

        if "countries" in validator.document:
            query = Q()
            for country_i in validator.document.getlist("countries", ""):
                query |= Q(
                    countries__icontains=country_i
                )
            filters.append(query)

        if "status" in validator.document:
            filters.append(
                Q(status=validator.document.get("status"))
            )

        if "sort_by" in validator.document:
            validator.document["sort_by"] = validator.document.get("sort_by")

        campaigns = Campaign.objects.using(DB_USER_PARTNER).annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        ).filter(*filters).order_by(validator.document.get("sort_by"))

        campaign_pag = self.paginate_queryset(
            queryset=campaigns,
            request=request,
            view=self,
        )

        campaign_serializer = CampaignSer(
            instance=campaign_pag,
            many=True,
        )

        return Response(
            data={
                "campaigns": campaign_serializer.data,
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        """ Create a campaign based in bookmakers """
        validator = Validator(
            schema={
                "bookmaker": {
                    "required": True,
                    "type": "integer",
                },
                "title": {
                    "required": True,
                    "type": "string",
                },
                "deposit_condition": {
                    "required": True,
                    "type": "float",
                },
                "stake_condition": {
                    "required": True,
                    "type": "float",
                },
                "lose_condition": {
                    "required": True,
                    "type": "float",
                },
                "deposit_condition_campaign_only": {
                    "required": True,
                    "type": "float",
                },
                "stake_condition_campaign_only": {
                    "required": True,
                    "type": "float",
                },
                "lose_condition_campaign_only": {
                    "required": True,
                    "type": "float",
                },
                "default_percentage": {
                    "required": False,
                    "type": "float",
                },
                "tracker": {
                    "required": False,
                    "type": "float",
                },
                "tracker_deposit": {
                    "required": False,
                    "type": "float",
                },
                "tracker_registered_count": {
                    "required": False,
                    "type": "float",
                },
                "tracker_first_deposit_count": {
                    "required": False,
                    "type": "float",
                },
                "tracker_wagering_count": {
                    "required": False,
                    "type": "float",
                },
                "currency_condition": {
                    "required": True,
                    "type": "string",
                    "allowed": CurrencyCondition.values,
                },
                "currency_condition_campaign_only": {
                    "required": True,
                    "type": "string",
                    "allowed": CurrencyAll.values,
                },
                "countries": {
                    "required": True,
                    "type": "list",
                    "schema": {
                        "type": "string",
                        "allowed": CountryCampaign.values,
                    },
                },
                "fixed_income_unitary": {
                    "required": True,
                    "type": "float",
                },
                "currency_fixed_income": {
                    "required": True,
                    "type": "string",
                    "allowed": CurrencyFixedIncome.values,
                },
                "cpa_limit": {
                    "required": False,
                    "type": "integer",
                    "default": -1,
                },
                "status": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Campaign.Status.allowed_save(),
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(id=validator.document.get("bookmaker")),
        )
        bookmaker = Bookmaker.objects.filter(*filters).first()

        if bookmaker is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "Bookmaker": [
                            "Bookmaker not found",
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.document['bookmaker'] = bookmaker.pk
        annotates = {
            "campaign_title": Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        }

        # Validate if exist another campaign_title incluided case insensitive
        campaign_title = f"{bookmaker.name} {validator.document.get('title')}"
        filters = (
            Q(campaign_title__iexact=campaign_title),
        )
        campaign = Campaign.objects.annotate(
            **annotates
        ).using(DB_USER_PARTNER).filter(*filters)

        if campaign:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "Campaign": [
                            _("Already a campaing exits with these name"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        campaign_ser = CampaignManageSer(data=validator.document)

        if (campaign_ser.is_valid()):
            campaign = campaign_ser.save()
            return Response(
                data={
                    "pk": campaign.pk,
                    "msg": _("Campaign created successfully")
                },
                status=status.HTTP_201_CREATED,
            )
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": campaign_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

    def patch(self, request):
        """ Update campaign """
        validator = Validator(
            schema={
                "id": {
                    "required": True,
                    "type": "integer",
                },
                "title": {
                    "required": False,
                    "type": "string",
                },
                "deposit_condition": {
                    "required": False,
                    "type": "float",
                },
                "stake_condition": {
                    "required": False,
                    "type": "float",
                },
                "lose_condition": {
                    "required": False,
                    "type": "float",
                },
                "deposit_condition_campaign_only": {
                    "required": False,
                    "type": "float",
                },
                "stake_condition_campaign_only": {
                    "required": False,
                    "type": "float",
                },
                "lose_condition_campaign_only": {
                    "required": False,
                    "type": "float",
                },
                "currency_condition_campaign_only": {
                    "required": False,
                    "type": "string",
                    "allowed": CurrencyAll.values,
                },
                "countries": {
                    "required": False,
                    "type": "list",
                    "schema": {
                        "type": "string",
                        "allowed": CountryCampaign.values,
                    },
                },
                "fixed_income_unitary": {
                    "required": False,
                    "type": "float",
                },
                "status": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Campaign.Status.allowed_save(),
                },
                "default_percentage": {
                    "required": False,
                    "type": "float",
                },
                "tracker": {
                    "required": False,
                    "type": "float",
                },
                "tracker_deposit": {
                    "required": False,
                    "type": "float",
                },
                "tracker_registered_count": {
                    "required": False,
                    "type": "float",
                },
                "tracker_first_deposit_count": {
                    "required": False,
                    "type": "float",
                },
                "tracker_wagering_count": {
                    "required": False,
                    "type": "float",
                },
                "cpa_limit": {
                    "required": False,
                    "type": "integer",
                    "default": -1,
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(pk=validator.document.get("id")),
        )
        campaign = Campaign.objects.select_related(
            "bookmaker"
        ).only(
            "bookmaker__name"
        ).filter(*filters).first()

        if campaign is None:
            return Response(
                {
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "Campaign": [
                            _("Campaign not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Validate if exist another campaign_title incluided case insensitive
        campaign_title_new = f"{campaign.bookmaker.name} {validator.document.get('title')}"
        campaign_title_old = f"{campaign.bookmaker.name} {campaign.title}"

        annotates = {
            "campaign_title": Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        }

        filters = (
            Q(campaign_title__iexact=campaign_title_new),
            ~Q(campaign_title__iexact=campaign_title_old),
        )
        campaign_check = Campaign.objects.annotate(
            **annotates
        ).using(DB_USER_PARTNER).filter(*filters).first()

        if campaign_check is not None:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "Campaign": [
                            _("Already a campaing exits with these name"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # datetime now
        datetime_now = timezone.now()

        validator.document['updated_at'] = datetime_now

        # Validate if the comission will be updated
        if validator.document.get("fixed_income_unitary") != campaign.fixed_income_unitary:
            validator.document['fixed_income_updated_at'] = datetime_now

        # Check if status change to inactive
        if validator.document.get("status") == Campaign.Status.INACTIVE and campaign.status != Campaign.Status.INACTIVE:
            validator.document['last_inactive_at'] = datetime_now

        campaign_status, campaing_temperature = calculate_temperature(campaign, validator.document.get("status"))
        validator.document["status"] = campaign_status
        validator.document["temperature"] = campaing_temperature

        # Update porcent in partner_link_accumulated
        with transaction.atomic(using=DB_USER_PARTNER, savepoint=True):
            sid = transaction.savepoint(using=DB_USER_PARTNER)
            if "default_percentage" in validator.document:
                percentages_new = validator.document.get("default_percentage")
                query = Q(is_percentage_custom=False, is_assigned=True, campaign=validator.document.get("id"))
                partner_accum = PartnerLinkAccumulated.objects.select_related(
                    "campaign",
                ).only(
                    "campaign__default_percentage",
                    "percentage_cpa",
                    "pk",
                    "partner_level",
                ).filter(query)

                level_percentage = LevelPercentageBase.objects.order_by("-created_at").first()
                level_percentage_data = level_percentage.percentages

                update_partner_accum = []
                for partner_accum_i in partner_accum:
                    partner_accum_i.percentage_cpa = (
                        (level_percentage_data.get(str(partner_accum_i.partner_level))) * percentages_new

                    )
                    update_partner_accum.append(partner_accum_i)

                if(update_partner_accum):
                    PartnerLinkAccumulated.objects.bulk_update(
                        objs=update_partner_accum,
                        fields=(
                            "percentage_cpa",
                        ),
                        batch_size=999,
                    )
                partner_accum = PartnerLinkAccumulated.objects.select_related(
                    "campaign",
                ).filter(query)

                for partner_accum_i in partner_accum:
                    create_history(
                        instance=partner_accum_i,
                        update_reason=PartnerAccumUpdateReasonCHO.CAMPAIGN,
                        adviser=request.user.id,
                    )

            if "tracker" in validator.document:
                partner_link_accumulateds = PartnerLinkAccumulated.objects.filter(
                    Q(campaign=campaign),
                    Q(tracker=campaign.tracker)
                )
                list_to_update = []
                for partner_link_accumulated_i in partner_link_accumulateds:
                    partner_link_accumulated_i.tracker = validator.document.get("tracker")
                    list_to_update.append(partner_link_accumulated_i)

                PartnerLinkAccumulated.objects.bulk_update(
                    objs=list_to_update,
                    fields=(
                        "tracker",
                    )
                )
            if "tracker_deposit" in validator.document:
                partner_link_accumulateds = PartnerLinkAccumulated.objects.filter(
                    Q(campaign=campaign),
                    Q(tracker_deposit=campaign.tracker_deposit)
                )
                list_to_update = []
                for partner_link_accumulated_i in partner_link_accumulateds:
                    partner_link_accumulated_i.tracker_deposit = validator.document.get("tracker_deposit")
                    list_to_update.append(partner_link_accumulated_i)

                PartnerLinkAccumulated.objects.bulk_update(
                    objs=list_to_update,
                    fields=(
                        "tracker_deposit",
                    )
                )
            if "tracker_registered_count" in validator.document:
                partner_link_accumulateds = PartnerLinkAccumulated.objects.filter(
                    Q(campaign=campaign),
                    Q(tracker_registered_count=campaign.tracker_registered_count)
                )
                list_to_update = []
                for partner_link_accumulated_i in partner_link_accumulateds:
                    partner_link_accumulated_i.tracker_registered_count = validator.document.get(
                        "tracker_registered_count")
                    list_to_update.append(partner_link_accumulated_i)

                PartnerLinkAccumulated.objects.bulk_update(
                    objs=list_to_update,
                    fields=(
                        "tracker_registered_count",
                    )
                )
            if "tracker_first_deposit_count" in validator.document:
                partner_link_accumulateds = PartnerLinkAccumulated.objects.filter(
                    Q(campaign=campaign),
                    Q(tracker_registered_count=campaign.tracker_registered_count)
                )
                list_to_update = []
                for partner_link_accumulated_i in partner_link_accumulateds:
                    partner_link_accumulated_i.tracker_first_deposit_count = validator.document.get(
                        "tracker_first_deposit_count")
                    list_to_update.append(partner_link_accumulated_i)

                PartnerLinkAccumulated.objects.bulk_update(
                    objs=list_to_update,
                    fields=(
                        "tracker_first_deposit_count",
                    )
                )
            if "tracker_wagering_count" in validator.document:
                partner_link_accumulateds = PartnerLinkAccumulated.objects.filter(
                    Q(campaign=campaign),
                    Q(tracker_wagering_count=campaign.tracker_wagering_count)
                )
                list_to_update = []
                for partner_link_accumulated_i in partner_link_accumulateds:
                    partner_link_accumulated_i.tracker_wagering_count = validator.document.get(
                        "tracker_wagering_count")
                    list_to_update.append(partner_link_accumulated_i)

                PartnerLinkAccumulated.objects.bulk_update(
                    objs=list_to_update,
                    fields=(
                        "tracker_wagering_count",
                    )
                )
            # campaign historic process
            campaign_ser = CampaignManageSer(
                instance=campaign,
                data=validator.document,
                partial=True,
            )

            if (campaign_ser.is_valid()):
                campaign = campaign_ser.save()

            else:
                transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "detail": campaign_ser.errors,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            list_campaign = []
            id = validator.document.get("id")
            modified_by_id = request.user.id
            campaign = Campaign.objects.filter(pk=id).values(
                "id",
                "bookmaker",
                "title",
                "deposit_condition",
                "stake_condition",
                "lose_condition",
                "deposit_condition_campaign_only",
                "stake_condition_campaign_only",
                "lose_condition_campaign_only",
                "currency_condition_campaign_only",
                "status",
                "fixed_income_unitary",
                "currency_fixed_income",
                "default_percentage",
                "tracker",
                "tracker_deposit",
                "tracker_registered_count",
                "tracker_first_deposit_count",
                "tracker_wagering_count",
                "temperature",
                "cpa_limit",
                "countries",
            ).first()
            current_campaign = HistoricalCampaign.objects.filter(campaign_id=id).values(
                "campaign",
                "modified_by_id",
                "bookmaker",
                "title",
                "deposit_condition",
                "stake_condition",
                "lose_condition",
                "deposit_condition_campaign_only",
                "stake_condition_campaign_only",
                "lose_condition_campaign_only",
                "currency_condition_campaign_only",
                "status",
                "fixed_income_unitary",
                "currency_fixed_income",
                "default_percentage",
                "tracker",
                "tracker_deposit",
                "tracker_registered_count",
                "tracker_first_deposit_count",
                "tracker_wagering_count",
                "temperature",
                "cpa_limit",
                "countries",
            ).first()
            # current_campaign.modified_by = modified_by_id
            current_campaign["modified_by_id"] = modified_by_id
            if "id" in campaign:
                current_campaign["campaign"] = campaign["id"]
            if "bookmaker" in campaign:
                current_campaign["bookmaker"] = campaign["bookmaker"]
            if "title" in campaign:
                current_campaign["title"] = campaign["title"]
            if "deposit_condition" in campaign:
                current_campaign["deposit_condition"] = campaign["deposit_condition"]
            if "stake_condition" in campaign:
                current_campaign["stake_condition"] = campaign["stake_condition"]
            if "lose_condition" in campaign:
                current_campaign["lose_condition"] = campaign["lose_condition"]
            if "deposit_condition_campaign_only" in campaign:
                current_campaign["deposit_condition_campaign_only"] = campaign["deposit_condition_campaign_only"]
            if "stake_condition_campaign_only" in campaign:
                current_campaign["stake_condition_campaign_only"] = campaign["stake_condition_campaign_only"]
            if "lose_condition_campaign_only" in campaign:
                current_campaign["lose_condition_campaign_only"] = campaign["lose_condition_campaign_only"]
            if "currency_condition_campaign_only" in campaign:
                current_campaign["currency_condition_campaign_only"] = campaign["currency_condition_campaign_only"]
            if "status" in campaign:
                current_campaign["status"] = campaign["status"]

            if "countries" in campaign:
                current_campaign["countries"] = campaign["countries"]
            if "fixed_income_unitary" in campaign:
                current_campaign["fixed_income_unitary"] = campaign["fixed_income_unitary"]

            if "default_percentage" in campaign:
                current_campaign["default_percentage"] = campaign["default_percentage"]
            if "tracker" in campaign:
                current_campaign["tracker"] = campaign["tracker"]
            if "tracker_deposit" in campaign:
                current_campaign["tracker_deposit"] = campaign["tracker_deposit"]
            if "tracker_registered_count" in campaign:
                current_campaign["tracker_registered_count"] = campaign["tracker_registered_count"]
            if "tracker_first_deposit_count" in campaign:
                current_campaign["tracker_first_deposit_count"] = campaign["tracker_first_deposit_count"]
            if "tracker_wagering_count" in campaign:
                current_campaign["tracker_wagering_count"] = campaign["tracker_wagering_count"]
            if "cpa_limit" in campaign:
                current_campaign["cpa_limit"] = campaign["cpa_limit"]

            historic_ser = HistoricalCampaignSER(
                data=current_campaign,
                partial=True,
            )
            with transaction.atomic(using=DB_ADMIN):
                if (historic_ser.is_valid()):
                    campaign = historic_ser.save()
                    return Response(
                        status=status.HTTP_204_NO_CONTENT,
                    )

                transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
                return Response(
                    status=status.HTTP_204_NO_CONTENT,
                )


class CampaignFilterAPI(APIView):

    """
        Class view that returns all campaigns with campaign_title and order by campaign_title
    """

    permission_classes = [
        IsAuthenticated,
    ]

    def get(self, request):
        annotates = {
            "campaign_title": Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        }
        campaigns = Campaign.objects.annotate(**annotates).all().order_by("campaign_title")
        campaign_serializer = CampaignBasicSer(
            instance=campaigns,
            many=True,
        )
        return Response(
            data={
                "campaigns": campaign_serializer.data,
            },
            status=status.HTTP_200_OK,
        )


class CampaignsAllAPI(APIView):
    """
        Class view with resoruces to return all campaigns
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
            Method to return all campaigns
        """
        campaigns = Campaign.objects.annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        ).all()
        campaign_serializer = CampaignPartnerBasicSER(campaigns, many=True)
        return Response(
            data={
                "campaign": campaign_serializer.data,
            },
        )


class HistoricCampaignAPI(APIView, GetAllCampaigns):
    """
        Class view for show all historical campaign
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def post(self, request):
        validator = Validator(
            schema={
                "filter": {
                    "required": True,
                    "type": "dict",
                    "default": {},
                    "schema": {
                        "id": {
                            "required": False,
                            "type": "integer",
                        },
                        "campaign": {
                            "required": False,
                            "type": "integer",
                        },
                    }
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "id",
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        order_by = validator.document.get("order_by")

        query = Q(**validator.document.get("filter"))

        history = HistoricalCampaign.objects.using(DB_USER_PARTNER).annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            )).filter(query).order_by(order_by)

        if not history:
            return Response(
                data={
                    "history": [],
                },
                status=status.HTTP_200_OK
            )

        user_paginated = self.paginate_queryset(
            queryset=history,
            request=request,
            view=self,
        )

        code_ser = HistoricalCampaignSER(
            instance=user_paginated,
            many=True,
            partial=True,
        )

        return Response(
            data={
                "history": code_ser.data,
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )
