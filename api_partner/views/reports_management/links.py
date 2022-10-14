import logging
from api_admin.helpers.partner_accum_history import create_history
from api_partner.helpers.choices.partner_link_accum_status import PartnerAccumUpdateReasonCHO

import pytz
from api_admin.helpers import recalculate_temperature
from api_admin.models import LevelPercentageBase
from api_partner.helpers import (
    DB_USER_PARTNER,
    IsActive,
    IsNotBanned,
    IsTerms,
    PartnerLevelCHO,
)
from api_partner.models import (
    BetenlaceDailyReport,
    Campaign,
    Link,
    Partner,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from cerberus import Validator
from core.helpers import CurrencyAll
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class LinkPartnerAPI(APIView):

    """
        Class view with resources that allow to user request an link
    """

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsTerms,
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
            Request a link available

            #Body

           -  campaign_id : "integer"
                Param to define campaign to request link
        """
        validator = Validator(
            schema={
                "campaign_id": {
                    "required": True,
                    "type": "integer",
                },
            },
        )

        user = request.user

        if not validator.validate(document=request.data):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(status=Link.Status.AVAILABLE),
            Q(campaign__id=validator.document.get("campaign_id")),
        )
        link = Link.objects.filter(*filters).first()

        if not link:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "link": [
                            _("No links available"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(partner__pk=user.id),
            Q(campaign__id=validator.document.get("campaign_id")),
        )
        partner_accumulated = PartnerLinkAccumulated.objects.filter(*filters).first()

        if partner_accumulated and partner_accumulated.is_assigned:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "link": [
                            _("Already has an link assigned"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q(
            partner_link_accumulated__partner__pk=request.user.id,
            cpa_count__gt=0,
        )
        daily_report = PartnerLinkDailyReport.objects.filter(query).count()
        """
        select currently partner and filter by cpa count grater than 0 if it is > 0 continue with if

        """
        partner = Partner.objects.filter(user_id=request.user.id).first()

        if daily_report == 0 and partner.level == PartnerLevelCHO.BASIC:
            filters = [
                Q(partnerlinkaccumulated_to_campaign__partner=request.user.partner),
                Q(partnerlinkaccumulated_to_campaign__is_assigned=True),
            ]
            Campaign_pref = Campaign.objects.filter(
                *filters)
            campaing_pks = Campaign_pref.values_list(
                "pk", flat=True)

            filters = [Q(pk__in=campaing_pks), ~Q(status=Campaign.Status.INACTIVE)]
            campaigns = Campaign.objects.filter(
                *filters
            ).count()

            """
                return number of campaign that user have
            """

            if campaigns >= settings.MAX_CAMPAIGN_PARTNER:
                return Response(
                    data={
                        "error": settings.FORBIDDEN,
                        "detail": {
                            "message": [
                                _("maximum number of campaigns reached"),
                            ],
                        },
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))

        campaign = link.campaign

        partner = Partner.objects.filter(user_id=request.user.id).first()
        level_percentages = LevelPercentageBase.objects.all().order_by("-created_at").first()

        # Update case
        if partner_accumulated is not None and not partner_accumulated.is_assigned:
            link.partner_link_accumulated = partner_accumulated
            link.status = Link.Status.ASSIGNED
            link.save()

            partner_accumulated.prom_code = link.prom_code
            partner_accumulated.is_assigned = True
            partner_accumulated.currency_fixed_income = campaign.currency_fixed_income

            # Currency local for all partners will now USD always
            partner_accumulated.currency_local = CurrencyAll.USD
            partner_accumulated.assigned_at = today

            # Default percentages
            partner_accumulated.percentage_cpa = (level_percentages.percentages.get(str(partner.level)) *
                                                  campaign.default_percentage)

            if partner.level == PartnerLevelCHO.BASIC:
                partner_accumulated.tracker = 1.0
                partner_accumulated.tracker = 1.0
                partner_accumulated.tracker_deposit = 1.0
                partner_accumulated.tracker_registered_count = 1.0
                partner_accumulated.tracker_first_deposit_count = 1.0
                partner_accumulated.tracker_wagering_count = 1.0
            else:
                partner_accumulated.tracker = campaign.tracker
                partner_accumulated.tracker_deposit = campaign.tracker_deposit
                partner_accumulated.tracker_registered_count = campaign.tracker_registered_count
                partner_accumulated.tracker_first_deposit_count = campaign.tracker_first_deposit_count
                partner_accumulated.tracker_wagering_count = campaign.tracker_wagering_count

            partner_accumulated.save()

        # Case Partnerlink None, to create
        else:
            if not partner.level == PartnerLevelCHO.BASIC:
                partner_accumulated = PartnerLinkAccumulated.objects.create(
                    partner_id=user.id,

                    campaign=campaign,
                    prom_code=link.prom_code,

                    is_assigned=True,

                    cpa_count=0,
                    fixed_income=0,

                    currency_fixed_income=campaign.currency_fixed_income,
                    fixed_income_local=0,

                    # Currency local for all partners will now USD always
                    currency_local=CurrencyAll.USD,

                    # Default percentages
                    percentage_cpa=(level_percentages.percentages.get(str(partner.level)) *
                                    campaign.default_percentage),

                    tracker=campaign.tracker,
                    tracker_deposit=campaign.tracker_deposit,
                    tracker_registered_count=campaign.tracker_registered_count,
                    tracker_first_deposit_count=campaign.tracker_first_deposit_count,
                    tracker_wagering_count=campaign.tracker_wagering_count,

                    assigned_at=today,
                )
            else:
                partner_accumulated = PartnerLinkAccumulated.objects.create(
                    partner_id=user.id,

                    campaign=campaign,
                    prom_code=link.prom_code,

                    is_assigned=True,

                    cpa_count=0,
                    fixed_income=0,

                    currency_fixed_income=campaign.currency_fixed_income,
                    fixed_income_local=0,

                    # Currency local for all partners will now USD always
                    currency_local=CurrencyAll.USD,

                    # Default percentages
                    percentage_cpa=(level_percentages.percentages.get(str(partner.level)) *
                                    campaign.default_percentage),
                    tracker=1.0,
                    tracker_deposit=1.0,
                    tracker_registered_count=1.0,
                    tracker_first_deposit_count=1.0,
                    tracker_wagering_count=1.0,

                    assigned_at=today,
                )

            link.partner_link_accumulated = partner_accumulated
            link.status = Link.Status.ASSIGNED
            link.save()

        recalculate_temperature(campaign)

        # Get today betenlace daily of determinated link
        filters = (
            Q(created_at=today.date()),
            Q(betenlace_cpa_id=link.id),
        )
        betenlace_daily = BetenlaceDailyReport.objects.filter(*filters).first()

        # Data of betenlace on same day is only with clicks, force relation
        if (betenlace_daily is not None):
            # if betenlace daily has a partner link daily delete
            if hasattr(betenlace_daily, 'partnerlinkdailyreport'):
                betenlace_daily.partnerlinkdailyreport.delete()

            PartnerLinkDailyReport.objects.create(
                partner_link_accumulated=partner_accumulated,
                betenlace_daily_report=betenlace_daily,
                adviser_id=partner_accumulated.partner.adviser_id,
                currency_fixed_income=campaign.currency_fixed_income,
                # Currency local for all partners is USD
                currency_local=partner_accumulated.currency_local,
                created_at=today.date(),
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)

        filters = (
            Q(partner__pk=user.id),
            Q(campaign__id=validator.document.get("campaign_id")),
        )
        partner_accumulated = PartnerLinkAccumulated.objects.filter(*filters).first()

        create_history(
            instance=partner_accumulated,
            update_reason=PartnerAccumUpdateReasonCHO.PARTNER_REQUEST,
            adviser=None,
        )

        return Response(
            data={
                "msg": _("The link was assigned successfully"),
                "prom_code": link.prom_code,
            },
            status=status.HTTP_200_OK,
        )
