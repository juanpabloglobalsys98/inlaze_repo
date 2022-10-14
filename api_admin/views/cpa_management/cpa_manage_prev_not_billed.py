import logging

import pandas as pd
import pytz
from api_admin.paginators import CpaPrevNotBillPag
from api_admin.serializers import (
    CpaManageLinksNotBilledSer,
    ParnertAssignSer,
    FxPartnerToUSD,
)
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import (
    BetenlaceCPA,
    BetenlaceDailyReport,
    FxPartner,
    Link,
    Partner,
    PartnerLinkDailyReport,
    WithdrawalPartnerMoneyAccum,
)
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    check_positive_float,
    check_positive_int,
    request_cfg,
    to_bool,
    to_date,
    to_int,
)
from core.models import User
from django.conf import settings
from django.db import transaction
from django.db.models import (
    F,
    Prefetch,
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils import timezone
from django.utils.timezone import timedelta
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class CpaManagePrevNotBilledAPI(APIView, CpaPrevNotBillPag):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        validator = Validator(
            schema={
                "date": {
                    "required": True,
                    "type": "date",
                    "coerce": to_date,
                },
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "prom_code": {
                    "required": False,
                    "type": "string",
                },
                "partner_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "only_partner": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "default": "-created_at",
                },
            },
        )

        if not validator.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        # Get yesterday
        yesterday_date = (timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE)) - timedelta(days=1)).date()

        if (validator.document.get("date") >= yesterday_date):
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "date": [
                            _("Date must be lesser than yesterday"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get the last accum_at on withdrawals
        latest_withdrawal_accum = WithdrawalPartnerMoneyAccum.objects.all().order_by("-accum_at").first()

        if (latest_withdrawal_accum is not None):
            latest_accum_at = latest_withdrawal_accum.accum_at
            if (validator.document.get("date") < latest_accum_at):
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "detail": {
                            "date": [
                                _("Input date is already billed"),
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

        else:
            latest_accum_at = None

        # Force route for partner user
        request_cfg.is_partner = True

        # Get Fx Rate
        query = Q(created_at__date=validator.document.get("date"))
        fx_partner = FxPartner.objects.filter(query).first()

        fx_partner_ser = FxPartnerToUSD(instance=fx_partner)

        query = Q()

        if "campaign" in validator.document:
            query &= Q(campaign_title__icontains=validator.document.get("campaign"))

        if "prom_code" in validator.document:
            query &= Q(prom_code__icontains=validator.document.get("prom_code"))

        if "partner_id" in validator.document:
            query &= Q(link__partner_link_accumulated__partner__user__id=validator.document.get("partner_id"))

        if "only_partner" in validator.document:
            query &= Q(link__partner_link_accumulated__isnull=validator.document.get("only_partner"))

        betenlace_cpas = BetenlaceCPA.objects.select_related(
            "link",
            "link__partner_link_accumulated",
        ).annotate(
            campaign_title=Concat(
                "link__campaign__bookmaker__name",
                Value(" "),
                "link__campaign__title",
            ),
            prom_code=F("link__prom_code"),

            fixed_income_unitary=F("link__campaign__fixed_income_unitary"),

            partner_full_name=Concat(
                F("link__partner_link_accumulated__partner__user__first_name"),
                Value(" "),
                F("link__partner_link_accumulated__partner__user__second_name"),
                Value(" "),
                F("link__partner_link_accumulated__partner__user__last_name"),
                Value(" "),
                F("link__partner_link_accumulated__partner__user__second_last_name"),
            ),
            partner_email=F("link__partner_link_accumulated__partner__user__email"),
        ).filter(query)

        betenlace_cpas_pk = betenlace_cpas.values_list("pk", flat=True)

        # BetenlaceDaily - Filters
        query = Q(betenlace_cpa__pk__in=betenlace_cpas_pk)

        query &= Q(created_at=validator.document.get("date"))

        betenlace_dailies = BetenlaceDailyReport.objects.using(DB_USER_PARTNER).select_related(
            "partnerlinkdailyreport",
            "partnerlinkdailyreport__partner_link_accumulated__partner__user",
        ).annotate(
            partner_full_name=Concat(
                F("partnerlinkdailyreport__partner_link_accumulated__partner__user__first_name"),
                Value(" "),
                F("partnerlinkdailyreport__partner_link_accumulated__partner__user__second_name"),
                Value(" "),
                F("partnerlinkdailyreport__partner_link_accumulated__partner__user__last_name"),
                Value(" "),
                F("partnerlinkdailyreport__partner_link_accumulated__partner__user__second_last_name"),
            ),
            partner_email=F("partnerlinkdailyreport__partner_link_accumulated__partner__user__email"),

            deposit_partner=F("partnerlinkdailyreport__deposit"),

            registered_count_partner=F("partnerlinkdailyreport__registered_count"),
            first_deposit_count_partner=F("partnerlinkdailyreport__first_deposit_count"),
            wagering_count_partner=F("partnerlinkdailyreport__wagering_count"),

            currency_local=F("partnerlinkdailyreport__currency_local"),

            fx_book_local=F("partnerlinkdailyreport__fx_book_local"),
            fx_book_net_revenue_local=F("partnerlinkdailyreport__fx_book_net_revenue_local"),
            percentage_cpa=F("partnerlinkdailyreport__percentage_cpa"),

            cpa_partner=F("partnerlinkdailyreport__cpa_count"),

            tracker=F("partnerlinkdailyreport__tracker"),
            tracker_deposit=F("partnerlinkdailyreport__tracker_deposit"),
            tracker_registered_count=F("partnerlinkdailyreport__tracker_registered_count"),
            tracker_first_deposit_count=F("partnerlinkdailyreport__tracker_first_deposit_count"),
            tracker_wagering_count=F("partnerlinkdailyreport__tracker_wagering_count"),

            adviser_id=F("partnerlinkdailyreport__adviser_id"),

            fixed_income_adviser_local=F("partnerlinkdailyreport__fixed_income_adviser_local"),
            net_revenue_adviser_local=F("partnerlinkdailyreport__net_revenue_adviser_local"),

            fixed_income_adviser_percentage=F("partnerlinkdailyreport__fixed_income_adviser_percentage"),
            net_revenue_adviser_percentage=F("partnerlinkdailyreport__net_revenue_adviser_percentage"),

            fixed_income_partner=F("partnerlinkdailyreport__fixed_income"),
            fixed_income_partner_unitary=F("partnerlinkdailyreport__fixed_income_unitary"),
            fixed_income_partner_local=F("partnerlinkdailyreport__fixed_income_local"),
            fixed_income_partner_unitary_local=F("partnerlinkdailyreport__fixed_income_unitary_local"),
            fixed_income_adviser=F("partnerlinkdailyreport__fixed_income_adviser"),
            net_revenue_adviser=F("partnerlinkdailyreport__net_revenue_adviser"),
            referred_by_id=F("partnerlinkdailyreport__referred_by_id"),
            fixed_income_referred_local=F("partnerlinkdailyreport__fixed_income_referred_local"),
            net_revenue_referred_local=F("partnerlinkdailyreport__net_revenue_referred_local"),
            fixed_income_referred_percentage=F("partnerlinkdailyreport__fixed_income_referred_percentage"),
            net_revenue_referred_percentage=F("partnerlinkdailyreport__net_revenue_referred_percentage"),
        ).filter(query)

        betenlace_cpa_betenlace_dailies = betenlace_cpas.prefetch_related(
            Prefetch(
                lookup="Betenlacedailyreport_to_BetenlaceCPA",
                queryset=betenlace_dailies,
                to_attr="betenlace_dailies",
            ),
        )

        betenlace_cpa_betenlace_dailies_pag = self.paginate_queryset(
            queryset=betenlace_cpa_betenlace_dailies,
            request=request,
            view=self,
        )

        betenlace_cpa_betenlace_dailies_ser = CpaManageLinksNotBilledSer(
            instance=betenlace_cpa_betenlace_dailies_pag,
            many=True,
        )

        return Response(
            data={
                "links_betenlace_dailies": betenlace_cpa_betenlace_dailies_ser.data,
                "created_at": validator.document.get("date"),
                "fx_partner": fx_partner_ser.data,
            },
            headers={
                "access-control-expose-headers": "count, next, previous",
                "count": self.count,
                "next": self.get_next_link(),
                "previous": self.get_previous_link()
            },
            status=status.HTTP_200_OK,
        )

    def put(self, request):
        validator = Validator(
            schema={
                "data": {
                    "required": True,
                    "type": "list",
                    "schema": {
                        "type": "dict",
                        "schema": {
                            "betenlace_cpa_id": {
                                "required": True,
                                "type": "integer",
                            },
                            "deposit": {
                                "required": False,
                                "type": "float",
                                "nullable": True,
                                "check_with": check_positive_float,
                            },
                            "deposit_partner": {
                                "required": False,
                                "type": "float",
                                "nullable": True,
                                "check_with": check_positive_float,
                            },

                            "stake": {
                                "required": False,
                                "type": "float",
                                "nullable": True,
                                "check_with": check_positive_float,
                            },
                            "net_revenue": {
                                "required": False,
                                "type": "float",
                                "nullable": True,
                            },
                            "revenue_share": {
                                "required": False,
                                "type": "float",
                                "nullable": True,
                            },

                            "registered_count": {
                                "required": False,
                                "type": "integer",
                                "nullable": True,
                                "check_with": check_positive_int,
                            },
                            "registered_count_partner": {
                                "required": False,
                                "type": "integer",
                                "nullable": True,
                                "check_with": check_positive_int,
                            },

                            "first_deposit_count": {
                                "required": False,
                                "type": "integer",
                                "nullable": True,
                                "check_with": check_positive_int,
                            },
                            "first_deposit_count_partner": {
                                "required": False,
                                "type": "integer",
                                "nullable": True,
                                "check_with": check_positive_int,
                            },

                            "wagering_count": {
                                "required": False,
                                "type": "integer",
                                "nullable": True,
                                "check_with": check_positive_int,
                            },
                            "wagering_count_partner": {
                                "required": False,
                                "type": "integer",
                                "nullable": True,
                                "check_with": check_positive_int,
                            },
                            "cpa_count": {
                                "required": True,
                                "type": "integer",
                                "check_with": check_positive_int,
                            },
                            "cpa_partner": {
                                "required": False,
                                "type": "integer",
                                "nullable": True,
                                "check_with": check_positive_int,
                            },
                            "created_at": {
                                "required": True,
                                "type": "date",
                                "coerce": to_date,
                            },
                        },
                    },
                },
            },
        )

        if not validator.validate(
            document=request.data,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        df = pd.DataFrame.from_dict(
            data=validator.document.get("data"),
        )

        # Cast to datetime object
        df["created_at"] = pd.to_datetime(
            arg=df["created_at"],
            format="%Y-%m-%d",
            errors="coerce",
            infer_datetime_format=False,
        )

        # Check duplicateds
        df_duplicated = df[df.duplicated(subset=["betenlace_cpa_id", "created_at"])]
        if (not df_duplicated.empty):
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "date": [
                            _("Has duplicated inputs / data with same betenlace_cpa_id and created_at must be CHECK"),
                            df_duplicated.to_dict('records'),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get yesterday
        yesterday_date = (timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE)) - timedelta(days=1)).date()

        # Get the last accum_at on withdrawals
        latest_withdrawal_accum = WithdrawalPartnerMoneyAccum.objects.all().order_by("-accum_at").first()
        if (latest_withdrawal_accum is not None):
            latest_accum_at = latest_withdrawal_accum.accum_at

        betenlace_cpas = set(df.betenlace_cpa_id)

        query = Q(pk__in=betenlace_cpas)
        links = Link.objects.select_related(
            "partner_link_accumulated",
            "partner_link_accumulated__partner",
            "betenlacecpa",
            "campaign",
        ).filter(query)

        created_at_dates = []
        query = Q()

        keys = {key: index for index, key in enumerate(df.columns.values)}

        for row in zip(*df.to_dict('list').values()):
            query |= Q(
                betenlace_cpa_id=row[keys.get("betenlace_cpa_id")],
                created_at=row[keys.get("created_at")].date(),
            )
            created_at_dates.append(row[keys.get("created_at")].date())

            if (row[keys.get("created_at")].date() < latest_accum_at):
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "detail": {
                            "date": [
                                _("Input date is already billed"),
                                row[keys.get("created_at")].date(),
                                row[keys.get("betenlace_cpa_id")],
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            if (row[keys.get("created_at")].date() >= yesterday_date):
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "detail": {
                            "date": [
                                _("Date must be lesser than yesterday"),
                                row[keys.get("created_at")].date(),
                                row[keys.get("betenlace_cpa_id")],
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

        betenlace_daily_reports = BetenlaceDailyReport.objects.select_related(
            "partnerlinkdailyreport",
        ).filter(query)

        query = Q(created_at__date__in=created_at_dates)
        fx_partners = FxPartner.objects.filter(query)

        # Acumulators bulk create and update
        member_reports_daily_betenlace_update = []
        member_reports_daily_betenlace_create = []

        member_reports_daily_partner_update = []
        member_reports_daily_partner_create = []
        for row in zip(*df.to_dict('list').values()):
            # Get link according to prom_code of current loop
            link = next(
                filter(
                    lambda link: link.pk == row[keys.get("betenlace_cpa_id")],
                    links,
                ),
                None,
            )
            if not link:
                logger.error(
                    f"Link with id {row[keys.get('betenlace_cpa_id')]} not found on DB or filter functions"
                )
                return Response(
                    data={
                        "error": settings.INTERNAL_SERVER_ERROR,
                        "detail": {
                            "data.betenlace_cpa_id": [
                                _("betenlace_cpa_id not in DB"),
                                row[keys.get('betenlace_cpa_id')],
                            ],
                        },
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            try:
                # Get current entry of member report based on link (prom_code)
                betenlace_cpa = link.betenlacecpa
            except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
                logger.error(f"Betenlace CPA entry not found for link {link.pk}")
                return Response(
                    data={
                        "error": settings.INTERNAL_SERVER_ERROR,
                        "detail": {
                            "non_field_errors": [
                                _("Link does not have relation, bad integrity"),
                                link.pk,
                            ],
                        },
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            # Get currencies
            currency_fixed_income_str = link.campaign.currency_fixed_income.lower()
            currency_condition_str = link.campaign.currency_condition.lower()

            # member_reports_betenlace_month_update.append(betenlace_cpa)

            # Betenlace Daily
            betenlace_daily = next(
                filter(
                    lambda betenlace_daily: (
                        betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                        betenlace_daily.created_at == row[keys.get("created_at")].date()
                    ),
                    betenlace_daily_reports,
                ),
                None,
            )

            fx_partner = next(
                filter(
                    lambda fx_partner: (
                        fx_partner.created_at.date() == row[keys.get("created_at")].date()
                    ),
                    fx_partners,
                ),
                None,
            )

            # Case not on filtered
            if (fx_partner is None):
                logger.warning(
                    f"Filters does not have a fx_partner for link {link.pk} at date {row[keys.get('created_at')]}"
                )
                # Get the last Fx value
                query = Q(created_at__gte=row[keys.get("created_at")])
                fx_partner = FxPartner.objects.filter(query).order_by("created_at").first()

                if(fx_partner is None):
                    # Get just next from supplied date
                    query = Q(created_at__lte=row[keys.get("created_at")])
                    fx_partner = FxPartner.objects.filter(query).order_by("-created_at").first()

                # If still none prevent execution
                if(fx_partner is None):
                    logger.critical("Undefined fx_partner on DB")
                    return Response(
                        data={
                            "error": settings.INTERNAL_SERVER_ERROR,
                            "detail": {
                                "non_field_errors": [
                                    _("Does not exist FxPartner on DB"),
                                ],
                            },
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    )

            if(betenlace_daily is not None):
                betenlace_daily = self._betenlace_daily_update(
                    keys=keys,
                    row=row,
                    betenlace_daily=betenlace_daily,
                    campaign=link.campaign,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_update.append(betenlace_daily)
            else:
                betenlace_daily = self._betenlace_daily_create(
                    from_date=row[keys.get("created_at")],
                    keys=keys,
                    row=row,
                    betenlace_cpa=betenlace_cpa,
                    campaign=link.campaign,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_create.append(betenlace_daily)

            # If partner cpa_count NAN pr Null will be ignored
            if (pd.isna(row[keys.get("cpa_partner")])):
                continue

            # Check if already have partner link daily report
            if (hasattr(betenlace_daily, "partnerlinkdailyreport")):
                partner_link_daily = betenlace_daily.partnerlinkdailyreport
                partner_link_accumulated = partner_link_daily.partner_link_accumulated
                partner_percentage_cpa = partner_link_daily.percentage_cpa
                # Case still none
                if (partner_percentage_cpa is None):
                    partner_percentage_cpa = partner_link_accumulated.percentage_cpa
            # Other case, get current partner_link_accumulated from link
            else:
                partner_link_daily = None
                partner_link_accumulated = link.partner_link_accumulated
                # Case no partnerlink accumulated
                if (partner_link_accumulated is not None):
                    partner_percentage_cpa = partner_link_accumulated.percentage_cpa

            # When link have not assigned partner must be continue to next loop
            if(partner_link_accumulated is None):
                continue

            # Fx Currency Fixed income
            partner_currency_str = partner_link_accumulated.currency_local.lower()
            fx_fixed_income_partner = self._calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner.fx_percentage,
                currency_from_str=currency_fixed_income_str,
                currency_to_str=partner_currency_str,
            )

            fixed_income_partner_unitary = betenlace_daily.fixed_income_unitary * partner_percentage_cpa
            fixed_income_partner = row[keys.get("cpa_partner")] * fixed_income_partner_unitary
            fixed_income_partner_unitary_local = (
                fixed_income_partner_unitary *
                fx_fixed_income_partner
            )
            fixed_income_partner_local = row[keys.get("cpa_partner")] * fixed_income_partner_unitary_local

            # Fx Currency Condition
            fx_condition_partner = self._calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner.fx_percentage,
                currency_from_str=currency_condition_str,
                currency_to_str=partner_currency_str,
            )

            if(partner_link_daily is not None):
                partner_link_daily = self._partner_link_daily_update(
                    keys=keys,
                    row=row,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner.fx_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner=fixed_income_partner,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    fixed_income_partner_local=fixed_income_partner_local,
                    partner_link_daily=partner_link_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    partner=partner_link_accumulated.partner,
                    betenlace_daily=betenlace_daily,
                )
                member_reports_daily_partner_update.append(partner_link_daily)
            else:
                partner_link_daily = self._partner_link_daily_create(
                    keys=keys,
                    row=row,
                    campaign=link.campaign,
                    betenlace_daily=betenlace_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner.fx_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner=fixed_income_partner,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    fixed_income_partner_local=fixed_income_partner_local,
                    partner=partner_link_accumulated.partner,
                )
                member_reports_daily_partner_create.append(partner_link_daily)

        with transaction.atomic(using=DB_USER_PARTNER):
            if(member_reports_daily_betenlace_update):
                BetenlaceDailyReport.objects.bulk_update(
                    objs=member_reports_daily_betenlace_update,
                    fields=(
                        "deposit",
                        "stake",
                        "net_revenue",
                        "revenue_share",
                        "fixed_income",
                        "fixed_income_unitary",
                        "fx_partner",
                        "registered_count",
                        "cpa_count",
                        "first_deposit_count",
                        "wagering_count",
                    ),
                    batch_size=999,
                )

            if(member_reports_daily_betenlace_create):
                BetenlaceDailyReport.objects.bulk_create(
                    objs=member_reports_daily_betenlace_create,
                    batch_size=999,
                )

            if(member_reports_daily_partner_update):
                PartnerLinkDailyReport.objects.bulk_update(
                    objs=member_reports_daily_partner_update,
                    fields=(
                        "fixed_income",
                        "fixed_income_unitary",
                        "fx_book_local",
                        "fx_book_net_revenue_local",
                        "fx_percentage",
                        "fixed_income_local",
                        "fixed_income_unitary_local",
                        "cpa_count",
                        "percentage_cpa",
                        "tracker",
                        "tracker_deposit",
                        "tracker_registered_count",
                        "tracker_first_deposit_count",
                        "tracker_wagering_count",
                        "deposit",
                        "registered_count",
                        "first_deposit_count",
                        "wagering_count",
                        "adviser_id",
                        "fixed_income_adviser",
                        "fixed_income_adviser_local",
                        "net_revenue_adviser",
                        "net_revenue_adviser_local",
                        "fixed_income_adviser_percentage",
                        "net_revenue_adviser_percentage",
                        "referred_by_id",
                        "fixed_income_referred_local",
                        "net_revenue_referred_local",
                        "fixed_income_referred_percentage",
                        "net_revenue_referred_percentage",
                    ),
                    batch_size=999,
                )

            if(member_reports_daily_partner_create):
                PartnerLinkDailyReport.objects.bulk_create(
                    objs=member_reports_daily_partner_create,
                    batch_size=999,
                )

        return Response(
            status=status.HTTP_204_NO_CONTENT,
        )

    def _partner_link_daily_create(
        self,
        keys,
        row,
        campaign,
        betenlace_daily,
        partner_link_accumulated,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner,
        fixed_income_partner_unitary_local,
        fixed_income_partner_local,
        partner,
    ):
        # Calculate Adviser payment
        if (partner.fixed_income_adviser_percentage is None):
            fixed_income_adviser = None
            fixed_income_adviser_local = None
        else:
            fixed_income_adviser = (
                fixed_income_partner *
                partner.fixed_income_adviser_percentage
            )
            fixed_income_adviser_local = (
                fixed_income_adviser *
                fx_fixed_income_partner
            )

        if (partner.net_revenue_adviser_percentage is None):
            net_revenue_adviser = None
            net_revenue_adviser_local = None
        else:
            net_revenue_adviser = (
                betenlace_daily.net_revenue * partner.net_revenue_adviser_percentage
                if betenlace_daily.net_revenue is not None
                else
                0
            )
            net_revenue_adviser_local = (
                net_revenue_adviser * fx_condition_partner
            )

        # Calculate referred payment
        if (partner.fixed_income_referred_percentage is None):
            fixed_income_referred = None
            fixed_income_referred_local = None
        else:
            fixed_income_referred = (
                fixed_income_partner *
                partner.fixed_income_referred_percentage
            )
            fixed_income_referred_local = (
                fixed_income_referred *
                fx_fixed_income_partner
            )

        if (partner.net_revenue_referred_percentage is None):
            net_revenue_referred = None
            net_revenue_referred_local = None
        else:
            net_revenue_referred = (
                betenlace_daily.net_revenue * partner.net_revenue_referred_percentage
                if betenlace_daily.net_revenue is not None
                else
                0
            )
            net_revenue_referred_local = (
                net_revenue_referred * fx_condition_partner
            )

        partner_link_daily = PartnerLinkDailyReport(
            betenlace_daily_report=betenlace_daily,
            partner_link_accumulated=partner_link_accumulated,

            currency_fixed_income=campaign.currency_fixed_income,
            fixed_income=fixed_income_partner,
            fixed_income_unitary=fixed_income_partner_unitary,

            currency_local=partner_link_accumulated.currency_local,
            fx_book_local=fx_fixed_income_partner,
            fx_book_net_revenue_local=fx_condition_partner,
            fx_percentage=fx_partner_percentage,

            fixed_income_local=fixed_income_partner_local,
            fixed_income_unitary_local=fixed_income_partner_unitary_local,

            cpa_count=row[keys.get("cpa_partner")],
            percentage_cpa=partner_link_accumulated.percentage_cpa,

            deposit=row[keys.get("deposit_partner")],
            registered_count=row[keys.get("registered_count_partner")],
            first_deposit_count=row[keys.get("first_deposit_count_partner")],
            wagering_count=row[keys.get("wagering_count_partner")],

            tracker=partner_link_accumulated.tracker,
            tracker_deposit=partner_link_accumulated.tracker_deposit,
            tracker_registered_count=partner_link_accumulated.tracker_registered_count,
            tracker_first_deposit_count=partner_link_accumulated.tracker_first_deposit_count,
            tracker_wagering_count=partner_link_accumulated.tracker_wagering_count,

            # Adviser base data
            adviser_id=partner.adviser_id,
            fixed_income_adviser_percentage=partner.fixed_income_adviser_percentage,
            net_revenue_adviser_percentage=partner.net_revenue_adviser_percentage,

            fixed_income_adviser=fixed_income_adviser,
            fixed_income_adviser_local=fixed_income_adviser_local,
            net_revenue_adviser=net_revenue_adviser,
            net_revenue_adviser_local=net_revenue_adviser_local,

            # referred base data
            referred_by=partner.referred_by,
            fixed_income_referred_percentage=partner.fixed_income_referred_percentage,
            net_revenue_referred_percentage=partner.net_revenue_referred_percentage,

            fixed_income_referred=fixed_income_referred,
            fixed_income_referred_local=fixed_income_referred_local,
            net_revenue_referred=net_revenue_referred,
            net_revenue_referred_local=net_revenue_referred_local,

            created_at=row[keys.get("created_at")],
        )
        return partner_link_daily

    def _partner_link_daily_update(
        self,
        keys,
        row,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner,
        fixed_income_partner_unitary_local,
        fixed_income_partner_local,
        partner_link_daily,
        partner_link_accumulated,
        partner,
        betenlace_daily,
    ):
        partner_link_daily.fixed_income = fixed_income_partner
        partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary

        partner_link_daily.fx_book_local = fx_fixed_income_partner
        partner_link_daily.fx_book_net_revenue_local = fx_condition_partner
        partner_link_daily.fx_percentage = fx_partner_percentage

        partner_link_daily.fixed_income_local = fixed_income_partner_local
        partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

        partner_link_daily.cpa_count = row[keys.get("cpa_partner")]
        partner_link_daily.percentage_cpa = partner_link_accumulated.percentage_cpa

        partner_link_daily.tracker = partner_link_accumulated.tracker
        partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
        partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
        partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
        partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

        partner_link_daily.deposit = row[keys.get("deposit_partner")]
        partner_link_daily.registered_count = row[keys.get("registered_count_partner")]
        partner_link_daily.first_deposit_count = row[keys.get("first_deposit_count_partner")]
        partner_link_daily.wagering_count = row[keys.get("wagering_count_partner")]

        # Calculate Adviser payment, change only if this is None
        if (partner_link_daily.adviser_id is None):
            partner_link_daily.adviser_id = partner.adviser_id

        if (partner_link_daily.fixed_income_adviser_percentage is None):
            partner_link_daily.fixed_income_adviser_percentage = partner.fixed_income_adviser_percentage

        if (partner_link_daily.net_revenue_adviser_percentage is None):
            partner_link_daily.net_revenue_adviser_percentage = partner.net_revenue_adviser_percentage

        if (partner.fixed_income_adviser_percentage is None):
            partner_link_daily.fixed_income_adviser = None
            partner_link_daily.fixed_income_adviser_local = None
        else:
            partner_link_daily.fixed_income_adviser = (
                partner_link_daily.fixed_income *
                partner.fixed_income_adviser_percentage
            )
            partner_link_daily.fixed_income_adviser_local = (
                partner_link_daily.fixed_income_adviser *
                fx_fixed_income_partner
            )

        if (partner.net_revenue_adviser_percentage is None):
            partner_link_daily.net_revenue_adviser = None
            partner_link_daily.net_revenue_adviser_local = None
        else:
            partner_link_daily.net_revenue_adviser = (
                betenlace_daily.net_revenue * partner.net_revenue_adviser_percentage
                if betenlace_daily.net_revenue is not None
                else
                0
            )
            partner_link_daily.net_revenue_adviser_local = (
                partner_link_daily.net_revenue_adviser * fx_condition_partner
            )

        ###
        # Calculate referred payment, change only if this is None
        if (partner_link_daily.referred_by_id is None):
            partner_link_daily.referred_by_id = partner.referred_by_id

        if (partner_link_daily.fixed_income_referred_percentage is None):
            partner_link_daily.fixed_income_referred_percentage = partner.fixed_income_referred_percentage

        if (partner_link_daily.net_revenue_referred_percentage is None):
            partner_link_daily.net_revenue_referred_percentage = partner.net_revenue_referred_percentage

        if (partner.fixed_income_referred_percentage is None):
            partner_link_daily.fixed_income_referred = None
            partner_link_daily.fixed_income_referred_local = None
        else:
            partner_link_daily.fixed_income_referred = (
                partner_link_daily.fixed_income *
                partner.fixed_income_referred_percentage
            )
            partner_link_daily.fixed_income_referred_local = (
                partner_link_daily.fixed_income_referred *
                fx_fixed_income_partner
            )

        if (partner.net_revenue_referred_percentage is None):
            partner_link_daily.net_revenue_referred = None
            partner_link_daily.net_revenue_referred_local = None
        else:
            partner_link_daily.net_revenue_referred = (
                betenlace_daily.net_revenue * partner.net_revenue_referred_percentage
                if betenlace_daily.net_revenue is not None
                else
                0
            )
            partner_link_daily.net_revenue_referred_local = (
                partner_link_daily.net_revenue_referred * fx_condition_partner
            )
        return partner_link_daily

    def _calc_fx(
        self,
        fx_partner,
        fx_partner_percentage,
        currency_from_str,
        currency_to_str,
    ):
        if(currency_from_str != currency_to_str):
            try:
                fx_book_partner = eval(
                    f"fx_partner.fx_{currency_from_str}_{currency_to_str}") * fx_partner_percentage
            except:
                logger.error(
                    f"Fx conversion from {currency_from_str} to {currency_to_str} undefined on DB")
        else:
            fx_book_partner = 1
        return fx_book_partner

    def _betenlace_daily_create(
        self,
        from_date,
        keys,
        row,
        betenlace_cpa,
        campaign,
        fx_partner,
    ):
        betenlace_daily = BetenlaceDailyReport(
            betenlace_cpa=betenlace_cpa,

            currency_condition=campaign.currency_condition,

            deposit=row[keys.get("deposit")],
            stake=row[keys.get("stake")],

            net_revenue=row[keys.get("net_revenue")],
            revenue_share=row[keys.get("revenue_share")],

            currency_fixed_income=campaign.currency_fixed_income,

            fixed_income_unitary=campaign.fixed_income_unitary,
            fixed_income=campaign.fixed_income_unitary * row[keys.get("cpa_count")],

            fx_partner=fx_partner,

            registered_count=row[keys.get("registered_count")],
            cpa_count=row[keys.get("cpa_count")],
            first_deposit_count=row[keys.get("first_deposit_count")],
            wagering_count=row[keys.get("wagering_count")],
            created_at=from_date,
        )

        return betenlace_daily

    def _betenlace_daily_update(
        self,
        keys,
        row,
        betenlace_daily,
        campaign,
        fx_partner,
    ):
        betenlace_daily.deposit = row[keys.get("deposit")]
        betenlace_daily.stake = row[keys.get("stake")]
        betenlace_daily.net_revenue = row[keys.get("net_revenue")]
        betenlace_daily.revenue_share = row[keys.get("revenue_share")]

        betenlace_daily.fx_partner = fx_partner

        betenlace_daily.registered_count = row[keys.get("registered_count")]
        betenlace_daily.cpa_count = row[keys.get("cpa_count")]
        betenlace_daily.first_deposit_count = row[keys.get("first_deposit_count")]
        betenlace_daily.wagering_count = row[keys.get("wagering_count")]

        if (betenlace_daily.fixed_income_unitary is None):
            betenlace_daily.fixed_income_unitary = campaign.fixed_income_unitary

        betenlace_daily.fixed_income = betenlace_daily.cpa_count * betenlace_daily.fixed_income_unitary
        return betenlace_daily


class CpaManagePrevNotBilledPartnersAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ Returning partners related to adviser """
        validator = Validator(
            schema={
                "partner_id": {
                    "required": False,
                    "type": "string",
                },
                "full_name": {
                    "required": False,
                    "type": "string",
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "identification_type": {
                    "required": False,
                    "type": "string",
                },
                "identification_number": {
                    "required": False,
                    "type": "string",
                },
            }
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q()

        if 'email' in validator.document:
            query_user = Q(email__icontains=validator.document.get("email"))
            user = User.objects.db_manager(DB_USER_PARTNER).filter(query_user).values('id')
            if user:
                query &= Q(user_id__in=user)
            else:
                query &= Q(user_id=None)

        if 'identification_number' in request.query_params:
            query &= Q(additionalinfo__identification__istartswith=request.query_params.get(
                "identification_number"))

        if 'identification_type' in request.query_params:
            query &= Q(additionalinfo__identification_type=request.query_params.get(
                "identification_type"))

        if 'partner_id' in request.query_params:
            query &= Q(user__id=request.query_params.get("partner_id"))

        if 'full_name' in request.query_params:
            query &= Q(full_name__icontains=request.query_params.get("full_name"))

        partners = Partner.objects.annotate(
            full_name=Concat(
                "user__first_name",
                Value(" "),
                "user__second_name",
                Value(" "),
                "user__last_name",
                Value(" "),
                "user__second_last_name",
            ),
            identification_number=F("additionalinfo__identification"),
            identification_type=F("additionalinfo__identification_type"),
            email=F("user__email"),
        ).filter(query)[:5]

        parnertassignser = ParnertAssignSer(
            instance=partners,
            many=True,
        )

        return Response(
            data={
                "count": partners.count(),
                "partners": parnertassignser.data,
            },
            status=status.HTTP_200_OK,
        )
