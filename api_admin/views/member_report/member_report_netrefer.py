import logging
import math
import sys
import traceback
from io import StringIO

import numpy as np
import pandas as pd
from api_admin.paginators import GetAllMemberReport
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import (
    BetenlaceCPA,
    BetenlaceDailyReport,
    Campaign,
    FxPartner,
    FxPartnerPercentage,
    Link,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    PartnerFilesNamesErrorHandler,
    StandardErrorHandler,
    ValidatorFile,
    timezone_customer,
    to_date,
    to_lower,
)
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.db import transaction
from django.db.models import (
    Q,
    Sum,
    Value,
)
from django.db.models.functions import (
    Coalesce,
    Concat,
)
from django.utils import timezone
from django.utils.timezone import (
    datetime,
    timedelta,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class ManageMemberReportMonthNetreferAPI(APIView, GetAllMemberReport):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def post(self, request):
        validator = ValidatorFile(
            {
                "campaign_title": {
                    "required": True,
                    "type": "string",
                    "coerce": to_lower,
                    "allowed": (
                        "zamba col",
                        "luckia col",
                        "luckia esp",
                        "betfair col",
                        "betfair col 2",
                        "betfair col 3",
                        "betfair col 4",
                        "betfair latam",
                        "betfair br",
                        "betfair pe",
                        "betfair mex",
                        "betfair esp",
                        "mozzart col",
                    )
                },
                "csv_data": {
                    "required": True,
                    "type": "file",
                },
                "upload_date": {
                    "required": True,
                    "type": "date",
                    "coerce": to_date,
                    "default": (timezone_customer(timezone.now()) - timedelta(days=1)).strftime("%Y-%m-%d")
                }

            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        files_names_validator = Validator(
            {
                "csv_data": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(csv|CSV)+",
                },
            },
            error_handler=PartnerFilesNamesErrorHandler,
        )

        csv_file = validator.document.get("csv_data")
        file_name = {"csv_data": csv_file.name}

        if not files_names_validator.validate(file_name):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": files_names_validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        campaign_title = validator.document.get("campaign_title")
        upload_date = validator.document.get("upload_date")
        today = datetime.now()

        # Get id of Campaign Title
        filters = [Q(campaign_title__iexact=campaign_title)]
        campaign = Campaign.objects.using(DB_USER_PARTNER).annotate(
            campaign_title=Concat('bookmaker__name', Value(' '), 'title')).filter(*filters).first()

        if not campaign:
            logger.error(f"Campaign with title \"{campaign_title}\" not found in DB")
            return Response({
                "error": settings.ERROR_CAMPAIGN_NOT_IN_DB,
                "details": {
                    "non_field_errors": [_("Undefined campaign on DB")]
                }
            }, status=status.HTTP_400_BAD_REQUEST)

        # Fx rate conversion for incoming data without fx percentage, defualt 1
        tx_param = 1

        yesterday_timezone = timezone.now() - timedelta(days=1)
        fx_created_at = yesterday_timezone.replace(minute=0, hour=0, second=0, microsecond=0)

        # Get the last Fx value
        filters = (
            Q(created_at__gte=fx_created_at),
        )
        fx_partner = FxPartner.objects.filter(*filters).order_by("created_at").first()

        if(fx_partner is None):
            # Get just next from supplied date
            filters = (
                Q(created_at__lte=fx_created_at),
            )
            fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        if(fx_partner is None):
            return Response({
                "error": settings.ERROR_FX_NOT_IN_DB,
                "details": {
                    "non_field_errors": [_("Undefined fx_partner on DB")]
                }
            }, status=status.HTTP_400_BAD_REQUEST)

        # Dataframe config
        cols_to_use_df = [
            "Marketing Source Name",
            "Deposits",
            "Turnover",
            "Net Revenue",
            "Signups",
            "First Time Depositing Customers",
            "CPA Triggered",
            "First Time Active Customers"
        ]
        dtype_df = {
            "Marketing Source Name": "string",
            "CPA Triggered": "string",
            "Deposits": "string",
            "Turnover": "string",
            "Net Revenue": "string",
            "Signups": "string",
            "First Time Depositing Customers": "string",
            "First Time Active Customers": "string",
        }
        rename_df = {
            "Marketing Source Name": "prom_code",
            "Deposits": "deposit",
            "Turnover": "stake",
            "Net Revenue": "net_revenue",
            "Signups": "registered_count",
            "First Time Depositing Customers": "first_deposit_count",
            "CPA Triggered": "cpa_count",
            "First Time Active Customers": "wagering_count",
        }

        if(campaign_title == "zamba col"):
            rs_percentage = settings.API_ZAMBA_COL_RS_PERCENTAGE
            # Remove vars that not supplied by Bookmaker
            cols_to_use_df.remove("Turnover")
            dtype_df.pop("Turnover")
            rename_df.pop("Turnover")
        elif(campaign_title == "luckia col"):
            rs_percentage = settings.API_LUCKIACOL_RS_PERCENTAGE
        elif(campaign_title == "luckia esp"):
            rs_percentage = settings.API_LUCKIAES_RS_PERCENTAGE
        elif (campaign_title == "betfair col"):
            rs_percentage = settings.API_BETFAIRCOL_RS_PERCENTAGE
        elif (campaign_title == "betfair latam"):
            rs_percentage = settings.API_BETFAIRLATAM_RS_PERCENTAGE
        elif (campaign_title == "betfair br"):
            rs_percentage = settings.API_BETFAIRBR_RS_PERCENTAGE
        elif (campaign_title == "betfair pe"):
            rs_percentage = settings.API_BETFAIRPE_RS_PERCENTAGE
        elif (campaign_title == "betfair mex"):
            rs_percentage = settings.API_BETFAIRMEX_RS_PERCENTAGE
        elif (campaign_title == "betfair esp"):
            rs_percentage = settings.API_BETFAIRESP_RS_PERCENTAGE
        elif (campaign_title == "betfair col 2"):
            rs_percentage = settings.API_BETFAIRCOL2_RS_PERCENTAGE
        elif (campaign_title == "betfair col 3"):
            rs_percentage = settings.API_BETFAIRCOL3_RS_PERCENTAGE
        elif (campaign_title == "betfair col 4"):
            rs_percentage = settings.API_BETFAIRCOL4_RS_PERCENTAGE
        elif (campaign_title == "mozzart col"):
            rs_percentage = settings.API_MOZZARTCOL_RS_PERCENTAGE

        # Load string like temp file in ram
        data_io = StringIO(request.data['csv_data'].read().decode('utf-8'))
        # Setup vars from Campaign
        currency_condition = campaign.currency_condition
        currency_fixed_income = campaign.currency_fixed_income

        # Create the DataFrame
        # Initialize Dataframe to Null
        df = None

        try:
            # set the characters and line based interface to stream I/O
            df = pd.read_csv(
                filepath_or_buffer=data_io,
                sep=",",
                usecols=cols_to_use_df,
                dtype=dtype_df,
            )[cols_to_use_df]

            df.rename(
                inplace=True,
                columns=rename_df,
            )

            # Remove prom_code na, case Totals
            df = df[df["prom_code"].notna()]

            # Remove currency character with sub_string (from list at index 1)
            # Remove character ',' from numbers for datatype casting
            columns_deposit = df["deposit"].tolist()
            columns_deposit = list(
                map(lambda x: np.float32(x.replace(x, x[1:]).replace(",", "")), columns_deposit))

            # Remove currency character with sub_string (from list at index 1)
            # Remove character ',' from numbers for datatype casting
            if ("Turnover" in cols_to_use_df):
                columns_turnover = df["stake"].tolist()
                columns_turnover = list(
                    map(lambda x: np.float32(x.replace(x, x[1:]).replace(",", "")), columns_turnover))
            else:
                columns_turnover = None

            # Remove currency character with sub_string (from list at index 1)
            # Remove character ',' from numbers for datatype casting
            columns_net_revenue = df["net_revenue"].tolist()
            columns_net_revenue = list(
                map(lambda x: np.float32(x.replace(x, x[1:]).replace(",", "")), columns_net_revenue))

            # Remove character ',' from numbers for datatype casting
            columns_signups = df["registered_count"].tolist()
            columns_signups = list(
                map(lambda x: np.uint32(x.replace(",", "")), columns_signups))

            # Remove character ',' from numbers for datatype casting
            columns_firts_deposit = df["first_deposit_count"].tolist()
            columns_firts_deposit = list(
                map(lambda x: np.uint32(x.replace(",", "")), columns_firts_deposit))

            # Remove character ',' from numbers for datatype casting
            columns_cpa_count = df["cpa_count"].tolist()
            columns_cpa_count = list(
                map(lambda x: np.uint32(x.replace(",", "")), columns_cpa_count))

            # Remove character ',' from numbers for datatype casting
            columns_wagering = df["wagering_count"].tolist()
            columns_wagering = list(
                map(lambda x: np.uint32(x.replace(",", "")), columns_wagering))

            df["deposit"] = columns_deposit

            if (columns_turnover is not None):
                df["stake"] = columns_turnover

            df["net_revenue"] = columns_net_revenue
            df["registered_count"] = columns_signups
            df["first_deposit_count"] = columns_firts_deposit
            df["cpa_count"] = columns_cpa_count
            df["wagering_count"] = columns_wagering

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            logger.error(
                "Something is wrong at read CSV data, check format must be keys separated by comma "
                "if problem persist check traceback:"
                f"\n\n{''.join(e)}"
            )
            return Response(
                data={
                    "error": settings.ERROR_CODE_BAD_CSV,
                    "details": {
                        "csv_data": [
                            _("Something is wrong at read CSV data, check format must be keys separated by comma"),
                        ],
                    }
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(prom_code__in=df.prom_code.unique()),
            Q(campaign_id=campaign.id),
        )
        links = Link.objects.filter(*filters).select_related("partner_link_accumulated").select_related("betenlacecpa")

        betnelacecpas = links.values_list("betenlacecpa", flat=True)

        yesterday = timezone_customer(today - timedelta(days=1)).date()
        first_day = timezone_customer(today).replace(day=1).date()

        filters_bet_daily_less = [Q(betenlace_cpa__in=betnelacecpas)]
        filters_bet_daily = [Q(betenlace_cpa__in=betnelacecpas)]

        # Case current month
        if yesterday > first_day:
            less_yesterday = yesterday - timedelta(days=1)
            filters_bet_daily_less.extend(
                [
                    Q(created_at__gte=first_day),
                    Q(created_at__lte=less_yesterday),
                ]
            )
            filters_bet_daily.extend(
                [
                    Q(created_at__gte=first_day),
                    Q(created_at__lte=yesterday),
                ]
            )
        # Case update data of first day of month (YYYY/MM/02)
        elif yesterday == first_day:  # When be in the second day month
            # filters_bet_daily_less.extend([Q(created_at=first_day)])
            filters_bet_daily.extend(
                [
                    Q(created_at=first_day),
                ]
            )
        # Other case, update data of last day of previous month
        else:
            day1_month_before = today + relativedelta(months=-1) + relativedelta(day=1)
            less_yesterday = yesterday - timedelta(days=1)
            filters_bet_daily_less.extend(
                [
                    Q(created_at__gte=day1_month_before),
                    Q(created_at__lte=less_yesterday),
                ]
            )
            filters_bet_daily.extend(
                [
                    Q(created_at__gte=day1_month_before),
                    Q(created_at__lte=yesterday),
                ]
            )

        # Betenlace dailies month to update without day to create/update
        betenlace_daily_reports_less = BetenlaceDailyReport.objects.filter(*filters_bet_daily_less)

        # Betenlace dailies month to update included day to create/update
        betenlace_daily_reports = BetenlaceDailyReport.objects.filter(
            *filters_bet_daily)

        # Partner dailies dailies month to update included day to create/update
        partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(
            Q(betenlace_daily_report__in=betenlace_daily_reports))

        # Get last fx_partner_percentage
        fx_partner_percentage = fx_partner.fx_percentage

        currency_condition_str = campaign.currency_condition.lower()
        currency_fixed_income_str = campaign.currency_fixed_income.lower()

        # Acumulators bulk create and update
        member_reports_betenlace_month_update = []
        member_reports_daily_betenlace_update = []
        member_reports_daily_betenlace_create = []

        member_reports_partner_month_update = []
        member_reports_daily_partner_update = []
        member_reports_daily_partner_create = []

        # Set keys by index based on colum names of Dataframe
        keys = {key: index for index, key in enumerate(df.columns.values)}
        list_logger_warn = []
        list_logger_error = []
        for row in zip(*df.to_dict('list').values()):
            # Get link according to prom_code of current loop
            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)
            if not link:
                msg = (
                    f"Link with prom_code=\"{row[keys.get('prom_code')]}\" and campaign=\"{campaign_title}\" "
                    "not found on database"
                )
                list_logger_warn.append(msg)
                continue
            try:
                # Get current entry of member report based on link (prom_code)
                betenlace_cpa = link.betenlacecpa
            except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
                msg = (
                    f"Betenlace CPA entry not found for link with prom_code={row[keys.get('prom_code')]}"
                )
                list_logger_error.append(msg)
                continue

            betenlace_daily = next(
                filter(
                    lambda betenlace_daily: (
                        betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                        betenlace_daily.created_at == yesterday
                    ),
                    betenlace_daily_reports,
                ),
                None,
            )
            filters_annotate_daily = {
                "cpa_count_sum": Coalesce(Sum("cpa_count"), 0),
                "registered_count_sum": Coalesce(Sum("registered_count"), 0),
                "deposit_sum": Coalesce(Sum("deposit"), 0.0),
                "stake_sum": Coalesce(Sum("stake"), 0.0),
                "net_revenue_sum": Coalesce(Sum("net_revenue"), 0.0),
                "first_deposit_count_sum": Coalesce(Sum("first_deposit_count"), 0),
                "wagering_count_sum": Coalesce(Sum("wagering_count"), 0),
            }

            value_group = "betenlace_cpa"

            # Search previous data only if yesterday is not first day of month
            betenlace_daily_sum = None
            if (yesterday != first_day):
                filters = (
                    Q(betenlace_cpa=betenlace_cpa),
                )
                betenlace_daily_sum = betenlace_daily_reports_less.filter(
                    *filters,
                ).values(
                    value_group,
                ).annotate(
                    **filters_annotate_daily,
                )

            data = {}
            # Create dict with data to add in BetenlaceDaily Model
            if betenlace_daily_sum:  # If exits previous records
                betenlace_daily_sum = betenlace_daily_sum.get()
                deposit_tx = row[keys.get('deposit')]*tx_param  # Transforms to the corresponding currency
                if (columns_turnover is not None):
                    stake_tx = row[keys.get('stake')]*tx_param  # Transforms to the corresponding currency
                else:
                    stake_tx = 0
                net_revenue_tx = row[keys.get('net_revenue')]*tx_param  # Transforms to the corresponding currency

                # Verify if cpa csv is less than sum all daily
                cpa_rest = row[keys.get('cpa_count')] - betenlace_daily_sum.get("cpa_count_sum")
                # Verify if registered_count csv is less than sum all daily
                registered_count_rest = row[keys.get('registered_count')
                                            ] - betenlace_daily_sum.get("registered_count_sum")

                deposit_tx = row[keys.get('deposit')]*tx_param  # Transforms to the corresponding currency
                if (columns_turnover is not None):
                    stake_tx = row[keys.get('stake')]*tx_param  # Transforms to the corresponding currency
                else:
                    stake_tx = 0
                net_revenue_tx = row[keys.get('net_revenue')]*tx_param  # Transforms to the corresponding currency
                first_deposit_rest = (
                    row[keys.get('first_deposit_count')] - betenlace_daily_sum.get("first_deposit_count_sum")
                )

                deposit = betenlace_daily_sum.get("deposit_sum")
                stake = betenlace_daily_sum.get("stake_sum")
                net_revenue = betenlace_daily_sum.get("net_revenue_sum")

                wagering_count_difference = row[keys.get(
                    'wagering_count')] - betenlace_daily_sum.get("wagering_count_sum")

                data["cpa_count"] = cpa_rest if cpa_rest > 0 else 0
                data["registered_count"] = registered_count_rest if registered_count_rest > 0 else 0
                data["deposit"] = (deposit_tx - deposit) if deposit_tx > deposit else 0
                data["stake"] = (stake_tx - stake) if stake_tx > stake else 0
                data["net_revenue"] = net_revenue_tx - net_revenue
                data["first_deposit_count"] = first_deposit_rest if first_deposit_rest > 0 else 0
                data["wagering_count"] = wagering_count_difference if wagering_count_difference > 0 else 0
            else:
                deposit_tx = row[keys.get('deposit')]*tx_param  # Transforms to the corresponding currency

                if (columns_turnover is not None):
                    stake_tx = row[keys.get('stake')]*tx_param  # Transforms to the corresponding currency
                else:
                    stake_tx = 0
                net_revenue_tx = row[keys.get('net_revenue')]*tx_param  # Transforms to the corresponding currency

                data["cpa_count"] = row[keys.get('cpa_count')]
                data["registered_count"] = row[keys.get('registered_count')]

                data["deposit"] = deposit_tx
                data["stake"] = stake_tx
                data["net_revenue"] = net_revenue_tx

                data["first_deposit_count"] = row[keys.get('first_deposit_count')]
                data["wagering_count"] = row[keys.get('wagering_count')]

            betenlace_cpa = self.betenlace_month_update(
                data=data,
                betenlace_cpa=betenlace_cpa,
                rs_percentage=rs_percentage,
                betenlace_daily=betenlace_daily,
            )
            member_reports_betenlace_month_update.append(betenlace_cpa)

            if not betenlace_daily:
                # Create
                betenlace_daily = self.betenlace_daily_create(
                    data=data,
                    betenlace_cpa=betenlace_cpa,
                    day=yesterday,
                    rs_percentage=rs_percentage,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_create.append(betenlace_daily)
            else:
                # Update
                betenlace_daily = self.betenlace_daily_update(
                    data=data,
                    betenlace_daily=betenlace_daily,
                    rs_percentage=rs_percentage,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_update.append(betenlace_daily)

            # Validate if campaign status is equal to INACTIVE
            if(campaign.status == Campaign.Status.INACTIVE) and (yesterday >= link.campaign.last_inactive_at.date()):
                continue

            partner_link_accumulated = link.partner_link_accumulated

            if(partner_link_accumulated is None):
                continue

            # Get all all partner daily objects before if exits
            # Partner Daily
            partner_daily = next(
                filter(
                    lambda partner_daily: (
                        partner_daily.partner_link_accumulated_id == partner_link_accumulated.pk and
                        partner_daily.created_at == yesterday
                    ),
                    partner_link_dailies_reports,
                ),
                None,
            )

            data_partner = {"cpa_count": data["cpa_count"]}
            # Tracker
            if(data_partner["cpa_count"] > settings.MIN_CPA_TRACKER_DAY):
                cpa_count = math.floor(
                    data_partner["cpa_count"]*partner_link_accumulated.tracker
                )
            else:
                cpa_count = data_partner["cpa_count"]

            tracked_data = self.get_tracker_values(
                data=data,
                partner_link_accumulated=partner_link_accumulated,
            )

            # Fx currency Fixed income
            partner_currency_str = partner_link_accumulated.currency_local.lower()
            fx_fixed_income_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_fixed_income_str,
                partner_currency_str=partner_currency_str,
                list_logger_error=list_logger_error,
            )

            fixed_income_partner_unitary = campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa
            fixed_income_partner = cpa_count * fixed_income_partner_unitary
            fixed_income_partner_unitary_local = (
                campaign.fixed_income_unitary *
                partner_link_accumulated.percentage_cpa *
                fx_fixed_income_partner
            )
            fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

            # Fx Currency Condition
            fx_condition_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_condition_str,
                partner_currency_str=partner_currency_str,
                list_logger_error=list_logger_error,
            )

            # Update Month
            partner_link_accumulated = self.partner_link_month_update(
                partner_link_accumulated=partner_link_accumulated,
                cpa_count=cpa_count,
                fixed_income_partner=fixed_income_partner,
                fixed_income_partner_local=fixed_income_partner_local,
                partner_daily=partner_daily,
            )
            member_reports_partner_month_update.append(partner_link_accumulated)

            if not partner_daily:
                partner_link_daily = self.partner_link_daily_create(
                    day=yesterday,
                    campaign=campaign,
                    betenlace_daily=betenlace_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    cpa_count=cpa_count,
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner=fixed_income_partner,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    fixed_income_partner_local=fixed_income_partner_local,
                    partner=partner_link_accumulated.partner,
                )
                member_reports_daily_partner_create.append(partner_link_daily)
            else:
                partner_link_daily = self.partner_link_daily_update(
                    cpa_count=cpa_count,
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner=fixed_income_partner,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    fixed_income_partner_local=fixed_income_partner_local,
                    partner_link_daily=partner_daily,
                    partner=partner_link_accumulated.partner,
                    betenlace_daily=betenlace_daily,
                    partner_link_accumulated=partner_link_accumulated,
                )
                member_reports_daily_partner_update.append(partner_link_daily)

        with transaction.atomic(using=DB_USER_PARTNER):
            if(member_reports_betenlace_month_update):
                BetenlaceCPA.objects.bulk_update(
                    objs=member_reports_betenlace_month_update,
                    fields=(
                        "deposit",
                        "stake",
                        "fixed_income",
                        "net_revenue",
                        "revenue_share",
                        "registered_count",
                        "cpa_count",
                        "first_deposit_count",
                        "wagering_count",
                    ),
                )

            if(member_reports_daily_betenlace_update):
                BetenlaceDailyReport.objects.bulk_update(
                    objs=member_reports_daily_betenlace_update,
                    fields=(
                        "deposit",
                        "stake",
                        "fixed_income",
                        "net_revenue",
                        "revenue_share",
                        "fixed_income_unitary",
                        "registered_count",
                        "cpa_count",
                        "first_deposit_count",
                        "wagering_count",
                        "fx_partner",
                    ),
                )

            if(member_reports_daily_betenlace_create):
                BetenlaceDailyReport.objects.bulk_create(
                    objs=member_reports_daily_betenlace_create,
                )

            if(member_reports_partner_month_update):
                PartnerLinkAccumulated.objects.bulk_update(
                    objs=member_reports_partner_month_update,
                    fields=(
                        "cpa_count",
                        "fixed_income",
                        "fixed_income_local",
                    ),
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
                    ),
                )

            if(member_reports_daily_partner_create):
                PartnerLinkDailyReport.objects.bulk_create(
                    objs=member_reports_daily_partner_create,
                )

        if (list_logger_warn):
            logger.warning("\n".join(list_logger_warn))
        if (list_logger_error):
            logger.error("\n".join(list_logger_error))

        return Response(200)

    def betenlace_month_update(
        self,
        data,
        betenlace_cpa,
        rs_percentage,
        betenlace_daily,
    ):

        if data != {}:
            # Remove previous month update
            if betenlace_daily:
                deposit_res = betenlace_cpa.deposit - (betenlace_daily.deposit or 0)
                stake_res = betenlace_cpa.stake - (betenlace_daily.stake or 0)
                fixed_income_res = betenlace_cpa.fixed_income - (betenlace_daily.fixed_income or 0)
                net_revenue_res = betenlace_cpa.net_revenue - (betenlace_daily.net_revenue or 0)
                revenue_share_res = betenlace_cpa.revenue_share - (betenlace_daily.revenue_share or 0)
                registered_count_res = betenlace_cpa.registered_count - (betenlace_daily.registered_count or 0)
                cpa_count_res = betenlace_cpa.cpa_count - (betenlace_daily.cpa_count or 0)
                first_deposit_count_res = (
                    betenlace_cpa.first_deposit_count - (betenlace_daily.first_deposit_count or 0)
                )
                wagering_count_res = betenlace_cpa.wagering_count - (betenlace_daily.wagering_count or 0)

                betenlace_cpa.deposit = deposit_res if deposit_res > 0 else 0
                betenlace_cpa.stake = stake_res if stake_res > 0 else 0
                betenlace_cpa.fixed_income = fixed_income_res if fixed_income_res > 0 else 0
                betenlace_cpa.net_revenue = net_revenue_res
                betenlace_cpa.revenue_share = revenue_share_res
                betenlace_cpa.registered_count = registered_count_res if registered_count_res > 0 else 0
                betenlace_cpa.cpa_count = cpa_count_res if cpa_count_res > 0 else 0
                betenlace_cpa.first_deposit_count = first_deposit_count_res if first_deposit_count_res > 0 else 0
                betenlace_cpa.wagering_count = wagering_count_res if wagering_count_res > 0 else 0

            fixed_income = betenlace_cpa.link.campaign.fixed_income_unitary * data["cpa_count"]

            betenlace_cpa.deposit += (data["deposit"])
            betenlace_cpa.stake += (data["stake"])
            betenlace_cpa.fixed_income += (fixed_income)
            betenlace_cpa.net_revenue += (data["net_revenue"])

            # Revenue share stimated according to net_revenue
            betenlace_cpa.revenue_share += (data["net_revenue"] * rs_percentage)

            betenlace_cpa.registered_count += (data["registered_count"])
            betenlace_cpa.cpa_count += (data["cpa_count"])
            betenlace_cpa.first_deposit_count += (data["first_deposit_count"])
            betenlace_cpa.wagering_count += (data["wagering_count"])

            return betenlace_cpa

    def betenlace_daily_create(
        self,
        data,
        betenlace_cpa,
        day,
        rs_percentage,
        fx_partner,
    ):

        data_to_create_betdaily = {
            "cpa_count": data["cpa_count"],
            "registered_count": data["registered_count"],
            "fixed_income_unitary": betenlace_cpa.link.campaign.fixed_income_unitary,
            "betenlace_cpa": betenlace_cpa,
            "currency_condition": betenlace_cpa.link.campaign.currency_condition,
            "currency_fixed_income": betenlace_cpa.link.campaign.currency_fixed_income,
            "deposit": data["deposit"],
            "stake": data["stake"],
            "net_revenue": data["net_revenue"],
            # Revenue share stimated according to net_revenue
            "revenue_share": (data["net_revenue"] * rs_percentage),

            "fx_partner": fx_partner,

            "first_deposit_count": data["first_deposit_count"],
            "wagering_count": data["wagering_count"],
            "created_at": day
        }
        data_to_create_betdaily["fixed_income"] = data_to_create_betdaily.get(
            "fixed_income_unitary") * data["cpa_count"]
        bet_daily = BetenlaceDailyReport(**data_to_create_betdaily)

        return bet_daily

    def betenlace_daily_update(
        self,
        data,
        betenlace_daily,
        rs_percentage,
        fx_partner,
    ):
        betenlace_daily.cpa_count = data["cpa_count"]
        betenlace_daily.registered_count = data["registered_count"]
        betenlace_daily.fixed_income_unitary = betenlace_daily.betenlace_cpa.link.campaign.fixed_income_unitary
        betenlace_daily.fixed_income = (betenlace_daily.fixed_income_unitary * data["cpa_count"])
        betenlace_daily.deposit = data["deposit"]
        betenlace_daily.stake = data["stake"]
        betenlace_daily.net_revenue = data["net_revenue"]
        betenlace_daily.revenue_share = data["net_revenue"] * rs_percentage
        betenlace_daily.first_deposit_count = data["first_deposit_count"]
        betenlace_daily.wagering_count = data["wagering_count"]
        betenlace_daily.fx_partner = fx_partner
        return betenlace_daily

    def calc_without_fx(self, fx_partner, campaing_currency_fixed_income_str, partner_currency_str, list_logger_error):
        if(campaing_currency_fixed_income_str != partner_currency_str):
            try:
                fx_book_partner = eval(
                    f"fx_partner.fx_{campaing_currency_fixed_income_str}_{partner_currency_str}")
            except:
                msg = (
                    f"Fx conversion from {campaing_currency_fixed_income_str} to {partner_currency_str} undefined on DB")
                list_logger_error.append(msg)
        else:
            fx_book_partner = 1

        return fx_book_partner

    def get_tracker_values(
        self,
        data,
        partner_link_accumulated,
    ):
        tracked_data = {}
        if (data.get("deposit") is not None):
            tracked_data["deposit"] = data.get("deposit")*partner_link_accumulated.tracker_deposit

        if (data.get("registered_count") is not None):
            if(data.get("registered_count") > 1):
                tracked_data["registered_count"] = math.floor(
                    data.get("registered_count")*partner_link_accumulated.tracker_registered_count
                )
            else:
                tracked_data["registered_count"] = data.get("registered_count")

        if (data.get("first_deposit_count") is not None):
            if(data.get("first_deposit_count") > 1):
                tracked_data["first_deposit_count"] = math.floor(
                    data.get("first_deposit_count")*partner_link_accumulated.tracker_first_deposit_count
                )
            else:
                tracked_data["first_deposit_count"] = data.get("first_deposit_count")

        if (data.get("wagering_count") is not None):
            if(data.get("wagering_count") > 1):
                tracked_data["wagering_count"] = math.floor(
                    data.get("wagering_count")*partner_link_accumulated.tracker_wagering_count
                )
            else:
                tracked_data["wagering_count"] = data.get("wagering_count")

        return tracked_data

    def calc_fx(
            self,
            fx_partner,
            fx_partner_percentage,
            currency_from_str,
            partner_currency_str,
            list_logger_error,
    ):
        if(currency_from_str != partner_currency_str):
            try:
                fx_book_partner = eval(
                    f"fx_partner.fx_{currency_from_str}_{partner_currency_str}") * fx_partner_percentage
            except:
                msg = (
                    f"Fx conversion from {currency_from_str} to {partner_currency_str} undefined on DB")
                list_logger_error.append(msg)
        else:
            fx_book_partner = 1
        return fx_book_partner

    def partner_link_month_update(
            self,
            partner_link_accumulated,
            cpa_count,
            fixed_income_partner,
            fixed_income_partner_local,
            partner_daily,
    ):
        if (partner_daily):
            cpa_count_res = (
                partner_link_accumulated.cpa_count - (partner_daily.cpa_count or 0)
            )
            fixed_income_res = (
                partner_link_accumulated.fixed_income - (partner_daily.fixed_income or 0)
            )
            fixed_income_local_res = (
                partner_link_accumulated.fixed_income_local - (partner_daily.fixed_income_local or 0)
            )

            partner_link_accumulated.cpa_count = cpa_count_res if cpa_count_res > 0 else 0
            partner_link_accumulated.fixed_income = fixed_income_res if fixed_income_res > 0 else 0
            partner_link_accumulated.fixed_income_local = fixed_income_local_res if fixed_income_local_res > 0 else 0

        partner_link_accumulated.cpa_count += cpa_count
        partner_link_accumulated.fixed_income += fixed_income_partner
        partner_link_accumulated.fixed_income_local += fixed_income_partner_local
        return partner_link_accumulated

    def partner_link_daily_create(
        self,
        day,
        campaign,
        betenlace_daily,
        partner_link_accumulated,
        cpa_count,
        tracked_data,
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
                betenlace_daily.revenue_share * partner.net_revenue_adviser_percentage
                if betenlace_daily.revenue_share is not None
                else
                0
            )
            net_revenue_adviser_local = (
                net_revenue_adviser * fx_condition_partner
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

            cpa_count=cpa_count,
            percentage_cpa=partner_link_accumulated.percentage_cpa,

            deposit=tracked_data.get("deposit"),
            registered_count=tracked_data.get("registered_count"),
            first_deposit_count=tracked_data.get("first_deposit_count"),
            wagering_count=tracked_data.get("wagering_count"),

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

            created_at=day,
        )
        return partner_link_daily

    def partner_link_daily_update(
        self,
        cpa_count,
        tracked_data,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner,
        fixed_income_partner_unitary_local,
        fixed_income_partner_local,
        partner_link_daily,
        partner,
        betenlace_daily,
        partner_link_accumulated,
    ):

        partner_link_daily.fixed_income = fixed_income_partner
        partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary

        partner_link_daily.fx_book_local = fx_fixed_income_partner
        partner_link_daily.fx_book_net_revenue_local = fx_condition_partner
        partner_link_daily.fx_percentage = fx_partner_percentage

        partner_link_daily.fixed_income_local = fixed_income_partner_local
        partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

        partner_link_daily.cpa_count = cpa_count
        partner_link_daily.percentage_cpa = partner_link_accumulated.percentage_cpa

        partner_link_daily.tracker = partner_link_accumulated.tracker
        partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
        partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
        partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
        partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

        partner_link_daily.deposit = tracked_data.get("deposit")
        partner_link_daily.registered_count = tracked_data.get("registered_count")
        partner_link_daily.first_deposit_count = tracked_data.get("first_deposit_count")
        partner_link_daily.wagering_count = tracked_data.get("wagering_count")

        # Calculate Adviser payment
        partner_link_daily.adviser_id = partner.adviser_id
        partner_link_daily.fixed_income_adviser_percentage = partner.fixed_income_adviser_percentage
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
                betenlace_daily.revenue_share * partner.net_revenue_adviser_percentage
                if betenlace_daily.revenue_share is not None
                else
                0
            )
            partner_link_daily.net_revenue_adviser_local = (
                partner_link_daily.net_revenue_adviser * fx_condition_partner
            )
        return partner_link_daily
