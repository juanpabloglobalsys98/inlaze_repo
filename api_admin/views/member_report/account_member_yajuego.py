import logging
import math
from datetime import (
    date,
    datetime,
)

import numpy as np
import pandas as pd
import pytz
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerAccumStatusCHO,
)
from api_partner.models import (
    AccountDailyReport,
    AccountReport,
    BetenlaceCPA,
    BetenlaceDailyReport,
    Campaign,
    FxPartner,
    Link,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from cerberus import Validator
from core.helpers import (
    AdminFilenameErrorHandler,
    HavePermissionBasedView,
    StandardErrorHandler,
    ValidatorFile,
    to_date,
)
from core.tasks import chat_logger
from django.conf import settings
from django.db import transaction
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils import timezone
from django.utils.timezone import (
    datetime,
    make_aware,
    timedelta,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

OCTOBER_2022 = make_aware(datetime.strptime("2022-10-01", "%Y-%m-%d"))

logger = logging.getLogger(__name__)


class AccMemYajuegoUploadAPI(APIView):
    """
    Process CSV files uploaded for account member reports for Yajuego 50.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def post(self, request):
        validator = ValidatorFile(
            schema={
                "account_csv_file": {
                    "required": True,
                    "type": "file",
                },
                "member_csv_file": {
                    "required": True,
                    "type": "file",
                },
                "date": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
                    "default": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        filename_validator = Validator(
            schema={
                "account_csv_file_name": {
                    "required": True,
                    "type": "string",
                    "regex": ".+\.csv$",
                },
                "member_csv_file_name": {
                    "required": True,
                    "type": "string",
                    "regex": ".+\.csv$",
                },
            },
            error_handler=AdminFilenameErrorHandler,
        )
        account_csv_file = validator.document.get("account_csv_file")
        member_csv_file = validator.document.get("member_csv_file")
        filename = {
            "account_csv_file_name": account_csv_file.name if account_csv_file else None,
            "member_csv_file_name": member_csv_file.name if member_csv_file else None,
        }
        # Validate data from cerberus and save in files_names dict
        if not filename_validator.validate(filename):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": filename_validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        campaign_name = "yajuego 50"
        # Get Campaign by its name
        query = Q(campaign_title__iexact=campaign_name)
        campaign = Campaign.objects.using(DB_USER_PARTNER).annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        ).filter(query).first()
        if not campaign or campaign.campaign_title.lower() != campaign_name:
            error_msg = f"Campaign with name \"{campaign_name}\" not found in DB"
            logger.error(error_msg)
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": "Invalid key",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        campaign_title = campaign.campaign_title
        fixed_income_unitary_campaign = campaign.fixed_income_unitary
        revenue_share_percentage = 0.4
        cpa_condition_from_revenue_share = 17500

        yesterday = make_aware(datetime.combine(validator.document.get("date"), datetime.min.time()))
        yesterday_str = yesterday.strftime("%Y-%m-%d")

        logger_msg = (
            "AccMemYajuegoUploadAPI called\n"
            f"Campaign Title -> {campaign_title}\n"
            f"From date -> {yesterday_str}\n"
            f"To date -> {yesterday_str}"
        )
        logger.info(logger_msg)

        # Create the DataFrame for Account report part
        cols_to_use = [
            "Activity Date",
            "Affiliate Profile Site Id",
            "Player Id",
            "Deposits",
            "Stakes",
            "CPA Commission",
            "Net Revenue",
            "RevShare Commission",
            "CPA Count",
            "Registered Date",
            "First Deposit Date",
            "First CPA Date",
        ]

        df_account = pd.read_csv(
            filepath_or_buffer=account_csv_file,
            sep=",",
            usecols=cols_to_use,
            dtype={
                "Activity Date": "string",
                "Affiliate Profile Site Id": "string",
                "Player Id": "string",
                "Deposits": np.float32,
                "Stakes": np.float32,
                "CPA Commission": np.float32,
                "Net Revenue": np.float32,
                "RevShare Commission": np.float32,
                "CPA Count": np.uint32,
                "Registered Date": "string",
                "First Deposit Date": "string",
                "First CPA Date": "string",
            },
        )[cols_to_use]

        df_account.rename(
            inplace=True,
            columns={
                "Activity Date": "activity_date",
                "Affiliate Profile Site Id": "prom_code",
                "Player Id": "punter_id",
                "Deposits": "deposit",
                "Stakes": "stake",
                "CPA Commission": "fixed_income",
                "Net Revenue": "net_revenue",
                "RevShare Commission": "revenue_share",
                "CPA Count": "cpa_count",
                "Registered Date": "registered_at",
                "First Deposit Date": "first_deposit_at",
                "First CPA Date": "cpa_at",
            },
        )

        # Create the DataFrame for Member report part
        cols_to_use = [
            "Activity Date",
            "Affiliate Profile Site Id",
            "Deposits",
            "Stakes",
            "CPA Commission",
            "Net Revenue",
            "RevShare Commission",
            "Registrations",
            "CPA Count",
            "FTD Count",
            "Wagering Accounts",
        ]

        df_member = pd.read_csv(
            filepath_or_buffer=member_csv_file,
            sep=",",
            usecols=cols_to_use,
            dtype={
                "Activity Date": "string",
                "Affiliate Profile Site Id": "string",
                "Deposits": np.float32,
                "Stakes": np.float32,
                "CPA Commission": np.float32,
                "Net Revenue": np.float32,
                "RevShare Commission": np.float32,
                "Registrations": np.uint32,
                "CPA Count": np.uint32,
                "FTD Count": np.uint32,
                "Wagering Accounts": np.uint32,
            },
        )[cols_to_use]

        df_member.rename(
            inplace=True,
            columns={
                "Activity Date": "activity_date",
                "Affiliate Profile Site Id": "prom_code",
                "Deposits": "deposit",
                "Stakes": "stake",
                "CPA Commission": "fixed_income",
                "Net Revenue": "net_revenue",
                "RevShare Commission": "revenue_share",
                "Registrations": "registered_count",
                "CPA Count": "cpa_count",
                "FTD Count": "first_deposit_count",
                "Wagering Accounts": "wagering_count",
            },
        )

        prom_codes = set(df_account.prom_code.unique()) | set(df_member.prom_code.unique())

        # Get related link from prom_codes and campaign
        query = Q(prom_code__in=prom_codes) & Q(campaign_id=campaign.id)
        links = Link.objects.filter(query).select_related(
            "partner_link_accumulated",
            "partner_link_accumulated__partner",
            "betenlacecpa",
        )

        links_pk = links.values_list("pk", flat=True)
        # Get account reports from previous links and punter_id, QUERY
        query = Q(link__in=links_pk) & Q(punter_id__in=df_account.punter_id.unique())
        account_reports = AccountReport.objects.filter(query)
        account_reports_pks = account_reports.values_list("pk", flat=True)

        query = Q(account_report__in=account_reports_pks)
        account_daily_reports = AccountDailyReport.objects.filter(query)

        currency_condition = campaign.currency_condition
        currency_condition_str = currency_condition.lower()
        currency_fixed_income = campaign.currency_fixed_income
        currency_fixed_income_str = currency_fixed_income.lower()

        # Accumulators bulk create and update
        account_reports_update = []
        account_reports_create = []

        account_daily_reports_update = []
        account_daily_reports_create = []

        # Set keys by index based on column names of Dataframe
        keys = {key: index for index, key in enumerate(df_account.columns.values)}

        # Dictionary with current applied sum of CPAs by prom_code
        cpa_by_prom_code_iter = {}
        for prom_code in prom_codes:
            cpa_by_prom_code_iter[prom_code] = []

        is_unique = len(df_account.activity_date.unique()) == 1
        if not is_unique:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": "Activity Date for account is not unique",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        activity_date = make_aware(datetime.strptime(df_account["activity_date"].iloc[0], "%Y-%m-%d")).date()
        query = (
            Q(created_at=activity_date)
            & Q(cpa_count__isnull=False)
            & Q(betenlace_cpa__link__campaign=campaign)
        )
        if BetenlaceDailyReport.objects.filter(query).exists():
            error_msg = f"BetenlaceDailyReport with date {activity_date} already has uploaded data"
            logger.error(error_msg)
        if activity_date != yesterday.date():
            error_msg = f"Date {activity_date} is different from {yesterday.date()}"
            logger.error(error_msg)
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": error_msg,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        is_unique = len(df_member.activity_date.unique()) == 1
        if not is_unique:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": "Activity Date for member is not unique",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        activity_date = make_aware(datetime.strptime(df_member["activity_date"].iloc[0], "%Y-%m-%d")).date()
        if activity_date != yesterday.date():
            error_msg = f"Date {activity_date} is different from {yesterday.date()}"
            logger.error(error_msg)
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": error_msg,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        for row in zip(*df_account.to_dict('list').values()):

            if row[keys.get("cpa_count")] != 0:
                # Prevent a cpa_count bad value
                error_msg = f"cpa_count is not 0, punter {row[keys.get('punter_id')]}, campaign {campaign_title}"
                logger.error(error_msg)
                return Response(
                    data={
                        "error": settings.INTERNAL_SERVER_ERROR,
                        "detail": error_msg,
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)
            if not link:
                logger.warning(
                    f"Link with prom_code={row[keys.get('prom_code')]} and campaign={campaign_title} not found"
                )
                continue

            # Get current entry of account report based on link and punter_id
            account_report = next(
                filter(
                    lambda account_report: account_report.link_id == link.pk and
                    account_report.punter_id == row[keys.get("punter_id")],
                    account_reports
                ),
                None,
            )

            # Get current partner that have the current link
            partner_link_accumulated = link.partner_link_accumulated
            if partner_link_accumulated:
                # Validate if link has relationship with partner and if has verify if status is equal to status campaign
                if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                    # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                    if(campaign.status == Campaign.Status.INACTIVE) and (yesterday.date() >= campaign.last_inactive_at.date()):
                        msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                        logger.warning(msg)
                        partner_link_accumulated = None
                elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                    msg = f"link with prom_code {partner_link_accumulated.prom_code} has custom status inactive"
                    logger.warning(msg)
                    partner_link_accumulated = None

            if account_report:
                current_account_daily_report = next(
                    (
                        adr for adr in account_daily_reports
                        if (adr.account_report == account_report and adr.created_at == yesterday.date())
                    ),
                    None,
                )
                data_to_update = None

                if current_account_daily_report:
                    current_account_daily_report, data_to_update = _account_daily_report_update(
                        row=row,
                        keys=keys,
                        account_daily_report=current_account_daily_report,
                    )
                    account_daily_reports_update.append(current_account_daily_report)
                else:
                    current_account_daily_report = _account_daily_report_create(
                        row=row,
                        keys=keys,
                        account_report=account_report,
                        currency_condition=currency_condition,
                        currency_fixed_income=currency_fixed_income,
                        from_date=yesterday.date(),
                    )
                    account_daily_reports_create.append(current_account_daily_report)

                #  Temp use to_date
                # Case and exist entry
                account_report_update = _account_report_update(
                    data_to_update=data_to_update,
                    keys=keys,
                    row=row,
                    account_daily_report=current_account_daily_report,
                    from_date=yesterday.date(),
                    partner_link_accumulated=partner_link_accumulated,
                    account_report=account_report,
                    cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                    revenue_share_percentage=revenue_share_percentage,
                    cpa_condition_from_revenue_share=cpa_condition_from_revenue_share,
                    currency_fixed_income=currency_fixed_income,
                    fixed_income_campaign=fixed_income_unitary_campaign,
                )
                account_reports_update.append(account_report_update)

            else:
                current_account_daily_report_create = _account_daily_report_create(
                    row=row,
                    keys=keys,
                    currency_condition=currency_condition,
                    currency_fixed_income=currency_fixed_income,
                    from_date=yesterday.date(),
                )
                # Temp use to_date
                # Case new entry
                account_report_new = _account_report_create(
                    row=row,
                    keys=keys,
                    link=link,
                    currency_condition=currency_condition,
                    currency_fixed_income=currency_fixed_income,
                    partner_link_accumulated=partner_link_accumulated,
                    from_date=yesterday.date(),
                    cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                    revenue_share_percentage=revenue_share_percentage,
                    cpa_condition_from_revenue_share=cpa_condition_from_revenue_share,
                    fixed_income_campaign=fixed_income_unitary_campaign,
                    account_daily_report=current_account_daily_report_create,
                )

                current_account_daily_report_create.account_report = account_report_new
                current_account_daily_report_create.is_first_deposit_count = bool(account_report_new.first_deposit_at)
                account_daily_reports_create.append(current_account_daily_report_create)
                account_reports_create.append(account_report_new)

        # Continue for Member report
        betenlacecpas_pk = links.values_list("betenlacecpa__pk", flat=True)

        query = Q(betenlace_cpa__pk__in=betenlacecpas_pk) & Q(created_at=yesterday.date())
        betenlace_daily_reports = BetenlaceDailyReport.objects.filter(query)

        query = Q(betenlace_daily_report__in=betenlace_daily_reports)
        partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(query)

        # Get the last Fx value
        fx_created_at = yesterday.replace(minute=0, hour=0, second=0, microsecond=0)
        query = Q(created_at__gte=fx_created_at)
        fx_partner = FxPartner.objects.filter(query).order_by("created_at").first()

        if fx_partner is None:
            # Get just next from supplied date
            query = Q(created_at__lte=fx_created_at)
            fx_partner = FxPartner.objects.filter(query).order_by("-created_at").first()

        # If still none prevent execution
        if fx_partner is None:
            error_msg = "Undefined fx_partner on DB"
            logger.error(error_msg)
            return Response(
                data={
                    "error": settings.INTERNAL_SERVER_ERROR,
                    "detail": error_msg,
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        fx_partner_percentage = fx_partner.fx_percentage

        # Accumulators bulk create and update
        member_reports_betenlace_month_update = []
        member_reports_daily_betenlace_update = []
        member_reports_daily_betenlace_create = []

        member_reports_partner_month_update = []
        member_reports_daily_partner_update = []
        member_reports_daily_partner_create = []

        # Set keys by index based on column names of Dataframe
        keys = {key: index for index, key in enumerate(df_member.columns.values)}

        for row in zip(*df_member.to_dict("list").values()):
            # Get link according to prom_code of current loop
            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)
            if not link:
                logger_msg = (
                    f"Link with prom_code=\"{row[keys.get('prom_code')]}\" and campaign=\"{campaign_title}\" "
                    "not found on database"
                )
                logger.warning(logger_msg)
                continue

            try:
                # Get current entry of member report based on link (prom_code)
                betenlace_cpa = link.betenlacecpa
            except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
                logger_msg = (
                    f"Betenlace CPA entry not found for link with prom_code={row[keys.get('prom_code')]}"
                )
                logger.error(logger_msg)
                continue

            # Generate data from account report by prom_code
            cpa_count = len(cpa_by_prom_code_iter.get(row[keys.get("prom_code")]))

            # Betenlace Month
            betenlace_cpa = _betenlace_month_update(
                keys=keys,
                row=row,
                betenlace_cpa=betenlace_cpa,
                cpa_count=cpa_count,
                fixed_income_campaign=fixed_income_unitary_campaign,
                revenue_share_percentage=revenue_share_percentage,
            )
            member_reports_betenlace_month_update.append(betenlace_cpa)

            # Betenlace Daily
            betenlace_daily = next(
                filter(
                    lambda betenlace_daily: (
                        betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                        betenlace_daily.created_at == yesterday.date()
                    ),
                    betenlace_daily_reports,
                ),
                None,
            )

            if betenlace_daily:
                betenlace_daily = _betenlace_daily_update(
                    keys=keys,
                    row=row,
                    betenlace_daily=betenlace_daily,
                    fixed_income_campaign=fixed_income_unitary_campaign,
                    cpa_count=cpa_count,
                    revenue_share_percentage=revenue_share_percentage,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_update.append(betenlace_daily)
            else:
                betenlace_daily = _betenlace_daily_create(
                    keys=keys,
                    row=row,
                    betenlace_cpa=betenlace_cpa,
                    from_date=yesterday.date(),
                    fixed_income_campaign=fixed_income_unitary_campaign,
                    cpa_count=cpa_count,
                    revenue_share_percentage=revenue_share_percentage,
                    currency_condition=currency_condition,
                    currency_fixed_income=currency_fixed_income,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_create.append(betenlace_daily)

            # Partner Month
            partner_link_accumulated = link.partner_link_accumulated
            # When partner have not assigned the link, must continue to next loop
            if partner_link_accumulated is None:
                continue

            # Validate if link has relationship with partner and if has verify if status is equal to status campaign
            if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                if(campaign.status == Campaign.Status.INACTIVE) and (yesterday.date() >= campaign.last_inactive_at.date()):
                    logger_msg = (
                        f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                    )
                    logger.warning(logger_msg)
                    continue
            elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                logger_msg = (
                    f"link with prom_code {partner_link_accumulated.prom_code} has custom status inactive"
                )
                logger.warning(logger_msg)
                continue

            # Tracker
            if cpa_count > settings.MIN_CPA_TRACKER_DAY:
                cpa_count_new = math.floor(cpa_count*partner_link_accumulated.tracker)
            else:
                cpa_count_new = cpa_count

            # Verify if cpa_count had a change from tracker calculation
            if cpa_count > cpa_count_new:
                # Reduce -1 additional for enum behavior
                diff_count = (cpa_count - cpa_count_new) - 1

                for enum, (account_instance_i, account_daily_report_i) in enumerate(
                        reversed(cpa_by_prom_code_iter.get(row[keys.get("prom_code")]))):
                    # Remove cpa partner
                    account_instance_i.cpa_partner = 0
                    account_daily_report_i.is_pa_partner = False
                    if (enum >= diff_count):
                        break

            tracked_data = _get_tracker_values(
                keys=keys,
                row=row,
                partner_link_accumulated=partner_link_accumulated,
            )

            # Fx Currency Fixed income
            partner_currency_str = partner_link_accumulated.currency_local.lower()
            fx_fixed_income_partner = _calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_fixed_income_str,
                partner_currency_str=partner_currency_str,
            )

            fixed_income_partner_unitary = fixed_income_unitary_campaign * partner_link_accumulated.percentage_cpa
            fixed_income_partner = cpa_count_new * fixed_income_partner_unitary
            fixed_income_partner_unitary_local = (
                fixed_income_unitary_campaign *
                partner_link_accumulated.percentage_cpa *
                fx_fixed_income_partner
            )
            fixed_income_partner_local = cpa_count_new * fixed_income_partner_unitary_local

            # Fx Currency Condition
            fx_condition_partner = _calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_condition_str,
                partner_currency_str=partner_currency_str,
            )

            partner_link_accumulated = _partner_link_month_update(
                partner_link_accumulated=partner_link_accumulated,
                cpa_count=cpa_count_new,
                fixed_income_partner=fixed_income_partner,
                fixed_income_partner_local=fixed_income_partner_local,
            )
            member_reports_partner_month_update.append(partner_link_accumulated)

            # Partner Daily
            partner_link_daily = next(
                filter(
                    lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id == betenlace_daily.id,
                    partner_link_dailies_reports,
                ),
                None,
            )

            if partner_link_daily:
                partner_link_daily = _partner_link_daily_update(
                    cpa_count=cpa_count_new,
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
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
                partner_link_daily = _partner_link_daily_create(
                    from_date=yesterday.date(),
                    campaign=campaign,
                    betenlace_daily=betenlace_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    cpa_count=cpa_count_new,
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

        with transaction.atomic(using=DB_USER_PARTNER):
            # Account case
            if account_reports_create:
                AccountReport.objects.bulk_create(
                    objs=account_reports_create,
                )
            if account_reports_update:
                AccountReport.objects.bulk_update(
                    objs=account_reports_update,
                    fields=(
                        "net_revenue",
                        "cpa_partner",
                        "deposit",
                        "cpa_at",
                        "fixed_income",
                        "cpa_betenlace",
                        "revenue_share_cpa",
                        "partner_link_accumulated",
                        "first_deposit_at",
                        "stake",
                        "revenue_share",
                        "currency_fixed_income",
                    ),
                )

            if account_daily_reports_create:
                AccountDailyReport.objects.bulk_create(
                    objs=account_daily_reports_create,
                )

            if account_daily_reports_update:
                AccountDailyReport.objects.bulk_update(
                    objs=account_daily_reports_update,
                    fields=(
                        "account_report",
                        "deposit",
                        "stake",
                        "currency_condition",
                        "fixed_income",
                        "net_revenue",
                        "revenue_share",
                        "revenue_share_cpa",
                        "currency_fixed_income",
                        "is_cpa_betenlace",
                        "is_cpa_partner",
                        "is_first_deposit_count",
                        "created_at",
                    ),
                )

            if member_reports_betenlace_month_update:
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

            if member_reports_daily_betenlace_update:
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
                )

            if member_reports_daily_betenlace_create:
                BetenlaceDailyReport.objects.bulk_create(
                    objs=member_reports_daily_betenlace_create,
                )

            if member_reports_partner_month_update:
                PartnerLinkAccumulated.objects.bulk_update(
                    objs=member_reports_partner_month_update,
                    fields=(
                        "cpa_count",
                        "fixed_income",
                        "fixed_income_local",
                    ),
                )

            if member_reports_daily_partner_update:
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
                        "referred_by",
                        "fixed_income_referred",
                        "fixed_income_referred_local",
                        "net_revenue_referred",
                        "net_revenue_referred_local",
                        "fixed_income_referred_percentage",
                        "net_revenue_referred_percentage",
                    ),
                )

            if member_reports_daily_partner_create:
                PartnerLinkDailyReport.objects.bulk_create(
                    objs=member_reports_daily_partner_create,
                )

        if len(df_member.index) == 0:
            logger_msg = f"Report day: {yesterday_str} Member for Campaign {campaign_title} No Records/No data"
            logger.warning(logger_msg)
        else:
            logger_msg = f"Report day: {yesterday_str} Member for Campaign {campaign_title} processed count {len(df_member.index)}"
            logger.warning(logger_msg)

        chat_logger.apply_async(
            kwargs={
                "msg": logger_msg,
                "msg_url": settings.CHAT_WEBHOOK_REPORT_UPLOAD,
            },
        )
        return Response(status=status.HTTP_200_OK)


def _check_revenue_percentage(row, keys, revenue_share_percentage):
    revenue_share = row[keys.get("revenue_share")]
    net_revenue = row[keys.get("net_revenue")]
    if not revenue_share or not net_revenue:
        return

    current_percentage = round(revenue_share / net_revenue, 2)
    revenue_share_percentage = round(revenue_share_percentage, 2)
    if current_percentage != revenue_share_percentage:
        logger.error(f"{current_percentage} percentage differs from {revenue_share_percentage}")


def _account_daily_report_update(
    row,
    keys,
    account_daily_report: AccountDailyReport,
):
    data_to_update = {
        "deposit": row[keys.get("deposit")] - account_daily_report.deposit,
        "stake": row[keys.get("stake")] - account_daily_report.stake,
        "fixed_income": row[keys.get("fixed_income")] - account_daily_report.fixed_income,
        "net_revenue": row[keys.get("net_revenue")] - account_daily_report.net_revenue,
        "revenue_share": row[keys.get("revenue_share")] - account_daily_report.revenue_share,
        "revenue_share_cpa": abs(row[keys.get("revenue_share")]) - account_daily_report.revenue_share,
    }
    account_daily_report.deposit = row[keys.get("deposit")]
    account_daily_report.stake = row[keys.get("stake")]
    account_daily_report.fixed_income = row[keys.get("fixed_income")]
    account_daily_report.net_revenue = row[keys.get("net_revenue")]
    account_daily_report.revenue_share = row[keys.get("revenue_share")]
    account_daily_report.revenue_share_cpa = abs(row[keys.get("revenue_share")])

    return account_daily_report, data_to_update


def _account_daily_report_create(
    row,
    keys,
    currency_condition,
    currency_fixed_income,
    from_date,
    account_report=None,
):
    account_daily_report = AccountDailyReport(
        account_report=account_report,
        deposit=row[keys.get("deposit")],
        stake=row[keys.get("stake")],
        fixed_income=row[keys.get("fixed_income")],
        net_revenue=row[keys.get("net_revenue")],
        revenue_share=row[keys.get("revenue_share")],
        revenue_share_cpa=abs(row[keys.get("revenue_share")]),
        is_first_deposit_count=bool(account_report.first_deposit_at) if account_report else False,
        currency_condition=currency_condition,
        currency_fixed_income=currency_fixed_income,
        created_at=from_date,
    )
    return account_daily_report


def _account_report_update(
    data_to_update,
    keys,
    row,
    from_date,
    partner_link_accumulated,
    account_report,
    cpa_by_prom_code_iter,
    revenue_share_percentage,
    currency_fixed_income,
    cpa_condition_from_revenue_share,
    fixed_income_campaign,
    account_daily_report,
):
    """
    Update account report data from row data like
    - first_deposit_at
    - revenue_share
    - prom_code

    prom_code is used to get the related link on database and sum iter
    count for easy tracker management, with revenue_share are calculated
    the net_revenue with `revenue_share_percentage` value, registered at
    must be already defined at punter data creation.
    """
    if data_to_update:
        account_report.deposit += data_to_update.get("deposit")
        account_report.stake += data_to_update.get("stake")
        account_report.fixed_income += data_to_update.get("fixed_income")
        account_report.net_revenue += data_to_update.get("net_revenue")
        account_report.revenue_share += data_to_update.get("revenue_share")
        account_report.revenue_share_cpa += abs(data_to_update.get("revenue_share"))
    else:
        account_report.deposit += row[keys.get("deposit")]
        account_report.stake += row[keys.get("stake")]
        account_report.fixed_income += row[keys.get("fixed_income")]
        account_report.net_revenue += row[keys.get("net_revenue")]
        account_report.revenue_share += row[keys.get("revenue_share")]
        account_report.revenue_share_cpa += abs(row[keys.get("revenue_share")])

    _check_revenue_percentage(row, keys, revenue_share_percentage)

    cpa_at = row[keys.get("cpa_at")]
    cpa_at = make_aware(datetime.strptime(cpa_at, "%Y-%m-%d")) if not pd.isna(cpa_at) else None
    if(not account_report.cpa_betenlace):
        account_report.partner_link_accumulated = partner_link_accumulated
        if cpa_at is not None and cpa_at < OCTOBER_2022:
            account_report.cpa_betenlace = 1
            account_report.fixed_income = 80000  # fixed_income agreement before october 2022
            account_report.cpa_partner = 0
            account_report.cpa_at = cpa_at
            account_report.partner_link_accumulated = None
            account_report.currency_fixed_income = "COP"

        # condition from revenue share and not already cpa
        elif (account_report.revenue_share >= cpa_condition_from_revenue_share and not account_report.cpa_betenlace):
            account_report.cpa_betenlace = 1
            account_report.cpa_at = from_date
            account_report.fixed_income = fixed_income_campaign
            account_report.currency_fixed_income = currency_fixed_income

            # Temp have value 1, later will removed
            account_report.cpa_partner = 0 if partner_link_accumulated is None else 1

            account_daily_report.is_cpa_betenlace = True
            account_daily_report.is_cpa_partner = True

            cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(
                (account_report, account_daily_report)
            )
        else:
            logger.debug(f"Account report id={account_report.pk} doesn't meet the cpa condition")

    elif account_report.cpa_at == from_date:
        account_report.cpa_partner = 0 if partner_link_accumulated is None else 1
        cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(
            (account_report, account_daily_report)
        )

    return account_report


def _account_report_create(
    row,
    keys,
    link,
    currency_condition,
    currency_fixed_income,
    partner_link_accumulated,
    from_date,
    cpa_by_prom_code_iter,
    revenue_share_percentage,
    cpa_condition_from_revenue_share,
    fixed_income_campaign,
    account_daily_report,
):
    """
    Create account report data from row data like
    - first_deposit_at
    - revenue_share
    - registered_at
    - prom_code

    prom_code is used to get the related link on database and sum iter
    count for easy tracker management, with revenue_share are calculated
    the net_revenue
    """
    if (not pd.isna(row[keys.get("cpa_at")])):
        cpa_at = make_aware(datetime.strptime(row[keys.get("cpa_at")], "%Y-%m-%d"))
    else:
        cpa_at = None

    if (not pd.isna(row[keys.get("registered_at")])):
        registered_at = make_aware(datetime.strptime(row[keys.get("registered_at")], "%Y-%m-%d"))
    else:
        registered_at = None

    if (not pd.isna(row[keys.get("first_deposit_at")])):
        first_deposit_at = make_aware(datetime.strptime(row[keys.get("first_deposit_at")], "%Y-%m-%d"))
    else:
        first_deposit_at = None

    cpa_count = 0
    if abs(row[keys.get("revenue_share")]) >= cpa_condition_from_revenue_share:
        cpa_count = 1
    else:
        logger.debug(f"Account report for punter {row[keys.get('punter_id')]} doesn't meet the cpa condition")

    account_report = AccountReport(
        partner_link_accumulated=partner_link_accumulated,
        punter_id=row[keys.get("punter_id")],
        deposit=row[keys.get("deposit")],
        stake=row[keys.get("stake")],
        fixed_income=0,
        net_revenue=row[keys.get("net_revenue")],
        revenue_share=row[keys.get("revenue_share")],
        revenue_share_cpa=abs(row[keys.get("revenue_share")]),
        currency_condition=currency_condition,
        currency_fixed_income=currency_fixed_income,
        cpa_betenlace=cpa_count,
        cpa_partner=(0 if partner_link_accumulated is None else cpa_count),
        link=link,
        cpa_at=cpa_at,
        registered_at=registered_at,
        first_deposit_at=first_deposit_at,
        created_at=from_date,
    )

    if cpa_at is not None and cpa_at < OCTOBER_2022:
        account_report.cpa_betenlace = 1
        account_report.fixed_income = 80000  # fixed_income agreement before october 2022
        account_report.cpa_partner = 0
        account_report.cpa_at = cpa_at
        account_report.partner_link_accumulated = None
        account_report.currency_fixed_income = "COP"

    # condition from revenue share and not already cpa
    elif (account_report.revenue_share >= cpa_condition_from_revenue_share and not account_report.cpa_betenlace):
        account_report.cpa_betenlace = 1
        account_report.cpa_at = from_date
        account_report.fixed_income = fixed_income_campaign
        account_report.currency_fixed_income = currency_fixed_income

        # Temp have value 1, later will removed
        account_report.cpa_partner = 0 if partner_link_accumulated is None else 1

        account_daily_report.is_cpa_betenlace = True
        account_daily_report.is_cpa_partner = True

        cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(
            (account_report, account_daily_report)
        )

    return account_report


def _get_tracker_values(
    keys,
    row,
    partner_link_accumulated,
):
    tracked_data = {}
    if (keys.get("deposit") is not None):
        tracked_data["deposit"] = row[keys.get("deposit")]*partner_link_accumulated.tracker_deposit

    if (keys.get("registered_count") is not None):
        if(row[keys.get("registered_count")] > 1):
            tracked_data["registered_count"] = math.floor(
                row[keys.get("registered_count")]*partner_link_accumulated.tracker_registered_count
            )
        else:
            tracked_data["registered_count"] = row[keys.get("registered_count")]

    if (keys.get("first_deposit_count") is not None):
        if(row[keys.get("first_deposit_count")] > 1):
            tracked_data["first_deposit_count"] = math.floor(
                row[keys.get("first_deposit_count")]*partner_link_accumulated.tracker_first_deposit_count
            )
        else:
            tracked_data["first_deposit_count"] = row[keys.get("first_deposit_count")]

    if (keys.get("wagering_count") is not None):
        if(row[keys.get("wagering_count")] > 1):
            tracked_data["wagering_count"] = math.floor(
                row[keys.get("wagering_count")]*partner_link_accumulated.tracker_wagering_count
            )
        else:
            tracked_data["wagering_count"] = row[keys.get("wagering_count")]

    return tracked_data


def _calc_fx(
    fx_partner,
    fx_partner_percentage,
    currency_from_str,
    partner_currency_str,
):
    if currency_from_str != partner_currency_str:
        try:
            fx_book_partner = eval(
                f"fx_partner.fx_{currency_from_str}_{partner_currency_str}"
            ) * fx_partner_percentage
        except:
            logger_msg = (
                f"Fx conversion from {currency_from_str} to {partner_currency_str} undefined on DB")
            logger.info(logger_msg)
    else:
        fx_book_partner = 1
    return fx_book_partner


def _partner_link_daily_create(
    from_date,
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
    """
    Create Member Daily report for Partner data with respective
    - fixed_income
    - fixed_income_unitary
    - fx_book_local
    - fx_percentage
    - fixed_income_unitary_local
    - cpa_count

    ### Relations data
    - betenlace_daily
    - partner_link_accumulated

    ### Currencies
    - currency_fixed_income
    - currency_local
    - tracker
    - created_at
    """
    # Calculate Adviser payment
    if partner.fixed_income_adviser_percentage is None:
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

    if partner.net_revenue_adviser_percentage is None:
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
    if partner.fixed_income_referred_percentage is None:
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

    if partner.net_revenue_referred_percentage is None:
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

        referred_by=partner.referred_by,
        fixed_income_referred_percentage=partner.fixed_income_referred_percentage,
        net_revenue_referred_percentage=partner.net_revenue_referred_percentage,

        fixed_income_referred=fixed_income_referred,
        fixed_income_referred_local=fixed_income_referred_local,
        net_revenue_referred=net_revenue_referred,
        net_revenue_referred_local=net_revenue_referred_local,

        created_at=from_date,
    )

    return partner_link_daily


def _partner_link_daily_update(
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
    partner_link_accumulated,
    partner,
    betenlace_daily,
):
    """
    Update Member Daily report for Partner data with respective
    - fixed_income
    - fixed_income_unitary
    - fx_book_local
    - fx_percentage
    - fixed_income_unitary_local
    - cpa_count
    """
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

    if partner.fixed_income_adviser_percentage is None:
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

    # Calculate referred payment
    partner_link_daily.referred_by = partner.referred_by
    partner_link_daily.fixed_income_referred_percentage = partner.fixed_income_referred_percentage
    partner_link_daily.net_revenue_referred_percentage = partner.net_revenue_referred_percentage

    if partner.fixed_income_referred_percentage is None:
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

    if partner.net_revenue_referred_percentage is None:
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


def _partner_link_month_update(
    partner_link_accumulated,
    cpa_count,
    fixed_income_partner,
    fixed_income_partner_local,
):
    """
    Update Member Current Month report for Partner data with respective
    - cpa_count
    - fixed_income_partner
    - fixed_income_partner_local
    """
    partner_link_accumulated.cpa_count += cpa_count
    partner_link_accumulated.fixed_income += fixed_income_partner
    partner_link_accumulated.fixed_income_local += fixed_income_partner_local
    return partner_link_accumulated


def _betenlace_daily_create(
    keys,
    row,
    betenlace_cpa,
    from_date,
    fixed_income_campaign,
    cpa_count,
    revenue_share_percentage,
    currency_condition,
    currency_fixed_income,
    fx_partner,
):
    """
    Create Member daily report data from row data like
    - deposit
    - first_deposit_at
    - revenue_share
    - registered_count
    - first_deposit_count
    - wagering_count

    revenue_share calculated from net_revenue
    """
    betenlace_daily = BetenlaceDailyReport(
        betenlace_cpa=betenlace_cpa,

        currency_condition=currency_condition,
        deposit=row[keys.get('deposit')],
        stake=row[keys.get('stake')],

        net_revenue=row[keys.get("net_revenue")],
        revenue_share=row[keys.get("revenue_share")],

        currency_fixed_income=currency_fixed_income,

        fixed_income=row[keys.get("fixed_income")],
        fixed_income_unitary=(
            row[keys.get("fixed_income")] / cpa_count
            if cpa_count != 0 and row[keys.get("fixed_income")]
            else
            0
        ),

        fx_partner=fx_partner,

        cpa_count=cpa_count,
        registered_count=row[keys.get('registered_count')],
        first_deposit_count=row[keys.get('first_deposit_count')],
        wagering_count=row[keys.get('wagering_count')],

        created_at=from_date,
    )

    return betenlace_daily


def _betenlace_daily_update(
    keys,
    row,
    betenlace_daily,
    fixed_income_campaign,
    cpa_count,
    revenue_share_percentage,
    fx_partner,
):
    """
    Update Member daily report data from row data like
    - deposit
    - first_deposit_at
    - revenue_share
    - registered_count
    - first_deposit_count
    - wagering_count

    revenue_share calculated from net_revenue
    """

    betenlace_daily.deposit = row[keys.get("deposit")]
    betenlace_daily.stake = row[keys.get("stake")]

    betenlace_daily.net_revenue = row[keys.get("net_revenue")]
    betenlace_daily.revenue_share = row[keys.get("revenue_share")]
    _check_revenue_percentage(row, keys, revenue_share_percentage)

    betenlace_daily.fixed_income = row[keys.get("fixed_income")]
    betenlace_daily.fixed_income_unitary = (
        row[keys.get("fixed_income")] / cpa_count
        if cpa_count != 0
        else
        0
    )

    betenlace_daily.fx_partner = fx_partner

    betenlace_daily.registered_count = row[keys.get('registered_count')]
    betenlace_daily.cpa_count = cpa_count
    betenlace_daily.first_deposit_count = row[keys.get('first_deposit_count')]
    betenlace_daily.wagering_count = row[keys.get('wagering_count')]

    return betenlace_daily


def _betenlace_month_update(
    keys,
    row,
    betenlace_cpa,
    cpa_count,
    fixed_income_campaign,
    revenue_share_percentage,
):
    """
    Update Member Current Month report data from row data like
    - deposit
    - first_deposit_at
    - revenue_share
    - registered_count
    - first_deposit_count
    - wagering_count

    revenue_share calculated from net_revenue.

    The results are sum
    """
    betenlace_cpa.deposit += row[keys.get("deposit")]
    betenlace_cpa.stake += row[keys.get("stake")]

    betenlace_cpa.fixed_income += 0

    betenlace_cpa.net_revenue += row[keys.get("net_revenue")]
    betenlace_cpa.revenue_share += row[keys.get("revenue_share")]
    _check_revenue_percentage(row, keys, revenue_share_percentage)

    betenlace_cpa.registered_count += row[keys.get('registered_count')]
    betenlace_cpa.cpa_count += cpa_count
    betenlace_cpa.first_deposit_count += row[keys.get("first_deposit_count")]
    betenlace_cpa.wagering_count += row[keys.get('wagering_count')]
    return betenlace_cpa
