import json
import math
import sys
import traceback
from io import StringIO

import numpy as np
import pandas as pd
import pytz
import requests
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerAccumStatusCHO,
)
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
from betenlace.celery import app
from celery.utils.log import get_task_logger
from core.tasks import chat_logger as chat_logger_task
from django.conf import settings
from django.db import transaction
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils import timezone
from django.utils.timezone import timedelta

logger_task = get_task_logger(__name__)


@app.task(
    ignore_result=True,
)
def member_betwinner(campaign_title):
    """
    Get data from API of bookmaker Betwinner with CSV files using 
    the pandas module with high performance, on command use tqdm for 
    progress bar.

    Member report is the summarized data from all punters of range of date.

    Betwinner retrieve data via post with X-Access-Key and X-Secret-Key on 
    headers

    CSV columns
    ---
    - prom_code : `string`
        Equivalent to raw var "SubID 1", metric var "sub1" used on Model 
        `Link` and `MemberReport (Month, daily) for betenalce and 
        partners`, this is the key that identifies a certain promotional 
        link
    - deposit : `np.float32`
        Equivalent raw var "Deposits, $", metric var "deposits_all_sum" 
        used on Models `BetenlaceCPA`, `BetenlaceDailyReport`, quantity of 
        deposited money by punters
    - stake : `np.float32`
        Equivalent to raw var "Bet, $", quantity of wagered money by 
        punters
    - registered_count : `np.uint32`
        Equivalent to raw var "Registrations", used on Models `BetenlaceCPA`, 
        `BetenlaceDailyReport`, Count of punters that are registered
    - first_deposit_count : `np.uint32`
        Equivalent to raw var "First Deposits" used on Models `BetenlaceCPA`, 
        `BetenlaceDailyReport`, count of punters that make a first deposit
    - revenue_share : `np.float32`
        Equivalent to raw var "Revenue, $", used on Models `BetenlaceCPA`, 
        `BetenlaceDailyReport`, shared money by bookmaker to betenlace.
        This value on this moment is fixed_income of betenlace only and not
        revenue share
    - cpa_count : `np.uint32`
        Quantity of cpa triggered on campaign, This value is calculated 
        based on "revenue_share" for each 35 USD is one cpa
    - wagering_count : `np.uint32`
        Equivalent to raw var "Players w/ Bet", used on Models `BetenlaceCPA`, 
        `BetenlaceDailyReport`, count of players that make a bet
    """

    # Definition of function
    today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    msg = (
        "Making call to API Account Betwinner\n"
        f"Campaign Title -> {campaign_title}\n"
        f"From date -> {yesterday_str}\n"
        f"To date -> {yesterday_str}"
    )
    logger_task.info(msg)
    msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"

    chat_logger_task.apply_async(
        kwargs={
            "msg": msg,
            "msg_url": settings.CHAT_WEBHOOK_CELERY,
        },
    )

    # Get id of Campaign Title
    filters = (
        Q(campaign_title__iexact=campaign_title),
    )
    campaign = Campaign.objects.using(DB_USER_PARTNER).annotate(
        campaign_title=Concat(
            "bookmaker__name",
            Value(" "),
            "title",
        ),
    ).filter(
        *filters,
    ).first()
    if not campaign:
        error_msg = f"Campaign with title \"{campaign_title}\" not found in DB"
        error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
        logger_task.error(error_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": error_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    # Quantity in USD for count every CPA
    revenue_for_cpa = 35

    if (campaign_title == "betwinner col"):
        betwinner_access_key = settings.API_MEMBER_REPORT_BETWINNERCOL_ACCESS_KEY
        betwinner_secret_key = settings.API_MEMBER_REPORT_BETWINNERCOL_SECRET_KEY

    if (campaign_title == "betwinner br"):
        betwinner_access_key = settings.API_MEMBER_REPORT_BETWINNERBR_ACCESS_KEY
        betwinner_secret_key = settings.API_MEMBER_REPORT_BETWINNERBR_SECRET_KEY

    if (campaign_title == "betwinner latam"):
        betwinner_access_key = settings.API_MEMBER_REPORT_BETWINNERLATAM_ACCESS_KEY
        betwinner_secret_key = settings.API_MEMBER_REPORT_BETWINNERLATAM_SECRET_KEY

    url = "https://api.betwinneraffiliates.com/affiliates/reports"
    headers = {
        "X-Access-Key": betwinner_access_key,
        "X-Secret-Key": betwinner_secret_key,
        "HTTP_ACCEPT": "application/json",
        "HTTP_CONTENT_TYPE": "application/json",
    }
    body = {
        "from": f"{yesterday_str} 00:00:00",
        "to": f"{yesterday_str} 23:59:59",
        "limit": 1,
        "offset": 0,
        "dimensions": [
            "subid1",
        ],
        "metrics": [
            "deposits_all_sum",
            "bet_new_sum",
            "revenue_sum",
            "registrations_count",
            "deposits_first_count",
            "players_with_bet_count",
        ],
        "sorting": [
            {
                "sort_by": "subid1",
                "sort_dir": "desc",
            },
        ],
        "filters": {},
        "having": {},
        "search": {},
        "players_filter": "all",
        "metrics_format": "raw",
    }

    response_obj = requests.post(url=url, json=body, headers=headers)

    try:
        response = json.loads(response_obj.text)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API, check if current "
            "connection IP/VPN is on Whitelist of API server,\n\n"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"request headers: {headers}\n"
            f"response status:\n{response_obj.status_code}\n"
            f"response text:\n{response_obj.text}\n\n"
            f"if problem still check traceback:\n{''.join(e)}"
        )
        error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
        logger_task.error(error_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": error_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    if not response.get("success"):
        error_msg = f"report not success with code: {response.get('code')}, message: {response.get('message')}"
        error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
        logger_task.error(error_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": error_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    try:
        response_obj = requests.get(url=response.get("misc").get("export_urls").get("csv"))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )

        error_msg = (
            f"Something is wrong at get data from API csv exported"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"request headers: {headers}\n\n"
            f"response dict: {response}\n\n"
            f"if problem persist check traceback:\n\n{''.join(e)}"
        )
        error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
        logger_task.error(error_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": error_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    # Case csv empty, no records
    if(not response_obj.text):
        msg_warning = (
            "Data not found at requested url"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"request headers: {headers}\n\n"
            f"response text: {response_obj.text}\n\n"
        )
        msg_warning = f"*LEVEL:* `WARNING` \n*message:* `{msg_warning}`\n\n"
        logger_task.warning(msg_warning)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg_warning,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    try:
        # set the characters and line based interface to stream I/O
        data_io = StringIO(response_obj.text)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )

        error_msg = (
            "Something is wrong at get data from API, check the credentials"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"request headers: {headers}\n\n"
            f"response text: {response_obj.text}\n\n"
            " if problem persist check traceback:"
            f"\n\n{''.join(e)}"
        )
        logger_task.error(error_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": error_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    # Create the DataFrame
    cols_to_use = [
        "SubID 1",
        "Deposits, $",
        "Bets, $",
        "Revenue, $",
        "Registrations",
        "First Deposits",
        "Players w/ Bet",
    ]
    df = pd.read_csv(
        filepath_or_buffer=data_io,
        sep=",",
        usecols=cols_to_use,
        dtype={
            "SubID 1": "string",
            "Deposits, $": np.float32,
            "Bets, $": np.float32,
            "Revenue, $": np.float32,
            "Registrations": np.uint32,
            "First Deposits": np.uint32,
            "Players w/ Bet": np.uint32,
        },
    )[cols_to_use]

    df.rename(
        inplace=True,
        columns={
            "SubID 1": "prom_code",
            "Deposits, $": "deposit",
            "Bets, $": "stake",
            "Revenue, $": "revenue_share",
            "Registrations": "registered_count",
            "First Deposits": "first_deposit_count",
            "Players w/ Bet": "wagering_count",
        },
    )

    # "SubID 1","Impressions","Visits","Registrations","First Deposits",
    # "Deposits","First Deposits, $","Deposits, $","Withdrawals,
    # $","Players w/ Bet","Profit, $","Chargebacks, $","Commissions, $",
    # "Revenue, $", "Bets, $"

    # Get related link from prom_codes and campaign, QUERY
    filters = (
        Q(prom_code__in=df.prom_code.unique()),
        Q(campaign_id=campaign.id),
    )
    links = Link.objects.filter(
        *filters,
    ).select_related(
        "partner_link_accumulated",
        "partner_link_accumulated__partner",
        "betenlacecpa",
    )

    betenlacecpas_pk = links.values_list("betenlacecpa__pk", flat=True)

    filters = (
        Q(betenlace_cpa__pk__in=betenlacecpas_pk),
        Q(created_at=yesterday.date()),
    )
    betenlace_daily_reports = BetenlaceDailyReport.objects.filter(*filters)

    filters = (
        Q(betenlace_daily_report__in=betenlace_daily_reports),
    )
    partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(*filters)

    # Get the last Fx value
    fx_created_at = yesterday.replace(minute=0, hour=0, second=0, microsecond=0)
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

    # If still none prevent execution
    if(fx_partner is None):
        error_msg = "Undefined fx_partner on DB"
        error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
        logger_task.error(error_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": error_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    filters = (
        Q(updated_at__lte=yesterday+timedelta(days=1)),
    )
    fx_partner_percentage = FxPartnerPercentage.objects.filter(*filters).order_by("-updated_at").first()

    if(fx_partner_percentage is None):
        # Get just next from supplied date
        filters = (
            Q(updated_at__gte=yesterday+timedelta(days=1)),
        )
        fx_partner_percentage = FxPartnerPercentage.objects.filter(*filters).order_by("updated_at").first()

    if(fx_partner_percentage is None):
        msg_warning = "Undefined fx_partner on DB, using default 95%"
        msg_warning = f"*LEVEL:* `WARNING` \n*message:* `{msg_warning}`\n\n"
        logger_task.warning(msg_warning)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg_warning,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        fx_partner_percentage = 0.95
    else:
        fx_partner_percentage = fx_partner_percentage.percentage_fx

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

    list_logs = []
    for row in zip(*df.to_dict('list').values()):
        """
        prom_code
        deposit
        stake
        revenue_share
        registered_count
        first_deposit_count
        wagering_count
        """
        # Get link according to prom_code of current loop
        link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)
        if not link:
            msg_warning = (
                f"Link with prom_code=\"{row[keys.get('prom_code')]}\" and campaign=\"{campaign_title}\" not "
                "found on database"
            )
            msg_warning = f"*LEVEL:* `WARNING` \n*message:* `{msg_warning}`\n\n"
            logger_task.warning(msg_warning)
            list_logs.append(msg_warning)
            continue

        try:
            # Get current entry of member report based on link (prom_code)
            betenlace_cpa = link.betenlacecpa
        except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
            msg_error = f"Betenlace CPA entry not found for link with prom_code={row[keys.get('prom_code')]}"
            msg_error = f"*LEVEL:* `ERROR` \n*message:* `{msg_error}`\n\n"
            logger_task.error(msg_error)
            list_logs.append(msg_error)
            continue

        # Create cpa_count
        cpa_count = int(row[keys.get('revenue_share')] / revenue_for_cpa)

        # Betenlace Month
        betenlace_cpa = _betenlace_month_update(
            keys=keys,
            row=row,
            betenlace_cpa=betenlace_cpa,
            cpa_count=cpa_count,
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

        if(betenlace_daily):
            betenlace_daily = _betenlace_daily_update(
                keys=keys,
                row=row,
                betenlace_daily=betenlace_daily,
                campaign=campaign,
                cpa_count=cpa_count,
                fx_partner=fx_partner,
            )
            member_reports_daily_betenlace_update.append(betenlace_daily)
        else:
            betenlace_daily = _betenlace_daily_create(
                from_date=yesterday.date(),
                keys=keys,
                row=row,
                betenlace_cpa=betenlace_cpa,
                campaign=campaign,
                cpa_count=cpa_count,
                fx_partner=fx_partner,
            )
            member_reports_daily_betenlace_create.append(betenlace_daily)

        # Partner Month
        partner_link_accumulated = link.partner_link_accumulated

        # When partner have not assigned the link must be continue to next loop
        if(partner_link_accumulated is None):
            continue

        # Validate if link has relationship with partner and if has verify if status is equal to status campaign
        if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
            # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
            if (campaign.status == Campaign.Status.INACTIVE and yesterday.date() >= campaign.last_inactive_at.date()):
                msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
                logger_task.warning(msg)
                list_logs.append(msg)
                continue
        elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
            msg = f"link with prom_code {partner_link_accumulated.prom_code} has custom status inactive"
            msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
            logger_task.warning(msg)
            list_logs.append(msg)
            continue

        # Tracker
        if(cpa_count > settings.MIN_CPA_TRACKER_DAY):
            cpa_count = math.floor(cpa_count*partner_link_accumulated.tracker)
        else:
            cpa_count = cpa_count

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

        fixed_income_partner_unitary = campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa
        fixed_income_partner = cpa_count * fixed_income_partner_unitary
        fixed_income_partner_unitary_local = (
            campaign.fixed_income_unitary *
            partner_link_accumulated.percentage_cpa *
            fx_fixed_income_partner
        )
        fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

        # Fx Currency Condition
        fx_condition_partner = _calc_fx(
            fx_partner=fx_partner,
            fx_partner_percentage=fx_partner_percentage,
            currency_from_str=currency_condition_str,
            partner_currency_str=partner_currency_str,
        )

        partner_link_accumulated = _partner_link_month_update(
            partner_link_accumulated=partner_link_accumulated,
            cpa_count=cpa_count,
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

        if(partner_link_daily):
            partner_link_daily = _partner_link_daily_update(
                cpa_count=cpa_count,
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

    join_list = "".join(list_logs)
    chat_logger_task.apply_async(
        kwargs={
            "msg": join_list,
            "msg_url": settings.CHAT_WEBHOOK_CELERY,
        },
    )

    with transaction.atomic(using=DB_USER_PARTNER):
        if(member_reports_betenlace_month_update):
            BetenlaceCPA.objects.bulk_update(
                objs=member_reports_betenlace_month_update,
                fields=(
                    "deposit",
                    "stake",
                    "fixed_income",
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
                    "deposit",
                    "registered_count",
                    "first_deposit_count",
                    "wagering_count",
                    "tracker",
                    "tracker_deposit",
                    "tracker_registered_count",
                    "tracker_first_deposit_count",
                    "tracker_wagering_count",
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

        if(member_reports_daily_partner_create):
            PartnerLinkDailyReport.objects.bulk_create(
                objs=member_reports_daily_partner_create,
            )

    if (len(df.index) == 0):
        msg = f"Member for Campaign {campaign_title} No Records/No data"
        msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
        logger_task.warning(msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
    else:
        msg = f"Member for Campaign {campaign_title} processed count {len(df.index)}"
        msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
        logger_task.warning(msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
    return


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
    if(currency_from_str != partner_currency_str):
        try:
            fx_book_partner = eval(
                f"fx_partner.fx_{currency_from_str}_{partner_currency_str}") * fx_partner_percentage
        except:
            logger_task.error(
                f"Fx conversion from {currency_from_str} to {partner_currency_str} undefined on DB")
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

        # referred base data
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

    partner_link_daily.fixed_income = fixed_income_partner
    partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary

    partner_link_daily.fx_book_local = fx_fixed_income_partner
    partner_link_daily.fx_book_net_revenue_local = fx_condition_partner
    partner_link_daily.fx_percentage = fx_partner_percentage

    partner_link_daily.fixed_income_local = fixed_income_partner_local
    partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

    partner_link_daily.cpa_count = cpa_count
    partner_link_daily.percentage_cpa = partner_link_accumulated.percentage_cpa

    partner_link_daily.deposit = tracked_data.get("deposit")
    partner_link_daily.registered_count = tracked_data.get("registered_count")
    partner_link_daily.first_deposit_count = tracked_data.get("first_deposit_count")
    partner_link_daily.wagering_count = tracked_data.get("wagering_count")

    partner_link_daily.tracker = partner_link_accumulated.tracker
    partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
    partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
    partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
    partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

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
            betenlace_daily.net_revenue * partner.net_revenue_adviser_percentage
            if betenlace_daily.net_revenue is not None
            else
            0
        )
        partner_link_daily.net_revenue_adviser_local = (
            partner_link_daily.net_revenue_adviser *
            fx_condition_partner
        )

        # Calculate referred payment
    partner_link_daily.referred_by = partner.referred_by
    partner_link_daily.fixed_income_referred_percentage = partner.fixed_income_referred_percentage
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


def _partner_link_month_update(
    partner_link_accumulated,
    cpa_count,
    fixed_income_partner,
    fixed_income_partner_local,
):
    partner_link_accumulated.cpa_count += cpa_count
    partner_link_accumulated.fixed_income += fixed_income_partner
    partner_link_accumulated.fixed_income_local += fixed_income_partner_local

    return partner_link_accumulated


def _betenlace_daily_create(
    from_date,
    keys,
    row,
    betenlace_cpa,
    campaign,
    cpa_count,
    fx_partner,
):
    betenlace_daily = BetenlaceDailyReport(
        betenlace_cpa=betenlace_cpa,

        currency_condition=campaign.currency_condition,
        deposit=row[keys.get('deposit')],
        stake=row[keys.get('stake')],

        revenue_share=0,

        currency_fixed_income=campaign.currency_fixed_income,
        # Temporaly fixed_income for betenlace is same to revenue share
        fixed_income=row[keys.get('revenue_share')],
        fixed_income_unitary=(
            row[keys.get('revenue_share')] / cpa_count
            if cpa_count != 0
            else
            campaign.fixed_income_unitary
        ),

        fx_partner=fx_partner,

        registered_count=row[keys.get('registered_count')],
        cpa_count=cpa_count,
        first_deposit_count=row[keys.get('first_deposit_count')],
        wagering_count=row[keys.get('wagering_count')],
        created_at=from_date,
    )

    return betenlace_daily


def _betenlace_daily_update(
    keys,
    row,
    betenlace_daily,
    campaign,
    cpa_count,
    fx_partner,
):
    betenlace_daily.deposit = row[keys.get('deposit')]
    betenlace_daily.stake = row[keys.get('stake')]

    # Temporaly fixed_income for betenlace is same to revenue share
    betenlace_daily.fixed_income = row[keys.get('revenue_share')]
    betenlace_daily.fixed_income_unitary = (
        row[keys.get('revenue_share')] / cpa_count
        if cpa_count != 0
        else
        campaign.fixed_income_unitary
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
):
    betenlace_cpa.deposit += row[keys.get('deposit')]
    betenlace_cpa.stake += row[keys.get('stake')]

    betenlace_cpa.revenue_share += 0
    betenlace_cpa.registered_count += row[keys.get('registered_count')]
    betenlace_cpa.cpa_count += cpa_count
    betenlace_cpa.first_deposit_count += row[keys.get('first_deposit_count')]
    betenlace_cpa.wagering_count += row[keys.get('wagering_count')]

    # Temporaly fixed_income for betenlace is same to revenue share
    betenlace_cpa.fixed_income += row[keys.get('revenue_share')]

    return betenlace_cpa
