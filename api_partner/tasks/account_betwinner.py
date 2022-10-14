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
    AccountReport,
    Campaign,
    Link,
    PartnerLinkAccumulated,
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
def account_betwinner(campaign_title):
    """
    Get data from API of bookmaker BetWinner with CSV files using 
    the pandas module with high performance, on command use tqdm for 
    progress bar.

    Account report is the actions of every punter on range of date.

    Betwinner retrieve data via post with X-Access-Key and X-Secret-Key on 
    headers, this is a property affiliates

    CSV columns
    ---
    - prom_code : `string`
        Equivalent to raw var "SubID 1", used on Model `Link` and
        `MemberReport (Month, daily) for betenalce and partners`, this is
        the key that identifies a certain promotional link
    - deposit : `np.float32`
        Equivalent raw var "Deposits, $", used on Models `AccountReport`, 
        quantity of deposited money by punter
    - stake : `np.float32`
        Equivalent to raw var "Bet, $", quantity of wagered money by 
        punter
    - registered_at : `np.uint32`
        Equivalent to raw var "Registrations", used on Models 
        `AccountReport`, if value is 1 is that day of report when this user 
        was registered in case 0 not registered the determined day, other 
        is error
    - first_deposit_at : `np.uint32`
        Equivalent to raw var "First Deposits" used on Models `AccountReport`, 
        if value is 1 is that day of report when this user was made a 
        first deposit in case 0 not first deposit the determined day, other
        case is error
    - revenue_share : `np.float32`
        Equivalent to raw var "Revenue, $", used on Models `AccountReport`, 
        shared money by bookmaker to betenlace, 
        actually that value is fixed_income and not revenue_share
    """
    def _calc_tracker(keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum,
                      cpa_by_prom_code_iter, cpa_count):
        """
        Calc cpa's according to Tracker value, tracker have values beetwhen 0 
        to 1.0 if partner have lesser to 1 apply tracker only if total of 
        cpa_count is higher than MIN_CPA_TRACKER_DAY
        """
        if(partner_link_accumulated.tracker < 1 and
            cpa_by_prom_code_sum.get(row[keys.get("prom_code")]) > settings.MIN_CPA_TRACKER_DAY
           ):
            tracker_cpa = math.floor(
                partner_link_accumulated.tracker*cpa_by_prom_code_sum.get(row[keys.get("prom_code")]))

            # if current_counted_cpa for partner is lesser than
            # tracker_cpa count for partner
            if(cpa_by_prom_code_iter.get(row[keys.get("prom_code")]) < tracker_cpa):
                account_report.cpa_partner = cpa_count
                cpa_by_prom_code_iter[row[keys.get("prom_code")]] += 1
        else:
            account_report.cpa_partner = cpa_count

        return account_report

    def _account_report_update(
            keys, row, from_date, first_deposit_at, registered_at, account_report, partner_link_accumulated,
            cpa_by_prom_code_sum, cpa_by_prom_code_iter, cpa_count):
        account_report.deposit += row[keys.get("deposit")]
        account_report.stake += row[keys.get("stake")]

        if registered_at:
            account_report.registered_at = registered_at

        if first_deposit_at:
            account_report.first_deposit_at = first_deposit_at

        if account_report.cpa_betenlace != 1:
            account_report.cpa_betenlace = cpa_count
            if cpa_count:
                # Case when cpa is True or 1
                account_report.cpa_at = from_date.date()
                account_report.fixed_income = row[keys.get("revenue_share")]
                account_report.partner_link_accumulated = partner_link_accumulated

                if(partner_link_accumulated):
                    account_report = _calc_tracker(
                        keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum,
                        cpa_by_prom_code_iter, cpa_count)
        return account_report

    def _account_report_create(
            row, keys, link, campaign, registered_at, first_deposit_at, partner_link_accumulated, from_date,
            cpa_by_prom_code_sum, cpa_by_prom_code_iter, cpa_count):
        account_report = AccountReport(
            partner_link_accumulated=partner_link_accumulated,
            punter_id=row[keys.get("punter_id")],
            deposit=row[keys.get("deposit")],
            stake=row[keys.get("stake")],

            fixed_income=row[keys.get("revenue_share")],

            currency_condition=campaign.currency_condition,
            currency_fixed_income=campaign.currency_fixed_income,

            cpa_betenlace=cpa_count,
            first_deposit_at=first_deposit_at,
            link=link,
            registered_at=registered_at,
            created_at=from_date,
        )

        if cpa_count:
            # Case when cpa is True or 1
            account_report.cpa_at = from_date
            if partner_link_accumulated:
                account_report = _calc_tracker(
                    keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum, cpa_by_prom_code_iter,
                    cpa_count)
        return account_report

    # Definition of function
    today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    msg = (
        "Making call to API Account BetWinner\n"
        f"Campaign Title -> {campaign_title}\n"
        f"From date -> {yesterday_str}\n"
        f"To date -> {yesterday_str}"
    )
    logger_task.info(msg)
    chat_logger_task.apply_async(
        kwargs={
            "msg": msg,
            "msg_url": settings.CHAT_WEBHOOK_CELERY,
        },
    )

    # Get id of Campaign Title
    filters = [Q(campaign_title__iexact=campaign_title)]
    campaign = Campaign.objects.using(DB_USER_PARTNER).annotate(
        campaign_title=Concat('bookmaker__name', Value(' '), 'title')).filter(*filters).first()
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
            "site_player_id",
        ],
        "metrics": [
            "deposits_all_sum",
            "bet_new_sum",
            "registrations_count",
            "deposits_first_count",
            "revenue_sum",
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

    response_obj = requests.post(url, json=body, headers=headers)

    try:
        response = json.loads(response_obj.text)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        error_msg = (
            "Something is wrong at get data from API, check if current "
            "connection IP/VPN is on Whitelist of API server,\n\n"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"request headers: {headers}\n"
            f"response status:\n{response_obj.status_code}\n"
            f"response text:\n{response_obj.text}\n\n"
            f"if problem still check traceback:\n{''.join((e))}"
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
        response_obj = requests.get(response.get("misc").get("export_urls").get("csv"))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(exc_type, exc_value, exc_traceback)

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
        warning_msg = (
            "Data not found at requested url"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"request headers: {headers}\n\n"
            f"response text: {response_obj.text}\n\n"
        )
        warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
        logger_task.warning(warning_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": warning_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    try:
        # set the characters and line based interface to stream I/O
        data_io = StringIO(response_obj.text)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        error_msg = (
            "Something is wrong at get data from API, check the credentials"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"request headers: {headers}\n\n"
            f"response text: {response_obj.text}\n\n"
            " if problem persist check traceback:"
            f"\n\n{''.join(e)}"
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

    # Create the DataFrame
    cols_to_use = [
        "SubID 1",
        "Player ID",
        "Deposits, $",
        "Bets, $",
        "Revenue, $",
        "Registrations",
        "First Deposits",
    ]
    df = pd.read_csv(data_io, sep=",",
                     usecols=cols_to_use, dtype={
                         "SubID 1": "string",
                         "Player ID": "string",
                         "Deposits, $": np.float32,
                         "Bets, $": np.float32,
                         "Revenue, $": np.float32,
                         "Registrations": np.uint32,
                         "First Deposits": np.uint32,
                     }
                     )[cols_to_use]

    df.rename(inplace=True,
              columns={
                  "SubID 1": "prom_code",
                  "Player ID": "punter_id",
                  "Deposits, $": "deposit",
                  "Bets, $": "stake",
                  "Revenue, $": "revenue_share",
                  "Registrations": "registered_count",
                  "First Deposits": "first_deposit_count",
              }
              )

    # "SubID 1","Player ID","Impressions","Visits","Registrations",
    # "First Deposits","Deposits","First Deposits, $","Deposits, $",
    # "Withdrawals,$","Players w/ Bet","Profit, $","Chargebacks, $",
    # "Commissions, $","Revenue, $"

    # Filter data - Override in same place of memory data about non
    # authenticated punters
    df.drop(df[df.eval("(punter_id == 'Not Registered')", engine='numexpr')].index, inplace=True)

    # Get related link from prom_codes and campaign, QUERY
    filters = [Q(prom_code__in=df.prom_code.unique()), Q(campaign_id=campaign.id)]
    links = Link.objects.filter(*filters).select_related("partner_link_accumulated")

    # Get account reports from previous links and punter_id, QUERY
    filters = [Q(link__in=links.values_list("pk", flat=True)), Q(punter_id__in=df.punter_id.unique())]
    account_reports = AccountReport.objects.filter(*filters)

    # Acumulators bulk create and update
    account_reports_update = []
    account_reports_create = []

    # Set keys by index based on colum names of Dataframe
    keys = {key: index for index, key in enumerate(df.columns.values)}

    # Dictionary with sum of cpa's by prom_code
    cpa_by_prom_code_sum = {}

    # Dictionary with current applied sum of cpa's by prom_code
    cpa_by_prom_code_iter = {}
    for prom_code in df.prom_code.unique():
        cpa_by_prom_code_sum[prom_code] = int(
            df.loc[df.prom_code.values == prom_code, "revenue_share"].sum()/revenue_for_cpa)
        cpa_by_prom_code_iter[prom_code] = 0

    list_logs = []
    for row in zip(*df.to_dict('list').values()):
        """
        prom_code
        punter_id
        deposit
        stake
        revenue_share
        registered_count
        first_deposit_count
        """
        cpa_count = int(row[keys.get("revenue_share")]/revenue_for_cpa)

        if(cpa_count > 1):
            # Prevent a cpacommissioncount bad value
            error_msg = (
                f"cpa_count is greather than one! punter {row[keys.get('punter_id')]}, campaign {campaign_title}, "
                f"revenue share based {row[keys.get('revenue_share')]}"
            )
            error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
            logger_task.error(error_msg)
            list_logs.append(error_msg)
            return

        link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)

        if not link:
            warning_msg = (
                f"Link with prom_code={row[keys.get('prom_code')]} and campaign={campaign_title}"
            )
            warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
            logger_task.warning(warning_msg)
            list_logs.append(warning_msg)
            continue

        # Check registrationdate null registered_count
        if (row[keys.get("registered_count")] == 1):
            registered_at = yesterday.date()
        elif(row[keys.get("registered_count")] > 1):
            warning_msg = (
                f"registered_count have value greather than 1 for account report with campaign name "
                f"{campaign_title}, prom_code {row[keys.get('prom_code')]}, punter_id {row[keys.get('punter_id')]} "
                f"registered count {row[keys.get('registered_count')]}"
            )
            warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
            logger_task.warning(warning_msg)
            list_logs.append(warning_msg)
            registered_at = None
        else:
            registered_at = None

        # Check registrationdate null
        if (row[keys.get("first_deposit_count")] == 1):
            first_deposit_at = yesterday.date()
        elif (row[keys.get("first_deposit_count")] > 1):
            warning_msg = (
                f"first_deposit_count have value greather than 1 for account report with campaign name "
                f"{campaign_title}, prom_code {row[keys.get('prom_code')]}, punter_id {row[keys.get('punter_id')]} "
                f"first deposit count {row[keys.get('first_deposit_count')]}"
            )
            warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
            logger_task.warning(warning_msg)
            list_logs.append(warning_msg)
            first_deposit_at = None
        else:
            first_deposit_at = None

        # Get current entry of account report based on link and punter_id
        account_report = next(
            filter(
                lambda account_report: account_report.link_id == link.pk and
                account_report.punter_id == row[keys.get("punter_id")], account_reports
            ),
            None
        )

        # Get current partner that have the current link
        partner_link_accumulated = link.partner_link_accumulated
        if partner_link_accumulated:
            # Validate if link has relationship with partner and if has verify if status is equal to status campaign
            if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                if(campaign.status == Campaign.Status.INACTIVE) and (yesterday.date() >= campaign.last_inactive_at.date()):
                    msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                    msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
                    logger_task.warning(msg)
                    list_logs.append(msg)
                    partner_link_accumulated = None
            elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                msg = f"link with prom_code {partner_link_accumulated.prom_code} has custom status inactive"
                msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
                logger_task.warning(msg)
                list_logs.append(msg)
                partner_link_accumulated = None

        if account_report:
            # Case and exist entry
            # Fixed income is according to cpa
            if(cpa_count == 1 and account_report.cpa_betenlace):
                warning_msg = (
                    f"cpa_commissioncount for punter {row[keys.get('punter_id')]} on campaign {campaign_title} "
                    "is already with value 1, something is wrong with data"
                )
                warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
                logger_task.warning(warning_msg)
                list_logs.append(warning_msg)
                continue
            account_report_update = _account_report_update(
                keys, row, yesterday, first_deposit_at, registered_at, account_report, partner_link_accumulated,
                cpa_by_prom_code_sum, cpa_by_prom_code_iter, cpa_count)
            account_reports_update.append(account_report_update)
        else:
            # Case new entry
            account_report_new = _account_report_create(
                row, keys, link, campaign, registered_at, first_deposit_at, partner_link_accumulated, yesterday,
                cpa_by_prom_code_sum, cpa_by_prom_code_iter, cpa_count)

            account_reports_create.append(account_report_new)

    join_list = "".join(list_logs)
    chat_logger_task.apply_async(
        kwargs={
            "msg": join_list,
            "msg_url": settings.CHAT_WEBHOOK_CELERY,
        },
    )
    with transaction.atomic(using=DB_USER_PARTNER):
        if(account_reports_create):
            AccountReport.objects.bulk_create(account_reports_create)
        if(account_reports_update):
            AccountReport.objects.bulk_update(account_reports_update, (
                "partner_link_accumulated",
                "deposit",
                "stake",
                "fixed_income",
                "cpa_betenlace",
                "cpa_partner",
                "registered_at",
                "first_deposit_at",
                "cpa_at",
            ))

    return
