import math
import random
import sys
import time
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
    BetenlaceCPA,
    BetenlaceDailyReport,
    Campaign,
    FxPartner,
    Link,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from betenlace.celery import app
from celery.utils.log import get_task_logger
from core.tasks import chat_logger as chat_logger_task
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.db import transaction
from django.db.models import (
    F,
    Q,
    Sum,
    Value,
)
from django.db.models.functions import (
    Coalesce,
    Concat,
)
from django.utils import timezone
from django.utils.timezone import timedelta

logger_task = get_task_logger(__name__)


@app.task(
    ignore_result=True,
)
def account_member_betwarrior(
    campaign_title,
    update_account=True,
    update_cpa=True,
):
    """
    Get data from API of bookmaker Betwarrior with data in JSON format
    the pandas module with high performance

    Member report is the summarized data from all punters of range of date.

    This data is retrieved with CURL using API keys
    # Known vars for Inlazz
    - prom_code : `string`
        Equivalent to raw var "SubID 1", metric var "sub1" used on Model 
        `Link` and `MemberReport (Month, daily) for betenalce and 
        partners`, this is the key that identifies a certain promotional 
        link
    - deposit : `np.float32`
        Equivalent to raw var "Deposits" acquired from Customer report of 
        Bookmaker. Quantity of deposited money by punters
    - stake : `np.float32`
        Equivalent to raw var "Turnover" acquired from Customer report of 
        Bookmaker. Quantity of wagered money by 
        punters
    - registered_count : `np.uint32`
        Equivalent to raw var "Signups" acquired from Member daily report
        of Bookmaker. Count of punters that are registered
    - first_deposit_count : `np.uint32`
        Equivalent to raw var "First Deposits" acquired from Member daily report
        of Bookmaker. Count of punters that make a first deposit
    - revenue_share : `np.float32`
        This var is not supplied by bookmaker, is calculated from net_revenue. 
        shared money by bookmaker to betenlace.
        This value on this moment is fixed_income of betenlace only and not
        revenue share
    - cpa_count : `np.uint32`
        Equivalent to raw var `CPA Processed`, acquired from Member daily report
        of Bookmaker.
    """

    # Definition of function
    today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    msg = (
        "Making call to API Betwarrior\n"
        f"Campaign Title -> {campaign_title}\n"
        f"From date -> {yesterday_str}\n"
        f"To date -> {yesterday_str}\n"
        f"update account -> {update_account}\n"
        f"update cpa -> {update_cpa}"
    )
    logger_task.info(msg)
    chat_logger_task.apply_async(
        kwargs={
            "msg": msg,
            "msg_url": settings.CHAT_WEBHOOK_CELERY,
        },
    )
    max_retries = 5

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

    if (campaign_title == "betwarrior latam"):
        api_key = settings.API_ACC_MEM_REPORT_BETWARRIOR_LATAM_KEY
        revenue_share_percentage = settings.API_ACC_MEM_REPORT_BETWARRIOR_LATAM_RS_PERCENTAGE
    elif(campaign_title == "betwarrior br"):
        api_key = settings.API_ACC_MEM_REPORT_BETWARRIOR_BR_KEY
        revenue_share_percentage = settings.API_ACC_MEM_REPORT_BETWARRIOR_BR_RS_PERCENTAGE
    elif(campaign_title == "betwarrior mex"):
        api_key = settings.API_ACC_MEM_REPORT_BETWARRIOR_MEX_KEY
        revenue_share_percentage = settings.API_ACC_MEM_REPORT_BETWARRIOR_MEX_RS_PERCENTAGE
    else:
        error_msg = f"Campaign with title {campaign_title} not defined on account member betwarrior"
        logger_task.error(error_msg)
        return

    # Get date for cpa
    date_cpa_str = (yesterday + relativedelta(day=1)).strftime("%Y-%m-%d")

    filter_not_data = "[{\"column\":\"No Data was Found\"}]"

    # Dataframe values
    cols_to_use = [
        "Customer Reference ID",
        "Marketing Source name",
        "Signup Date",
        "Deposits",
        "Turnover",
        "Total Net Revenue",
        "CPA Processed Date",
    ]

    # Excluded vars on Dataframe for Customer report
    dtype_exclude = {
        "Alias": bool,
        "AffiliateID": bool,
        "Country": bool,
        "Reward Plan": bool,
        "Marketing Source ID": bool,
        "URL": bool,
        "Expiry Date": bool,
        "Customer Type": bool,
        "Customer Level": bool,
        "Brand": bool,
        "Customer Signup Source": bool,
        "Gross Revenue": bool,
        "Contributions": bool,
        "Transactions": bool,
        "Points": bool,
        "Adj (Chargebacks)": bool,
        "Total Net Revenue MTD": bool,
    }

    # Punters by activity
    url = (
        "https://betwarrior.dataexport.netrefer.com/v2/export/reports/affiliate/XML_CustomerReporting_InclSubAff?"
        f"authorization={api_key}&playerID=all&username=all&websiteID=all&productID=all&brandID=all&"
        "customersource=all&customerTypeID=all&rewardplanID=all&countryID=all&FilterBySignUpDate=0&"
        f"FilterBySignUpDateFrom={yesterday_str}&FilterBySignUpDateTo={yesterday_str}&FilterByExpirationDate=0&"
        f"FilterByExpirationDateFrom=2016-10-01&FilterByExpirationDateTo=2016-10-31&FilterByActivityDate=1&"
        f"FilterByActivityDateFrom={yesterday_str}&FilterByActivityDateTo={yesterday_str}"
    )

    try:
        for i in range(max_retries):
            response_obj = requests.get(url)
            if (response_obj.status_code == 200):
                break
            else:
                error_msg = (
                    f"Response for Punters by activity reach status {response_obj.status_code}, retrying..."
                )
                error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
                logger_task.error(error_msg)
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": error_msg,
                        "msg_url": settings.CHAT_WEBHOOK_CELERY,
                    },
                )
                time.sleep(random.randint(1, 3))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API for punters actvity filter, check if current "
            "connection IP/VPN is on Whitelist of API server."
            f"url: {url}\n\n"
            f"if problem still check traceback:\n\n{''.join(e)}"
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

    if (response_obj.status_code != 200):
        error_msg = (
            f"Response for Punters by activity reach status {response_obj.status_code}, check "
            "credentials, connection IP/VPN is on Whitelist of API server or server status.\n"
            f"url: {url}"
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

    found_data = True
    if (response_obj.text == filter_not_data):
        warning_msg = (
            f"Not found data by punters activity\n"
            f"url: {url}\n"
            f"Response text:\n{response_obj.text}\n"
            f"Response status:\n{response_obj.status_code}"
        )
        warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
        logger_task.warning(warning_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": warning_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        found_data = False

    # Initialize dataframe for punters filtered by activity date
    df_act = pd.DataFrame()
    if (found_data):
        # Stream I/O
        data_io = StringIO(response_obj.text)
        try:
            # Create the DataFrame
            df_act = pd.read_json(
                path_or_buf=data_io,
                orient="records",
                convert_dates=False,
                dtype={
                    "Customer Reference ID": "string",
                    "Marketing Source name": "string",
                    "Signup Date": "string",
                    "Deposits": np.float32,
                    "Turnover": np.float32,
                    "Total Net Revenue": np.float32,
                    "CPA Processed Date": "string",
                    # Others values that will ignored, set value with
                    # data type of smallest size
                    **dtype_exclude,
                }
            )
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            error_msg = (
                "Something is wrong at get data from API by punters activity, check the Authorization KEY "
                "and whitelist on server API .\n"
                f"url: {url}\n"
                f"Response text:\n{response_obj.text}\n"
                f"Response statis:\n{response_obj.status_code}\n\n"
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

        df_act = df_act[cols_to_use]
        df_act.rename(
            inplace=True,
            columns={
                "Customer Reference ID": "punter_id",
                "Marketing Source name": "prom_code",
                "Signup Date": "registered_at",
                "Deposits": "deposit",
                "Turnover": "stake",
                "Total Net Revenue": "net_revenue",
                "CPA Processed Date": "cpa_at",
            },
        )

    # Member report (Daily report) first day - CPA only
    url = (
        "https://betwarrior.dataexport.netrefer.com/v2/export/reports/affiliate/XML_MS_DailyFigures_InclSubAff?"
        f"authorization={api_key}&yearmonthdayfrom={date_cpa_str}&yearmonthdayto={date_cpa_str}"
        "&productID=all&PublishPointID=all"
    )

    try:
        for i in range(max_retries):
            response_obj = requests.get(url)
            if (response_obj.status_code == 200):
                break
            else:
                error_msg = (
                    f"Response for Daily report CPA only reach status {response_obj.status_code}, retrying..."
                )
                error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
                logger_task.error(error_msg)
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": error_msg,
                        "msg_url": settings.CHAT_WEBHOOK_CELERY,
                    },
                )
                time.sleep(random.randint(1, 3))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API for Daily report CPA only, check the Authorization KEY "
            "and whitelist on server API .\n"
            f"url: {url}\n"
            f"Response text:\n{response_obj.text}\n"
            f"Response statis:\n{response_obj.status_code}\n\n"
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

    if (response_obj.status_code != 200):
        error_msg = (
            f"Response for Daily report CPA only reach status {response_obj.status_code}, check "
            "credentials, connection IP/VPN is on Whitelist of API server or server status.\n"
            f"url: {url}"
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

    found_data = True
    if (response_obj.text == filter_not_data):
        warning_msg = (
            f"Not found data on Daily report CPA only\n"
            f"url: {url}\n"
            f"Response text:\n{response_obj.text}\n"
            f"Response status:\n{response_obj.status_code}"
        )
        warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
        logger_task.warning(warning_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": warning_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        found_data = False

    # Manage dataframe for Daily report cpa only
    if(found_data):
        # Stream I/O
        data_io = StringIO(response_obj.text)
        try:
            # Create the DataFrame
            df_cpa = pd.read_json(
                path_or_buf=data_io,
                orient="records",
                convert_dates=False,
                dtype={
                    "Marketing Source Name": "string",
                    "First Time Depositing Customers": bool,
                    "Signups": bool,
                    "CPA Processed": np.uint32,
                    "Net Revenue": bool,
                    # Others values that will ignored, set value with
                    # smallest size
                    "Affiliate ID": bool,
                    "Date": bool,
                    "Product Name": bool,
                    "Brand Name": bool,
                    "Marketing Source ID": bool,
                    "Views": bool,
                    "Unique Views": bool,
                    "Clicks": bool,
                    "Depositing Customers": bool,
                    "ChargeBacks": bool,
                },
            )
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            error_msg = (
                "Something is wrong at get data from API by Daily report CPA only, check the Authorization KEY "
                "and whitelist on server API .\n"
                f"url: {url}\n"
                f"Response text:\n{response_obj.text}\n"
                f"Response statis:\n{response_obj.status_code}\n\n"
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

        cols_to_use = [
            "Marketing Source Name",
            "CPA Processed",
        ]
        df_cpa = df_cpa[cols_to_use]
        df_cpa.rename(
            inplace=True,
            columns={
                "Marketing Source Name": "prom_code",
                "CPA Processed": "cpa_count",
            },
        )
        df_cpa = df_cpa.groupby(
            by=["prom_code"],
            as_index=False,
        ).sum()

    else:
        df_cpa = pd.DataFrame()

    # Member report (Daily report) CURRENT day
    url = (
        "https://betwarrior.dataexport.netrefer.com/v2/export/reports/affiliate/XML_MS_DailyFigures_InclSubAff?"
        f"authorization={api_key}&yearmonthdayfrom={yesterday_str}&yearmonthdayto={yesterday_str}"
        "&productID=all&PublishPointID=all"
    )

    try:
        for i in range(max_retries):
            response_obj = requests.get(url)
            if (response_obj.status_code == 200):
                break
            else:
                error_msg = (
                    f"Response for Punters by activity reach status {response_obj.status_code}, retrying..."
                )
                logger_task.error(error_msg)
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": error_msg,
                        "msg_url": settings.CHAT_WEBHOOK_CELERY,
                    },
                )
                time.sleep(random.randint(1, 3))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API for Signup Date, check the Authorization KEY "
            "and whitelist on server API .\n"
            f"url: {url}\n"
            f"Response text:\n{response_obj.text}\n"
            f"Response statis:\n{response_obj.status_code}\n\n"
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

    if (response_obj.status_code != 200):
        error_msg = (
            f"Response for Daily Figures Report reach status {response_obj.status_code}, check "
            "credentials, connection IP/VPN is on Whitelist of API server or server status.\n"
            f"url: {url}"
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

    found_data = True
    if (response_obj.text == filter_not_data):
        warning_msg = (
            f"Not found data on Daily Figures Report\n"
            f"url: {url}\n"
            f"Response text:\n{response_obj.text}\n"
            f"Response status:\n{response_obj.status_code}"
        )
        warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
        logger_task.warning(warning_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": warning_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        found_data = False

    # Manage dataframe for Daily report current day
    if(found_data):
        # Stream I/O
        data_io = StringIO(response_obj.text)
        try:
            # Create the DataFrame
            df_daily = pd.read_json(
                path_or_buf=data_io,
                orient="records",
                convert_dates=False,
                dtype={
                    "Marketing Source Name": "string",
                    "First Time Depositing Customers": np.uint32,
                    "Signups": np.uint32,
                    "CPA Processed": np.uint32,
                    "Net Revenue": np.float32,
                    # Others values that will ignored, set value with
                    # smallest size
                    "Affiliate ID": bool,
                    "Date": bool,
                    "Product Name": bool,
                    "Brand Name": bool,
                    "Marketing Source ID": bool,
                    "Views": bool,
                    "Unique Views": bool,
                    "Clicks": bool,
                    "Depositing Customers": bool,
                    "ChargeBacks": bool,
                },
            )
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            error_msg = (
                "Something is wrong at get data from API by Daily Figures Report, check the Authorization KEY "
                "and whitelist on server API .\n"
                f"url: {url}\n"
                f"Response text:\n{response_obj.text}\n"
                f"Response statis:\n{response_obj.status_code}\n\n"
                f"if problem persist check traceback:\n\n{''.join(e)}"
            )
            logger_task.error(error_msg)
            chat_logger_task.apply_async(
                kwargs={
                    "msg": error_msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )
            return
        cols_to_use = [
            "Marketing Source Name",
            "First Time Depositing Customers",
            "Signups",
            "CPA Processed",
            "Net Revenue",
        ]
        df_daily = df_daily[cols_to_use]
        df_daily.rename(
            inplace=True,
            columns={
                "Marketing Source Name": "prom_code",
                "First Time Depositing Customers": "first_deposit_count",
                "Signups": "registered_count",
                "CPA Processed": "cpa_count",
                "Net Revenue": "net_revenue",
            },
        )

        df_daily = df_daily.groupby(
            by=["prom_code"],
            as_index=False,
        ).sum()

    else:
        df_daily = pd.DataFrame()

    if(df_act.empty and df_cpa.empty and df_daily.empty):
        warning_msg = f"No data for campaign {campaign_title} date_from {yesterday_str} date_to {yesterday_str}"
        warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
        logger_task.warning(warning_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": warning_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    if (update_account and not df_act.empty):
        # If account will be not updated prevent cast
        df_act["cpa_at"] = pd.to_datetime(
            arg=df_act["cpa_at"],
            format="%d-%m-%Y",
            errors="coerce",
            infer_datetime_format=False,
        )
        df_act["registered_at"] = pd.to_datetime(
            arg=df_act["registered_at"],
            format="%d-%m-%Y",
            errors="coerce",
            infer_datetime_format=False,
        )
    # Remove Repeated punters, keep the first ocurrency and remove others
    # df.drop_duplicates(subset=["punter_id"], inplace=True)

    # Get prom_codes from data
    if (not df_act.empty):
        prom_codes_act = set(df_act.prom_code.unique())
    else:
        prom_codes_act = set()

    if (not df_cpa.empty):
        prom_codes_cpa = set(df_cpa.prom_code.unique())
    else:
        prom_codes_act = set()

    if (not df_daily.empty):
        prom_codes_daily = set(df_daily.prom_code.unique())
    else:
        prom_codes_daily = set()

    prom_codes = set.union(prom_codes_act, prom_codes_cpa, prom_codes_daily)

    # Get related link from prom_codes and campaign, QUERY
    filters = (
        Q(prom_code__in=prom_codes),
        Q(campaign_id=campaign.id),
    )
    links = Link.objects.filter(*filters).select_related(
        "partner_link_accumulated",
        "betenlacecpa",
    )
    valid_prom_codes = links.values_list("prom_code", flat=True)

    # Get data for Account report
    # Get account reports from previous links and punter_id, QUERY
    if (not df_act.empty):
        punters_id = df_act.punter_id.unique()
    else:
        punters_id = []
    filters = (
        Q(link__in=links.values_list("pk", flat=True)),
        Q(punter_id__in=punters_id),
        Q(link__campaign__pk=campaign.pk),
    )
    account_reports = AccountReport.objects.filter(*filters)

    # Dictionary with current applied sum of cpa's by prom_code
    cpa_by_prom_code_iter = {}
    deposit_by_prom_code_sum = {}
    stake_by_prom_code_sum = {}

    # Remove data that now have a valid prom_code for campaign
    if (not df_act.empty):
        df_act.drop(
            labels=df_act[~df_act.prom_code.isin(valid_prom_codes)].index,
            inplace=True,
        )

    if (not df_daily.empty):
        df_daily.drop(
            labels=df_daily[~df_daily.prom_code.isin(valid_prom_codes)].index,
            inplace=True,
        )
    if (not df_cpa.empty):
        df_cpa.drop(
            labels=df_cpa[~df_cpa.prom_code.isin(valid_prom_codes)].index,
            inplace=True,
        )

    for prom_code_i in valid_prom_codes:
        cpa_by_prom_code_iter[prom_code_i] = []
        deposit_by_prom_code_sum[prom_code_i] = 0
        stake_by_prom_code_sum[prom_code_i] = 0

    # Acumulators bulk create and update
    account_reports_update = []
    account_reports_create = []

    # Set keys by index based on colum names of Dataframe
    keys = {key: index for index, key in enumerate(df_act.columns.values)}

    list_logs = []
    # Account Report Case
    for row in zip(*df_act.to_dict('list').values()):
        """
        punter_id
        prom_code
        registered_at
        deposit
        stake
        net_revenue
        cpa_at
        """
        deposit_by_prom_code_sum[row[keys.get("prom_code")]] += row[keys.get("deposit")]
        stake_by_prom_code_sum[row[keys.get("prom_code")]] += row[keys.get("stake")]

        if (not update_account):
            # Force loop when account will not updated
            continue

        link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)

        if link is None:
            warning_msg = (
                f"Link with prom_code={row[keys.get('prom_code')]} and campaign={campaign_title} "
                "not found on database"
            )
            warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
            logger_task.warning(warning_msg)
            list_logs.append(warning_msg)
            continue

        # Check registration_date null
        if (not pd.isna(row[keys.get("registered_at")])):
            registered_at = row[keys.get("registered_at")].date()
        else:
            warning_msg = (
                f"registered_at is null on campaign title \"{campaign_title}\" with prom_code "
                f"\"{row[keys.get('prom_code')]}\""
            )
            warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
            logger_task.warning(warning_msg)
            chat_logger_task.apply_async(
                kwargs={
                    "msg": warning_msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )
            registered_at = None

        # Flag for account/punter have cpa of requests month
        cpa_account_current_month = 0

        if (not pd.isna(row[keys.get("cpa_at")])):
            if (
                row[keys.get("cpa_at")].year == yesterday.date().year and
                row[keys.get("cpa_at")].month == yesterday.date().month
            ):
                # Case current Year, month
                cpa_at = yesterday.date()
                cpa_account_current_month = 1
            else:
                cpa_at = row[keys.get("cpa_at")].date()
        else:
            cpa_at = None

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

        if account_report:
            # Case and exist entry
            account_report_update = _account_report_update(
                keys=keys,
                row=row,
                account_report=account_report,
                partner_link_accumulated=partner_link_accumulated,
                cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                campaign=campaign,
                cpa_at=cpa_at,
                revenue_share_percentage=revenue_share_percentage,
                cpa_account_current_month=cpa_account_current_month,
            )
            account_reports_update.append(account_report_update)
        else:
            # Case new entry
            account_report_new = _account_report_create(
                row=row,
                keys=keys,
                link=link,
                registered_at=registered_at,
                partner_link_accumulated=partner_link_accumulated,
                created_at=yesterday,
                cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                campaign=campaign,
                cpa_at=cpa_at,
                revenue_share_percentage=revenue_share_percentage,
                cpa_account_current_month=cpa_account_current_month,
            )
            account_reports_create.append(account_report_new)

    # Get data for Member report
    betenlacecpas = links.values_list("betenlacecpa", flat=True)

    filters = (
        Q(betenlace_cpa__in=betenlacecpas),
        Q(created_at=yesterday.date()),
    )
    betenlace_daily_reports = BetenlaceDailyReport.objects.filter(*filters)

    filters = (
        Q(betenlace_daily_report__in=betenlace_daily_reports),
    )
    partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(*filters)

    # Get the last Fx value
    filters = (
        Q(created_at__gte=yesterday),
    )
    fx_partner = FxPartner.objects.filter(*filters).order_by("created_at").first()

    if(fx_partner is None):
        # Get just next from supplied date
        filters = (
            Q(created_at__lte=yesterday),
        )
        fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

    # If still none prevent execution
    if(fx_partner is None):
        msg_error = (
            "Undefined fx_partner on DB"
        )
        msg_error = f"*LEVEL:* `ERROR` \n*message:* `{msg_error}`\n\n"
        logger_task.error(msg_error)
        list_logs.append(msg_error)
        return

    fx_partner_percentage = fx_partner.fx_percentage

    currency_condition_str = campaign.currency_condition.lower()
    currency_fixed_income_str = campaign.currency_fixed_income.lower()

    # Calculate cpa by accum of month
    filters = (
        Q(betenlace_cpa__in=betenlacecpas),
        Q(created_at__year=yesterday.date().year),
        Q(created_at__month=yesterday.date().month),
        ~Q(created_at=yesterday.date()),
    )
    betenlace_daily_month = BetenlaceDailyReport.objects.filter(
        *filters,
    ).values(
        "betenlace_cpa",
    ).annotate(
        prom_code=F("betenlace_cpa__link__prom_code"),
        cpa_count_sum=Coalesce(Sum("cpa_count"), 0),
    )
    cpa_daily = {}
    for betenlace_daily_month_i in betenlace_daily_month:
        cpa_incoming_row = df_cpa.loc[
            df_cpa.prom_code.values == betenlace_daily_month_i.get("prom_code")
        ]
        if (not cpa_incoming_row.empty):
            cpa_incoming = cpa_incoming_row["cpa_count"].iloc[0]
            df_cpa.drop(
                labels=cpa_incoming_row.index,
                inplace=True,
            )
        else:
            cpa_incoming = 0
        cpa_to_save = (
            cpa_incoming - betenlace_daily_month_i.get("cpa_count_sum")
        )
        cpa_to_save = cpa_to_save if cpa_to_save > 0 else 0
        cpa_daily[betenlace_daily_month_i.get("prom_code")] = cpa_to_save

    keys = {key: index for index, key in enumerate(df_cpa.columns.values)}

    # Load values without accum
    for row in zip(*df_cpa.to_dict("list").values()):
        cpa_daily[row[keys.get("prom_code")]] = row[keys.get("cpa_count")]

    # Acumulators bulk create and update
    member_reports_betenlace_month_update = []
    member_reports_daily_betenlace_update = []
    member_reports_daily_betenlace_create = []

    member_reports_partner_month_update = []
    member_reports_daily_partner_update = []
    member_reports_daily_partner_create = []

    # Set keys by index based on colum names of Dataframe
    keys = {key: index for index, key in enumerate(df_daily.columns.values)}

    # Member report Case
    for row in zip(*df_daily.to_dict('list').values()):
        """
        "prom_code"
        "deposit"
        "stake"
        "net_revenue"
        "registered_count"
        "cpa_count"
        """
        # Get link according to prom_code of current loop
        link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)
        if not link:
            warning_msg = (
                f"Link with prom_code \"{row[keys.get('prom_code')]}\" and campaign \"{campaign_title}\" "
                "not found on database at Member report Case"
            )
            warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
            logger_task.warning(warning_msg)
            chat_logger_task.apply_async(
                kwargs={
                    "msg": warning_msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )
            continue

        try:
            # Get current entry of member report based on link (prom_code)
            betenlace_cpa = link.betenlacecpa
        except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
            msg_error = (
                f"Betenlace CPA entry not found for link with prom_code \"{row[keys.get('prom_code')]}\""
            )
            msg_error = f"*LEVEL:* `ERROR` \n*message:* `{msg_error}`\n\n"
            logger_task.error(msg_error)
            list_logs.append(msg_error)
            continue

        # Get data from member from accumulated cpa at month
        cpa_count = cpa_daily.pop(row[keys.get("prom_code")], 0)

        # Get data for member from punters
        deposit = deposit_by_prom_code_sum.get(row[keys.get("prom_code")])
        stake = stake_by_prom_code_sum.get(row[keys.get("prom_code")])

        # Betenlace Month
        betenlace_cpa = _betenlace_month_update(
            keys=keys,
            row=row,
            betenlace_cpa=betenlace_cpa,
            deposit=deposit,
            stake=stake,
            cpa_count=cpa_count,
            campaign=campaign,
            revenue_share_percentage=revenue_share_percentage,
        )
        member_reports_betenlace_month_update.append(betenlace_cpa)

        # Betenlace Daily
        betenlace_daily = next(
            filter(
                lambda betenlace_daily: (
                    betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                    betenlace_daily.created_at == yesterday.date()
                ), betenlace_daily_reports
            ), None
        )

        if(betenlace_daily):
            betenlace_daily = _betenlace_daily_update(
                keys=keys,
                row=row,
                betenlace_daily=betenlace_daily,
                deposit=deposit,
                stake=stake,
                cpa_count=cpa_count,
                campaign=campaign,
                fx_partner=fx_partner,
                revenue_share_percentage=revenue_share_percentage,
            )
            member_reports_daily_betenlace_update.append(betenlace_daily)
        else:
            betenlace_daily = _betenlace_daily_create(
                from_date=yesterday.date(),
                keys=keys,
                row=row,
                betenlace_cpa=betenlace_cpa,
                deposit=deposit,
                stake=stake,
                cpa_count=cpa_count if update_cpa else 0,
                campaign=campaign,
                fx_partner=fx_partner,
                revenue_share_percentage=revenue_share_percentage,
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
            if(campaign.status == Campaign.Status.INACTIVE) and (yesterday.date() >= campaign.last_inactive_at.date()):
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
            cpa_count_partner = math.floor(cpa_count*partner_link_accumulated.tracker)
        else:
            cpa_count_partner = cpa_count

        # verify if cpa_count had a change from tracker calculation
        if (update_account and cpa_count > cpa_count_partner):
            # Reduce -1 additional for enum behavior
            diff_count = (cpa_count - cpa_count_partner) - 1

            for enum, account_instance_i in enumerate(
                    reversed(cpa_by_prom_code_iter.get(row[keys.get("prom_code")]))):
                # Remove cpa partner
                account_instance_i.cpa_partner = 0
                if (enum >= diff_count):
                    break

        tracked_data = _get_tracker_values(
            keys=keys,
            row=row,
            deposit=deposit,
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
        fixed_income_partner = cpa_count_partner * fixed_income_partner_unitary
        fixed_income_partner_unitary_local = (
            campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa * fx_fixed_income_partner)
        fixed_income_partner_local = cpa_count_partner * fixed_income_partner_unitary_local

        # Fx Currency Condition
        fx_condition_partner = _calc_fx(
            fx_partner=fx_partner,
            fx_partner_percentage=fx_partner_percentage,
            currency_from_str=currency_condition_str,
            partner_currency_str=partner_currency_str,
        )

        partner_link_accumulated = _partner_link_month_update(
            partner_link_accumulated=partner_link_accumulated,
            cpa_count=cpa_count_partner,
            fixed_income_partner=fixed_income_partner,
            fixed_income_partner_local=fixed_income_partner_local,
        )
        member_reports_partner_month_update.append(partner_link_accumulated)

        # Partner Daily
        partner_link_daily = next(
            filter(
                lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id ==
                betenlace_daily.id, partner_link_dailies_reports),
            None)

        if(partner_link_daily):
            # Recalculate fixed_incomes for update
            # fixed_income_partner_unitary = betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa
            # fixed_income_partner = cpa_count_partner * fixed_income_partner_unitary
            # fixed_income_partner_unitary_local = (
            #     betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa * fx_book_partner)
            # fixed_income_partner_local = cpa_count_partner * fixed_income_partner_unitary_local
            partner_link_daily = _partner_link_daily_update(
                cpa_count=cpa_count_partner,
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
                from_date=yesterday,
                campaign=campaign,
                betenlace_daily=betenlace_daily,
                partner_link_accumulated=partner_link_accumulated,
                cpa_count=cpa_count_partner if update_cpa else 0,
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

    # Member report Case CPA only
    for prom_code in cpa_daily.keys():
        """
        "prom_code"
        "deposit"
        "stake"
        "net_revenue"
        "registered_count"
        "cpa_count"
        """
        # Get link according to prom_code of current loop
        link = next(filter(lambda link: link.prom_code == prom_code, links), None)
        if not link:
            warning_msg = (
                f"Link with prom_code \"{prom_code}\" and campaign \"{campaign_title}\" "
                "not found on database at Member report Case"
            )
            warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
            logger_task.warning(warning_msg)
            list_logs.append(warning_msg)
            continue

        try:
            # Get current entry of member report based on link (prom_code)
            betenlace_cpa = link.betenlacecpa
        except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
            msg_error = (
                f"Betenlace CPA entry not found for link with prom_code \"{row[keys.get('prom_code')]}\""
            )
            msg_error = f"*LEVEL:* `ERROR` \n*message:* `{msg_error}`\n\n"
            logger_task.error(msg_error)
            list_logs.append(msg_error)
            continue

        # Get data from member from accumulated cpa at month
        cpa_count = cpa_daily.get(prom_code, 0)

        # Prevent data with only 0
        if (cpa_count == 0):
            continue

        # Betenlace Month
        betenlace_cpa = _betenlace_month_update_cpa(
            betenlace_cpa=betenlace_cpa,
            cpa_count=cpa_count,
            campaign=campaign,
        )
        member_reports_betenlace_month_update.append(betenlace_cpa)

        # Betenlace Daily
        betenlace_daily = next(
            filter(
                lambda betenlace_daily: (
                    betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                    betenlace_daily.created_at == yesterday.date()
                ), betenlace_daily_reports
            ), None
        )

        if(betenlace_daily):
            betenlace_daily = _betenlace_daily_update_cpa(
                betenlace_daily=betenlace_daily,
                cpa_count=cpa_count,
                campaign=campaign,
                fx_partner=fx_partner,
            )
            member_reports_daily_betenlace_update.append(betenlace_daily)
        else:
            betenlace_daily = _betenlace_daily_create_cpa(
                from_date=yesterday.date(),
                betenlace_cpa=betenlace_cpa,
                cpa_count=cpa_count if update_cpa else 0,
                campaign=campaign,
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
            if(campaign.status == Campaign.Status.INACTIVE) and (yesterday.date() >= campaign.last_inactive_at.date()):
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
            cpa_count_partner = math.floor(cpa_count*partner_link_accumulated.tracker)
        else:
            cpa_count_partner = cpa_count

        # verify if cpa_count had a change from tracker calculation
        if (update_account and cpa_count > cpa_count_partner):
            # Reduce -1 additional for enum behavior
            diff_count = (cpa_count - cpa_count_partner) - 1

            for enum, account_instance_i in enumerate(
                    reversed(cpa_by_prom_code_iter.get(row[keys.get("prom_code")]))):
                # Remove cpa partner
                account_instance_i.cpa_partner = 0
                if (enum >= diff_count):
                    break

        # Fx Currency Fixed income
        partner_currency_str = partner_link_accumulated.currency_local.lower()
        fx_fixed_income_partner = _calc_fx(
            fx_partner=fx_partner,
            fx_partner_percentage=fx_partner_percentage,
            currency_from_str=currency_fixed_income_str,
            partner_currency_str=partner_currency_str,
        )

        fixed_income_partner_unitary = campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa
        fixed_income_partner = cpa_count_partner * fixed_income_partner_unitary
        fixed_income_partner_unitary_local = (
            campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa * fx_fixed_income_partner)
        fixed_income_partner_local = cpa_count_partner * fixed_income_partner_unitary_local

        # Fx Currency Condition
        fx_condition_partner = _calc_fx(
            fx_partner=fx_partner,
            fx_partner_percentage=fx_partner_percentage,
            currency_from_str=currency_condition_str,
            partner_currency_str=partner_currency_str,
        )

        partner_link_accumulated = _partner_link_month_update(
            partner_link_accumulated=partner_link_accumulated,
            cpa_count=cpa_count_partner,
            fixed_income_partner=fixed_income_partner,
            fixed_income_partner_local=fixed_income_partner_local,
        )
        member_reports_partner_month_update.append(partner_link_accumulated)

        # Partner Daily
        partner_link_daily = next(
            filter(
                lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id ==
                betenlace_daily.id, partner_link_dailies_reports),
            None)

        if(partner_link_daily):
            # Recalculate fixed_incomes for update
            # fixed_income_partner_unitary = betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa
            # fixed_income_partner = cpa_count_partner * fixed_income_partner_unitary
            # fixed_income_partner_unitary_local = (
            #     betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa * fx_book_partner)
            # fixed_income_partner_local = cpa_count_partner * fixed_income_partner_unitary_local
            partner_link_daily = _partner_link_daily_update_cpa(
                cpa_count=cpa_count_partner,
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
            )
            member_reports_daily_partner_update.append(partner_link_daily)
        else:
            partner_link_daily = _partner_link_daily_create_cpa(
                from_date=yesterday,
                campaign=campaign,
                betenlace_daily=betenlace_daily,
                partner_link_accumulated=partner_link_accumulated,
                cpa_count=cpa_count_partner if update_cpa else 0,
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
        if (update_account):
            if(account_reports_create):
                AccountReport.objects.bulk_create(
                    objs=account_reports_create,
                )
            if(account_reports_update):
                AccountReport.objects.bulk_update(
                    objs=account_reports_update,
                    fields=(
                        "deposit",
                        "net_revenue",
                        "stake",
                        "fixed_income",
                        "net_revenue",
                        "revenue_share",
                        "cpa_betenlace",
                        "cpa_partner",
                        "registered_at",
                        "cpa_at",
                    ),
                )
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
                ),
            )

        if(member_reports_daily_betenlace_update):
            if (update_cpa):
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
                        # "wagering_count",
                    ),
                )
            else:
                BetenlaceDailyReport.objects.bulk_update(
                    objs=member_reports_daily_betenlace_update,
                    fields=(
                        "deposit",
                        "stake",
                        "net_revenue",
                        "revenue_share",
                        # "fixed_income",
                        # "fixed_income_unitary",
                        "fx_partner",
                        "registered_count",
                        # "cpa_count",
                        "first_deposit_count",
                        # "wagering_count",
                    ),
                )

        if(member_reports_daily_betenlace_create):
            BetenlaceDailyReport.objects.bulk_create(
                objs=member_reports_daily_betenlace_create,
            )

        if(member_reports_partner_month_update):
            if (update_cpa):
                PartnerLinkAccumulated.objects.bulk_update(
                    objs=member_reports_partner_month_update,
                    fields=(
                        "cpa_count",
                        "fixed_income",
                        "fixed_income_local",
                    ),
                )

        if(member_reports_daily_partner_update):
            if (update_cpa):
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
                        # "tracker_wagering_count",
                        "deposit",
                        "registered_count",
                        "first_deposit_count",
                        # "wagering_count",
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
            else:
                PartnerLinkDailyReport.objects.bulk_update(
                    objs=member_reports_daily_partner_update,
                    fields=(
                        # "fixed_income",
                        # "fixed_income_unitary",
                        # "fx_book_local",
                        "fx_book_net_revenue_local",
                        "fx_percentage",
                        # "fixed_income_local",
                        # "fixed_income_unitary_local",
                        # "cpa_count",
                        # "percentage_cpa",
                        # "tracker",
                        "tracker_deposit",
                        "tracker_registered_count",
                        "tracker_first_deposit_count",
                        # "tracker_wagering_count",
                        "deposit",
                        "registered_count",
                        "first_deposit_count",
                        # "wagering_count",
                        "adviser_id",
                        # "fixed_income_adviser",
                        # "fixed_income_adviser_local",
                        "net_revenue_adviser",
                        "net_revenue_adviser_local",
                        # "fixed_income_adviser_percentage",
                        "net_revenue_adviser_percentage",
                        "referred_by",
                        # "fixed_income_referred",
                        # "fixed_income_referred_local",
                        "net_revenue_referred",
                        "net_revenue_referred_local",
                        # "fixed_income_referred_percentage",
                        "net_revenue_referred_percentage",

                    ),
                )

        if(member_reports_daily_partner_create):
            PartnerLinkDailyReport.objects.bulk_create(
                objs=member_reports_daily_partner_create,
            )

    if (df_daily.empty):
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
        msg = f"Member for Campaign {campaign_title} processed count {len(df_daily.index)}"
        msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
        logger_task.warning(msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )

    if (df_act.empty):
        msg = f"Account Report for Campaign {campaign_title} No Records/No data"
        msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
        logger_task.warning(msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
    else:
        msg = f"Account Report for Campaign {campaign_title} processed count {len(df_act.index)}"
        msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
        logger_task.warning(msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
    return


def _account_report_update(
    keys,
    row,
    account_report,
    partner_link_accumulated,
    cpa_by_prom_code_iter,
    campaign,
    cpa_at,
    revenue_share_percentage,
    cpa_account_current_month,
):
    account_report.deposit += row[keys.get("deposit")]
    account_report.stake += row[keys.get("stake")]

    account_report.net_revenue += row[keys.get("net_revenue")]
    account_report.revenue_share += revenue_share_percentage * row[keys.get("net_revenue")]

    if (cpa_at is not None):
        if (account_report.cpa_at is None):
            # Update cpa date only if this is not none
            account_report.cpa_at = cpa_at
        elif (cpa_at.month != account_report.cpa_at.month and cpa_at.year != account_report.cpa_at.year):
            logger_task.warning(
                f"Punter pk {account_report.pk} have incoming cpa_at \"{account_report.cpat_ap}\" on "
                "different month, is forced to update"
            )
            account_report.cpa_at = cpa_at
    else:
        if (account_report.cpa_at is not None):
            # Prevent multi count at same punter, but alert
            logger_task.warning(
                f"punter_pk=\"{account_report.pk}\" for campaign_title=\"{campaign.campaign_title}\" "
                f"has cpa_at {account_report.cpa_at} and now coming None"
            )
        else:
            account_report.cpa_at = None

    if (not account_report.cpa_partner):
        account_report.partner_link_accumulated = partner_link_accumulated

    if account_report.cpa_betenlace != 1:
        account_report.cpa_betenlace = cpa_account_current_month
        if (cpa_account_current_month):
            account_report.fixed_income = campaign.fixed_income_unitary
            if (partner_link_accumulated is not None):
                account_report.cpa_partner = 1
                # Append only punters that are not already counted
                cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(account_report)

        account_report.partner_link_accumulated = partner_link_accumulated

    return account_report


def _account_report_create(
    row,
    keys,
    link,
    registered_at,
    partner_link_accumulated,
    created_at,
    cpa_by_prom_code_iter,
    campaign,
    cpa_at,
    revenue_share_percentage,
    cpa_account_current_month,
):
    account_report = AccountReport(
        partner_link_accumulated=partner_link_accumulated,
        punter_id=row[keys.get("punter_id")],
        deposit=row[keys.get("deposit")],
        stake=row[keys.get("stake")],
        fixed_income=campaign.fixed_income_unitary * cpa_account_current_month,
        net_revenue=row[keys.get("net_revenue")],
        revenue_share=row[keys.get("net_revenue")]*revenue_share_percentage,
        currency_condition=campaign.currency_condition,
        currency_fixed_income=campaign.currency_fixed_income,
        cpa_betenlace=cpa_account_current_month,
        link=link,
        registered_at=registered_at,
        cpa_at=cpa_at,
        created_at=created_at,
    )

    if (cpa_account_current_month):
        cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(account_report)

    return account_report


def _partner_link_daily_create_cpa(
    from_date,
    campaign,
    betenlace_daily,
    partner_link_accumulated,
    cpa_count,
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

        deposit=0,
        registered_count=0,
        first_deposit_count=0,
        # wagering_count=0,

        tracker=partner_link_accumulated.tracker,
        tracker_deposit=partner_link_accumulated.tracker_deposit,
        tracker_registered_count=partner_link_accumulated.tracker_registered_count,
        tracker_first_deposit_count=partner_link_accumulated.tracker_first_deposit_count,
        # tracker_wagering_count=partner_link_accumulated.tracker_wagering_count,

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


def _partner_link_daily_update_cpa(
    cpa_count,
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

    partner_link_daily.deposit = 0
    partner_link_daily.registered_count = 0
    partner_link_daily.first_deposit_count = 0
    # partner_link_daily.wagering_count = tracked_data.get("wagering_count")

    partner_link_daily.tracker = partner_link_accumulated.tracker
    partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
    partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
    partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
    # partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

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
        partner_link_daily.net_revenue_adviser = 0
        partner_link_daily.net_revenue_adviser_local = (
            partner_link_daily.net_revenue_adviser * fx_condition_partner
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
        partner_link_daily.net_revenue_referred = 0
        partner_link_daily.net_revenue_referred_local = (
            partner_link_daily.net_revenue_referred * fx_condition_partner
        )

    return partner_link_daily


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


def _get_tracker_values(
    keys,
    row,
    deposit,
    partner_link_accumulated,
):
    tracked_data = {}
    tracked_data["deposit"] = deposit*partner_link_accumulated.tracker_deposit

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

    # if (keys.get("wagering_count") is not None):
    #     if(row[keys.get("wagering_count")] > 1):
    #         tracked_data["wagering_count"] = math.floor(
    #             row[keys.get("wagering_count")]*partner_link_accumulated.tracker_wagering_count
    #         )
    #     else:
    #         tracked_data["wagering_count"] = row[keys.get("wagering_count")]

    return tracked_data


def _betenlace_daily_create_cpa(
    from_date,
    betenlace_cpa,
    cpa_count,
    campaign,
    fx_partner,
):
    betenlace_daily = BetenlaceDailyReport(
        betenlace_cpa=betenlace_cpa,
        deposit=0,
        stake=0,
        fixed_income_unitary=campaign.fixed_income_unitary,
        fixed_income=campaign.fixed_income_unitary * cpa_count,
        net_revenue=0,
        revenue_share=0,
        fx_partner=fx_partner,
        currency_condition=campaign.currency_condition,
        currency_fixed_income=campaign.currency_fixed_income,
        registered_count=0,
        first_deposit_count=0,
        cpa_count=cpa_count,
        created_at=from_date,
    )

    return betenlace_daily


def _betenlace_daily_update_cpa(
    betenlace_daily,
    cpa_count,
    campaign,
    fx_partner,
):
    betenlace_daily.deposit = 0
    betenlace_daily.stake = 0
    betenlace_daily.net_revenue = 0
    betenlace_daily.revenue_share = 0

    betenlace_daily.registered_count = 0
    betenlace_daily.first_deposit_count = 0

    betenlace_daily.cpa_count = cpa_count

    betenlace_daily.fixed_income_unitary = campaign.fixed_income_unitary
    betenlace_daily.fixed_income = campaign.fixed_income_unitary * cpa_count

    betenlace_daily.fx_partner = fx_partner
    return betenlace_daily


def _betenlace_month_update_cpa(
    betenlace_cpa,
    cpa_count,
    campaign,
):
    betenlace_cpa.fixed_income += campaign.fixed_income_unitary * cpa_count
    betenlace_cpa.cpa_count += cpa_count
    return betenlace_cpa


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
        # wagering_count=tracked_data.get("wagering_count"),

        tracker=partner_link_accumulated.tracker,
        tracker_deposit=partner_link_accumulated.tracker_deposit,
        tracker_registered_count=partner_link_accumulated.tracker_registered_count,
        tracker_first_deposit_count=partner_link_accumulated.tracker_first_deposit_count,
        # tracker_wagering_count=partner_link_accumulated.tracker_wagering_count,

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
    # partner_link_daily.wagering_count = tracked_data.get("wagering_count")

    partner_link_daily.tracker = partner_link_accumulated.tracker
    partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
    partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
    partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
    # partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

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
            partner_link_daily.net_revenue_adviser * fx_condition_partner
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
    deposit,
    stake,
    cpa_count,
    campaign,
    fx_partner,
    revenue_share_percentage,
):
    betenlace_daily = BetenlaceDailyReport(
        betenlace_cpa=betenlace_cpa,
        deposit=deposit,
        stake=stake,
        fixed_income_unitary=campaign.fixed_income_unitary,
        fixed_income=campaign.fixed_income_unitary * cpa_count,
        net_revenue=row[keys.get("net_revenue")],
        revenue_share=row[keys.get("net_revenue")] * revenue_share_percentage,
        fx_partner=fx_partner,
        currency_condition=campaign.currency_condition,
        currency_fixed_income=campaign.currency_fixed_income,
        registered_count=row[keys.get("registered_count")],
        first_deposit_count=row[keys.get("first_deposit_count")],
        cpa_count=cpa_count,
        created_at=from_date,
    )

    return betenlace_daily


def _betenlace_daily_update(
    keys,
    row,
    betenlace_daily,
    deposit,
    stake,
    cpa_count,
    campaign,
    fx_partner,
    revenue_share_percentage,
):
    betenlace_daily.deposit = deposit
    betenlace_daily.stake = stake
    betenlace_daily.net_revenue = row[keys.get('net_revenue')]
    betenlace_daily.revenue_share = row[keys.get('net_revenue')] * revenue_share_percentage

    betenlace_daily.registered_count = row[keys.get('registered_count')]
    betenlace_daily.first_deposit_count = row[keys.get('first_deposit_count')]

    betenlace_daily.cpa_count = cpa_count

    betenlace_daily.fx_partner = fx_partner

    betenlace_daily.fixed_income_unitary = campaign.fixed_income_unitary
    betenlace_daily.fixed_income = campaign.fixed_income_unitary * cpa_count

    return betenlace_daily


def _betenlace_month_update(
    keys,
    row,
    betenlace_cpa,
    deposit,
    stake,
    cpa_count,
    campaign,
    revenue_share_percentage,
):
    betenlace_cpa.deposit += deposit
    betenlace_cpa.stake += stake

    betenlace_cpa.net_revenue += row[keys.get("net_revenue")]
    betenlace_cpa.revenue_share += row[keys.get("net_revenue")] * revenue_share_percentage
    betenlace_cpa.registered_count += row[keys.get("registered_count")]

    betenlace_cpa.fixed_income += campaign.fixed_income_unitary * cpa_count
    betenlace_cpa.cpa_count += cpa_count

    return betenlace_cpa
