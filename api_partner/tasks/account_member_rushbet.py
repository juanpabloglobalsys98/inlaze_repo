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
from core.helpers import CurrencyAll
from core.tasks import chat_logger as chat_logger_task
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

logger_task = get_task_logger(__name__)


@app.task(
    ignore_result=True,
)
def account_member_rushbet(campaign_title):
    """
    Get data from API of bookmaker Rushbet with CSV files using
    the pandas module with high performance, on command use tqdm for
    progress bar

    CSV columns
    ---
    - rowid : `np.unint8`
        Indicates if value is summary or single entry (1 for single entry,
        2 for summarized data)
    - currencysymbol : `string`
        Indicates the used currency on money operations like deposits,
        stake operations like `COP`
    - siteid : `string`
        Equivalent to prom_code from Model `Link` and `AccountReport`
    - playerid : `string`
        Equivalent to punter_id from Model `AccountReport`, unique
        identification (username, database id) for identificate a punter on
        Bookmaker
    - Deposits : `np.float32`
        Equivalent to deposit from Model `BetenlaceDailyReport` and
        `PartnerLinkDailyReport`, quantity of money
        that user has Deposited to their account for bet's on boomaker web.
        This value is not supplied only on Member
    - stake:`np.uint32`
        Total of wagered money by player/punter_id on supplied date,
        the Netrevunue can determinate if player won or loss the
        bets. This value is NOT supplied
    - CPACommission : `np.float32`
        Equivalent to fixed_income from Model `AccountReport`,
        `BetenlaceDailyReport` and `PartnerLinkDailyReport`, fixed_income payed
        fixed_income for cpa's completed. This value is NOT supplied
    - Netrevenue : `np.float32`
        Equivalent to net_revenue from Model `AccountReport`
        `BetenlaceDailyReport` and `PartnerLinkDailyReport`, Profit earned
        so far from that player/punter_id, usually this is the stake - 20%
        of stake (only if player/punter_id loss all bet), this take a
        positive value when player/punter_id loss money and take negative
        when player/punter_id won the bets. This is a sum of outcomes
        (results) of the bets has the player/punter_id placed and the
        bookmaker received a result. This not have the count of bets.
        This value is NOT supplied
    - %Commission : `np.float32`
        Equivalent to revenue_share from Model `AccountReport`,
        `BetenlaceDailyReport` and `PartnerLinkDailyReport` Revenue
        Share from users, this is the 20% of stake (only if
        player/punter_id loss all bet). this take a positive value when
        player/punter_id loss money and take negative when player/punter_id
        won the bets. This is a sum of outcomes (results) of the bets has
        the player/punter_id placed and the bookmaker received a result.
        This not have the count of bets
    - cpacommissioncount : `np.uint32`
        Equivalent to cpa_betenlace and cpa_partner from Model
        `AccountReport`, managed with value cpa_count, this is the quantity
        of cpa's, for every punter can have value 0 or 1, in case of member report
        is the summaried by prom_code. This value is NOT supplied
    - registrationdate : `string`
        Equivalent to registration_at from Model `AccountReport`
        with format mm/dd/aaaa
    - firstdeposit : `string`
        Equivalent to registration_at from Model `AccountReport`
        with format mm/dd/aaaa
    - newpurchases : `np.float32`
            Equivalent to deposit from Model MemberReport (Month, daily) for
            betenalce and partners`, this is apositive integer, the expect var
            is purcharses but the data is similar
    - netwagers:`np.uint32`
        Equivalent to stake, total of wagered money by player/punter_id on
        supplied date, the Netrevunue can determinate if player won or loss
        the bets
    - netrevenue : `np.float32`
        Profit earned so far from that player/punter_id, usually this is
        the stake - 20% of stake (only if player/punter_id loss all bet), this
        take a positive value when player/punter_id loss money and take
        negative when player/punter_id won the bets. This is a sum of
        outcomes (results) of the bets has the player/punter_id placed
        and the bookmaker received a result. This not have the count
        of bets
    - revsharecommission : `np.float32`
        Comission earned from users, this is the 20% of stake (only
        if player/punter_id loss all bet). this take a positive value
        when player/punter_id loss money and take negative when
        player/punter_id won the bets. This is a sum of
        outcomes (results) of the bets has the player/punter_id placed
        and the bookmaker received a result. This not have the count
        of bets
    """

    # Definition of function
    today = timezone.now()
    yesterday = today.astimezone(pytz.timezone(settings.TIME_ZONE)) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y/%m/%d")
    msg = (
        "Making call to API Account And Member Rushbet\n"
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

    fixed_income_unitary_campaign = campaign.fixed_income_unitary

    if (campaign_title == "rushbet col"):
        rushbet_key_account = settings.API_ACCOUNT_REPORT_RUSHBETCOL_KEY
        rushbet_account_id_account = settings.API_ACCOUNT_REPORT_RUSHBETCOL_ACCOUNT_ID
        revenue_share_percentage = 0.30
        cpa_condition_from_revenue_share = 12000

        rushbet_key_member = settings.API_MEMBER_REPORT_RUSHBETCOL_KEY
        rushbet_account_id_member = settings.API_MEMBER_REPORT_RUSHBETCOL_ACCOUNT_ID

    try:
        url = (
            "https://latampartners.rush-affiliates.com/api/affreporting.asp?"
            f"key={rushbet_key_account}&reportname=AccountReport&reportformat=csv&"
            f"reportmerchantid={rushbet_account_id_account}&"
            f"reportstartdate={yesterday_str}&reportenddate={yesterday_str}"
        )
        response = requests.get(url=url)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API, check if current connection IP/VPN is on Whitelist of API server"
            f", if problem still check traceback:\n\n{''.join(e)}"
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

    try:
        # set the characters and line based interface to stream I/O
        data_io = StringIO(response.text[response.text.index("\"rowid\""):])
    except:
        if "No Records" in response.text:
            warning_msg = (
                "Data not found at requested url"
            )
            warning_msg = (
                f"{warning_msg}\n"
                f"campaign_title: \"{campaign_title}\""
                f"Request url: {url}\n"
                "Data obtained\n"
                f"{response.text}"
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

        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API, check the credentials (key and reportmerchantid) if problem "
            f"persist check traceback:\n\n{''.join(e)}"
        )
        error_msg = (
            f"{error_msg}\n"
            f"Request url: {url}\n"
            "Data obtained\n"
            f"{response.text}"
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

    # Create the DataFrame for Account report part
    cols_to_use = [
        "rowid",
        "siteid",
        "playerid",
        "%Commission",
        "registrationdate",
    ]
    df_account = pd.read_csv(
        filepath_or_buffer=data_io,
        sep=",",
        usecols=cols_to_use,
        dtype={
            "rowid": np.uint8,
            "siteid": "string",
            "playerid": "string",
            "%Commission": np.float32,
            "registrationdate": "string",
        },
    )[cols_to_use]

    df_account.rename(
        inplace=True,
        columns={
            "rowid": "row_id",
            "siteid": "prom_code",
            "playerid": "punter_id",
            "%Commission": "revenue_share",
            "registrationdate": "registered_at",
        },
    )
    # "rowid","currencysymbol","totalrecords","merchantname","memberid",
    # "username","siteid","bannerid","creativename","bannertype","playerid",
    # "registrationdate","firstdeposit","%Commission","totalcommission","new"

    # Filter data - Override in same place of memory group/sum data
    # rowid == 2
    df_account.drop(
        labels=df_account[df_account.eval("(row_id == 2)", engine='numexpr')].index,
        inplace=True,
    )

    try:
        url = (
            f"https://latampartners.rush-affiliates.com/api/affreporting.asp?key={rushbet_key_member}&"
            f"reportname=Member%20Report%20-%20Detailed&reportformat=csv&reportmerchantid={rushbet_account_id_member}&"
            f"reportstartdate={yesterday_str}&reportenddate={yesterday_str}")
        response = requests.get(url)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API, check the credentials (key and reportmerchantid) if problem "
            f"persist check traceback:\n\n{''.join(e)}"
        )
        error_msg = (
            f"{error_msg}\n"
            f"Request url: {url}\n"
            "Data obtained\n"
            f"{response.text}"
        )
        logger_task.error(error_msg)
        return
    try:
        # set the characters and line based interface to stream I/O
        data_io = StringIO(response.text[response.text.index("\"rowid\""):])
    except:
        if "No Records" in response.text:
            warning_msg = (
                "Data not found at requested url"
            )
            warning_msg = (
                f"{warning_msg}\n"
                f"campaign_title: \"{campaign_title}\""
                f"Request url: {url}\n"
                "Data obtained\n"
                f"{response.text}"
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

        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API, check the credentials (key and reportmerchantid) if problem "
            f"persist check traceback:\n\n{''.join(e)}"
        )
        error_msg = (
            f"{error_msg}\n"
            f"Request url: {url}\n"
            "Data obtained\n"
            f"{response.text}"
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

    # Create the DataFrame for Member report part
    cols_to_use = [
        "rowid",
        "siteid",
        "newpurchases",
        "revsharecommission",
        "downloads",
        "wageraccountcount",
    ]
    df_member = pd.read_csv(
        filepath_or_buffer=data_io,
        sep=",",
        usecols=cols_to_use,
        dtype={
            "rowid": np.uint8,
            "siteid": "string",
            "newpurchases": np.float32,
            "revsharecommission": np.float32,
            "downloads": np.uint32,
            "wageraccountcount": np.uint32,
        },
    )[cols_to_use]

    df_member.rename(
        inplace=True,
        columns={
            "rowid": "row_id",
            "siteid": "prom_code",
            "newpurchases": "deposit",
            "revsharecommission": "revenue_share",
            "downloads": "registered_count",
            "wageraccountcount": "wagering_count",
        },
    )
    # "rowid","currencysymbol","totalrecords","period","merchantname",
    # "memberid","username","country","memberid","siteid","sitename",
    # "impressions","clicks","installs","clickthroughratio","downloads",
    # "downloadratio","newaccountratio","newdepositingacc","newaccounts",
    # "firstdepositcount","activeaccounts","activedays","newpurchases",
    # "purchaccountcount","wageraccountcount","avgactivedays",
    # "revsharecommission","referralcommissiontotal","totalcommission"

    # Filter data - Override in same place of memory group/sum data
    # rowid == 2
    df_member.drop(
        labels=df_member[df_member.eval("(row_id == 2)", engine='numexpr')].index,
        inplace=True,
    )

    # Temp group by for get data of Big range date
    df_member = df_member.groupby(
        by=['prom_code'],
        as_index=False,
    ).sum()

    prom_codes = set(df_account.prom_code.unique()) | set(df_member.prom_code.unique())

    # Get related link from prom_codes and campaign, QUERY
    filters = (
        Q(prom_code__in=prom_codes),
        Q(campaign_id=campaign.id),
    )
    links = Link.objects.filter(
        *filters,
    ).select_related(
        "partner_link_accumulated",
        "partner_link_accumulated__partner",
        "betenlacecpa",
    )

    links_pk = links.values_list("pk", flat=True)

    # Get account reports from previous links and punter_id, QUERY
    filters = (
        Q(link__in=links_pk),
        Q(punter_id__in=df_account.punter_id.unique()),
    )
    account_reports = AccountReport.objects.filter(*filters)

    currency_condition = campaign.currency_condition
    currency_condition_str = currency_condition.lower()
    currency_fixed_income = campaign.currency_fixed_income
    currency_fixed_income_str = currency_fixed_income.lower()

    # Acumulators bulk create and update
    account_reports_update = []
    account_reports_create = []

    # Set keys by index based on colum names of Dataframe
    keys = {key: index for index, key in enumerate(df_account.columns.values)}

    # Dictionary with current applied sum of cpa's by prom_code
    cpa_by_prom_code_iter = {}
    for prom_code in prom_codes:
        cpa_by_prom_code_iter[prom_code] = []

    list_logs = []
    for row in zip(*df_account.to_dict('list').values()):
        """
        'row_id': 0,
        'prom_code': 2,
        'punter_id': 3,
        'revenue_share': 4,
        'registered_at': 5,
        'first_deposit_at': 6,
        """
        link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)

        if not link:
            warning_msg = (
                f"Link with prom_code={row[keys.get('prom_code')]} and campaign={campaign_title} "
                "not found on database"
            )
            warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
            logger_task.warning(warning_msg)
            list_logs.append(warning_msg)
            continue

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
            # Temp use to_date
            # Case and exist entry
            account_report_update = _account_report_update(
                keys=keys,
                row=row,
                from_date=yesterday,
                partner_link_accumulated=partner_link_accumulated,
                account_report=account_report,
                cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                revenue_share_percentage=revenue_share_percentage,
                cpa_condition_from_revenue_share=cpa_condition_from_revenue_share,
                fixed_income_campaign=fixed_income_unitary_campaign,
            )
            account_reports_update.append(account_report_update)
        else:
            # Temp use to_date
            # Case new entry
            account_report_new = _account_report_create(
                row=row,
                keys=keys,
                link=link,
                currency_condition=currency_condition,
                currency_fixed_income=currency_fixed_income,
                partner_link_accumulated=partner_link_accumulated,
                from_date=yesterday,
                cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                revenue_share_percentage=revenue_share_percentage,
                cpa_condition_from_revenue_share=cpa_condition_from_revenue_share,
                fixed_income_campaign=fixed_income_unitary_campaign,
            )

            account_reports_create.append(account_report_new)

    # Continue for Member report

    # Betenlacecpas
    betenlacecpas_pk = links.values_list("betenlacecpa__pk", flat=True)

    # Get member reports from previous links, QUERY
    filters = (
        Q(betenlace_cpa__pk__in=betenlacecpas_pk),
        Q(created_at=yesterday.date()),
    )
    betenlace_daily_reports = BetenlaceDailyReport.objects.filter(*filters)

    filters = (
        Q(betenlace_daily_report__in=betenlace_daily_reports),
    )
    partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter()

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
        msg_error = (
            "Undefined fx_partner on DB"
        )
        msg_error = f"*LEVEL:* `ERROR` \n*message:* `{msg_error}`\n\n"
        logger_task.error(msg_error)
        list_logs.append(msg_error)
        return

    filters = (
        Q(updated_at__lte=today),
    )
    fx_partner_percentage = FxPartnerPercentage.objects.filter(*filters).order_by("-updated_at").first()

    if(fx_partner_percentage is None):
        # Get just next from supplied date
        filters = (
            Q(updated_at__gte=today),
        )
        fx_partner_percentage = FxPartnerPercentage.objects.filter(*filters).order_by("updated_at").first()

    if(fx_partner_percentage is None):
        warning_msg = (
            "Undefined fx_partner on DB, using default 95%"
        )
        warning_msg = f"*LEVEL:* `WARNING` \n*message:* `{warning_msg}`\n\n"
        logger_task.warning(warning_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": warning_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        fx_partner_percentage = 0.95
    else:
        fx_partner_percentage = fx_partner_percentage.percentage_fx

    # Acumulators bulk create and update
    member_reports_betenlace_month_update = []
    member_reports_daily_betenlace_update = []
    member_reports_daily_betenlace_create = []

    member_reports_partner_month_update = []
    member_reports_daily_partner_update = []
    member_reports_daily_partner_create = []

    # Set keys by index based on colum names of Dataframe
    keys = {key: index for index, key in enumerate(df_member.columns.values)}

    list_logs = []
    for row in zip(*df_member.to_dict('list').values()):
        # Get link according to prom_code of current loop
        link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)
        if not link:
            warning_msg = (
                f"Link with prom_code={row[keys.get('prom_code')]} and campaign={campaign_title} "
                "not found on database"
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
                f"Betenlace CPA entry not found for link with prom_code={row[keys.get('prom_code')]}"
            )
            msg_error = f"*LEVEL:* `ERROR` \n*message:* `{msg_error}`\n\n"
            logger_task.error(msg_error)
            list_logs.append(msg_error)
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

        cpa_count = len(cpa_by_prom_code_iter.get(row[keys.get("prom_code")]))

        if(betenlace_daily):
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
            # keys, row, betenlace_daily, fixed_income_campaign, cpa_count, revenue_share_percentage
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

        # Get current partner that have the current link
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
            cpa_count_new = math.floor(cpa_count*partner_link_accumulated.tracker)
        else:
            cpa_count_new = cpa_count

        # verify if cpa_count had a change from tracker calculation
        if (cpa_count > cpa_count_new):
            # Reduce -1 additional for enum behavior
            diff_count = (cpa_count - cpa_count_new) - 1

            for enum, account_instance_i in enumerate(reversed(cpa_by_prom_code_iter.get(row[keys.get("prom_code")]))):
                # Remove cpa partner
                account_instance_i.cpa_partner = 0
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

        # Update month
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
            None)

        if(partner_link_daily):
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
                from_date=yesterday,
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

    join_list = "".join(list_logs)
    chat_logger_task.apply_async(
        kwargs={
            "msg": join_list,
            "msg_url": settings.CHAT_WEBHOOK_CELERY,
        },
    )

    with transaction.atomic(using=DB_USER_PARTNER):
        # Account case
        if(account_reports_create):
            AccountReport.objects.bulk_create(
                objs=account_reports_create,
            )
        if(account_reports_update):
            AccountReport.objects.bulk_update(
                objs=account_reports_update,
                fields=(
                    "net_revenue",
                    "fixed_income",
                    "net_revenue",
                    "revenue_share",
                    "cpa_betenlace",
                    "cpa_partner",
                    "cpa_at",
                ),
            )

        # Member case
        if(member_reports_betenlace_month_update):
            BetenlaceCPA.objects.bulk_update(
                objs=member_reports_betenlace_month_update,
                fields=(
                    "deposit",
                    "fixed_income",
                    "net_revenue",
                    "revenue_share",
                    "registered_count",
                    "cpa_count",
                    "wagering_count",
                ),
            )

        if(member_reports_daily_betenlace_update):
            BetenlaceDailyReport.objects.bulk_update(
                objs=member_reports_daily_betenlace_update,
                fields=(
                    "deposit",
                    "net_revenue",
                    "revenue_share",
                    "fixed_income",
                    "fixed_income_unitary",
                    "fx_partner",
                    "cpa_count",
                    "registered_count",
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
                    "wagering_count",
                    "tracker",
                    "tracker_deposit",
                    "tracker_registered_count",
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

    if (len(df_member.index) == 0):
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
        msg = f"Member for Campaign {campaign_title} processed count {len(df_member.index)}"
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
    from_date,
    partner_link_accumulated,
    account_report,
    cpa_by_prom_code_iter,
    revenue_share_percentage,
    cpa_condition_from_revenue_share,
    fixed_income_campaign,
):
    """
    Update account report data from row data like
    - first_deposit_at
    - revenue_share
    - prom_code

    prom_code is used to get the related link on database and sum iter
    count for easy tracker management, with revenue_share are calculated
    the net_revenue with `revenue_share_percentage` value, registered at
    must be alredy defined at punter data creation.
    """

    # Only positive value for accelerate cpa trigger 0 for negative case
    account_report.revenue_share += abs(row[keys.get("revenue_share")])
    account_report.net_revenue += row[keys.get("revenue_share")] / revenue_share_percentage

    if(not account_report.cpa_betenlace):
        account_report.partner_link_accumulated = partner_link_accumulated

    # condition from revenue share and not already cpa
    if (account_report.revenue_share >= cpa_condition_from_revenue_share and not account_report.cpa_betenlace):
        account_report.cpa_betenlace = 1
        account_report.cpa_at = from_date
        # Rushbet pay only Revenue share
        account_report.fixed_income = 0

        # Temp have value 1, later will removed
        account_report.cpa_partner = 0 if partner_link_accumulated is None else 1

        cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(account_report)

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
    # Check registrationdate null
    if (not pd.isna(row[keys.get("registered_at")])):
        registered_at = make_aware(datetime.strptime(row[keys.get("registered_at")], "%m/%d/%Y"))
    else:
        registered_at = None

    cpa_count = 0
    if (abs(row[keys.get("revenue_share")]) >= cpa_condition_from_revenue_share):
        cpa_count = 1

    account_report = AccountReport(
        partner_link_accumulated=partner_link_accumulated,
        punter_id=row[keys.get("punter_id")],
        # Rushbet pay only Revenue share
        fixed_income=0,
        net_revenue=row[keys.get("revenue_share")] / revenue_share_percentage,
        # Only positive value for accelerate cpa trigger 0 for negative case
        revenue_share=abs(row[keys.get("revenue_share")]),
        currency_condition=currency_condition,
        currency_fixed_income=currency_fixed_income,
        cpa_betenlace=cpa_count,
        cpa_partner=(0 if partner_link_accumulated is None else cpa_count),
        link=link,
        registered_at=registered_at,
        created_at=from_date,
    )

    if(cpa_count):
        # Case when cpa is True or 1
        account_report.cpa_at = from_date

        cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(account_report)
    return account_report


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
    fixed_income = cpa_count * fixed_income_campaign

    betenlace_daily.deposit = row[keys.get("deposit")]

    betenlace_daily.net_revenue = row[keys.get("revenue_share")] / revenue_share_percentage
    betenlace_daily.revenue_share = row[keys.get("revenue_share")]

    # Rushbet pay only Revenue share
    betenlace_daily.fixed_income = 0
    betenlace_daily.fixed_income_unitary = (
        fixed_income / cpa_count
        if cpa_count != 0
        else
        fixed_income_campaign
    )

    betenlace_daily.fx_partner = fx_partner

    betenlace_daily.cpa_count = cpa_count
    betenlace_daily.registered_count = row[keys.get('registered_count')]
    betenlace_daily.wagering_count = row[keys.get('wagering_count')]

    return betenlace_daily


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
    # Get fixed income accordng to cpa_count calculation
    fixed_income = cpa_count * fixed_income_campaign

    betenlace_daily = BetenlaceDailyReport(
        betenlace_cpa=betenlace_cpa,

        currency_condition=currency_condition,
        deposit=row[keys.get('deposit')],

        net_revenue=row[keys.get("revenue_share")]/revenue_share_percentage,
        revenue_share=row[keys.get("revenue_share")],

        currency_fixed_income=currency_fixed_income,
        # Rushbet pay only Revenue share
        fixed_income=0,
        fixed_income_unitary=(
            fixed_income / cpa_count
            if cpa_count != 0
            else
            fixed_income_campaign
        ),

        fx_partner=fx_partner,

        cpa_count=cpa_count,
        registered_count=row[keys.get('registered_count')],
        wagering_count=row[keys.get('wagering_count')],
        created_at=from_date,
    )

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
    - cpa_count
    - registered_count
    - first_deposit_count
    - wagering_count

    revenue_share calculated from net_revenue.

    The results are sum
    """
    # Rushbet pay only Revenue share
    betenlace_cpa.fixed_income += 0

    betenlace_cpa.deposit += row[keys.get("deposit")]

    betenlace_cpa.net_revenue += row[keys.get("revenue_share")] / revenue_share_percentage
    betenlace_cpa.revenue_share += row[keys.get("revenue_share")]

    betenlace_cpa.registered_count += row[keys.get('registered_count')]
    betenlace_cpa.cpa_count += cpa_count
    betenlace_cpa.wagering_count += row[keys.get('wagering_count')]
    return betenlace_cpa


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
    """
    Calculate Fx conversion according to campaign currency and partner
    currency. if both currency are same fx will value 1 in another case
    get respective value multiply by fx_percentage
    """
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

    partner_link_daily.deposit = tracked_data.get("deposit")
    partner_link_daily.registered_count = tracked_data.get("registered_count")
    partner_link_daily.wagering_count = tracked_data.get("wagering_count")

    partner_link_daily.tracker = partner_link_accumulated.tracker
    partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
    partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
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
        wagering_count=tracked_data.get("wagering_count"),

        tracker=partner_link_accumulated.tracker,
        tracker_deposit=partner_link_accumulated.tracker_deposit,
        tracker_registered_count=partner_link_accumulated.tracker_registered_count,
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
