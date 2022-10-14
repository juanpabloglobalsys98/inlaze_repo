import json
import sys
import traceback

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
def member_galera_bet(campaign_title, is_today=True):
    """
    Get data from API of bookmaker Member Galera bet with CSV files using 
    the pandas module with high performance

    Member report is the summarized data from all punters of range of date

    Yesterday is date based for update the data, bool is_today allow to update
    data of the same current day, in another case update data of the previous 
    day
    """

    # Definition of function
    today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))

    # If this runing is today update today, in another case will update
    # yesterday
    if(is_today):
        update_datetime = today
    else:
        update_datetime = today - timedelta(days=1)

    update_datetime_str = update_datetime.strftime("%Y/%m/%d")
    msg = (
        "Making call to API Galera.Bet\n"
        f"Campaign Title -> {campaign_title}\n"
        f"From date -> {update_datetime_str}\n"
        f"To date -> {update_datetime_str}\n"
        f"is_today -> {is_today}"
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
    filters = [
        Q(campaign_title__iexact=campaign_title),
    ]
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

    if (campaign_title == "galera.bet br"):
        galera_password = settings.API_GALERABET_BR_PASSWORD
        galera_username = settings.API_GALERABET_BR_USERNAME
        revenue_share_percentage = settings.API_GALERABET_BR_RS_PERCENTAGE

    url = "https://glraff.com/global/api/User/signIn"
    body = {
        "password": galera_password,
        "username": galera_username,
    }
    # Login into galera
    try:
        response_login = requests.post(
            url=url,
            json=body,
        )
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            f"Something is wrong at login into Galera\n"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"response dict: {response_login.text}\n\n"
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

    if response_login.status_code != 200:
        error_msg = (
            "Not status 200 when try to login into Galera, check credentials\n"
            f"Response text: {response_login.text}"
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

    # Get cookie auth
    try:
        set_cookie = response_login.headers.get("set-cookie")
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            "Something is wrong at get data from API, check if current username, password and status of server\n"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"response status:\n{response_login.status_code}\n"
            f"response text:\n{response_login.text}\n\n"
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

    # Get data from specific date
    url = "https://glraff.com/global/api/Statistics/getPlayersLinksStatistics"
    headers = {
        "Cookie": set_cookie,
    }
    body = {
        "filter": {
            "date": {
                "action": "between",
                "from": update_datetime_str,
                "to": update_datetime_str,
                "valueLabel": f"{update_datetime_str} - {update_datetime_str}"
            }
        },
        # Limit -1 means ALL data
        "limit": -1,
        "start": 0,
    }

    try:
        response_obj = requests.post(
            url=url,
            json=body,
            headers=headers,
        )
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        error_msg = (
            f"Something is wrong at get data from API csv exported\n"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"request headers: {headers}\n\n"
            f"response dict: {response_obj.text}\n\n"
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

    if(response_obj.status_code != 200):
        error_msg = f"request not success with code: {response_obj.status_code}, message: {response_obj.text}"
        error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
        logger_task.error(error_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": error_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    response_data = json.loads(response_obj.text)

    if(response_data.get("result") == "ex"):
        error_msg = f"request result exception.\n\n {response_obj.text}"
        error_msg = f"*LEVEL:* `ERROR` \n*message:* `{error_msg}`\n\n"
        logger_task.error(error_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": error_msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    #  ['affiliateId' 'name' 'linkId' 'createDate' 'marketingSourceName'
    #  'website' 'marketingSourceId' 'clickLink' 'signUp' 'ratio' 'playersCount'
    #  'deposits' 'turnover' 'profitness' 'commissions' 'grossRevenue'
    #  'netRevenue' 'NDDACC' 'NDACC']

    cols_to_use = [
        "name",
        # "signUp",
        # "NDACC",
        "deposits",
        "turnover",
        "netRevenue",
    ]
    df = pd.DataFrame(data=response_data.get("result").get("records"))
    df = df[cols_to_use]
    df = df.astype(
        {
            "name": "string",
            # "signUp": np.uint32,
            # "NDACC": np.uint32,
            "deposits": np.float32,
            "turnover": np.float32,
            "netRevenue": np.float32,
        },
        copy=False,
    )
    df.rename(
        inplace=True,
        columns={
            "name": "prom_code",
            # "signUp": "registered_count",
            # "NDACC": "first_deposit_count",
            "deposits": "deposit",
            "turnover": "stake",
            "netRevenue": "net_revenue",
        },
    )

    # Remove data with ALL zeros
    df.drop(
        labels=df[
            df.eval(
                # "(registered_count == 0) & "
                # "(first_deposit_count == 0) & "
                "(deposit == 0) & "
                "(stake == 0) & "
                "(net_revenue == 0)",
                engine="numexpr",
            )
        ].index,
        inplace=True,
    )

    # Check if dataframe was empty
    if (df.empty):
        warning_msg = (
            f"Data not found at requested url with date \"{update_datetime_str}\"\n"
            f"Request url: {url}\n\n"
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
        Q(created_at=update_datetime.date()),
    )
    betenlace_daily_reports = BetenlaceDailyReport.objects.filter(*filters)

    filters = (
        Q(betenlace_daily_report__in=betenlace_daily_reports),
    )
    partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(*filters)

    # Get the Fx of previous day (today at 02:00 or today lte)
    fx_created_at = update_datetime.replace(minute=0, hour=0, second=0, microsecond=0)
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

    fx_partner_percentage = fx_partner.fx_percentage

    currency_condition_str = campaign.currency_condition.lower()
    currency_fixed_income_str = campaign.currency_fixed_income.lower()

    # Acumulators bulk create and update
    # BetenlaceCPA
    member_reports_betenlace_month_update = []
    # Betenlace Daily Report update
    member_reports_daily_betenlace_update = []
    # Betenlace Daily Report create
    member_reports_daily_betenlace_create = []

    # Partner Link Daily Report update
    member_reports_daily_partner_update = []
    # Partner Link Daily Report create
    member_reports_daily_partner_create = []

    keys = {key: index for index, key in enumerate(df.columns.values)}

    list_logs = []
    for row in zip(*df.to_dict('list').values()):
        """
        - prom_code
        - registered_count
        - first_deposit_count
        - deposit
        - stake
        - net_revenue
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

        # Betenlace Daily -  Betenlace Month
        betenlace_daily = next(
            filter(
                lambda betenlace_daily: (
                    betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                    betenlace_daily.created_at == update_datetime.date()
                ),
                betenlace_daily_reports,
            ),
            None,
        )

        if(betenlace_daily):
            betenlace_daily, betenlace_cpa = _betenlace_daily_update(
                keys=keys,
                row=row,
                betenlace_daily=betenlace_daily,
                fx_partner=fx_partner,
                betenlace_cpa=betenlace_cpa,
                revenue_share_percentage=revenue_share_percentage,
            )
            member_reports_daily_betenlace_update.append(betenlace_daily)
            member_reports_betenlace_month_update.append(betenlace_cpa)
        else:
            betenlace_daily, betenlace_cpa = _betenlace_daily_create(
                from_date=update_datetime.date(),
                keys=keys,
                row=row,
                betenlace_cpa=betenlace_cpa,
                campaign=campaign,
                fx_partner=fx_partner,
                revenue_share_percentage=revenue_share_percentage,
            )
            member_reports_daily_betenlace_create.append(betenlace_daily)
            member_reports_betenlace_month_update.append(betenlace_cpa)

        # Partner Month
        partner_link_accumulated = link.partner_link_accumulated

        # When partner have not assigned the link must be continue to next loop
        if(partner_link_accumulated is None):
            continue

        # Validate if link has relationship with partner and if has verify if status is equal to status campaign
        if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
            # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
            if (campaign.status == Campaign.Status.INACTIVE and update_datetime.date() >= campaign.last_inactive_at.date()):
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

        tracked_data = _get_tracker_values(
            keys=keys,
            row=row,
            partner_link_accumulated=partner_link_accumulated,
        )

        # Get currency local from partner link accumulated
        partner_currency_str = partner_link_accumulated.currency_local.lower()

        # Fx Currency Fixed income
        fx_fixed_income_partner = _calc_fx(
            fx_partner=fx_partner,
            fx_partner_percentage=fx_partner_percentage,
            currency_from_str=currency_fixed_income_str,
            partner_currency_str=partner_currency_str,
        )

        # Calculate fixed income for partner, on this case only for create
        fixed_income_partner_unitary = campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa
        fixed_income_partner = 0
        fixed_income_partner_unitary_local = (
            fixed_income_partner_unitary *
            fx_fixed_income_partner
        )
        fixed_income_partner_local = 0

        # Fx Currency Condition
        fx_condition_partner = _calc_fx(
            fx_partner=fx_partner,
            fx_partner_percentage=fx_partner_percentage,
            currency_from_str=currency_condition_str,
            partner_currency_str=partner_currency_str,
        )

        # Partner Daily
        partner_link_daily = next(
            filter(
                lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id == betenlace_daily.id,
                partner_link_dailies_reports,
            ),
            None,
        )

        if(partner_link_daily):
            # Recalculate fixed_incomes for update
            partner_link_daily = _partner_link_daily_update(
                tracked_data=tracked_data,
                fx_fixed_income_partner=fx_fixed_income_partner,
                fx_condition_partner=fx_condition_partner,
                fx_partner_percentage=fx_partner_percentage,
                fixed_income_partner_unitary=fixed_income_partner_unitary,
                fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                partner_link_daily=partner_link_daily,
                partner_link_accumulated=partner_link_accumulated,
                betenlace_daily=betenlace_daily,
                partner=partner_link_accumulated.partner,
            )
            member_reports_daily_partner_update.append(partner_link_daily)
        else:
            partner_link_daily = _partner_link_daily_create(
                from_date=update_datetime.date(),
                campaign=campaign,
                betenlace_daily=betenlace_daily,
                partner_link_accumulated=partner_link_accumulated,
                cpa_count=0,
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

                    "net_revenue",
                    "revenue_share",

                    # "registered_count",
                    # "first_deposit_count",
                ),
            )

        if(member_reports_daily_betenlace_update):
            BetenlaceDailyReport.objects.bulk_update(
                objs=member_reports_daily_betenlace_update,
                fields=(
                    "deposit",
                    "stake",

                    "net_revenue",
                    "revenue_share",

                    "fixed_income_unitary",

                    "fx_partner",

                    # "registered_count",
                    # "first_deposit_count",
                ),
            )

        if(member_reports_daily_betenlace_create):
            BetenlaceDailyReport.objects.bulk_create(
                objs=member_reports_daily_betenlace_create,
            )

        if(member_reports_daily_partner_update):
            PartnerLinkDailyReport.objects.bulk_update(
                objs=member_reports_daily_partner_update,
                fields=(
                    # "fixed_income",
                    # "fixed_income_unitary",

                    # "fx_book_local",
                    "fx_book_net_revenue_local",
                    "fx_percentage",

                    # "cpa_count",
                    # "fixed_income_local",
                    # "fixed_income_unitary_local",
                    "deposit",
                    # "registered_count",
                    # "first_deposit_count",
                    # "wagering_count",
                    # "tracker",
                    "tracker_deposit",
                    # "tracker_registered_count",
                    # "tracker_first_deposit_count",
                    # "tracker_wagering_count",

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

    # if (keys.get("registered_count") is not None):
    #     if(row[keys.get("registered_count")] > 1):
    #         tracked_data["registered_count"] = math.floor(
    #             row[keys.get("registered_count")]*partner_link_accumulated.tracker_registered_count
    #         )
    #     else:
    #         tracked_data["registered_count"] = row[keys.get("registered_count")]

    # if (keys.get("first_deposit_count") is not None):
    #     if(row[keys.get("first_deposit_count")] > 1):
    #         tracked_data["first_deposit_count"] = math.floor(
    #             row[keys.get("first_deposit_count")]*partner_link_accumulated.tracker_first_deposit_count
    #         )
    #     else:
    #         tracked_data["first_deposit_count"] = row[keys.get("first_deposit_count")]

    # if (keys.get("wagering_count") is not None):
    #     if(row[keys.get("wagering_count")] > 1):
    #         tracked_data["wagering_count"] = math.floor(
    #             row[keys.get("wagering_count")]*partner_link_accumulated.tracker_wagering_count
    #         )
    #     else:
    #         tracked_data["wagering_count"] = row[keys.get("wagering_count")]

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


def _betenlace_daily_update(
    keys,
    row,
    betenlace_daily,
    fx_partner,
    betenlace_cpa,
    revenue_share_percentage,
):
    revenue_share_calculate = row[keys.get('net_revenue')] * revenue_share_percentage

    # get difference from current daily to update previous to update
    difference_deposit = row[keys.get('deposit')] - (betenlace_daily.deposit or 0)
    difference_stake = row[keys.get('stake')] - (betenlace_daily.stake or 0)
    difference_net_revenue = row[keys.get('net_revenue')] - (betenlace_daily.net_revenue or 0)
    difference_revenue_share = revenue_share_calculate - (betenlace_daily.revenue_share or 0)
    # difference_registered_count = row[keys.get('registered_count')] - (betenlace_daily.registered_count or 0)
    # difference_first_deposit_count = (
    #     row[keys.get('first_deposit_count')] - (betenlace_daily.first_deposit_count or 0)
    # )

    # Betenlace Month update
    betenlace_cpa.deposit += difference_deposit
    betenlace_cpa.stake += difference_stake
    betenlace_cpa.net_revenue += difference_net_revenue
    betenlace_cpa.revenue_share += difference_revenue_share
    # betenlace_cpa.registered_count += difference_registered_count
    # betenlace_cpa.first_deposit_count += difference_first_deposit_count

    # Betenlace daily update
    betenlace_daily.deposit = row[keys.get('deposit')]
    betenlace_daily.stake = row[keys.get('stake')]
    betenlace_daily.net_revenue = row[keys.get('net_revenue')]
    # Revenue share is calculated from net revenue according to current revenue share value
    betenlace_daily.revenue_share = (betenlace_daily.net_revenue*revenue_share_percentage)

    betenlace_daily.fx_partner = fx_partner

    # betenlace_daily.registered_count = row[keys.get('registered_count')]
    # betenlace_daily.first_deposit_count = row[keys.get('first_deposit_count')]
    return betenlace_daily, betenlace_cpa


def _betenlace_daily_create(
    from_date,
    keys,
    row,
    betenlace_cpa,
    campaign,
    fx_partner,
    revenue_share_percentage,
):

    # Betenlace month update
    betenlace_cpa.deposit += row[keys.get('deposit')]
    betenlace_cpa.stake += row[keys.get('stake')]
    betenlace_cpa.net_revenue += row[keys.get('net_revenue')]
    betenlace_cpa.revenue_share += row[keys.get('net_revenue')]*revenue_share_percentage
    # betenlace_cpa.registered_count += row[keys.get('registered_count')]
    # betenlace_cpa.first_deposit_count += row[keys.get('first_deposit_count')]

    # Betenlace Daily
    betenlace_daily = BetenlaceDailyReport(
        betenlace_cpa=betenlace_cpa,

        currency_condition=campaign.currency_condition,
        deposit=row[keys.get('deposit')],
        stake=row[keys.get('stake')],

        net_revenue=row[keys.get('net_revenue')],
        # Revenue share is calculated based on net_revenue
        revenue_share=row[keys.get('net_revenue')]*revenue_share_percentage,

        currency_fixed_income=campaign.currency_fixed_income,
        fixed_income=0,
        fixed_income_unitary=campaign.fixed_income_unitary,

        fx_partner=fx_partner,

        cpa_count=0,
        # registered_count=row[keys.get('registered_count')],
        # first_deposit_count=row[keys.get('first_deposit_count')],
        created_at=from_date,
    )

    return betenlace_daily, betenlace_cpa


def _partner_link_daily_update(
    tracked_data,
    fx_fixed_income_partner,
    fx_condition_partner,
    fx_partner_percentage,
    fixed_income_partner_unitary,
    fixed_income_partner_unitary_local,
    partner_link_daily,
    partner_link_accumulated,
    betenlace_daily,
    partner,
):

    # partner_link_daily.fx_book_local = fx_fixed_income_partner
    partner_link_daily.fx_book_net_revenue_local = fx_condition_partner
    partner_link_daily.fx_percentage = fx_partner_percentage

    # partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary
    # partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

    partner_link_daily.deposit = tracked_data.get("deposit")
    # partner_link_daily.registered_count = tracked_data.get("registered_count")
    # partner_link_daily.first_deposit_count = tracked_data.get("first_deposit_count")
    # partner_link_daily.wagering_count = tracked_data.get("wagering_count")

    # partner_link_daily.tracker = partner_link_accumulated.tracker
    partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
    # partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
    # partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
    # partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

    # Calculate Adviser payment
    partner_link_daily.adviser_id = partner.adviser_id
    partner_link_daily.fixed_income_adviser_percentage = partner.fixed_income_adviser_percentage
    partner_link_daily.net_revenue_adviser_percentage = partner.net_revenue_adviser_percentage

    # Update fixed income with current cpa value
    # if (partner_link_daily.cpa_count is not None):
    #     partner_link_daily.fixed_income = partner_link_daily.fixed_income_unitary * partner_link_daily.cpa_count
    #     partner_link_daily.fixed_income_local = partner_link_daily.fixed_income_unitary_local * partner_link_daily.cpa_count

    #     # Adviser case
    #     if (partner.fixed_income_adviser_percentage is None):
    #         partner_link_daily.fixed_income_adviser = None
    #         partner_link_daily.fixed_income_adviser_local = None
    #     else:
    #         partner_link_daily.fixed_income_adviser = (
    #             partner_link_daily.fixed_income *
    #             partner.fixed_income_adviser_percentage
    #         )
    #         partner_link_daily.fixed_income_adviser_local = (
    #             partner_link_daily.fixed_income_adviser *
    #             fx_fixed_income_partner
    #         )

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

    ######
    # Calculate referred payment
    partner_link_daily.referred_by = partner.referred_by
    partner_link_daily.fixed_income_referred_percentage = partner.fixed_income_referred_percentage
    partner_link_daily.net_revenue_referred_percentage = partner.net_revenue_referred_percentage

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
        # registered_count=tracked_data.get("registered_count"),
        # first_deposit_count=tracked_data.get("first_deposit_count"),
        # wagering_count=tracked_data.get("wagering_count"),

        tracker=partner_link_accumulated.tracker,
        tracker_deposit=partner_link_accumulated.tracker_deposit,
        # tracker_registered_count=partner_link_accumulated.tracker_registered_count,
        # tracker_first_deposit_count=partner_link_accumulated.tracker_first_deposit_count,
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
