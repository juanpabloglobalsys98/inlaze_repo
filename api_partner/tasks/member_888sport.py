import json
import math
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
def member_888sport(campaign_title):
    """
    Get data from API of bookmaker 888Sport with CSV files using
    the pandas module with high performance, on command use tqdm for
    progress bar.

    Member report is the summarized data from all punters of range of date

    CSV columns
    ---
    - TrackingCodeDescription : `string`
        Equivalent to prom_code from Model `Link` and
        `MemberReport (Month, daily) for betenalce and partners`
    - GrossRevenue : `np.float32`
        equivalent to net_revene, Profit earned so far from that
        player/punter_id, usually this is the stake - 20% of stake
        (only if player/punter_id loss all bet), this take a positive
        value when player/punter_id loss money and take
        negative when player/punter_id won the bets. This is a sum of
        outcomes (results) of the bets has the player/punter_id placed
        and the bookmaker received a result. This not have the count
        of bets
    - Registrations : `np.uint32`
        Equivalent to registered_count. Number of punters that registered
    - MoneyPlayers : `np.uint32`
        MoneyPlayers equivalent to cpa_count
    - revenue_share
        This value is calculated from incoming net_reveue is 30% for Latam and brasil, 35%
        for spain

    # Index columns
    The columns of pandas dataframe are indexed for this way
    prom_code
    net_revenue
    registered_count
    cpa_count
    """

    # Definition of function
    today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    msg = (
        "Making call to API Account 888 Sport\n"
        f"Campaign Title -> {campaign_title}\n"
        f"From date -> {yesterday_str}\n"
        f"To date -> {yesterday_str}\n"
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

    if (campaign_title == "888sport latam"):
        username_888sport = settings.API_888SPORT_USERNAME_LATAM
        password_888sport = settings.API_888SPORT_PASSWORD_LATAM
        brand_group_name = '888.com'
        revenue_share_percentage = settings.API_888SPORT_RS_PERCENTAGE_LATAM

    elif (campaign_title == "888sport esp"):
        username_888sport = settings.API_888SPORT_USERNAME_ESP
        password_888sport = settings.API_888SPORT_PASSWORD_ESP
        brand_group_name = '888.es'
        revenue_share_percentage = settings.API_888SPORT_RS_PERCENTAGE_ESP

    elif (campaign_title == "888sport br"):
        username_888sport = settings.API_888SPORT_USERNAME_BR
        password_888sport = settings.API_888SPORT_PASSWORD_BR
        brand_group_name = '888.com'
        revenue_share_percentage = settings.API_888SPORT_RS_PERCENTAGE_BR

    elif (campaign_title == "888sport it"):
        username_888sport = settings.API_888SPORT_USERNAME_IT
        password_888sport = settings.API_888SPORT_PASSWORD_IT
        brand_group_name = "888.it"
        revenue_share_percentage = settings.API_888SPORT_RS_PERCENTAGE_IT

    url = "https://api.aff-online.com/login?"
    body = {
        'username': username_888sport,
        'password': password_888sport,
    }
    response_obj = requests.post(url=url, data=body)

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
            f"connection IP/VPN is on Whitelist of API server\n\n"
            f"request url: {url}\n"
            f"request body: {body}\n"
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

    if("ErrorMessage" in response):
        error_msg = (
            f"Error at get the access token of 888 sport with ErrorMessage:{response.get('ErrorMessage')}, "
            "Check VPN connection and ip whitelist"
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

    # Get dynamic access token
    access_token = response.get('AccessToken')
    token_type = response.get('TokenType')
    url = "https://api.aff-online.com/reports/traffic"
    body = {
        'BrandGroupName': brand_group_name,
        'FromDate': yesterday_str,
        'ToDate': yesterday_str,
    }
    authorization = f"{token_type} {access_token}"
    headers = {
        "Authorization": authorization,
    }

    response_obj = requests.post(
        url=url,
        data=body,
        headers=headers,
    )

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
            "Something is wrong at get data from API, Authorization are getted in bad way,\n\n"
            f"request url: {url}\n"
            f"request body: {body}\n"
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

    if not "TrafficStatRows" in response:
        error_msg = (
            "Bad authorization or params check!\n\n"
            f"request url: {url}\n"
            f"request body: {body}\n"
            f"response status:\n{response_obj.status_code}\n"
            f"response text:\n{response_obj.text}\n\n"
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

    # "Date", "TrackingCodeDescription", "TrackingCode", "Brand",
    # "PlayerAffinity", "GrossRevenue", "Registrations", "Leads",
    # "MoneyPlayers", "CommissionType", "CommissionCountry",
    # "PlayerCountry", "Anid", "PlayerDevice"

    if(response.get("TrafficStatRows")):
        df = pd.DataFrame(response.get("TrafficStatRows"))

        cols_to_use = [
            "TrackingCodeDescription",
            "GrossRevenue",
            "Registrations",
            "MoneyPlayers",
        ]
        df = df[cols_to_use]
        df = df.astype(
            {
                "TrackingCodeDescription": "string",
                "GrossRevenue": np.float32,
                "Registrations": np.uint32,
                "MoneyPlayers": np.uint32,
            },
            copy=False
        )
        df.rename(
            inplace=True,
            columns={
                "TrackingCodeDescription": "prom_code",
                "GrossRevenue": "net_revenue",
                "Registrations": "registered_count",
                "MoneyPlayers": "cpa_count",
            },
        )
    else:
        df = None

    if(df is None):
        msg_warning = f"No data for campaign {campaign_title} date_from {yesterday_str} date_to {yesterday_str}"
        msg_warning = f"*LEVEL:* `WARNING` \n*message:* `{msg_warning}`\n\n"
        logger_task.warning(msg_warning)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg_warning,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    # Row count 0 is no data case
    if (len(df.index) == 0):
        warning_msg = f"No data for campaign {campaign_title} date_from {yesterday_str} date_to {yesterday_str}"
        msg_warning = f"*LEVEL:* `WARNING` \n*message:* `{msg_warning}`\n\n"
        logger_task.warning(warning_msg)
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg_warning,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )
        return

    # Get related link from prom_codes and campaign, QUERY
    filters = [Q(prom_code__in=df.prom_code.unique()), Q(campaign_id=campaign.id)]
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
        msg_warning = (
            "Undefined fx_partner on DB, using default 95%"
        )
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
        - prom_code
        - net_revenue
        - registered_count
        - cpa_count
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

        # Betenlace Month
        betenlace_cpa = _betenlace_month_update(
            keys=keys,
            row=row,
            betenlace_cpa=betenlace_cpa,
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
                revenue_share_percentage=revenue_share_percentage,
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
                revenue_share_percentage=revenue_share_percentage,
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
            # Validate if campaign status is equal to INACTIVE and last inactive at is great than
            if (campaign.status == Campaign.Status.INACTIVE and yesterday.date() >= campaign.last_inactive_at.date()):
                msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
                logger_task.warning(msg)
                list_logs.append(msg)
                continue
        elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
            msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
            msg = f"*LEVEL:* `WARNING` \n*message:* `{msg}`\n\n"
            logger_task.warning(msg)
            list_logs.append(msg)
            continue

        # Tracker
        if(row[keys.get('cpa_count')] > settings.MIN_CPA_TRACKER_DAY):
            cpa_count = math.floor(row[keys.get('cpa_count')]*partner_link_accumulated.tracker)
        else:
            cpa_count = row[keys.get('cpa_count')]

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

        # Update month
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
                    "fixed_income",
                    "net_revenue",
                    "revenue_share",
                    "registered_count",
                    "cpa_count",
                ),
            )

        if(member_reports_daily_betenlace_update):
            BetenlaceDailyReport.objects.bulk_update(
                objs=member_reports_daily_betenlace_update,
                fields=(
                    "net_revenue",
                    "revenue_share",
                    "fixed_income",
                    "fixed_income_unitary",
                    "fx_partner",
                    "registered_count",
                    "cpa_count",
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
                    # "deposit",
                    "registered_count",
                    # "first_deposit_count",
                    # "wagering_count",
                    "tracker",
                    # "tracker_deposit",
                    "tracker_registered_count",
                    # "tracker_first_deposit_count",
                    # "tracker_wagering_count",
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
    # if (keys.get("deposit") is not None):
    #     tracked_data["deposit"] = row[keys.get("deposit")]*partner_link_accumulated.tracker_deposit

    if (keys.get("registered_count") is not None):
        if(row[keys.get("registered_count")] > 1):
            tracked_data["registered_count"] = math.floor(
                row[keys.get("registered_count")]*partner_link_accumulated.tracker_registered_count
            )
        else:
            tracked_data["registered_count"] = row[keys.get("registered_count")]

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

        # deposit=tracked_data.get("deposit"),
        registered_count=tracked_data.get("registered_count"),
        # first_deposit_count=tracked_data.get("first_deposit_count"),
        # wagering_count=tracked_data.get("wagering_count"),

        tracker=partner_link_accumulated.tracker,
        # tracker_deposit=partner_link_accumulated.tracker_deposit,
        tracker_registered_count=partner_link_accumulated.tracker_registered_count,
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

    # partner_link_daily.deposit = tracked_data.get("deposit")
    partner_link_daily.registered_count = tracked_data.get("registered_count")
    # partner_link_daily.first_deposit_count = tracked_data.get("first_deposit_count")
    # partner_link_daily.wagering_count = tracked_data.get("wagering_count")

    partner_link_daily.tracker = partner_link_accumulated.tracker
    # partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
    partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
    # partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
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
    revenue_share_percentage,
    fx_partner,
):

    fixed_income = campaign.fixed_income_unitary * row[keys.get('cpa_count')]

    betenlace_daily = BetenlaceDailyReport(
        betenlace_cpa=betenlace_cpa,

        currency_condition=campaign.currency_condition,
        net_revenue=row[keys.get('net_revenue')],
        revenue_share=row[keys.get('net_revenue')] * revenue_share_percentage,

        currency_fixed_income=campaign.currency_fixed_income,
        fixed_income=fixed_income,
        fixed_income_unitary=(
            fixed_income / row[keys.get('cpa_count')]
            if row[keys.get('cpa_count')] != 0
            else
            campaign.fixed_income_unitary
        ),

        fx_partner=fx_partner,

        registered_count=row[keys.get('registered_count')],
        cpa_count=row[keys.get('cpa_count')],
        created_at=from_date,
    )

    return betenlace_daily


def _betenlace_daily_update(
    keys,
    row,
    betenlace_daily,
    campaign,
    revenue_share_percentage,
    fx_partner,
):
    betenlace_daily.net_revenue = row[keys.get('net_revenue')]
    betenlace_daily.revenue_share = row[keys.get('net_revenue')] * revenue_share_percentage

    betenlace_daily.fixed_income = campaign.fixed_income_unitary * row[keys.get('cpa_count')]
    betenlace_daily.fixed_income_unitary = (
        betenlace_daily.fixed_income / row[keys.get('cpa_count')]
        if row[keys.get('cpa_count')] != 0
        else
        campaign.fixed_income_unitary
    )

    betenlace_daily.fx_partner = fx_partner

    betenlace_daily.registered_count = row[keys.get('registered_count')]
    betenlace_daily.cpa_count = row[keys.get('cpa_count')]
    return betenlace_daily


def _betenlace_month_update(
    keys,
    row,
    betenlace_cpa,
    campaign,
    revenue_share_percentage,
):
    betenlace_cpa.fixed_income += campaign.fixed_income_unitary * row[keys.get('cpa_count')]
    betenlace_cpa.net_revenue += row[keys.get('net_revenue')]
    betenlace_cpa.revenue_share += row[keys.get('net_revenue')] * revenue_share_percentage
    betenlace_cpa.registered_count += row[keys.get('registered_count')]
    betenlace_cpa.cpa_count += row[keys.get('cpa_count')]
    return betenlace_cpa
