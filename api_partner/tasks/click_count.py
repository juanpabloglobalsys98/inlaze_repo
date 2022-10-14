import pytz
from api_log.helpers import DB_HISTORY
from api_log.models import ClickTracking
from api_partner.helpers import (
    get_client_ip,
    make_iplist_call,
)
from api_partner.models import (
    BetenlaceDailyReport,
    Campaign,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from betenlace.celery import app
from celery.utils.log import get_task_logger
from core.helpers import CurrencyPartner
from django.conf import settings
from django.db.models import Q
from django.utils import timezone
from urllib3.exceptions import ProtocolError

logger_task = get_task_logger(__name__)


@app.task(
    ignore_result=True,
    autoretry_for=(
        ConnectionError,
        ProtocolError,
    ),
    retry_kwargs={"max_retries": 5},
    retry_backoff=True,
    retry_backoff_max=30,
    retry_jitter=True,
)
def click_count(
    link_pk,
    c_currency_condition,
    c_currency_fixed_income,
    ip_client,
):
    """
        Get link from clicks/count and add click record into Betenlacedaily model
        if exists or create if not exists
    """
    logger_task.info("Starting cpa sum")
    # Get current time in Time zone of settings
    today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))

    filters = (
        Q(betenlace_cpa__pk=link_pk),
        Q(created_at=today.date()),
    )
    # Get related betenlace daily, each link have betenalcecpa with same pk
    # of today
    betenlace_daily = BetenlaceDailyReport.objects.filter(*filters).first()
    filters = (Q(link_to_campaign__pk=link_pk),)
    campaign = Campaign.objects.filter(*filters).first()

    if campaign.status == Campaign.Status.INACTIVE:
        partner_link_accumulated = None
    else:
        # Get related Partnerlinkaccumulated, to link_pk
        filters = (
            Q(link_to_partner_link_accumulated__pk=link_pk),
        )
        partner_link_accumulated = PartnerLinkAccumulated.objects.filter(*filters).first()

    # Case not found data
    if (betenlace_daily is None):
        betenlace_daily = BetenlaceDailyReport.objects.create(
            betenlace_cpa_id=link_pk,
            currency_condition=c_currency_condition,
            currency_fixed_income=c_currency_fixed_income,
            created_at=today.date(),
        )

        # Create relation with partner link daily if this have current relation
        if (partner_link_accumulated is not None):
            PartnerLinkDailyReport.objects.create(
                partner_link_accumulated=partner_link_accumulated,
                betenlace_daily_report=betenlace_daily,
                adviser_id=partner_link_accumulated.partner.adviser_id,
                currency_fixed_income=c_currency_fixed_income,
                currency_local=CurrencyPartner.USD,
                created_at=today.date(),
            )

    # Click count initial value
    if (betenlace_daily.click_count is None):
        betenlace_daily.click_count = 0

    if ip_client:
        _ip_not_null_click_report(link_pk, partner_link_accumulated, today, betenlace_daily, ip_client)
        # Case ip is null
    else:
        _ip_null_click_report(link_pk, partner_link_accumulated, today, betenlace_daily)

    betenlace_daily.save()
    logger_task.info("Ending cpa sum")


def _create_clickreport(link_pk, partner_link_accumulated, ip):
    ip_detail = make_iplist_call(ip)
    if ip_detail and ip_detail.get("registry") != 'PRIVATE':
        click_tracking = ClickTracking(
            partner_link_accumulated_id=partner_link_accumulated.pk if partner_link_accumulated else None,
            link_id=link_pk,
            ip=ip_detail.get("ip", None),
            registry=ip_detail.get("registry", None),
            countrycode=ip_detail.get("countrycode", None),
            countryname=ip_detail.get("countryname", None),
            city=ip_detail.get("city", None),
            spam=ip_detail.get("spam", None),
            tor=ip_detail.get("tor", None)
        )
        if asn := ip_detail.get("asn"):
            click_tracking.asn_code = asn.get("code", None)
            click_tracking.asn_name = asn.get("name")
            click_tracking.asn_route = asn.get("route")
            click_tracking.asn_start = asn.get("start")
            click_tracking.asn_end = asn.get("end")
            click_tracking.asn_count = asn.get("count")
            click_tracking.save(using=DB_HISTORY)
        return True
    else:
        # Click with Null details
        ClickTracking.objects.using(DB_HISTORY).create(
            partner_link_accumulated_id=partner_link_accumulated.pk if partner_link_accumulated else None,
            link_id=link_pk,
            ip=ip,
            registry=None,
            countrycode=None,
            countryname=None,
            city=None,
            spam=None,
            tor=None,
            asn_code=None,
            asn_name=None,
            asn_route=None,
            asn_start=None,
            asn_end=None,
            asn_count=None,
        )
    return False


def _ip_not_null_click_report(
    link_pk,
    partner_link_accumulated,
    today,
    betenlace_daily,
    ip_client,
):
    # Special case for some vpns
    if "," in ip_client:
        ips = ip_client.split(",")
        for ip_i in ips:
            _ip_not_null_click_report(
                link_pk=link_pk,
                partner_link_accumulated=partner_link_accumulated,
                today=today,
                betenlace_daily=betenlace_daily,
                ip_client=ip_i,
            )
        return
    filters = (
        Q(ip=ip_client),
        Q(link_id=link_pk),
    )
    click_tracking = ClickTracking.objects.using(DB_HISTORY).filter(
        *filters,
    ).order_by("-created_at").first()

    # State for catch error and flush on log
    state = None
    if click_tracking:
        less_time = today.timestamp() - click_tracking.created_at.timestamp()
        if less_time < settings.CLICK_PERIOD_SECONDS:
            click_tracking.count += 1
            click_tracking.save()
            state = True
        else:
            state = _create_clickreport(link_pk, partner_link_accumulated, ip_client)
    else:
        state = _create_clickreport(link_pk, partner_link_accumulated, ip_client)

    if not state:
        logger_task.error(f"Error with ip_client, failed to log with ipclient {ip_client} to link id: {link_pk}")

    # Increase click count
    betenlace_daily.click_count += 1


def _create_clickreport_without(link_pk, partner_link_accumulated):
    ClickTracking.objects.using(DB_HISTORY).create(
        partner_link_accumulated_id=partner_link_accumulated.pk if partner_link_accumulated else None,
        link_id=link_pk,
        ip=None,
        registry=None,
        countrycode=None,
        countryname=None,
        city=None,
        spam=None,
        tor=None,
        asn_code=None,
        asn_name=None,
        asn_route=None,
        asn_start=None,
        asn_end=None,
        asn_count=None,
    )


def _ip_null_click_report(link_pk, partner_link_accumulated, today, betenlace_daily):
    filters = (
        Q(ip__isnull=True),
        Q(link_id=link_pk),
    )
    click_tracking = ClickTracking.objects.using(DB_HISTORY).filter(
        *filters,
    ).order_by("-created_at").first()
    if click_tracking:
        less_time = today.timestamp() - click_tracking.created_at.timestamp()
        if less_time < settings.CLICK_PERIOD_SECONDS:
            click_tracking.count += 1
            click_tracking.save()
        else:
            _create_clickreport_without(link_pk, partner_link_accumulated)
    else:
        _create_clickreport_without(link_pk, partner_link_accumulated)

    betenlace_daily.click_count += 1
    logger_task.error(f"Error to fetch ip_client link id: {link_pk}")
