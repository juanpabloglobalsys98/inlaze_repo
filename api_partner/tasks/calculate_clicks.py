from api_log.helpers import DB_HISTORY
from api_log.models import ClickTracking
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import BetenlaceDailyReport
from betenlace.celery import app
from celery.utils.log import get_task_logger
from django.db import transaction
from django.db.models import (
    Q,
    Sum,
)

logger_task = get_task_logger(__name__)


@app.task(
    ignore_result=True,
)
def calculate_clicks():
    logger_task.info("Recalculate click counts, only null values at member report")

    # Get list of dates
    filters = [Q(click_count__isnull=True)]
    betenlace_dailies = BetenlaceDailyReport.objects.filter(*filters)
    date_list = betenlace_dailies.values_list("created_at", flat=True)

    # Agroup and sum the clicks by prom_code and campaign
    clicks_tracking = ClickTracking.using(DB_HISTORY).objects.filter(
        created_at__date__in=date_list,
    ).values(
        "link_id",
        "created_at__date",
    ).annotate(
        click_count=Sum(
            "count",
        ),
    )

    betenlace_dailes_list = []
    for betenlace_daily_i in betenlace_dailies:
        click_i = next(
            filter(
                lambda click_i: (
                    click_i.get("created_at__date") == betenlace_daily_i.created_at and
                    click_i.get("link_id") == betenlace_daily_i.betenlace_cpa.pk
                ),
                clicks_tracking
            ),
            None
        )

        # Case for no reason no clicks for that link on that day
        if (click_i is None):
            betenlace_daily_i.click_count = 0
            betenlace_dailes_list.append(betenlace_daily_i)
            continue

        betenlace_daily_i.click_count = click_i.get("click_count")
        betenlace_dailes_list.append(betenlace_daily_i)

    with transaction.atomic(using=DB_USER_PARTNER):
        if(betenlace_dailes_list):
            BetenlaceDailyReport.objects.bulk_update(
                objs=betenlace_dailes_list,
                fields=(
                    "click_count",
                )
            )
        else:
            logger_task.warning("No clicks to update")
