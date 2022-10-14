import logging

from api_log.helpers import DB_HISTORY
from api_log.models import ClickTracking
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import BetenlaceDailyReport
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import (
    Q,
    Sum,
)
from django.utils.timezone import (
    datetime,
    make_aware,
    timedelta,
)
from tqdm import tqdm

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        """
        Arguments that have the custom command runserver
        """
        parser.add_argument(
            "-fd", "--fromdate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d"),
            help="Determine date from for get data of re-calculate clicks")
        parser.add_argument(
            "-td", "--todate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d"),
            help="Determine date to for get data of re-calculate clicks"
        )
        parser.add_argument("-onul", "--only_null", choices=["False", "True"], default="True",
                            help="Calculate clicks that only null, in another case will recalculate by date")

    def handle(self, *args, **options):
        logger.info("Recalculate click counts")
        logger.info(f"From date -> {options.get('fromdate')}")
        logger.info(f"To date -> {options.get('todate')}")
        logger.info(f"Only nulls -> {options.get('only_null')}")
        from_date_str = options.get("fromdate")
        to_date_str = options.get("todate")
        only_null = eval(options.get("only_null"))

        # If only null is false take dates
        if (not only_null):
            try:
                from_date = make_aware(datetime.strptime(from_date_str, "%Y/%m/%d"))
                to_date = make_aware(datetime.strptime(to_date_str, "%Y/%m/%d"))
                if from_date > to_date:
                    logger.error("\"From date\" is greather than \"to date\"")
                    return
            except:
                logger.error("'From date' or 'to date' have bad format. Expected format\"aaaa/mm/dd\"")
                return

        if(only_null):
            # Get list of dates
            filters = [Q(click_count__isnull=True)]
            betenlace_dailies = BetenlaceDailyReport.objects.filter(*filters)
            date_list = betenlace_dailies.values_list("created_at", flat=True)
        else:
            filters = [Q(created_at__gte=from_date, created_at__lte=to_date)]
            betenlace_dailies = BetenlaceDailyReport.objects.filter(*filters)
            date_list = betenlace_dailies.values_list("created_at", flat=True)

        # Agroup and sum the clicks by prom_code and campaign
        clicks_tracking = ClickTracking.objects.using(DB_HISTORY).filter(
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
        for betenlace_daily_i in tqdm(betenlace_dailies):
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
            BetenlaceDailyReport.objects.bulk_update(
                objs=betenlace_dailes_list,
                fields=(
                    "click_count",
                )
            )
