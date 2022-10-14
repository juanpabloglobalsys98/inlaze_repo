import json
import logging

import pytz
import requests
from core.helpers import CurrencyAll
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.utils.timezone import (
    datetime,
    make_aware,
    timedelta,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.fastforex.io/historical?"


class Command(BaseCommand):
    def add_arguments(self, parser):
        """
        Arguments that have the custom command runserver
        """
        parser.add_argument("-d", "--date",
                            default=(
                                timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE)) -
                                timedelta(days=1)
                            ).strftime("%Y-%m-%d"),
                            ),
        parser.add_argument(
            "-cf", "--currency_from",
            default="USD",
            choices=CurrencyAll.values
        )
        parser.add_argument(
            "-ct", "--currency_to",
            default="",
            choices=CurrencyAll.values + [""]
        )

    def handle(self, *args, **options):
        logger.debug("Starting FX call")

        date_str = options.get("date")
        currency_from = options.get("currency_from")
        currency_to = options.get("currency_to")

        try:
            date = make_aware(datetime.strptime(date_str, "%Y-%m-%d"))
        except:
            logger.error("from_date or to_date have bad format. Expected format\"AAAA/mm/dd\"")
            return

        logger.info(f"date -> {date_str}")
        logger.info(f"currency from -> {currency_from}")
        logger.info(f"currency to -> {currency_to}")

        try:
            request_fx = requests.get(BASE_URL, params={
                "date": date_str,
                "from": currency_from,
                "api_key": settings.API_KEY_FX
            })
        except Exception as e:
            logger.error(f"An error in the request FX from {currency_from} \n\n{''.join(e)}")

        if("error" in request_fx):
            logger.critical(f"Api FX sends error fat get COP data, message:{request_fx.text}")
            return

        currency_transform = json.loads(request_fx.text)

        if(currency_to):
            currency_transform.get('results').get(currency_to)
            logger.info(
                f"Currency from {currency_from} to {currency_to} have value "
                f"{currency_transform.get('results').get(currency_to)}")

        for key in currency_transform.get("results"):
            logger.info(f"Currency from {currency_from} to {key} have value "
                        f"{currency_transform.get('results').get(key)}\n")
