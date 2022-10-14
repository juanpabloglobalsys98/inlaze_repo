import json

import pytz
import requests
from api_partner.models import (
    FxPartner,
    FxPartnerPercentage,
)
from betenlace.celery import app
from celery.utils.log import get_task_logger
from django.conf import settings
from django.db.models import Q
from django.utils import timezone
from django.utils.timezone import timedelta

logger_task = get_task_logger(__name__)

BASE_URL = "https://api.fastforex.io/historical?"


@app.task(ignore_result=True)
def fx_base():
    logger_task.debug("Starting FX")
    yesterday = (
        timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE)) -
        timedelta(days=1)
    ).strftime("%Y-%m-%d")

    request_fx_eur = requests.get(
        url=BASE_URL, params={
            "date": yesterday,
            "from": 'EUR',
            "api_key": settings.API_KEY_FX,
        },
    )

    if("error" in request_fx_eur):
        logger_task.critical(f"Api FX sends error at get EUR data, message:{request_fx_eur.text}")
        return

    request_fx_usd = requests.get(
        url=BASE_URL,
        params={
            "date": yesterday,
            "from": 'USD',
            "api_key": settings.API_KEY_FX,
        },
    )

    if("error" in request_fx_usd):
        logger_task.critical(f"Api FX sends error at get EUR data, message:{request_fx_eur.text}")
        return

    request_fx_mxn = requests.get(
        url=BASE_URL,
        params={
            "date": yesterday,
            "from": 'MXN',
            "api_key": settings.API_KEY_FX,
        },
    )

    if("error" in request_fx_mxn):
        logger_task.critical(f"Api FX sends error at get MXN data, message:{request_fx_mxn.text}")
        return

    request_fx_brl = requests.get(
        url=BASE_URL,
        params={
            "date": yesterday,
            "from": 'BRL',
            "api_key": settings.API_KEY_FX,
        },
    )

    if("error" in request_fx_brl):
        logger_task.critical(f"Api FX sends error at get BRL data, message:{request_fx_brl.text}")
        return

    request_fx_gbp = requests.get(
        url=BASE_URL,
        params={
            "date": yesterday,
            "from": 'GBP',
            "api_key": settings.API_KEY_FX,
        },
    )

    if("error" in request_fx_gbp):
        logger_task.critical(f"Api FX sends error at get GBP data, message:{request_fx_gbp.text}")
        return

    request_fx_pen = requests.get(
        url=BASE_URL,
        params={
            "date": yesterday,
            "from": 'PEN',
            "api_key": settings.API_KEY_FX,
        },
    )

    if("error" in request_fx_pen):
        logger_task.critical(f"Api FX sends error at get PEN data, message:{request_fx_pen.text}")
        return

    request_fx_clp = requests.get(
        url=BASE_URL,
        params={
            "date": yesterday,
            "from": 'CLP',
            "api_key": settings.API_KEY_FX,
        },
    )

    if("error" in request_fx_clp):
        logger_task.critical(f"Api FX sends error at get PEN data, message:{request_fx_clp.text}")
        return

    currency_transform_eur = json.loads(request_fx_eur.text)
    currency_transform_usd = json.loads(request_fx_usd.text)
    currency_transform_mxn = json.loads(request_fx_mxn.text)
    currency_transform_brl = json.loads(request_fx_brl.text)
    currency_transform_gbp = json.loads(request_fx_gbp.text)
    currency_transform_pen = json.loads(request_fx_pen.text)
    currency_transform_clp = json.loads(request_fx_clp.text)

    today = timezone.now()
    filters = [Q(updated_at__lte=today)]
    fx_partner_percentage = FxPartnerPercentage.objects.filter(*filters).order_by("-updated_at").first()

    if(fx_partner_percentage is None):
        # Get just next from supplied date
        filters = [Q(updated_at__gte=today)]
        fx_partner_percentage = FxPartnerPercentage.objects.filter(*filters).order_by("updated_at").first()

    if(fx_partner_percentage is None):
        logger_task.warning("Undefined fx_partner on DB, using default 95%")
        fx_partner_percentage = 0.95
    else:
        fx_partner_percentage = fx_partner_percentage.percentage_fx

    FxPartner.objects.create(
        fx_eur_cop=currency_transform_eur.get("results").get("COP"),
        fx_eur_mxn=currency_transform_eur.get("results").get("MXN"),
        fx_eur_usd=currency_transform_eur.get("results").get("USD"),
        fx_eur_brl=currency_transform_eur.get("results").get("BRL"),
        fx_eur_pen=currency_transform_eur.get("results").get("PEN"),
        fx_eur_gbp=currency_transform_eur.get("results").get("GBP"),
        fx_eur_clp=currency_transform_eur.get("results").get("CLP"),

        fx_usd_cop=currency_transform_usd.get("results").get("COP"),
        fx_usd_mxn=currency_transform_usd.get("results").get("MXN"),
        fx_usd_eur=currency_transform_usd.get("results").get("EUR"),
        fx_usd_brl=currency_transform_usd.get("results").get("BRL"),
        fx_usd_pen=currency_transform_usd.get("results").get("PEN"),
        fx_usd_gbp=currency_transform_usd.get("results").get("GBP"),
        fx_usd_clp=currency_transform_usd.get("results").get("CLP"),

        fx_cop_usd=1/currency_transform_usd.get("results").get("COP"),
        fx_cop_mxn=1/currency_transform_mxn.get("results").get("COP"),
        fx_cop_eur=1/currency_transform_eur.get("results").get("COP"),
        fx_cop_brl=1/currency_transform_brl.get("results").get("COP"),
        fx_cop_pen=1/currency_transform_pen.get("results").get("COP"),
        fx_cop_gbp=1/currency_transform_gbp.get("results").get("COP"),
        fx_cop_clp=1/currency_transform_clp.get("results").get("COP"),

        fx_mxn_usd=1/currency_transform_usd.get("results").get("MXN"),
        fx_mxn_cop=currency_transform_mxn.get("results").get("COP"),
        fx_mxn_eur=1/currency_transform_eur.get("results").get("MXN"),
        fx_mxn_brl=currency_transform_mxn.get("results").get("BRL"),
        fx_mxn_pen=currency_transform_mxn.get("results").get("PEN"),
        fx_mxn_gbp=1/currency_transform_gbp.get("results").get("MXN"),
        fx_mxn_clp=currency_transform_mxn.get("results").get("CLP"),

        fx_gbp_usd=currency_transform_gbp.get("results").get("USD"),
        fx_gbp_cop=currency_transform_gbp.get("results").get("COP"),
        fx_gbp_mxn=currency_transform_gbp.get("results").get("MXN"),
        fx_gbp_eur=currency_transform_gbp.get("results").get("EUR"),
        fx_gbp_brl=currency_transform_gbp.get("results").get("BRL"),
        fx_gbp_pen=currency_transform_gbp.get("results").get("PEN"),
        fx_gbp_clp=currency_transform_gbp.get("results").get("CLP"),

        fx_pen_usd=currency_transform_pen.get("results").get("USD"),
        fx_pen_cop=currency_transform_pen.get("results").get("COP"),
        fx_pen_mxn=currency_transform_pen.get("results").get("MXN"),
        fx_pen_eur=currency_transform_pen.get("results").get("EUR"),
        fx_pen_brl=currency_transform_pen.get("results").get("BRL"),
        fx_pen_gbp=currency_transform_pen.get("results").get("GBP"),
        fx_pen_clp=currency_transform_pen.get("results").get("CLP"),

        fx_clp_usd=1/currency_transform_usd.get("results").get("CLP"),
        fx_clp_cop=currency_transform_clp.get("results").get("COP"),
        fx_clp_mxn=1/currency_transform_mxn.get("results").get("CLP"),
        fx_clp_eur=1/currency_transform_eur.get("results").get("CLP"),
        fx_clp_brl=1/currency_transform_brl.get("results").get("CLP"),
        fx_clp_gbp=1/currency_transform_gbp.get("results").get("CLP"),
        fx_clp_pen=1/currency_transform_pen.get("results").get("CLP"),

        fx_brl_usd=1/currency_transform_usd.get("results").get("BRL"),
        fx_brl_cop=currency_transform_brl.get("results").get("COP"),
        fx_brl_mxn=currency_transform_brl.get("results").get("MXN"),
        fx_brl_eur=1/currency_transform_eur.get("results").get("BRL"),
        fx_brl_gbp=1/currency_transform_gbp.get("results").get("BRL"),
        fx_brl_pen=1/currency_transform_pen.get("results").get("BRL"),
        fx_brl_clp=currency_transform_brl.get("results").get("CLP"),

        fx_percentage=fx_partner_percentage,
    )
