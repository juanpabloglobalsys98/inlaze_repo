import logging

from api_partner.helpers import click_error
from api_partner.views import (
    ClickNothingParamsAPI,
    ClickReportThreeParamsAPI,
    ClickReportTwoParamsAPI,
    AdsAntiBotAPI,
)
from django.conf import settings
from django.urls import path

logger = logging.getLogger(__name__)


# Get urls from .urls file, this is managed from custom command
# runserverapp

# Add admin for admin.site distinction
apps_to_run = settings.INSTALLED_CUSTOM_APPS.copy()

logger.info("Custom Apps Installed:")
for app in settings.INSTALLED_CUSTOM_APPS:
    logger.info(f"\t{app}")

logger.info("Custom Apps Allowed in urls")
for app_name in apps_to_run:
    logger.info("\t"+app_name)

logger.info("\n")

urlpatterns = [
    path("<str:encrypt_link>/", AdsAntiBotAPI.as_view()),
    path("<str:campaign>/<str:prom_code>", ClickReportTwoParamsAPI.as_view()),
    path("<str:campaign>/<str:prom_code>/", ClickReportTwoParamsAPI.as_view()),
    path("<str:langague>/<str:campaign>/<str:prom_code>", ClickReportThreeParamsAPI.as_view()),
    path("<str:langague>/<str:campaign>/<str:prom_code>/", ClickReportThreeParamsAPI.as_view()),
    path("", ClickNothingParamsAPI.as_view()),
]

handler404 = click_error
