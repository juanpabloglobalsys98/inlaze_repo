import logging
import os

from django.conf import settings
from django.conf.urls.static import static
from django.urls import (
    include,
    path,
)


logger = logging.getLogger(__name__)


def _redirect_campaigns(urlpatterns):
    from api_partner.views import ClickReportAPI
    urlpatterns.append(path("<str:campaign>/<str:prom_code>", ClickReportAPI.as_view()))
    urlpatterns.append(path("<str:langague>/<str:campaign>/<str:prom_code>", ClickReportAPI.as_view()))


# Get urls from .urls file, this is managed from custom command
# runserverapp
if (os.path.isfile(settings.URLS_CUSTOM_APPS_FILE_PATH)):
    urls_file = open(settings.URLS_CUSTOM_APPS_FILE_PATH, "r")
    apps_to_run = urls_file.read().split()
    urls_file.close()

    # Delete file, this allow use runserver default command without
    # error or pre-config for file .urls
    os.remove(settings.URLS_CUSTOM_APPS_FILE_PATH)
else:
    # Add admin for admin.site distinction
    apps_to_run = settings.INSTALLED_CUSTOM_APPS.copy()

logger.info("Custom Apps Installed:")
for app in settings.INSTALLED_CUSTOM_APPS:
    logger.info(f"\t{app}")

logger.info("Custom Apps Allowed in urls")
for app_name in apps_to_run:
    logger.info("\t"+app_name)

logger.info("\n")

urlpatterns = []

# Admin is not an app, this is used for name to aware admin.site
if "api_admin" in apps_to_run:
    urlpatterns.append(path("api_admin/", include("api_admin.urls")))
if "api_partner" in apps_to_run:
    urlpatterns.append(path("api_partner/", include("api_partner.urls")))
    _redirect_campaigns(urlpatterns)


urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
