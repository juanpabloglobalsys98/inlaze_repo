import json
import logging

import requests
from api_admin.models import CliqLogger
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.utils.timezone import timedelta

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        """
        Get new acess token with Refresh token
        Expected values at response
        * access_token, 
        * api_domain, 
        * token_type, 
        * expires_in

        Case error
        * error
        """
        response = requests.post(settings.LOGGING_CLIQ_REFRESH_URL, params={
            "client_id": settings.LOGGING_CLIQ_CLIENT_ID,
            "client_secret": settings.LOGGING_CLIQ_CLIENT_SECRET,
            "redirect_uri": settings.LOGGING_CLIQ_REDIRECT,
            "grant_type": "refresh_token",
            "refresh_token": settings.LOGGING_CLIQ_REFRESH_TOKEN,
            "scope": settings.LOGGING_CLIQ_SCOPE,
        })

        response_dict = json.loads(response.text)

        if "error" in response_dict:
            logger.critical(f"Failed to Get new ACCESS TOKEN, error:{response_dict.get('error')}")
            return

        CliqLogger.objects.update_or_create(
            client_id=settings.LOGGING_CLIQ_CLIENT_ID,
            defaults={
                "access_token": response_dict.get("access_token"),
                "token_type": response_dict.get("token_type"),
                "api_domain": response_dict.get("api_domain"),
                "expires_in": timezone.now() + timedelta(seconds=int(response_dict.get("expires_in"))),
            }
        )
