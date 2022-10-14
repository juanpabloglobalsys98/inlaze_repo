import logging
import sys
import traceback
from functools import partial

from api_partner.helpers import (
    DB_USER_PARTNER,
    IsActive,
    IsNotBanned,
    IsNotTerms,
    IsTerms,
)
from api_partner.serializers import PartnerTermsSer
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from sendgrid import SendGridAPIClient

logger = logging.getLogger(__name__)


class TermsPartnerAPI(APIView):

    """
        Class view to update terms to partner
    """

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsNotTerms,
    )

    def _update_contact(self, user):
        """
            Function to update contact in sendgrid
        """
        if not settings.SENDGRID_API_KEY:
            logger.error(f"SENDGRID_API_KEY is none")
            return None

        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        is_notify_campaign = int(user.partner.is_notify_campaign)
        is_notify_notice = int(user.partner.is_notify_notice)
        data = {
            "contacts": [
                {
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "email": user.email,
                    "city":  user.partner.additionalinfo.city,
                    "country": user.partner.additionalinfo.country,
                    "phone_number": user.phone,
                    "custom_fields": {
                        settings.SENDGRID_CUSTOM_FIELD_CAMPAIGN: is_notify_campaign,
                        settings.SENDGRID_CUSTOM_FIELD_NOTICE: is_notify_notice,
                    },
                },
            ],
        }
        try:
            response = sg.client.marketing.contacts.put(
                request_body=data
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return e

    def patch(self, request):
        """
            Method to udpate notify_campaign, notify_notice in 

            #Body

           -  is_notify_campaign : "boolean"
                Param to update notify campaign in model 

           -  is_notify_notice : "boolean"
                Param to define notify notice in model
        """

        validator = Validator(
            schema={
                "notify_campaign": {
                    "required": False,
                    "type": "boolean",
                },
                "notify_notice": {
                    "required": False,
                    "type": "boolean",
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = request.user.partner
        data = {
            "is_terms": True,
            "terms_at": timezone.now(),
        }
        if "notify_campaign" in validator.document:
            data["is_notify_campaign"] = validator.document.get("notify_campaign")

        if "notify_notice" in validator.document:
            data["is_notify_notice"] = validator.document.get("notify_notice")

        partnertermsserializer = PartnerTermsSer(
            instance=partner,
            data=data,
            partial=True,
        )

        if partnertermsserializer.is_valid():
            partnertermsserializer.save()
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": partnertermsserializer.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            status=status.HTTP_204_NO_CONTENT,
        )


class TermsPartnerProfileAPI(APIView):

    """
        Class view to update terms in profile
    """

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsTerms,
    )

    def _update_contact(self, user):
        """
            Function to update contact in sendgrid
        """
        if not settings.SENDGRID_API_KEY:
            logger.error(f"SENDGRID_API_KEY is none")
            return None

        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        is_notify_campaign = int(user.partner.is_notify_campaign)
        is_notify_notice = int(user.partner.is_notify_notice)
        data = {
            "contacts": [
                {
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "email": user.email,
                    "city":  user.partner.additionalinfo.city,
                    "country": user.partner.additionalinfo.country,
                    "phone_number": user.phone,
                    "custom_fields": {
                        settings.SENDGRID_CUSTOM_FIELD_CAMPAIGN: is_notify_campaign,
                        settings.SENDGRID_CUSTOM_FIELD_NOTICE: is_notify_notice,
                    },
                },
            ],
        }
        try:
            response = sg.client.marketing.contacts.put(
                request_body=data
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return e

    def patch(self, request):
        """
            Method to udpate notify_campaign, notify_notice in 

            #Body

           -  is_notify_campaign : "boolean"
                Param to update notify campaign in model 

           -  is_notify_notice : "boolean"
                Param to define notify notice in model

        """
        validator = Validator(
            schema={
                "notify_campaign": {
                    "required": False,
                    "type": "boolean",
                },
                "notify_notice": {
                    "required": False,
                    "type": "boolean",
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = request.user.partner
        data = {}
        if "notify_campaign" in validator.document:
            data["is_notify_campaign"] = validator.document.get("notify_campaign")

        if "notify_notice" in validator.document:
            data["is_notify_notice"] = validator.document.get("notify_notice")

        partnertermsserializer = PartnerTermsSer(
            instance=partner,
            data=data,
            partial=True,
        )

        if partnertermsserializer.is_valid():
            partnertermsserializer.save()
            self._update_contact(request.user)
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": partnertermsserializer.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            status=status.HTTP_204_NO_CONTENT,
        )
