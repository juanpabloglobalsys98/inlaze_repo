import logging

from api_partner.helpers import (
    DB_USER_PARTNER,
    IsTerms,
)
from api_partner.helpers.permissions import (
    IsNotBanned,
    IsNotToBeVerified,
)
from cerberus import Validator
from core.helpers import StandardErrorHandler
from core.serializers.user import UserBasicSerializer
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class ChangeMarketingTermsAPI(APIView):

    """
        Class view with resources to update marketing terms fields into model
    """

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified,
        IsTerms,
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def put(self, request):
        """
            Change terms and conditions

            #Body

           -  is_notify_campaign : "boolean"
                Param to update notify campaign in model 

           -  is_notify_notice : "boolean"
                Param to define notify notice iin model
        """
        validator = Validator(
            schema={
                "is_notify_campaign": {
                    "required": False,
                    "type": "boolean",
                },
                "is_notify_notice": {
                    "required": False,
                    "type": "boolean",
                }
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = request.user.partner
        if "is_notify_campaign" in validator.document:
            partner.is_notify_campaign = validator.document.get("is_notify_campaign")

        if "is_notify_notice" in validator.document:
            partner.is_notify_notice = validator.document.get("is_notify_notice")

        if not "is_notify_campaign" in validator.document and not "is_notify_notice" in validator.document:
            return Response(
                {
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "not_field_erros": [
                            _("Is_notify_campaign or is_notify_notice fields are required")
                        ]
                    }
                },
                status=status.HTTP_409_CONFLICT,
            )

        partner.save()
        return Response(
            data={
                "msg": _("Marketing field updated updated successfully"),
            },
            status=status.HTTP_200_OK,
        )
