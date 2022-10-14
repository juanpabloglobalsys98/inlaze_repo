import logging

from api_partner.helpers import (
    DB_USER_PARTNER,
    IsNotBanned,
    IsTerms,
)
from api_partner.helpers.permissions import IsNotBanned
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class ConfirmPasswordAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsTerms,
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Check if user's password matches
        """
        validator = Validator(
            schema={
                "password": {
                    "required": True,
                    "type": "string",
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not request.user.check_password(validator.document.get("password")):
            return Response(
                data={
                    "error": settings.PASSWORD_DOES_NOT_MATCH,
                    "detail": {
                        "password": [
                            _("The password you provided does not match"),
                        ],
                    },
                },
                status=status.HTTP_401_UNAUTHORIZED,
            )

        return Response(status=status.HTTP_200_OK)
