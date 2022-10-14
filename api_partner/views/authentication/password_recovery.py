import logging

from api_partner.helpers import DB_USER_PARTNER
from api_partner.helpers.permissions import IsNotBanned
from cerberus import Validator
from core.helpers import StandardErrorHandler
from core.serializers.user import UserPasswordSerializer
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class PasswordRecoveryAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Let the user change their password if the old one matches
        """
        validator = Validator(
            {
                "new_password": {
                    "required": True,
                    "type": "string",
                }
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        user = request.user
        new_password = validator.document.get("new_password")
        serialized_user = UserPasswordSerializer(data=validator.document)
        serialized_user.update(new_password, user)
        return Response(status=status.HTTP_200_OK)
