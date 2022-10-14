import logging

from api_admin.helpers import DB_ADMIN
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


class PasswordChangeAPI(APIView):

    permission_classes = (IsAuthenticated, )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def put(self, request):
        """
        Lets an admin change their password
        """
        validator = Validator(
            {
                "old_password": {
                    "required": True,
                    "type": "string",
                },
                "new_password": {
                    "required": True,
                    "type": "string",
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        user = request.user
        sid = transaction.savepoint(using=DB_ADMIN)
        serialized_user = UserPasswordSerializer(data=validator.document)
        serialized_user.validate_old_password(user, validator.document.get("old_password"))
        serialized_user.update(user, validator.document.get("new_password"))
        transaction.savepoint_commit(sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)
