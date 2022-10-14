import logging

from api_admin.helpers.routers_db import DB_ADMIN
from api_admin.serializers import PermissionSerializer
from api_partner.helpers import DB_USER_PARTNER
from cerberus import Validator
from core.helpers import StandardErrorHandler
from core.models import (
    Permission,
    User,
)
from django.conf import settings
from django.contrib.auth import authenticate
from django.contrib.auth.models import update_last_login
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class LogInAPI(APIView):

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin log into Betenlace
        """
        validator = Validator(
            schema={
                "email": {
                    "required": True,
                    "type": "string",
                },
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

        user = User.objects.using(DB_ADMIN).filter(email=validator.document.get("email")).first()
        if user is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "email": [
                            _("Email does not exist"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        user = authenticate(username=request.data.get("email"), password=request.data.get("password"))
        if user is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "password": [
                            _("Invalid password"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        update_last_login(None, user)

        if not user.is_active:
            return Response(
                data={
                    "error": settings.INACTIVE_USER_CODE,
                    "detail": {
                        "email": [
                            _("Your user is not activated in the system")
                        ],
                    },
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        if user.is_banned:
            return Response(
                data={
                    "error": settings.BANNED_USER_CODE,
                    "detail": {
                        "email": [
                            _("Your user is banned, contact an administrator"),
                        ],
                    },
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        token = Token.objects.db_manager(DB_ADMIN).update_or_create(user=user, defaults={"user": user})[0]

        # Get permission partner
        permissions = []
        if user.is_superuser:
            permissions = Permission.objects.all()
        elif user.rol:
            permissions = Permission.objects.filter(
                permissions_to_rol=user.rol
            )

        return Response(
            data={
                "token": token.key,
                "permissions": PermissionSerializer(permissions, many=True).data,
            },
            status=status.HTTP_200_OK,
        )
