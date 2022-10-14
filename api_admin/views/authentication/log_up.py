import re
from api_admin.helpers import DB_ADMIN
from api_admin.helpers.normalize_admin_reg_info import NormalizeAdminRegInfo
from api_admin.serializers import AdminSerializer
from cerberus import Validator
from core.helpers import StandardErrorHandler
from core.helpers.path_route_db import request_cfg
from core.serializers.user import (
    UserBasicForAdminSerializer,
    UserPasswordSerializer,
    UserSerializer,
)
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class AdminManagementAPI(APIView):
    """
    UserApi View retreive and edit own Staff user data
    """
    permission_classes = (IsAuthenticated, )

    @transaction.atomic(using=DB_ADMIN)
    def post(self, request):
        """
        Lets creates an admin in the system
        """
        request_cfg.is_partner = False
        validator = Validator(
            {
                "email": {
                    "required": True,
                    "type": "string",
                },
                "password": {
                    "required": True,
                    "type": "string",
                },
                "first_name": {
                    "required": True,
                    "type": "string",
                    "empty": False,
                    "regex": "(.|\s)*\S(.|\s)*",
                },
                "last_name": {
                    "required": True,
                    "type": "string",
                    "empty": False,
                    "regex": "(.|\s)*\S(.|\s)*",
                },
                "identification": {
                    "required": True,
                    "type": "string"
                },
                "identification_type": {
                    "required": True,
                    "type": "integer"
                },
                "address": {
                    "required": True,
                    "type": "string"
                },
                "phone": {
                    "required": True,
                    "type": "string"
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

        sid = transaction.savepoint(using=DB_ADMIN)
        NormalizeAdminRegInfo().normalize_admin_info(validator.document)
        user_password = UserPasswordSerializer(data=validator.document)
        user_password.validate_password(validator.document, validator.document.get("password"))
        serialized_user = UserSerializer(data=validator.document)
        if serialized_user.is_valid():
            user = serialized_user.create_admin(using=DB_ADMIN)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_user.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        validator.document["user"] = user.id

        serialized_admin = AdminSerializer(data=validator.document)
        if serialized_admin.is_valid():
            serialized_admin.create(database=DB_ADMIN)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_admin.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_ADMIN)
    def patch(self, request):
        "Lets updates an admin in the system"
        request_cfg.is_partner = False
        validator = Validator(
            {
                "email": {
                    "required": True,
                    "type": "string",
                },
                "first_name": {
                    "required": True,
                    "type": "string",
                    "empty": False,
                    "regex": "(.|\s)*\S(.|\s)*",
                },
                "last_name": {
                    "required": True,
                    "type": "string",
                    "empty": False,
                    "regex": "(.|\s)*\S(.|\s)*",
                },
                "identification": {
                    "required": True,
                    "type": "string"
                },
                "identification_type": {
                    "required": True,
                    "type": "integer"
                },
                "address": {
                    "required": True,
                    "type": "string"
                },
                "phone": {
                    "required": True,
                    "type": "string"
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

        NormalizeAdminRegInfo().normalize_admin_info(validator.document)
        sid = transaction.savepoint(using=DB_ADMIN)

        user = UserBasicForAdminSerializer.get_by_email(None, validator.document.get("email"), using=DB_ADMIN)
        if not user:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"email": [_("There is not such user in the system")]}
                },
                status=status.HTTP_404_NOT_FOUND
            )

        serialized_user = UserBasicForAdminSerializer(instance=user, data=validator.document)
        if serialized_user.is_valid():
            user = serialized_user.save()
        else:
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_user.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        validator.document["user"] = user.id
        admin = user.admin
        serialized_admin = AdminSerializer(admin, data=validator.document)
        if serialized_admin.is_valid():
            serialized_admin.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_admin.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)
