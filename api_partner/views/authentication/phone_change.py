import logging

from api_partner.helpers import (
    DB_USER_PARTNER,
    HasLevel,
    IsNotBanned,
    IsTerms,
)
from api_partner.models import (
    Partner,
    ValidationCode,
)
from cerberus import Validator
from core.helpers import (
    StandardErrorHandler,
    generate_validation_code,
    send_phone_message,
    to_int,
)
from core.models import User
from core.serializers import UserSER
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class ChangePhoneAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsTerms,
        HasLevel,
    )

    def post(self, request):
        """
        User requests a validation code to change their phone number.
        """
        validator = Validator(
            schema={
                "new_phone": {
                    "required": True,
                    "type": "string",
                },
                "valid_phone_by": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Partner.ValidPhoneBy.values
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = request.user
        new_phone = validator.document.get("new_phone")
        if User.objects.filter(phone=new_phone).exists():
            if new_phone == user.phone:
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "detail": {
                            "new_phone": [
                                _("Please enter a new phone number")
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "new_phone": [
                            _("Invalid phone. Please try with another one"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not hasattr(user, "validation_code"):
            user.validation_code = ValidationCode(user=user)

        code, expiration = generate_validation_code()
        validation_code: ValidationCode = user.validation_code
        validation_code.code = code
        validation_code.expiration = expiration
        validation_code.phone = new_phone
        validation_code.attempts = 0

        if (res := send_phone_message(
            phone=new_phone,
            valid_phone_by=validator.document.get("valid_phone_by"),
            validation_code=code,
        )):
            return res

        validation_code.save()
        return Response(status=status.HTTP_200_OK)

    def patch(self, request):
        validator = Validator(
            schema={
                "code": {
                    "required": True,
                    "type": "string",
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = request.user
        code = validator.document.get("code")
        if not hasattr(user, "validation_code"):
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "detail": {
                        "code": [
                            _("Invalid code. Try again, or request a new one"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif user.validation_code.attempts > settings.MAX_VALIDATION_CODE_ATTEMPTS:
            return Response(
                data={
                    "error": settings.MAX_ATTEMPTS_REACHED,
                    "detail": {
                        "code": [
                            _("Max attempts for that code was reached"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif user.validation_code.code != code:
            user.validation_code.attempts += 1
            user.validation_code.save()
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "detail": {
                        "code": [
                            _("Invalid code. Try again, or request a new one"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif user.validation_code.expiration < timezone.now():
            return Response(
                data={
                    "error": settings.EXPIRED_VALIDATION_CODE,
                    "detail": {
                        "code": [
                            _("Code expired, please request a new one"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if User.objects.filter(phone=user.validation_code.phone).exists():
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Invalid phone. Please try with another one"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user_ser = UserSER(
            instance=user,
            data={
                "phone": user.validation_code.phone,
            },
            partial=True,
        )
        if not user_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": user_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic(using=DB_USER_PARTNER):
            user.validation_code.delete()
            user_ser.save()

        return Response(status=status.HTTP_204_NO_CONTENT)
