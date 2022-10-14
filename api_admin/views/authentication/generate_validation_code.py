import logging
import sys
import traceback

from api_admin.helpers import DB_ADMIN
from api_admin.models import ValidationCode
from api_admin.serializers import ValidationCodeSer
from cerberus import Validator
from core.helpers import (
    StandardErrorHandler,
    to_lower,
)
from core.helpers.email_thread import EmailThread
from core.models import User
from django.conf import settings
from django.contrib.auth.models import BaseUserManager
from django.db import transaction
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class GenerateValidationCodeAPI(APIView):

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def post(self, request):
        """
        Generate a validation code and send it to user's email considering three scenarios:
        * validation code for password recovery
        """
        validator = Validator(
            schema={
                "email": {
                    "required": True,
                    "type": "string",
                    "coerce": to_lower,
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

        email = validator.document.get("email")

        filters = [Q(email=validator.document.get("email"))]
        user = User.objects.using(DB_ADMIN).filter(*filters).first()
        if not user:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "email": [
                            _("That account does not exist"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Generate validation code based on make_random_password
        code_value = BaseUserManager.make_random_password(
            self=None,
            length=settings.VALIDATION_CODE_LENGTH,
            allowed_chars="0123456789",
        )

        # Initialize data to write on serializer
        data = {
            "email": validator.document.get("email"),
            "code": code_value,
            "expiration": ValidationCode._get_current_expiration()
        }

        # Start transaction
        sid = transaction.savepoint(using=DB_ADMIN)

        filters = [Q(email=validator.document.get("email"))]
        validation_code_old = ValidationCode.objects.filter(*filters).first()

        # Case update
        if (validation_code_old):
            validation_code_ser = ValidationCodeSer(
                instance=validation_code_old,
                data=data,
                partial=True,
            )
        # Case create
        else:
            validation_code_ser = ValidationCodeSer(
                data=data,
            )

        if (validation_code_ser.is_valid()):
            validation_code_ser.save()
        else:
            transaction.savepoint_rollback(sid, using=DB_ADMIN)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": validation_code_ser.errors
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        try:
            partner_full_name = user.get_full_name()
            subject = _("Validation code")
            EmailThread(
                html="send_alert_password_recovery.html",
                email=email,
                subject=subject,
                data={
                    "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
                    "GREETING": _("Hi"),
                    "GREETING_2": _("Greetings, "),
                    "PARTNER_FULL_NAME": partner_full_name,
                    "TEMPLATE_IMAGE_INLAZZ": settings.TEMPLATE_HEADER_LOGO,
                    "CUSTOMER_MESSAGE_PART_1": _("Your validation code is"),
                    "CODE": code_value,
                    "CUSTOMER_SERVICE_PART_3": _("Remember your password must meet next requirements:"),
                    "CUSTOMER_SERVICE_PART_4": _("Minimum 8 characters length"),
                    "CUSTOMER_SERVICE_PART_5": _("At least 1 uppercase character"),
                    "CUSTOMER_SERVICE_PART_6": _("At least 1 lowercase character"),
                    "CUSTOMER_SERVICE_PART_7": _("At least 1 special character (@#*_)"),
                    "CUSTOMER_SERVICE_PART_2": _("Your validation code is"),
                    "CUSTOMER_SERVICE_PART_11": _("If you have not requested a new password, you can ignore this email. Your current password will continue to work."),
                    "FOOTER_SERVICE_PART_1": _("inlaze team"),
                    "FOOTER_SERVICE_PART_2": _("© inlaze 2022. All rights reserved."),


                    "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,

                    "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact us"),

                    "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
                    "CUSTOMER_SERVICE": _("Customer service."),

                    "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
                    "COMPANY_URL": settings.COMPANY_URL,

                    "FOOTER_MESSAGE_PART_5": _("Access here"),
                    "FOOTER_MESSSAGE_PART_6": _("© inlaze 2022. All rights reserved."),
                },
            ).start()
        except Exception as e:
            transaction.savepoint_rollback(sid, using=DB_ADMIN)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                {
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {
                        "non_field_errors": [
                            _("Error at send email"),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        transaction.savepoint_commit(sid, using=DB_ADMIN)
        return Response(
            status=status.HTTP_200_OK,
        )
