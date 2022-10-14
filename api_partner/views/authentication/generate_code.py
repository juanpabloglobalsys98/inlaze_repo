import json
import logging
import sys
import traceback
from math import floor
from random import random

import python_http_client
from api_partner.helpers import (
    DB_USER_PARTNER,
    TwillioCodeType,
    ValidationCodeType,
)
from api_partner.models import (
    Partner,
    ValidationCode,
)
from api_partner.serializers import (
    PartnerSerializer,
    ValidationCodePhase1BSer,
    ValidationCodeRegisterSerializer,
)
from api_partner.serializers.authentication.validation_code import (
    ValidationCodeSerializer,
)
from cerberus import Validator
from core.helpers import (
    EmailThread,
    StandardErrorHandler,
    generate_validation_code,
    send_phone_message,
    to_int,
    to_lower,
)
from core.models import User
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from sendgrid import SendGridAPIClient

logger = logging.getLogger(__name__)


class GenerateCodeAPI(APIView):

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Generate a validation code and send it to user's email considering three scenarios:
        * validation code for register}
        * validation code for password recovery
        * validation code for email change
        """
        validator = Validator(
            {
                "code_type": {
                    "required": True,
                    "type": "integer",
                },
                "email": {
                    "required": True,
                    "type": "string",
                },
                "type_code": {
                    "required": True,
                    "type": "integer",
                    "allowed": TwillioCodeType.values
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        email = validator.document.get("email")
        code_type = validator.document.get("code_type")
        # generate validation code
        code_value = str(int(settings.MIN_DIGITS) +
                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))

        def get_current_expiration():
            """
            Get expiration time from settings
            """
            default_minutes = int(settings.EXPIRATION_ADDER_MINUTES)
            return timezone.now() + timezone.timedelta(minutes=default_minutes)

        user = None

        if code_type == ValidationCodeType.CODE_REGISTER:
            target_serializer = ValidationCodeRegisterSerializer
        elif code_type == ValidationCodeType.CODE_PASSWORD_RECOVERY:
            user = User.objects.using(DB_USER_PARTNER).filter(email=email).exists()
            if not user:
                return Response(data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"email": [_("That account does not exist")]}
                }, status=status.HTTP_400_BAD_REQUEST)
            target_serializer = ValidationCodeSerializer
        elif code_type == ValidationCodeType.CODE_EMAIL_CHANGE_REQUEST:
            target_serializer = ValidationCodeSerializer
        else:
            return Response(data={
                "error": settings.BAD_REQUEST_CODE,
                "details": {"code_type": ["You have selected a bad code_type"]},
            }, status=status.HTTP_400_BAD_REQUEST)

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        code = target_serializer().get_by_email(email, DB_USER_PARTNER)
        data = {
            "email": email,
            "code": code_value,
            "expiration": get_current_expiration()
        }

        if code:
            serialized_validation_code = target_serializer(instance=code, data=data)
        else:
            serialized_validation_code = target_serializer(data=data)

        if serialized_validation_code.is_valid():
            serialized_validation_code.save()
        else:
            return Response(
                {
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_validation_code.errors
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        if code_type == ValidationCodeType.CODE_PASSWORD_RECOVERY:
            try:
                partner_full_name = user.get_full_name()
                subject = _("Validation code")
                EmailThread(
                    html="send_alert_password_recovery.html",
                    email=email,
                    subject=subject,
                    data={
                        "PARTNER_FULL_NAME": partner_full_name,
                        "VALIDATION_CODE": code_value,
                        "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
                        "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,
                        "TEMPLATE_IMAGE_INLAZZ": settings.TEMPLATE_IMAGE_INLAZZ,
                        "BETENLACE_LOGIN": settings.BETENLACE_LOGIN,
                        "COMPANY_URL": settings.COMPANY_URL,
                        "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
                        "CUSTOMER_MESSAGE": _("Your validation code is"),
                        "GREETING": _("Hi"),
                        "GREETING_2": _("greetings,"),
                        "CUSTOMER_MESSAGE_PART_1": _("Your validation code is"),
                        "CUSTOMER_MESSAGE_PART_2": _("Remember your password must meet next requirements:"),
                        "CUSTOMER_MESSAGE_PART_3": _("Minimum 8 characters length"),
                        "CUSTOMER_MESSAGE_PART_4": _("At least 1 uppercase character"),
                        "CUSTOMER_MESSAGE_PART_5": _("At least 1 lowercase character"),
                        "CUSTOMER_MESSAGE_PART_6": _("At least 1 special character (@#*_)"),
                        "CUSTOMER_SERVICE_PART_1": _("Customer service"),
                        "CUSTOMER_SERVICE_PART_2": _("Monday to Friday"),
                        "CUSTOMER_SERVICE_PART_3": _("Colombia (UTC-5)"),
                        "CUSTOMER_SERVICE_PART_4": _("Saturdays"),
                        "CUSTOMER_SERVICE_PART_5": _("Colombia (UTC-5)"),
                        "FOOTER_MESSAGE_PART_1": _("Best regards,"),
                        "FOOTER_MESSAGE_PART_2": _("inlaze team"),
                        "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
                        "CUSTOMER_SERVICE": _("Customer service."),
                        "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
                        "FOOTER_MESSAGE_PART_5": _("Access here"),
                        "FOOTER_MESSAGE_PART_6": _("© inlaze 2022. All rights reserved."),

                    }
                ).start()
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.critical("".join(e))
                return Response(
                    {
                        "error": settings.ERROR_SENDING_EMAIL,
                        "details": {"non_field_errors": ["".join(str(exc_value))]}
                    }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        else:
            try:
                partner_full_name = user.get_full_name()
                subject = _("Validation code")
                EmailThread(
                    html="send_validation_code.html",
                    email=email,
                    subject=subject,
                    data={
                        "PARTNER_FULL_NAME": partner_full_name,
                        "VALIDATION_CODE": code_value,
                        "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
                        "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,
                        "BETENLACE_LOGIN": settings.BETENLACE_LOGIN,
                        "TEMPLATE_IMAGE_INLAZZ": settings.TEMPLATE_IMAGE_INLAZZ,
                        "COMPANY_URL": settings.COMPANY_URL,
                        "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
                        "CUSTOMER_MESSAGE": _("Your validation code is"),
                        "GREETING": _("Hi"),
                        "CUSTOMER_SERVICE_PART_1": _("Customer service"),
                        "CUSTOMER_SERVICE_PART_2": _("Monday to Friday"),
                        "CUSTOMER_SERVICE_PART_3": _("Colombia (UTC-5)"),
                        "CUSTOMER_SERVICE_PART_4": _("Saturdays"),
                        "CUSTOMER_SERVICE_PART_5": _("Colombia (UTC-5)"),
                        "FOOTER_MESSAGE_PART_1": _("Best regards,"),
                        "FOOTER_MESSAGE_PART_2": _("Inlazz team"),
                        "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
                        "CUSTOMER_SERVICE": _("Customer service."),
                        "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
                        "FOOTER_MESSAGE_PART_5": _("Access here"),
                    }
                ).start()
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.critical("".join(e))
                transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)
                return Response(
                    {
                        "error": settings.ERROR_SENDING_EMAIL,
                        "details": {"non_field_errors": ["".join(str(exc_value))]}
                    }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

        transaction.savepoint_commit(sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)


class CodeChangeEmailAPI(APIView):

    """
        Class view with resources to get and update email in profile
    """

    permission_classes = (
        IsAuthenticated,
    )

    def _update_contact(self, user, email_before):
        """
            Function that update contact into sendgrid twillio
        """
        create_contact = False
        if not settings.SENDGRID_API_KEY:
            logger.error(f"SENDGRID_API_KEY is none")
            return None

        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        # Make dict to send in request
        data = {
            "emails": [
                email_before,
            ]
        }
        try:
            # Get Contact Info
            response = sg.client.marketing.contacts.search.emails.post(
                request_body=data
            )
        except python_http_client.exceptions.NotFoundError as e:
            create_contact = True

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return e

        if create_contact == False and response.status_code == 200:
            # Get ID Contact
            json_response = json.loads(response.body).get("result")
            results = json_response.get(email_before).get("contact")
            # Delete Contact by id
            id_ = results.get("id")
            to_delete = {'ids': id_}
            try:
                response = sg.client.marketing.contacts.delete(
                    query_params=to_delete
                )
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.critical("".join(e))
                return e

            # Add new Contact
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
        else:
            # Add new Contact
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

    def _send_email(self, user, email, validation_code):
        """
            Function to send email
        """
        subject = _("Validation email")
        EmailThread(
            html="send_validation_code.html",
            email=email,
            subject=subject,
            data={
                "TITLE": _("Validation Code"),
                "TIME_CODE": settings.EXPIRATION_ADDER_MINUTES,
                "GREETING": _("Hi"),
                "PARTNER_FULL_NAME": user.get_full_name(),
                "GREETING_2": _("greetings,"),
                "TEMPLATE_IMAGE_INLAZZ": settings.TEMPLATE_IMAGE_INLAZZ,
                "LOGO_HEADER": settings.TEMPLATE_HEADER_LOGO,
                "CUSTOMER_SERVICE_PART_2": _("Your validation code is"),
                "CODE": validation_code,
                "CUSTOMER_SERVICE_PART_3": _("Remember that your password must meet the following requirements"),
                "CUSTOMER_SERVICE_PART_4": _("We are sending this email to keep you informed about your inlaze account"),
                "CUSTOMER_SERVICE_PART_5": _("At least 1 upper case character"),
                "CUSTOMER_SERVICE_PART_6": _("At least 1 lower case character"),
                "CUSTOMER_SERVICE_PART_7": _("At least 1 special character (@#*_)"),
                "CUSTOMER_SERVICE_PART_8": _("do not share the confirmation code with any other person for any reason"),
                "CUSTOMER_SERVICE_PART_9": _("This code is valid for 10 minutes, to keep your safety, don't share it with other people"),
                "CUSTOMER_SERVICE_PART_10": _("Your email will serve as a backup in case the other means of security do not work or you do not have access to them."),
                "FOOTER_MESSAGE_PART_1": _("inlaze team"),
                "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
                "CUSTOMER_SERVICE": _("Customer service."),
                "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
                "FOOTER_MESSAGE_PART_5": _("Access here"),
                "FOOTER_MESSAGE_PART_2": _("© inlaze 2022. All rights reserved."),
                "DATE": "2022",
            }
        ).start()

    def post(self, request):
        """
            Send email with code to change email

            #Body

           -  email : "str"
                Param to define email to send code
        """
        validator = Validator(
            {
                "email": {
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
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Validate if exist a user with this email
        filters = (
            Q(email=validator.document.get("email")),
        )
        user_already = User.objects.filter(*filters).exclude(pk=request.user.pk).first()
        if user_already:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "detail": {
                        "user": [
                            _("The email that you are trying to add already exits"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = request.user
        email_user = request.user.email
        filters = (
            Q(user__email=email_user),
        )

        validationcode = ValidationCode.objects.filter(*filters).first()
        default_minutes = int(settings.EXPIRATION_ADDER_MINUTES)

        # Create new code if time is expired
        if not validationcode:
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "user": user.pk,
                "code": validation_code_number,
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                data=data,
            )
            code_to_send = validation_code_number

        elif validationcode and validationcode.expiration < timezone.now():
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "email": validator.document.get("email"),
                "code": validation_code_number,
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True
            )
            code_to_send = validation_code_number

        elif validationcode and validator.document.get("email") == validationcode.email:
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "code": validation_code_number,
                "email": validator.document.get("email"),
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True
            )
            code_to_send = validation_code_number

        elif validationcode and validator.document.get("email") == user.email:
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "code": validation_code_number,
                "email": validator.document.get("email"),
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True
            )
            code_to_send = validation_code_number

        elif validationcode and not validator.document.get("email") == user.email:
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "code": validation_code_number,
                "email": validator.document.get("email"),
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True
            )
            code_to_send = validation_code_number
        else:
            data = {
                "email": validator.document.get("email"),
                "created_at": timezone.now(),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True
            )
            code_to_send = validationcode.code

        if validation_code_serializer.is_valid():
            validation_code_serializer.save()
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": validation_code_serializer.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            self._send_email(
                user=user,
                email=validator.document.get("email"),
                validation_code=code_to_send,
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                {
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {
                        "non_field_errors": [
                            "".join(str(exc_value)),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            data={
                "msg": _("Code was sent successfully"),
            },
            status=status.HTTP_200_OK,
        )

    def put(self, request):
        """
            Method to update email receiving code send

            #Body

           -  code : "str"
                Param to define code send to email
           -  email : "str"
                Param to define new email

        """

        validator = Validator(
            {
                "code": {
                    "required": True,
                    "type": "string",
                },
                "email": {
                    "required": True,
                    "type": "string",
                },
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

        # Validate if exist a user with this email
        query = Q(email=validator.document.get("email"))
        user_already = User.objects.filter(query).exclude(pk=request.user.pk).first()
        if user_already:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "detail": {
                        "user": [
                            _("The email that you are trying to add already exits"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = request.user
        filters = (
            Q(user__email=user.email),
        )
        validationcode = ValidationCode.objects.filter(*filters).first()
        if not validationcode:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Invalid email, phone or user already register"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if validationcode.code != validator.document.get("code"):
            attempts = validationcode.attempts + 1
            if attempts > int(settings.MAX_VALIDATION_CODE_ATTEMPTS):
                validationcode.delete()
                return Response(
                    {
                        "error": settings.MAX_ATTEMPTS_REACHED,
                        "details": {
                            "validation_code": [
                                _("Max attempts for that code was reached"),
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )

            validationcode.attempts = attempts
            validationcode.save()
            return Response(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Invalid code"),
                        ],
                    },
                },
                status=status.HTTP_409_CONFLICT,
            )

        if timezone.now() > validationcode.expiration:
            validationcode.delete()
            return Response(
                {
                    "error": settings.EXPIRED_VALIDATION_CODE,
                    "details": {
                        "code": [
                            _("Validation code has expired"),
                        ],
                    },
                },
                status=status.HTTP_200_OK,
            )
        email_before = user.email
        user.email = validator.document.get("email")
        self._update_contact(user, email_before)
        user.save()
        partner = Partner.objects.filter(user_id=request.user.pk).first()
        partner_ser = PartnerSerializer(
            instance=partner,
            data={
                "is_email_valid": True,
            },
            partial=True,
        )
        if partner_ser.is_valid():
            partner_ser.save()
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        return Response(
            data={
                "msg": _("Email updated successfully"),
            },
            status=status.HTTP_200_OK
        )


class CodeChangePhoneAPI(APIView):

    """
        Class view with resources to get code and update phone in profile
    """

    permission_classes = (
        IsAuthenticated,
    )

    def _update_contact(self, user):
        """
            Function that update phone contact into sendgrid twillio
        """

        if not settings.SENDGRID_API_KEY:
            logger.error(f"SENDGRID_API_KEY is none")
            return None

        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        data = {
            "contacts": [
                {
                    "email": user.email,
                    "phone_number": user.phone,
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

    def post(self, request):
        """
            Send code to change phone

            #Body

           -  phone : "str"
                Param to define phone to sent code
           -  valid_phone_by : "int"
                Param to define type of method to valid phone (SMS, WPP) based in ValidPhoneBy field
        """
        validator = Validator(
            {
                "phone": {
                    "required": True,
                    "type": "string",
                },
                "valid_phone_by": {
                    "required": True,
                    "type": "integer",
                    "coerce": int,
                    "allowed": Partner.ValidPhoneBy.values
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Validate if exist a user with this phone
        filters = (
            Q(phone=validator.document.get("phone")),
        )
        user_already = User.objects.filter(*filters).first()
        if user_already:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "user": [
                            _("The phone that you are trying to add already exits"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        user = request.user
        phone_user = request.user.phone
        filters = (
            Q(user__phone=phone_user),
        )
        validationcode = ValidationCode.objects.filter(*filters).first()

        # Create new code if time is expired
        default_minutes = int(settings.EXPIRATION_ADDER_MINUTES)
        if not validationcode:
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "user": user.pk,
                "code": validation_code_number,
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                data=data
            )
            code_to_send = validation_code_number
        elif validationcode and validationcode.expiration < timezone.now():
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "code": validation_code_number,
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True
            )
            code_to_send = validation_code_number
        else:
            data = {
                "created_at": timezone.now(),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True
            )
            code_to_send = validationcode.code

        if validation_code_serializer.is_valid():
            validation_code_serializer.save()
        else:
            return Response(
                {
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": validation_code_serializer.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        if validator.document.get("valid_phone_by") == Partner.ValidPhoneBy.SMS:
            status_send = self._send_sms(
                validation_code=code_to_send,
                phone=validator.document.get("phone")
            )
        else:
            status_send = self._send_wpp(
                validation_code=code_to_send,
                phone=validator.document.get("phone")
            )

        # if not status_send:
        #     return Response(
        #         {
        #             "error": settings.ERROR_SENDING_EMAIL,
        #             "details": {
        #                 "non_field_errors": [
        #                     _(f"Error to send code")
        #                 ]
        #             }
        #         },
        #         status=status.HTTP_400_BAD_REQUEST,
        #     )

        return Response(
            data={
                "msg": _("Code was sent successfully")
            },
            status=status.HTTP_200_OK,
        )

    def put(self, request):
        """
            Method to update phone based in code sent

            #Body
           -  code : "str"
                Param to define code to validate
           -  phone : "str"
                Param to define new phone to update
           -  valid_phone_by : "int"
                Param to define type of method to valid phone (SMS, WPP) based in ValidPhoneBy field

        """
        validator = Validator(
            schema={
                "code": {
                    "required": True,
                    "type": "string",
                },
                "phone": {
                    "required": True,
                    "type": "string",
                },
                "valid_phone_by": {
                    "required": True,
                    "type": "integer",
                    "coerce": int,
                    "allowed": Partner.ValidPhoneBy.values,
                },
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

        # Validate if exist a user with this phone
        filters = (
            Q(phone=validator.document.get("phone")),
        )
        user_already = User.objects.filter(*filters).first()
        if user_already:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "user": [
                            _("The phone that you are trying to add already exits"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = request.user
        filters = (
            Q(user__phone=user.phone),
        )
        validationcode = ValidationCode.objects.filter(*filters).first()
        if not validationcode:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Invalid email, phone or user already register"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if validationcode.code != validator.document.get("code"):
            attempts = validationcode.attempts + 1
            if attempts > int(settings.MAX_VALIDATION_CODE_ATTEMPTS):
                validationcode.delete()
                return Response(
                    data={
                        "error": settings.MAX_ATTEMPTS_REACHED,
                        "details": {
                            "validation_code": [
                                _("Max attempts for that code was reached"),
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )

            validationcode.attempts = attempts
            validationcode.save()
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Invalid code"),
                        ],
                    },
                },
                status=status.HTTP_409_CONFLICT,
            )

        if timezone.now() > validationcode.expiration:
            validationcode.delete()
            return Response(
                data={
                    "error": settings.EXPIRED_VALIDATION_CODE,
                    "details": {
                        "code": [
                            _("Validation code has expired"),
                        ],
                    },
                },
                status=status.HTTP_200_OK,
            )
        user.phone = validator.document.get("phone")
        user.partner.valid_phone_by = validator.document.get("valid_phone_by")
        user.save()
        user.partner.save()
        self._update_contact(user)

        return Response(
            data={
                "msg": _("Email updated successfully")
            },
            status=status.HTTP_200_OK
        )


class CodeRecoveryPasswordEmailAPI(APIView):

    def _send_email(self, user, email, code_value):
        """
            Function to send email with code value
        """
        partner_full_name = user.get_full_name()
        subject = _("Validation code - Password Recovery")
        EmailThread(
            html="send_alert_password_recovery.html",
            email=email,
            subject=subject,
            data={
                "PARTNER_FULL_NAME": partner_full_name,
                "TITLE": _("Validation Code"),
                "GREETING": _("Hi,"),
                "GREETING_2": _("Greetings, "),
                "LOGO_HEADER": settings.TEMPLATE_HEADER_LOGO,
                "TEMPLATE_IMAGE_INLAZZ": settings.TEMPLATE_IMAGE_INLAZZ,
                "CUSTOMER_SERVICE_PART_2": _("Your validation code is"),
                "CODE": code_value,
                "CUSTOMER_SERVICE_PART_3": _("Remember that your password must meet the following requirements"),
                "CUSTOMER_SERVICE_PART_4": _("Minimum length 8 characters"),
                "CUSTOMER_SERVICE_PART_5": _("At least 1 upper case character"),
                "CUSTOMER_SERVICE_PART_6": _("At least 1 lower case character"),
                "CUSTOMER_SERVICE_PART_7": _("At least 1 special character (@#*_)"),
                "CUSTOMER_SERVICE_PART_8": _("do not share the confirmation code with any other person for any reason"),
                "CUSTOMER_SERVICE_PART_9": _("We are sending this email to keep you informed about your inlaze account"),
                "CUSTOMER_SERVICE_PART_10": _("Almost done. To get a new password for your account enter the following code"),
                "CUSTOMER_SERVICE_PART_11": _("If you have not requested a new password, you can ignore this email. Your current password will continue to work."),
                "FOOTER_SERVICE_PART_1": _("inlaze team"),
                "FOOTER_SERVICE_PART_2": _("© inlaze 2022. All rights reserved."),
                "DATE": "2022",
            }
        ).start()

    def post(self, request):
        """
            Send code verification to email

            #Body
           -  email : "str"
                Param to define email to send code
        """
        validator = Validator(
            {
                "email": {
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
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Validate if exist a user with this email
        filters = (
            Q(email=validator.document.get("email")),
        )
        user = User.objects.filter(*filters).first()
        if not user:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user": [
                            _("This email does not exists"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        email_user = user.email
        filters = (
            Q(user__email=email_user),
        )
        validationcode = ValidationCode.objects.filter(*filters).first()

        # Create new code if time is expired
        default_minutes = int(settings.EXPIRATION_ADDER_MINUTES)
        if not validationcode:
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "user": user.pk,
                "code": validation_code_number,
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                data=data,
            )
            code_to_send = validation_code_number
        elif validationcode and validationcode.expiration < timezone.now():
            validation_code_number = str(int(settings.MIN_DIGITS) +
                                         floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
            data = {
                "code": validation_code_number,
                "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True,
            )
            code_to_send = validation_code_number
        else:
            data = {
                "created_at": timezone.now(),
            }
            validation_code_serializer = ValidationCodePhase1BSer(
                instance=validationcode,
                data=data,
                partial=True,
            )
            code_to_send = validationcode.code

        if validation_code_serializer.is_valid():
            validation_code_serializer.save()
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": validation_code_serializer.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            self._send_email(
                user=user,
                email=email_user,
                code_value=code_to_send,
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                data={
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {
                        "non_field_errors": [
                            "".join(str(exc_value)),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            data={
                "msg": _("Code was sent successfully"),
            },
            status=status.HTTP_200_OK,
        )


class CodeRecoveryPasswordPhoneAPI(APIView):

    def post(self, request):
        """
            Send code to change phone

            #Body

           -  email : "str"
                Param used to get a user's phone to send code
           -  valid_phone_by : "integer"
                Param to define type method to send code (SMS, WPP)
        """
        validator = Validator(
            schema={
                "email": {
                    "required": True,
                    "type": "string",
                    "coerce": to_lower,
                    "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
                },
                "valid_phone_by": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Partner.ValidPhoneBy.values,
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

        user = User.objects.filter(email=validator.document.get("email")).first()
        if not user:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "email": [
                            _("User not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif not user.phone:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("User has no phone"),
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
        validation_code.phone = user.phone
        validation_code.attempts = 0

        if (res := send_phone_message(
            phone=user.phone,
            valid_phone_by=validator.document.get("valid_phone_by"),
            validation_code=code,
        )):
            return res

        validation_code.save()
        return Response(status=status.HTTP_200_OK)


class ValidateCodeRecoveryPasswordAPI(APIView):
    """
    Class view with resources to validate the validation code sent for password recovery
    """

    def post(self, request):
        """
        Method that validates the code sent
        """
        validator = Validator(
            schema={
                "email": {
                    "required": True,
                    "type": "string",
                },
                "code": {
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

        user = User.objects.filter(email=validator.document.get("email")).first()
        if not user:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "email": [
                            _("User not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

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

        with transaction.atomic(using=DB_USER_PARTNER):
            token = Token.objects.update_or_create(
                user=user,
                defaults={
                    "user": user,
                }
            )[0]
            user.validation_code.delete()

        return Response(
            data={
                "token": token.key,
            },
            status=status.HTTP_200_OK,
        )
