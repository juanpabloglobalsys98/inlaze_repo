import logging
import sys
import traceback
from math import floor
from random import random

from api_admin.helpers import DB_ADMIN
from api_partner.helpers import (
    DB_USER_PARTNER,
    AllowedChannels,
    IsNotBanned,
    IsNotFullRegister,
    IsNotToBeVerified,
    IsTerms,
    IsUploadedAll,
    NoLevel,
    NormalizePartnerRegInfo,
    PartnerLevelCHO,
    PartnerStatusCHO,
    ValidationPhoneEmail,
    get_adviser_id_for_partner,
)
from api_partner.models import (
    AdditionalInfo,
    BankAccount,
    Company,
    DocumentCompany,
    DocumentPartner,
    Partner,
    PartnerLevelRequest,
    SocialChannelRequest,
    ValidationCode,
    ValidationCodeRegister,
)
from api_partner.serializers import (
    AdditionalInfoSerializer,
    BankAccountBasicSerializer,
    CompanySerializer,
    DocumentsCompanySerializer,
    DocumentsPartnerSerializer,
    PartnerLevelRequestSER,
    PartnerSerializer,
    PartnerStatusSER,
    ValidationCodePhase1BSer,
    ValidationCodeRegisterSerializer,
)
from cerberus import Validator
from core.helpers import (
    CountryPhone,
    EmailThread,
    PartnerFilesNamesErrorHandler,
    StandardErrorHandler,
    ValidatorFile,
    generate_validation_code,
    normalize,
    normalize_capitalize,
    request_cfg,
    send_phone_message,
    to_int,
    to_lower,
)
from core.models import User
from core.serializers.user import (
    UserBasicSerializer,
    UserRequiredInfoSerializer,
)
from core.tasks import chat_logger as chat_logger_task
from django.conf import settings
from django.contrib.auth.hashers import make_password
from django.contrib.auth.models import update_last_login
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
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
from twilio.base.exceptions import TwilioException
from twilio.rest import Client

logger = logging.getLogger(__name__)


class PreLogUpAPI(APIView):
    """
    Process pre-signup.
    """

    def post(self, request):

        validator_query = Validator(
            schema={
                "valid_phone_by": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Partner.ValidPhoneBy.values,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator_query.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = Validator(
            schema={
                "full_name": {
                    "required": True,
                    "coerce": normalize_capitalize,
                    "type": "string",
                },
                "email": {
                    "required": True,
                    "type": "string",
                    "coerce": to_lower,
                    "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
                },
                "phone": {
                    "required": True,
                    "type": "string",
                },
                "password": {
                    "required": True,
                    "type": "string",
                },
                "adviser_id": {
                    "required": False,
                    "type": "integer",
                },
                "reg_source": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Partner.RegSource.values,
                    "default": Partner.RegSource.LANDING,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(
            document=request.data,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        full_name = validator.document.get("full_name")
        email = validator.document.get("email")
        phone = validator.document.get("phone")
        password = validator.document.get("password")
        adviser_id = validator.document.get("adviser_id")
        reg_source = validator.document.get("reg_source")
        valid_phone_by = validator_query.document.get("valid_phone_by")

        query = Q(email=email) | Q(phone=phone)
        user = User.objects.using(DB_USER_PARTNER).filter(
            query,
        ).first()

        if user:
            data = {
                "error": settings.USER_ALREADY_EXIST,
                "detail": {},
            }
            if email == user.email:
                data["detail"]["email"] = [
                    _("There is a user in the system with that email"),
                ]
            if phone == user.phone:
                data["detail"]["phone"] = [
                    _("There is a user in the system with that phone"),
                ]

            return Response(
                data=data,
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Create validation code
        first_name = second_name = last_name = second_last_name = ""
        full_name_splitted = full_name.split(" ")
        if (len(full_name_splitted) == 4):
            first_name, second_name, last_name, second_last_name = full_name_splitted
        elif(len(full_name_splitted) == 3):
            first_name, last_name, second_last_name = full_name_splitted
        elif(len(full_name_splitted) == 2):
            first_name, last_name = full_name_splitted
        else:
            first_name = full_name[:150]

        validation_code, expiration = generate_validation_code()
        data = {
            "first_name": first_name,
            "second_name": second_name,
            "last_name": last_name,
            "second_last_name": second_last_name,
            "email": email,
            "password": make_password(password=password),
            "phone": phone,
            "valid_phone_by": valid_phone_by,
            "adviser_id": get_adviser_id_for_partner(adviser_id),
            "code": validation_code,
            "expiration": expiration,
            "reg_source": reg_source,
        }
        validation_code_register_ser = ValidationCodeRegisterSerializer(data=data)

        if not validation_code_register_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": validation_code_register_ser.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        # Create user object without saving it to the DB
        user = User(
            email=email,
            phone=phone,
        )

        try:
            # Validate password with user object
            validate_password(password=password, user=user)
        except ValidationError as e:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "password": e,
                    },
                },
            )

        if (res := send_phone_message(
            phone=phone,
            valid_phone_by=valid_phone_by,
            validation_code=validation_code,
        )):
            return res
        validation_code_register = validation_code_register_ser.save()
        return Response(
            data={
                "pk": validation_code_register.pk,
            },
            status=status.HTTP_201_CREATED,
        )


class PreLogUpValidateAPI(APIView):
    """
    Validate the code sent to user's phone.
    """

    def post(self, request):

        validator = Validator(
            schema={
                "pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "code": {
                    "required": True,
                    "type": "string",
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(
            document=request.data,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q(pk=validator.document.get("pk"))
        validation_code_register = ValidationCodeRegister.objects.db_manager(DB_USER_PARTNER).filter(query).first()
        code = validator.document.get("code")

        if validation_code_register is None:
            return Response(
                data={
                    "error": settings.CODE_REGISTER_PK_NOT_FOUND,
                    "detail": {
                        "pk": [
                            _("Validation Code Register not found"),
                        ]
                    }
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif validation_code_register.code != code:
            validation_code_register_ser = ValidationCodeRegisterSerializer(
                instance=validation_code_register,
                data={
                    "attempts": validation_code_register.attempts + 1,
                },
                partial=True,
            )
            if not validation_code_register_ser.is_valid():
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "detail": validation_code_register_ser.errors
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            validation_code_register_ser.save()

            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "code": [
                            _("Invalid code"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif validation_code_register.expiration < timezone.now():
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
        elif validation_code_register.attempts > int(settings.MAX_VALIDATION_CODE_ATTEMPTS):
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
        elif validation_code_register.is_used:
            logger.warning("Validation code Register with is_used True was re-used")
            return Response(
                data={
                    "error": settings.CODE_REGISTER_IS_USED,
                    "detail": {
                        "code": [
                            _("Account already exists"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q(email=validation_code_register.email) | Q(phone=validation_code_register.phone)
        if User.objects.using(DB_USER_PARTNER).filter(query).first():
            return Response(
                data={
                    "error": settings.USER_ALREADY_EXIST,
                    "detail": {
                        "code": [
                            _("Account already exists"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Update is_used
        validation_code_register_ser = ValidationCodeRegisterSerializer(
            instance=validation_code_register,
            data={
                "is_used": True,
            },
            partial=True,
        )
        with transaction.atomic(using=DB_USER_PARTNER):
            if not validation_code_register_ser.is_valid():
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "detail": validation_code_register_ser.errors
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            validation_code_register_ser.save()

            language = request.META.get('HTTP_APP_LANGUAGE', 'en')[:2]
            language = next(
                (l[0] for l in settings.LANGUAGES if l[0] == language),
                'en'
            )

            # Create user and partner
            user = User.objects.create(
                first_name=validation_code_register.first_name,
                second_name=validation_code_register.second_name,
                last_name=validation_code_register.last_name,
                second_last_name=validation_code_register.second_last_name,
                email=validation_code_register.email,
                password=validation_code_register.password,
                phone=validation_code_register.phone,
                language=language,
            )
            partner = Partner.objects.create(
                user=user,
                adviser_id=validation_code_register.adviser_id,
                valid_phone_by=validation_code_register.valid_phone_by,
                is_phone_valid=True,
                reg_source=validation_code_register.reg_source,
            )
            additional_info = AdditionalInfo.objects.create(
                partner=partner,
                country=CountryPhone.get_country(user.phone),
            )

            update_last_login(None, user)
            token = Token.objects.update_or_create(
                user=user,
                defaults={
                    "user": user,
                },
            )[0]

        adviser = User.objects.using(DB_ADMIN).filter(pk=partner.adviser_id).first()
        chat_logger_task.apply_async(
            kwargs={
                "msg": (
                    f"a user has registered: {user.email}  {user.phone}. "
                    f"It was assigned to {adviser}."
                ),
                "msg_url": settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION,
            },
        )

        return Response(
            data={
                "token": token.key,
            },
            status=status.HTTP_201_CREATED
        )


class PreLogUpResendAPI(APIView):
    """
    Check if a validation code can be re sent.
    """
    pass

    def post(self, request):
        validator = Validator(
            schema={
                "pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
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

        if not validator.validate(
            document=request.data,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q(pk=validator.document.get("pk"))
        validation_code_register = ValidationCodeRegister.objects.db_manager(DB_USER_PARTNER).filter(query).first()

        if (
            validation_code_register is None
            or
            (
                (timezone.now() - validation_code_register.created_at).seconds >
                settings.CODE_REGISTER_MAX_CREATED_SECONDS
            )
        ):
            # Code not found or it was created long ago
            return Response(
                data={
                    "error": settings.CODE_REGISTER_PK_NOT_FOUND,
                    "detail": {
                        "pk": [
                            _("Validation Code Register not found"),
                        ]
                    }
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif validation_code_register.is_used:
            logger.warning("Validation code Register with is_used True was re-used")
            return Response(
                data={
                    "error": settings.CODE_REGISTER_IS_USED,
                    "detail": {
                        "code": [
                            _("Account already exists"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        else:
            # Code has not expired yet, resend it and update its expiration
            # or code expired, send a new one

            is_expired = validation_code_register.expiration < timezone.now()
            code, expiration = generate_validation_code()
            valid_phone_by = validator.document.get("valid_phone_by")
            data = {
                "expiration": expiration,
                "valid_phone_by": valid_phone_by,
            }
            if is_expired:
                data["code"] = code

            validation_code_register_ser = ValidationCodeRegisterSerializer(
                instance=validation_code_register,
                data=data,
                partial=True,
            )
            if not validation_code_register_ser.is_valid():
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "detail": validation_code_register_ser.errors,
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
            if (res := send_phone_message(
                phone=validation_code_register.phone,
                valid_phone_by=valid_phone_by,
                validation_code=validation_code_register.code,
            )):
                return res
            validation_code_register_ser.save()
            logger.debug(f"sent code {validation_code_register.code} with new expiration")

        return Response(status=status.HTTP_200_OK)


class LogUpAccountLevelBasicAPI(APIView):
    permission_classes = (
        IsAuthenticated,
        IsTerms,
        NoLevel,
    )

    def post(self, request):
        validator = Validator(
            schema={
                "account_level": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": [PartnerLevelCHO.BASIC],
                },
            },
        )

        if not validator.validate(
            document=request.data,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = request.user.partner
        partner_ser = PartnerSerializer(
            instance=partner,
            data={
                "level": PartnerLevelCHO.BASIC,
            },
            partial=True,
        )
        if not partner_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        partner_ser.save()
        return Response(status=status.HTTP_204_NO_CONTENT)


class LogUpAccountLevelPrimeAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsTerms,
    )

    def post(self, request):
        validator = Validator(
            schema={
                "account_level": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": [PartnerLevelCHO.PRIME],
                },
                "social_channels": {
                    "required": True,
                    "type": "list",
                    "empty":  False,
                    "schema": {
                        "type": "dict",
                        "required": True,
                        "schema": {
                            "name": {
                                "type": "string",
                                "required": True,
                            },
                            "type": {
                                "type": "integer",
                                "required": True,
                                "allowed": AllowedChannels.values
                            },
                            "url": {
                                "type": "string",
                                "required": True,
                            },
                        },
                    },
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(
            document=request.data,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner: Partner = request.user.partner
        if partner.level == PartnerLevelCHO.PRIME:
            msg = _("Your account level is already {}")
            msg = msg.format(PartnerLevelCHO.PRIME.label)
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "level": [
                            msg,
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        query = Q(partner=partner) & Q(status=PartnerStatusCHO.REQUESTED)
        partner_level_requested = PartnerLevelRequest.objects.filter(query).first()
        if partner_level_requested is not None:
            partner.level_status = PartnerStatusCHO.REQUESTED,
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "level": [
                            _("Already requested"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        data = {}
        if partner.level is None:
            data["level"] = PartnerLevelCHO.BASIC

        partner_request_ser = PartnerLevelRequestSER(
            data={
                "partner": partner,
            },
        )

        if not partner_request_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_request_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_ser = PartnerSerializer(
            instance=partner,
            data={
                "level_status": PartnerStatusCHO.REQUESTED,
            } | data,
            partial=True,
        )
        if not partner_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        partner_level_request = PartnerLevelRequest(
            partner=partner,
            level=PartnerLevelCHO.PRIME,
        )
        social_channels = validator.document.get("social_channels")
        channel_requests_to_create = []
        channel_urls = []
        for channel in social_channels:
            data = {"partner_level_request": partner_level_request} | channel
            channel_urls.append(channel.get("url"))
            channel_requests_to_create.append(SocialChannelRequest(**data))

        if len(channel_urls) != len(set(channel_urls)):
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "social_channels": [
                            _("You can't upload the same url more than once"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic(using=DB_USER_PARTNER):
            partner_level_request.save()
            SocialChannelRequest.objects.bulk_create(
                objs=channel_requests_to_create,
            )
            if partner_ser is not None:
                partner_ser.save()

        chat_logger_task.apply_async(
            kwargs={
                "msg": (
                    f"Partner {partner.user.get_full_name()} - {partner.user.email} "
                    "has requested a level change to Prime."
                ),
                "msg_url": settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION,
            },
        )
        return Response(status=status.HTTP_201_CREATED)


class LogUpPhase1API(APIView):

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Let the user pass from pre-log up status to a log up phase 1 status.\n
        The system will request the validation code generated in the previous phase
        If the code is correct the system gives 200 as response\n
        If the code is not correct, it could be for two reasons:\n
        * Bad code, if it is the case the system counts one attempt.
        if for some reason the code reaches its max attempts the system will delete the code
        and give a 409 response.
        * Code has expired, the code has reached its max allowed time. The system will give 409
        response for this case.\n\n
        After ending this phase 1 the user will already exist in Betenlace and it must complete
        the log up process.\n
        If the user is not linked by an adviser the system will do it automatically for them.
        """
        validator = Validator(
            {
                "email": {
                    "required": False,
                    "type": "string",
                },
                "phone": {
                    "required": False,
                    "type": "string",
                    "coerce": to_lower,
                },
                "valid_phone_by": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                    "allowed": Partner.ValidPhoneBy.values
                },
                "code": {
                    "required": True,
                    "type": "string",
                },
                "notify_campaign": {
                    "required": True,
                    "type": "boolean",
                },
                "notify_notice": {
                    "required": True,
                    "type": "boolean",
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

        #  Validate that email and phone not come together
        if "email" in validator.document and "phone" in validator.document:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Email and phone can not come together"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Validate if phone and valid_phone_by fields come together
        if (
            "phone" in validator.document and not "valid_phone_by" in validator.document
            or
            "valid_phone_by" in validator.document and not "phone" in validator.document
        ):
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Phone and valid_phone_by are neccesary together"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        # Get validation code by email
        query = ()
        if "email" in validator.document:
            query = (
                Q(
                    email=validator.document.get("email")
                ),
            )
        elif "phone" in validator.document:
            query = (
                Q(
                    phone=validator.document.get("phone")
                ),
            )
        else:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Email or phone not sent"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validation_code = ValidationCodeRegister.objects.db_manager(DB_USER_PARTNER).filter(*query).first()

        if not validation_code:
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

        if validation_code.code != validator.document.get("code"):
            attempts = validation_code.attempts + 1
            if attempts > int(settings.MAX_VALIDATION_CODE_ATTEMPTS):
                validation_code.delete()
                return Response(
                    {
                        "error": settings.MAX_ATTEMPTS_REACHED,
                        "details": {
                            "validation_code": [
                                _("Max attempts for that code was reached")
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )

            validation_code.attempts = attempts
            validation_code.save()
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

        if timezone.now() > validation_code.expiration:
            validation_code.delete()
            return Response(
                {
                    "error": settings.EXPIRED_VALIDATION_CODE,
                    "details": {
                        "code": [
                            _("Validation code has expired"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user_info = {
            "email": validation_code.email,
            "phone": validation_code.phone,
            "password": validation_code.password,
        }

        serialized_user = UserBasicSerializer(data=user_info)
        if serialized_user.is_valid():
            user = serialized_user.create_without_encrypted_password(database=DB_USER_PARTNER)
        else:
            transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)
            return Response(
                {
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_user.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # setting adviser code
        adviser_id = adviser_id = settings.ADVISER_ID_LINKED_DEFAULT
        was_linked = False
        if validation_code.adviser_id is None:
            query = (
                Q(
                    was_linked=False
                ),
            )
            partners_non_linked = Partner.objects.db_manager(DB_USER_PARTNER).filter(*query).count()
            query = (
                Q(
                    is_active=True,
                    is_staff=True
                ),
            )
            available_advisers = User.objects.db_manager(DB_ADMIN).filter(*query)
            if available_advisers.count() > 0:
                index = (partners_non_linked % available_advisers.count())
                adviser_id = available_advisers[index].id
        else:
            was_linked = True
            adviser_id = validation_code.adviser_id

        # Verify if adviser exits
        query = (Q(id=adviser_id),)
        admin = User.objects.using(DB_ADMIN).filter(*query).first()

        if not admin:
            return Response(
                {
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "code": [
                            _("Adviser not found in DB"),
                        ],
                    },
                },
                status=status.HTTP_200_OK,
            )

        partner_info = {
            "user": user.id,
            "is_on_phase_two": True,
            "adviser_id": adviser_id,
            "was_linked": was_linked,
            "is_notify_campaign": validator.document.get("notify_campaign"),
            "is_notify_notice": validator.document.get("notify_notice"),
            "terms_at": timezone.now()
        }

        if "email" in validator.document:
            partner_info["is_email_valid"] = True
        elif "phone" in validator.document:
            partner_info["is_phone_valid"] = True
            partner_info["valid_phone_by"] = validator.document.get("valid_phone_by")

        serialized_partner = PartnerSerializer(data=partner_info, partial=True)
        if serialized_partner.is_valid():
            partner = serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)
            return Response(
                {
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )
        try:
            validation_code.delete()
        except Exception as e:
            transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)
            return Response(
                {
                    "error": settings.INTERNAL_SERVER_ERROR,
                    "details": {
                        "code": [
                            _("An error occurred deleting validation code register"),
                        ],
                    },
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        transaction.savepoint_commit(sid, using=DB_USER_PARTNER)
        # section to send emails
        token = Token.objects.update_or_create(user=user, defaults={"user": user})[0]
        if not admin:
            return Response({
                "token": token.key
            },
                status=status.HTTP_200_OK,
            )

        # Send notice to adviser to cliq
        try:
            adviser_full_name = admin.first_name + " " + admin.second_name + " " + admin.last_name + " " + admin.second_last_name
            CUSTOMER_HEADER_MESSAGE = "Nuevo partner"
            CUSTOMER_BODY_MESSAGE = "Un nuevo partner se ha preregistrado satisfactoriamente y el sistema se lo ha asignado a "
            msg = (
                f"*{CUSTOMER_HEADER_MESSAGE}*\n`{user.email}` \n{CUSTOMER_BODY_MESSAGE} {adviser_full_name}\n"
            )
            webhook_url = settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": webhook_url,
                },
            )
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error(
                f"ERROR to write log on Chat logger for webhook_url {settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION}, check traceback:\n\n{''.join((e))}"
            )

        partner_status = PartnerStatusSER(instance=partner)
        return Response(
            data={
                "partner_status":  partner_status.data,
                "token": token.key
            },
            status=status.HTTP_200_OK,
        )


class LogUpPhase1BAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsTerms,
    )

    def _send_email(self, email, validation_code):
        subject = _("Validation email")
        EmailThread(
            html="send_validation_code.html",
            email=email,
            subject=subject,
            data={
                "TITLE": _("Validation Code"),
                "LOGO_HEADER": settings.TEMPLATE_HEADER_LOGO,
                "CUSTOMER_SERVICE_PART_2": _("Your validation code is"),
                "CODE": validation_code,
                "CUSTOMER_SERVICE_PART_3": _("do not share the confirmation code with any other person for any reason"),
                "CUSTOMER_SERVICE_PART_4": _("We are sending this email to keep you informed about your Inlazz account"),
                "DATE": "2022",
            }
        ).start()

    def _send_wpp(self, validation_code, phone):
        msg = _("{} is your code. Never share this code with anyone, only use it on Inlazz.com.")
        msg = msg.format(validation_code)
        client = Client(
            settings.TWILIO_ACCOUNT_SID,
            settings.TWILIO_AUTH_TOKEN
        )
        from_send = "whatsapp:"+settings.TWILIO_BASE_NUMBER_WHATSAPP
        to_send = "whatsapp:"+phone.replace(" ", "")
        message = client.messages \
            .create(
                body=msg,
                from_=from_send,
                to=to_send
            )
        logger.warning(
            f"Whatsapp MSM sending to phone: {phone} with SID: {message.sid}"
        )

    def _send_sms(self, validation_code, phone):
        msg = _("{} is your code. Never share this code with anyone, only use it on Inlazz.com.")
        msg = msg.format(validation_code)
        client = Client(
            settings.TWILIO_ACCOUNT_SID,
            settings.TWILIO_AUTH_TOKEN
        )
        message = client.messages \
            .create(
                body=msg,
                from_=settings.TWILIO_BASE_NUMBER,
                to=phone.replace(" ", "")
            )
        logger.warning(
            f"SMS sending to phone: {phone} with SID: {message.sid}"
        )

    def post(self, request):
        """
            Send validation code to confirm either email or phone
        """
        validator = Validator(
            {
                "email": {
                    "required": False,
                    "type": "string",
                },
                "phone": {
                    "required": False,
                    "type": "string",
                },
                "valid_phone_by": {
                    "required": False,
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
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = request.user

        if not user:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user": [
                            _("User not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = user.partner
        state = False
        msg = ""

        if(partner.is_email_valid == True and "email" in validator.document):
            msg = _("The email already is validated")
            state = True
        elif (partner.is_phone_valid == True and "phone" in validator.document):
            msg = _("The phone already is validated")
            state = True

        if state:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "non_field_errors": [
                            msg,
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        default_minutes = int(settings.EXPIRATION_ADDER_MINUTES)

        # Validate if phone and valid_phone_by fields come together
        if (
            "phone" in validator.document and not "valid_phone_by" in validator.document
            or
            "valid_phone_by" in validator.document and not "phone" in validator.document
        ):
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Phone and valid_phone_by are neccesary together"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Send code to email
        if "email" in validator.document:
            # Validate that the email not exists in DB
            query = (
                Q(email=validator.document.get("email")),
            )
            user_validate_email = User.objects.filter(*query).first()
            if user_validate_email:
                return Response(
                    data={
                        "error": settings.CONFLICT_CODE,
                        "details": {
                            "user": [
                                _("The email that you trying to add already exists"),
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            query = (
                Q(email=validator.document.get("email")) |
                Q(user=user),
            )
            validationcode = ValidationCode.objects.filter(*query).first()
            code_to_send = 0
            if not validationcode:
                validation_code_number = str(int(settings.MIN_DIGITS) +
                                             floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
                data = {
                    "user": user.pk,
                    "email": validator.document.get("email"),
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
                    {
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "details": validation_code_serializer.errors
                    }, status=status.HTTP_400_BAD_REQUEST
                )
            try:
                self._send_email(
                    email=validator.document.get("email"),
                    validation_code=code_to_send
                )
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.critical("".join(e))
                return Response(
                    {
                        "error": settings.ERROR_SENDING_EMAIL,
                        "details": {
                            "non_field_errors": _("Error to send email")
                        }
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
        # Send code to phone
        elif "phone" in validator.document:
            # Validate that the phone not exists in DB
            query = (
                Q(phone=validator.document.get("phone")),
            )
            user_validate_phone = User.objects.filter(*query).first()
            if user_validate_phone:
                return Response(
                    data={
                        "error": settings.CONFLICT_CODE,
                        "details": {
                            "user": [
                                _("The phone that you trying to add already exists"),
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            # Send code to phone
            query = (
                Q(phone=validator.document.get("phone")) |
                Q(user=user),
            )
            validationcode = ValidationCode.objects.filter(*query).first()
            code_to_send = 0
            if not validationcode:
                validation_code_number = str(int(settings.MIN_DIGITS) +
                                             floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
                data = {
                    "user": user.pk,
                    "phone": validator.document.get("phone"),
                    "code": validation_code_number,
                    "expiration": timezone.now() + timezone.timedelta(minutes=default_minutes),
                }
                validation_code_serializer = ValidationCodePhase1BSer(data=data)
                code_to_send = validation_code_number
            elif validationcode and validationcode.expiration < timezone.now():
                validation_code_number = str(int(settings.MIN_DIGITS) +
                                             floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random()))
                data = {
                    "phone": validator.document.get("phone"),
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
                    "phone": validator.document.get("phone"),
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
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            try:
                if (validator.document.get("valid_phone_by") == Partner.ValidPhoneBy.SMS):
                    self._send_sms(
                        validation_code=code_to_send,
                        phone=validator.document.get("phone")
                    )
                else:
                    self._send_wpp(
                        validation_code=code_to_send,
                        phone=validator.document.get("phone")
                    )
            except TwilioException as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.critical("".join(e))
                if exc_value.code == settings.TWILIO_ERROR_CODE_INVALID_TO_PHONE:
                    msg = _("The number {} is not a valid phone number")
                    return Response(
                        data={
                            "error": settings.BAD_REQUEST_CODE,
                            "details": {
                                "non_field_errors": [
                                    _(msg.format(validator.document.get('phone'))),
                                ],
                            },
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                else:
                    return Response(
                        data={
                            "error": settings.INTERNAL_SERVER_ERROR,
                            "details": {
                                "non_field_errors": [_("Error to send message")],
                            },
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.critical("".join(e))
                error_msg = "is not a valid phone number"
                if error_msg in exc_value.msg:
                    return Response(
                        {
                            "error": settings.BAD_REQUEST_CODE,
                            "details": {
                                "non_field_errors": [_(exc_value.msg)],
                            }
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )
                else:
                    return Response(
                        {
                            "error": settings.INTERNAL_SERVER_ERROR,
                            "details": {
                                "non_field_errors": [_("Error to send message")],
                            }
                        },
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR
                    )

        return Response(status=status.HTTP_200_OK)


class LogUpPhase1CAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsTerms,
    )

    def put(self, request):
        """
            Update email or phone depending on the option chosen by the user and validating code number
        """
        validator = Validator(
            {
                "code": {
                    "required": True,
                    "type": "string",
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "phone": {
                    "required": False,
                    "type": "string",
                },
                "valid_phone_by": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                    "allowed": Partner.ValidPhoneBy.values,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = request.user
        partner = user.partner
        state = False
        msg = ""

        if(partner.is_email_valid == True and "email" in validator.document):
            msg = _("The email already is validated")
            state = True
        elif (partner.is_phone_valid == True and "phone" in validator.document):
            msg = _("The phone already is validated")
            state = True

        if state:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "non_field_errors": [
                            msg,
                        ]
                    }
                }
            )
        # Validate code
        if "email" in validator.document:
            query = (
                Q(
                    email=validator.document.get("email")
                ),
            )
        elif "phone" in validator.document:
            query = (
                Q(
                    phone=validator.document.get("phone")
                ),
            )
            if not "valid_phone_by" in validator.document:
                return Response(
                    data={
                        "error": settings.CONFLICT_CODE,
                        "details": {
                            "non_field_errors": [
                                _("Valid_phone_by not send and is necessary"),
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
        else:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Email or phone not sent"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validation_code = ValidationCode.objects.using(DB_USER_PARTNER).filter(*query).first()
        if not validation_code:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Not found validation code"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if validation_code.code != validator.document.get("code"):
            attempts = validation_code.attempts + 1
            if attempts > int(settings.MAX_VALIDATION_CODE_ATTEMPTS):
                validation_code.delete()
                return Response(
                    {
                        "error": settings.MAX_ATTEMPTS_REACHED,
                        "details": {
                            "validation_code": [
                                _("Max attempts for that code was reached")
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )

            validation_code.attempts = attempts
            validation_code.save()
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

        if timezone.now() > validation_code.expiration:
            validation_code.delete()
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

        msg = ""
        if "email" in validator.document:
            msg = "Email updated successfully"
            user.email = validator.document.get("email")
            user.partner.is_email_valid = True
            user.partner.save()
        elif "phone" in validator.document:
            msg = "Phone updated successfully"
            user.phone = validator.document.get("phone")
            user.partner.is_phone_valid = True

            user.partner.valid_phone_by = validator.document.get("valid_phone_by")
            user.partner.save()

        user.save()
        validation_code.delete()

        return Response(
            data={
                "msg": _(msg)
            },
            status=status.HTTP_200_OK
        )


class LogUpPhase2AAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified,
        IsNotFullRegister,
        ValidationPhoneEmail,
        IsTerms,
    )

    @ transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def put(self, request):
        """
        Let the user pass from phase 1 status to phase 2A status.\n
        In this phase the system asks the user for their basic information.\n
        According to user's information the system will determine the user general status.
        The general status is calculated considering the atomic status for every phase:
        * basic_info_status: status according to suministred basic info.
        * bank_status: status according to suministred bank info.
        * documents_status: status according to suministred documents.
        \nfor detailed information about status check partner model
        """

        validator = Validator(
            {
                "person_type": {
                    "required": True,
                    "type": "integer",
                    "coerce": int,
                    "allowed": AdditionalInfo.PersonType.values
                },
                "first_name": {
                    "required": True,
                    "type": "string",
                    "regex": "(.|\s)*\S(.|\s)*",
                    "coerce": normalize_capitalize,
                },
                "last_name": {
                    "required": True,
                    "type": "string",
                    "regex": "(.|\s)*\S(.|\s)*",
                    "coerce": normalize_capitalize,
                },
                "identification": {
                    "required": True,
                    "type": "string",
                    "nullable": True,
                },
                "identification_type": {
                    "required": True,
                    "type": "integer",
                    "nullable": True,
                    "coerce": to_int,
                },
                "country": {
                    "required": True,
                    "type": "string",
                },
                "city": {
                    "required": True,
                    "type": "string",
                    "coerce": normalize,
                },
                "fiscal_address": {
                    "required": True,
                    "type": "string",
                    "coerce": normalize,
                },
                "channel_name": {
                    "required": True,
                    "type": "string",
                    "coerce": normalize,
                },
                "channel_url": {
                    "required": True,
                    "type": "string",
                },
                "channel_type": {
                    "required": True,
                    "type": "integer",
                    "coerce": int,
                },
                "company_id": {
                    "required": False,
                    "nullable": True,
                    "type": "string",
                },
                "social_reason": {
                    "required": False,
                    "nullable": True,
                    "type": "string",
                    "coerce": normalize,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        # Split names and last_names
        NormalizePartnerRegInfo.normalize_additinal_info(None, validator.document)

        person_type = validator.document.get("person_type")
        user = request.user
        query = (
            Q(user=user),
        )
        partner = Partner.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
        query = (
            Q(partner=partner),
        )
        additional_info = AdditionalInfo.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
        # Update User with second name and second last name
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        serialized_user = UserRequiredInfoSerializer(instance=user, data=validator.document, partial=True)
        if serialized_user.is_valid():
            serialized_user.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_user.errors
            }, status=status.HTTP_400_BAD_REQUEST)
        COMPANY = AdditionalInfo.PersonType.COMPANY
        PERSON = AdditionalInfo.PersonType.PERSON
        if partner.basic_info_status == Partner.ValidationStatus.EMPTY:  # Validate basic partner's basic info
            validator.document["basic_info_status"] = Partner.ValidationStatus.UPLOADED
            partner.basic_info_status = Partner.ValidationStatus.UPLOADED
            validator.document["status"] = partner.get_status()
        elif partner.basic_info_status == Partner.ValidationStatus.REJECTED:
            if (
                (additional_info.person_type == PERSON and person_type == COMPANY) or
                (additional_info.person_type == COMPANY and person_type == PERSON)
            ):
                validator.document["documents_status"] = Partner.ValidationStatus.EMPTY
                partner.documents_status = Partner.ValidationStatus.EMPTY
                query = (
                    Q(partner=partner.user_id),
                )
                Company.objects.db_manager(DB_USER_PARTNER).filter(*query).delete()
                DocumentPartner.objects.db_manager(DB_USER_PARTNER).filter(*query).delete()
            validator.document["basic_info_status"] = Partner.ValidationStatus.FIXING_BAD_FIELDS
            partner.basic_info_status = Partner.ValidationStatus.FIXING_BAD_FIELDS
            partner.save()
            validator.document["status"] = partner.get_status()
        elif partner.basic_info_status == Partner.ValidationStatus.ACCEPTED:
            return Response({
                "error": settings.FORBIDDEN,
                "details": {
                    "identification": [
                        _("You have a accepted validation status"),
                    ],
                },
            },
                status=status.HTTP_403_FORBIDDEN,
            )
        elif partner.basic_info_status == Partner.ValidationStatus.TO_BE_VERIFIED:
            return Response({
                "error": settings.BAD_REQUEST_CODE,
                "details": {
                    "identification": [
                        _("You have to be verified first before attempting to update"),
                    ],
                },
            },
                status=status.HTTP_400_BAD_REQUEST,
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        validator.document["user"] = user.id
        validator.document["is_enterprise"] = person_type == AdditionalInfo.PersonType.COMPANY

        serialized_partner = PartnerSerializer(instance=partner, data=validator.document, partial=True)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_partner.errors
            }, status=status.HTTP_400_BAD_REQUEST)
        validator.document["partner"] = partner.user_id
        if additional_info:
            serialized_additional_info = AdditionalInfoSerializer(
                instance=additional_info, data=validator.document, partial=True)
        else:
            serialized_additional_info = AdditionalInfoSerializer(data=validator.document)

        if serialized_additional_info.is_valid():
            serialized_additional_info.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_additional_info.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner_status = PartnerStatusSER(instance=partner)
        old_person_type = additional_info.person_type if additional_info else None

        if old_person_type == COMPANY and person_type == PERSON or old_person_type == PERSON and person_type == COMPANY:
            query = (
                Q(company=partner.user_id),
            )
            company_documents = DocumentCompany.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
            if company_documents:
                company_documents.delete_rut_file()
                company_documents.delete_exist_legal_repr_file()
                query = (
                    Q(partner=id),
                )
                Company.objects.db_manager(DB_USER_PARTNER).filter(*query).delete()

            query = (
                Q(partner=id),
            )
            partner_documents = DocumentPartner.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
            if partner_documents:
                partner_documents.delete_bank_certification_file()
                partner_documents.delete_identification_file()
                # DocumentPartner.objects.db_manager(DB_USER_PARTNER).filter(partner=id).delete()
                partner_documents.delete()

            validator.document["documents_status"] = Partner.ValidationStatus.EMPTY
            partner.documents_status = Partner.ValidationStatus.EMPTY
            query = (
                Q(partner=partner.user_id),
            )
            bank_account = BankAccount.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
            if bank_account:
                validator.document["bank_status"] = Partner.ValidationStatus.UPLOADED
                partner.bank_status = Partner.ValidationStatus.UPLOADED
                partner.save()

            validator.document["status"] = partner.get_status()
            serialized_partner = PartnerSerializer(instance=partner, data=validator.document, partial=True)
            if serialized_partner.is_valid():
                serialized_partner.save()
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
                return Response({
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors
                }, status=status.HTTP_400_BAD_REQUEST)

        query = (
            Q(partner=partner.user_id),
        )
        partner_documents = DocumentPartner.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
        if not partner_documents:
            temporal_partner_documents = {
                'partner': partner.user_id
            }
            partner_documents = DocumentsPartnerSerializer(data=temporal_partner_documents)
            if partner_documents.is_valid():
                partner_documents = partner_documents.create(partner_documents.validated_data)
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "details": partner_documents.errors
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
        if person_type == AdditionalInfo.PersonType.PERSON:
            transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "partner_status":  partner_status.data
                },
                status=status.HTTP_200_OK,
            )

        there_was_not_company = False
        query = (
            Q(partner=partner.user_id),
        )
        company = Company.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
        if company:
            serialized_company = CompanySerializer(instance=company, data=validator.document, partial=True)
        else:
            there_was_not_company = True
            serialized_company = CompanySerializer(data=validator.document)

        if serialized_company.is_valid():
            company = serialized_company.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_company.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        if there_was_not_company:
            temporal_company_documents = {
                'partner': partner.user_id,
                'company': company.partner,
            }
            company_documents = DocumentsCompanySerializer(data=temporal_company_documents)
            if company_documents.is_valid():
                company_documents = company_documents.create(company_documents.validated_data)
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "details": company_documents.errors
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response({
            "partner_status":  partner_status.data
        },
            status=status.HTTP_200_OK,
        )


class LogUpPhase2BAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified,
        IsNotFullRegister,
        ValidationPhoneEmail,
        IsTerms,
    )

    @ transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def put(self, request):
        """
        Let the user pass from phase 2A status to phase 2B status.\n
        In this phase the system asks the user for their bank information.
        According to user's information the system will determine the user general status.
        The general status is calculated considering the atomic status for every phase:
        * basic_info_status: status according to suministred basic info.
        * bank_status: status according to suministred bank info.
        * documents_status: status according to suministred documents.
        \nfor detailed information about status check partner model
        """

        validator = ValidatorFile(
            {
                "bank_name": {
                    "required": False,
                    "nullable": True,
                    "type": "string",
                    "default": None,
                    "coerce": normalize
                },
                "account_number": {
                    "required": False,
                    "nullable": True,
                    "type": "string",
                    "default": None,
                },
                "account_type": {
                    "required": False,
                    "nullable": True,
                    "type": "integer",
                    "coerce": to_int,
                    "default": None,
                    "allowed": BankAccount.AccounType.values,
                },
                "swift_code": {
                    "required": False,
                    "nullable": True,
                    "type": "string",
                    "default": None,
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

        user = request.user
        # partner = PartnerSerializer.exist(None, user.id, DB_USER_PARTNER)
        query = (
            Q(user=user.id),
        )
        partner = Partner.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
        if partner.bank_status in (Partner.ValidationStatus.EMPTY, Partner.ValidationStatus.SKIPPED):
            validator.document["bank_status"] = Partner.ValidationStatus.UPLOADED
            partner.bank_status = Partner.ValidationStatus.UPLOADED
            partner.save()
            validator.document["status"] = partner.get_status()
        elif partner.bank_status == Partner.ValidationStatus.ACCEPTED:
            return Response(
                {
                    "error": settings.FORBIDDEN,
                    "details": {
                        "identification": [
                            _("You have a accepted validation status")
                        ],
                    },
                },
                status=status.HTTP_403_FORBIDDEN,
            )
        elif partner.bank_status == Partner.ValidationStatus.REJECTED:
            validator.document["bank_status"] = Partner.ValidationStatus.FIXING_BAD_FIELDS
            partner.bank_status = Partner.ValidationStatus.FIXING_BAD_FIELDS
            partner.save()
            validator.document["status"] = partner.get_status()
        elif partner.bank_status == Partner.ValidationStatus.TO_BE_VERIFIED:
            return Response(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "identification": [
                            _("You have to be verified first before attempting to update")
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        validator.document["user"] = user.id
        serialized_partner = PartnerSerializer(instance=partner, data=validator.document, partial=True)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.document["partner"] = user.id
        query = (
            Q(partner=user.id),
        )
        bank_account = BankAccount.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
        if bank_account:
            serialized_bank_account = BankAccountBasicSerializer(
                instance=bank_account, data=validator.document, partial=True)
        else:
            serialized_bank_account = BankAccountBasicSerializer(data=validator.document)

        if serialized_bank_account.is_valid():
            serialized_bank_account.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_bank_account.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        partner_status = PartnerStatusSER(instance=partner)
        return Response(
            data={
                "partner_status": partner_status.data,
            },
            status=status.HTTP_200_OK,
        )


class LogUpPhase2CAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified,
        IsNotFullRegister,
        ValidationPhoneEmail,
        IsTerms,
    )

    def put(self, request):
        """
        Let the user pass from phase 2B status to phase 2C status.\n
        In this phase the system asks the user for their documents.
        According to user's information the system will determine the user general status.
        The general status is calculated considering the atomic status for every phase:
        * basic_info_status: status according to suministred basic info.
        * bank_status: status according to suministred bank info.
        * documents_status: status according to suministred documents.
        \nfor detailed information about status check partner model
        """

        validator = ValidatorFile(
            {
                "bank_certification_file": {
                    "required": False,
                    "type": "file"
                },
                "identification_file": {
                    "required": False,
                    "type": "file"
                },
                "rut_file": {
                    "required": False,
                    "type": "file"
                },
                "exist_legal_repr_file": {
                    "required": False,
                    "type": "file"
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

        files_names_validator = Validator(
            {
                "bank_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|webp|JPEG|PDF|PNG|JPG|WEBP|)+",
                },
                "identification_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|webp|JPEG|PDF|PNG|JPG|WEBP|)+",
                },
                "rut_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|webp|JPEG|PDF|PNG|JPG|WEBP|)+",
                },
                "exist_legal_repr_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|webp|JPEG|PDF|PNG|JPG|WEBP|)+",
                },
            },
            error_handler=PartnerFilesNamesErrorHandler,
        )

        files_names = {}
        # Validate data from cerberus and save in files_names dict
        bank_certification = request.data.get("bank_certification_file")
        if bank_certification:
            files_names["bank_name"] = bank_certification.name

        identification_file = request.data.get("identification_file")
        if identification_file:
            files_names["identification_name"] = identification_file.name

        rut_file = request.data.get("rut_file")
        if rut_file:
            files_names["rut_name"] = rut_file.name

        exist_legal_repr_file = request.data.get("exist_legal_repr_file")
        if exist_legal_repr_file:
            files_names["exist_legal_repr_name"] = exist_legal_repr_file.name

        if not files_names_validator.validate(files_names):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": files_names_validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        # Get documents
        bank_certification_file = validator.document.get("bank_certification_file")
        identification_file = validator.document.get("identification_file")
        rut_file = validator.document.get("rut_file")
        exist_legal_repr_file = validator.document.get("exist_legal_repr_file")

        if not bank_certification_file and not identification_file and not rut_file and not exist_legal_repr_file:
            return Response(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("You are sending everything empty, you need to send at least one file")
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get Validation status and Person types
        EMPTY = Partner.ValidationStatus.EMPTY
        SKIPPED = Partner.ValidationStatus.SKIPPED
        UPLOADED = Partner.ValidationStatus.UPLOADED
        REJECTED = Partner.ValidationStatus.REJECTED
        FIXING_BAD_FIELDS = Partner.ValidationStatus.FIXING_BAD_FIELDS
        ACCEPTED = Partner.ValidationStatus.ACCEPTED
        TO_BE_VERIFIED = Partner.ValidationStatus.TO_BE_VERIFIED
        PERSON = AdditionalInfo.PersonType.PERSON
        COMPANY = AdditionalInfo.PersonType.COMPANY

        user = request.user
        partner = user.partner
        # partner_documents = DocumentsPartnerSerializer().exist(partner.user_id, DB_USER_PARTNER)
        query = (
            Q(partner=partner.user_id),
        )
        partner_documents = DocumentPartner.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
        # company_documents = DocumentsCompanySerializer().exist(partner.user_id, DB_USER_PARTNER)
        query = (
            Q(company=partner.user_id),
        )
        company_documents = DocumentCompany.objects.db_manager(DB_USER_PARTNER).filter(*query).first()

        person_type = partner.additionalinfo.person_type
        can_change_status = False

        if person_type == PERSON and partner_documents:
            sent_two_documents = identification_file and bank_certification_file
            sent_one_documentA = bank_certification_file and bool(partner_documents.identification_file)
            sent_one_documentB = identification_file and bool(partner_documents.bank_certification_file)
            has_two_documents = bool(
                partner_documents.identification_file) and bool(
                partner_documents.bank_certification_file)
            can_change_status = bool(
                sent_two_documents or sent_one_documentA or sent_one_documentB or has_two_documents)
        elif person_type == COMPANY and company_documents:
            sent_three_documentsA = identification_file and bank_certification_file and exist_legal_repr_file
            sent_three_documentsB = identification_file and bool(
                partner_documents.bank_certification_file) and bool(
                company_documents.exist_legal_repr_file)
            sent_three_documentsC = bool(
                partner_documents.identification_file) and bank_certification_file and bool(
                company_documents.exist_legal_repr_file)
            sent_three_documentsD = bool(partner_documents.identification_file) and bool(
                partner_documents.bank_certification_file) and exist_legal_repr_file
            sent_three_documentsE = identification_file and bank_certification_file and bool(
                company_documents.exist_legal_repr_file)
            sent_three_documentsF = identification_file and bool(
                partner_documents.bank_certification_file) and exist_legal_repr_file
            sent_three_documentsG = bool(
                partner_documents.identification_file) and bank_certification_file and exist_legal_repr_file
            sent_three_documentsH = identification_file and bank_certification_file and rut_file
            has_three_documents = bool(
                partner_documents.identification_file) and bool(
                partner_documents.bank_certification_file) and bool(
                company_documents.exist_legal_repr_file)
            can_change_status = bool(
                sent_three_documentsA
                or
                sent_three_documentsB
                or
                sent_three_documentsC
                or
                sent_three_documentsD
                or
                sent_three_documentsE
                or
                sent_three_documentsF
                or sent_three_documentsG or has_three_documents or sent_three_documentsH)

        elif person_type == PERSON:
            sent_two_documents = identification_file and bank_certification_file
            can_change_status = bool(sent_two_documents)
        elif person_type == COMPANY:
            sent_three_documents = identification_file and bank_certification_file and exist_legal_repr_file
            can_change_status = bool(sent_three_documents)

        if can_change_status and partner.documents_status in (EMPTY, SKIPPED):
            validator.document["documents_status"] = UPLOADED
            partner.documents_status = UPLOADED
            partner.save()
            validator.document["status"] = partner.get_status()
        elif can_change_status and partner.documents_status == REJECTED:
            validator.document["documents_status"] = FIXING_BAD_FIELDS
            partner.documents_status = FIXING_BAD_FIELDS
            partner.save()
            validator.document["status"] = partner.get_status()
        elif partner.documents_status == ACCEPTED:
            return Response({
                "error": settings.FORBIDDEN,
                "details": {
                    "identification": [
                        _("You have a accepted validation status"),
                    ],
                },
            },
                status=status.HTTP_403_FORBIDDEN,
            )
        elif partner.documents_status == TO_BE_VERIFIED:
            return Response({
                "error": settings.BAD_REQUEST_CODE,
                "details": {
                    "identification": [
                        _("You have to be verified first before attempting to update"),
                    ],
                },
            },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.document["user"] = user.id
        validator.document["company"] = validator.document["partner"] = partner.user_id

        if not partner_documents:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "identification": [
                            _("There are not such partner documents in the system"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            if not bool(partner_documents.bank_certification_file) and bank_certification_file:
                # DocumentsPartnerSerializer().create_file("bank_certification", partner_documents, bank_certification_file)
                partner_documents.create_bank_certification_file(bank_certification_file)
                partner_documents.save()
            elif partner_documents.bank_certification_file and bank_certification_file:
                # DocumentsPartnerSerializer().update_file("bank_certification", partner_documents, bank_certification_file)
                partner_documents.delete_bank_certification_file()
                partner_documents.update_bank_certification_file(bank_certification_file)
                partner_documents.save()

            if not bool(partner_documents.identification_file) and identification_file:
                # DocumentsPartnerSerializer().create_file("identification", partner_documents, identification_file)
                partner_documents.create_identification_file(identification_file)
                partner_documents.save()
            elif partner_documents.identification_file and identification_file:
                # DocumentsPartnerSerializer().update_file("identification", partner_documents, identification_file)
                partner_documents.delete_identification_file()
                partner_documents.update_identification_file(identification_file)
                partner_documents.save()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))

            if bool(partner_documents.bank_certification_file) and bank_certification_file:
                partner_documents.delete_bank_certification_file()

            if partner_documents.identification_file and identification_file:
                partner_documents.delete_identification_file()

            return Response(
                data={
                    "error": settings.ERROR_SAVING_PARTNER_DOCUMENTS,
                    "details": {
                        "non_field_errors": ["".join(str(exc_value))],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # partner_documents = DocumentsPartnerSerializer().exist(partner.user_id, DB_USER_PARTNER)
        query = (
            Q(partner=partner.user_id),
        )
        partner_documents = DocumentPartner.objects.db_manager(DB_USER_PARTNER).filter(*query).first()
        # checking bad integrity
        bad_files = []
        if not bool(partner_documents.identification_file) and identification_file:
            bad_files.append("identification_file")

        if not bool(partner_documents.bank_certification_file) and bank_certification_file:
            bad_files.append("bank_certification_file")

        if person_type == PERSON:
            if len(bad_files):
                return Response(
                    data={
                        "error": settings.BAD_FILES_INTEGRITY,
                        "details": {"bad_files": [bad_files]}
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            serialized_partner = PartnerSerializer(instance=partner, data=validator.document,  partial=True)
            if serialized_partner.is_valid():
                serialized_partner.save()
            else:
                return Response(
                    {
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "details": serialized_partner.errors
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            partner_status = PartnerStatusSER(instance=partner)
            return Response(
                data={
                    "partner_status": partner_status.data
                },
                status=status.HTTP_200_OK,
            )

        # company case
        # company = CompanySerializer().exist(partner.user_id, DB_USER_PARTNER)
        query = (
            Q(partner=partner.user_id),
        )
        company = Company.objects.db_manager(DB_USER_PARTNER).filter(*query).first()

        if not company:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "non_field_errors": [_("There is not such company in the system")],
                    }
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not company_documents:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "non_field_errors": [
                            _("There are not such company documents in the system"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            if not bool(company_documents.rut_file) and rut_file:
                # DocumentsCompanySerializer().create_file("rut_file", company_documents, rut_file)
                company_documents.create_rut_file(rut_file)
                company_documents.save()
            elif company_documents.rut_file and rut_file:
                # DocumentsCompanySerializer().update_file("rut_file", company_documents, rut_file)
                company_documents.delete_rut_file()
                company_documents.update_rut_file(rut_file)
                company_documents.save()
            if not bool(company_documents.exist_legal_repr_file) and exist_legal_repr_file:
                # DocumentsCompanySerializer().create_file("exist_legal_repr_file", company_documents, exist_legal_repr_file)
                company_documents.create_exist_legal_repr_file(exist_legal_repr_file)
                company_documents.save()
            elif company_documents.exist_legal_repr_file and exist_legal_repr_file:
                # DocumentsCompanySerializer().update_file("exist_legal_repr_file", company_documents, exist_legal_repr_file)
                company_documents.delete_exist_legal_repr_file()
                company_documents.update_exist_legal_repr_file(exist_legal_repr_file)
                company_documents.save()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            if bool(company_documents.rut_file) and rut_file:
                company_documents.delete_rut_file()
            if company_documents.exist_legal_repr_file and exist_legal_repr_file:
                company_documents.delete_exist_legal_repr_file()

            return Response(
                data={
                    "error": settings.ERROR_SAVING_PARTNER_DOCUMENTS,
                    "details": {
                        "non_field_errors": [
                            "".join(str(exc_value)),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        # company_documents = DocumentsCompanySerializer().exist(partner.user_id, DB_USER_PARTNER)

        if not bool(company_documents.rut_file) and rut_file:
            bad_files.append("rut_file")

        if not bool(company_documents.exist_legal_repr_file) and exist_legal_repr_file:
            bad_files.append("exist_legal_repr_file")

        if len(bad_files):
            return Response(
                data={
                    "error": settings.BAD_FILES_INTEGRITY,
                    "details": {
                        "bad_files": [bad_files],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        # upgrading status
        serialized_partner = PartnerSerializer(instance=partner, data=validator.document, partial=True)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            return Response(
                {
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        partner_status = PartnerStatusSER(instance=partner)
        return Response(
            data={
                "partner_status": partner_status.data
            },
            status=status.HTTP_200_OK,
        )


class ConcludeLogUp(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsUploadedAll,
        ValidationPhoneEmail,
        IsTerms,
    )

    @ transaction.atomic(
        using=DB_USER_PARTNER,
        savepoint=True,
    )
    def post(self, request):
        """
        Let the user pass from phase 2C status to be verified status.\n
        In this phase the system warns the assigned adviser via email for user's information
        validation.
        According to user's information the system will determine the user general status.
        The general status is calculated considering the atomic status for every phase:
        * basic_info_status: status according to suministred basic info.
        * bank_status: status according to suministred bank info.
        * documents_status: status according to suministred documents.
        \nfor detailed information about status check partner model
        """
        validator = Validator(
            schema={
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

        user = request.user
        partner = user.partner
        # Get partner account status before to change temporaly object status
        old_partner_status = partner.status

        if (old_partner_status == Partner.Status.UPLOADED_ALL):
            request.data["basic_info_status"] = Partner.ValidationStatus.TO_BE_VERIFIED
            request.data["bank_status"] = Partner.ValidationStatus.TO_BE_VERIFIED
            request.data["documents_status"] = Partner.ValidationStatus.TO_BE_VERIFIED

            partner.basic_info_status = Partner.ValidationStatus.TO_BE_VERIFIED
            partner.bank_status = Partner.ValidationStatus.TO_BE_VERIFIED
            partner.documents_status = Partner.ValidationStatus.TO_BE_VERIFIED
        elif (old_partner_status == Partner.Status.FULL_REGISTERED_SKIPPED_UPLOADED_ALL):
            # Already normal accepted prevent change
            if (partner.bank_status != Partner.ValidationStatus.ACCEPTED):
                request.data["bank_status"] = Partner.ValidationStatus.SKIPPED_TO_BE_VERIFIED
                partner.bank_status = Partner.ValidationStatus.SKIPPED_TO_BE_VERIFIED
            # Already normal accepted prevent change
            if (partner.documents_status != Partner.ValidationStatus.ACCEPTED):
                request.data["documents_status"] = Partner.ValidationStatus.SKIPPED_TO_BE_VERIFIED
                partner.documents_status = Partner.ValidationStatus.SKIPPED_TO_BE_VERIFIED

        request.data["status"] = partner.get_status()
        request.data["user"] = user.id

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        serialized_partner = PartnerSerializer(instance=partner, data=request.data, partial=True)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(
                sid=sid,
                using=DB_USER_PARTNER,
            )
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER,)

        # Send notification to adviser
        try:
            if (old_partner_status == Partner.Status.UPLOADED_ALL):
                CUSTOMER_BODY_MESSAGE = (
                    "Uno de los partners asignados ha completado el registro, sus documentos estan pendientes por "
                    "aprobar o rechazar"
                )
                CUSTOMER_HEADER_MESSAGE = ("*Proceso de registro de socios completado*")
                msg = (
                    f"{CUSTOMER_HEADER_MESSAGE} `{user.email}` \n{CUSTOMER_BODY_MESSAGE}\n"
                )
                msg_url = settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": msg,
                        "msg_url": msg_url,
                    },
                )
            # Case when user submit at after to skipped on registration process
            elif (Partner.Status.FULL_REGISTERED_SKIPPED_UPLOADED_ALL):
                CUSTOMER_BODY_MESSAGE = (
                    "Uno de sus socios asignados ha completado los datos que se habían omitido. sus documentos están "
                    "pendientes de revisión para su aprobación o rechazo."
                )
                CUSTOMER_HEADER_MESSAGE = ("*Registro de socios después de completar el proceso de omisión*")
                msg = (
                    f"{CUSTOMER_HEADER_MESSAGE} `{user.email}` \n{CUSTOMER_BODY_MESSAGE}\n"
                )
                msg_url = settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": msg,
                        "msg_url": msg_url,
                    },
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
                            _("Error at send email"),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        partner_status = PartnerStatusSER(instance=partner)
        return Response(
            data={
                "partner_status": partner_status.data
            },
            status=status.HTTP_200_OK
        )
