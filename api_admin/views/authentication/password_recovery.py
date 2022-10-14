from api_admin.helpers import DB_ADMIN
from api_admin.models import ValidationCode
from api_admin.serializers import ValidationCodeSer
from cerberus import Validator
from core.helpers import StandardErrorHandler
from core.models import User
from django.conf import settings
from django.contrib.auth.password_validation import (
    password_changed,
    validate_password,
)
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class ValidatePasswordRecoveryCodeAPI(APIView):

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def post(self, request):
        """
        Verifies if the validation code entered by the user is correct or not 
        If the code is correct the system gives status 200 with session token
        If the code is not correct, it could be for two reasons: 
        * Bad code, if it is the case the system counts one attempt. 
        if for some reason the code reaches its max attempts the system will 
        delete the code and give status 409.
        * Code has expired, the code has reached its max allowed time. The 
        system will give 409 response for this case. 
        For another case, the user could be entered a bad email, in this case 
        the syste will give a 404 code.
        """

        validator = Validator(
            schema={
                "email": {
                    "required": True,
                    "type": "string",
                },
                "validation_code": {
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
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(code=validator.document.get("validation_code")),
            Q(email=validator.document.get("email")),
        )
        validation_code = ValidationCode.objects.using(DB_ADMIN).filter(*filters).first()
        filters = (
            Q(email=validator.document.get("email")),
        )
        validation_code_by_email = ValidationCode.objects.using(DB_ADMIN).filter(*filters).first()

        sid = transaction.savepoint(using=DB_ADMIN)

        if validation_code is None:
            if validation_code_by_email is None:
                return Response(
                    data={
                        "error": settings.CONFLICT_CODE,
                        "detail": {
                            "validation_code": [
                                _("Invalid code or email"),
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )

            # Add +1 attemp and verify Max tries
            attempts = validation_code_by_email.attempts + 1
            if attempts > int(settings.MAX_VALIDATION_CODE_ATTEMPTS):
                validation_code_by_email.delete()
                return Response(
                    data={
                        "error": settings.MAX_ATTEMPTS_REACHED,
                        "detail": {
                            "validation_code": [
                                _("Max attempts for that code was reached"),
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )

            data = {
                "attempts": validation_code_by_email.attempts + 1,
            }
            validation_code_by_email_ser = ValidationCodeSer(
                instance=validation_code_by_email,
                data=data,
                partial=True,
            )
            if validation_code_by_email_ser.is_valid():
                validation_code_by_email_ser.save()
                return Response(
                    data={
                        "error": settings.CONFLICT_CODE,
                        "detail": {
                            "non_field_errors": [
                                _("Invalid code or email"),
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )
            else:
                transaction.savepoint_rollback(sid, using=DB_ADMIN)
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "detail": validation_code_by_email_ser.errors,
                    },
                    status=status.HTTP_409_CONFLICT,
                )

        if timezone.now() > validation_code.expiration:
            validation_code.delete()
            return Response(
                data={
                    "error": settings.EXPIRED_VALIDATION_CODE,
                    "detail": {
                        "validation_code": [
                            _("Validation code has expired"),
                        ],
                    },
                },
                status=status.HTTP_409_CONFLICT,
            )

        # Get instance user for setup authtoken
        filters = (
            Q(email=validator.document.get("email")),
        )
        user = User.objects.using(DB_ADMIN).filter(*filters).first()
        if user is None:
            return Response(
                {
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "email": [
                            _("There is not such user in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Generate a temp session
        token = Token.objects.update_or_create(
            user=user,
            defaults={
                "user": user,
            },
        )[0]

        return Response(
            data={
                "token": token.key,
            },
            status=status.HTTP_200_OK,
        )


class PasswordChangeRecoveryAPI(APIView):

    permission_classes = (
        IsAuthenticated,
    )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def put(self, request):
        """
        Lets an admin change their password
        """
        validator = Validator(
            {
                "validation_code": {
                    "required": True,
                    "type": "string",
                },
                "new_password": {
                    "required": True,
                    "type": "string",
                }
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(
            document=request.data,
        ):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        sid = transaction.savepoint(using=DB_ADMIN)

        user = request.user

        filters = (
            Q(email=request.user.email),
        )
        validation_code = ValidationCode.objects.using(DB_ADMIN).filter(*filters).first()

        # If validation ode for some reason was changed
        if(validation_code.code != validator.document.get("validation_code")):
            # Add +1 attemp and verify Max tries
            attempts = validation_code.attempts + 1
            if attempts > int(settings.MAX_VALIDATION_CODE_ATTEMPTS):
                validation_code.delete()
                return Response(
                    data={
                        "error": settings.MAX_ATTEMPTS_REACHED,
                        "detail": {
                            "validation_code": [
                                _("Max attempts for that code was reached"),
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )

            data = {
                "attempts": validation_code.attempts + 1,
            }
            validation_code_ser = ValidationCodeSer(
                instance=validation_code,
                data=data,
                partial=True,
            )
            if validation_code_ser.is_valid():
                validation_code_ser.save()
                return Response(
                    data={
                        "error": settings.CONFLICT_CODE,
                        "detail": {
                            "non_field_errors": [
                                _("Invalid code or email"),
                            ],
                        },
                    },
                    status=status.HTTP_409_CONFLICT,
                )
            else:
                transaction.savepoint_rollback(sid, using=DB_ADMIN)
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "detail": validation_code_ser.errors,
                    },
                    status=status.HTTP_409_CONFLICT,
                )

        if timezone.now() > validation_code.expiration:
            validation_code.delete()
            return Response(
                data={
                    "error": settings.EXPIRED_VALIDATION_CODE,
                    "detail": {
                        "validation_code": [
                            _("Validation code has expired"),
                        ],
                    },
                },
                status=status.HTTP_409_CONFLICT,
            )

        try:
            validate_password(
                password=validator.document.get("new_password"),
                user=user,
            )
        except Exception as e:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "new_password": [
                            "\n".join(e),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        user.set_password(validator.document.get("new_password"))
        user.save()
        password_changed(
            password=validator.document.get("new_password"),
            user=user,
        )

        # Delete validation code at sucess
        validation_code.delete()
        transaction.savepoint_commit(sid, using=DB_ADMIN)

        return Response(status=status.HTTP_200_OK)
