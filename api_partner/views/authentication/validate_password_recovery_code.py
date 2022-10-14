from api_partner.helpers import DB_USER_PARTNER
from api_partner.serializers.authentication import ValidationCodeSerializer
from cerberus import Validator
from core.helpers import StandardErrorHandler
from core.models import User
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.views import APIView


class ValidatePasswordRecoveryCodeAPI(APIView):

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Verifies if the validation code entered by the user is correct or not\n
        If the code is correct the system gives 200 as response\n
        If the code is not correct, it could be for two reasons:\n
        * Bad code, if it is the case the system counts one attempt.
        if for some reason the code reaches its max attempts the system will delete the code
        and give a 409 response.
        * Code has expired, the code has reached its max allowed time. The system will give 409
        response for this case.\n
        For another case, the user could be entered a bad email, in this case the syste will give a 404
        code.
        """

        validator = Validator(
            {
                "email": {
                    "required": True,
                    "type": "string"
                },
                "validation_code": {
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

        validation_code = validator.document.get("validation_code")
        email = validator.document.get("email")
        validation_code = ValidationCodeSerializer.get_by_email_code(None, validation_code, email, DB_USER_PARTNER)
        validation_code_by_email = ValidationCodeSerializer.get_by_email(None, email, DB_USER_PARTNER)

        if validation_code is None:
            if not validation_code_by_email:
                return Response(
                    {
                        "error": settings.CONFLICT_CODE,
                        "details": {"validation_code": [_("Invalid code or email")]}
                    }, status=status.HTTP_409_CONFLICT
                )

            attempts = validation_code_by_email.attempts + 1
            if attempts > int(settings.MAX_VALIDATION_CODE_ATTEMPTS):
                ValidationCodeSerializer().delete(validation_code_by_email.id, DB_USER_PARTNER)
                return Response(
                    {
                        "error": settings.MAX_ATTEMPTS_REACHED,
                        "details": {"validation_code": [_("Max attempts for that code was reached")]}
                    }, status=status.HTTP_409_CONFLICT
                )

            data = {"attempts": validation_code_by_email.attempts + 1}
            serialized_validation_code = ValidationCodeSerializer(instance=validation_code_by_email, data=data)
            if serialized_validation_code.is_valid():
                serialized_validation_code.save()
                return Response(
                    {
                        "error": settings.CONFLICT_CODE,
                        "details": {"non_field_errors": [_("Invalid code or email")]}
                    }, status=status.HTTP_409_CONFLICT
                )
            else:
                return Response(
                    {
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "details": serialized_validation_code.errors
                    }, status=status.HTTP_409_CONFLICT
                )

        if timezone.now() > validation_code.expiration:
            return Response(
                {
                    "error": settings.EXPIRED_VALIDATION_CODE,
                    "details": {"validation_code": [_("Validation code has expired")]}
                }, status=status.HTTP_409_CONFLICT
            )

        user = User.objects.using(DB_USER_PARTNER).filter(email=email).exists()
        if not user:
            return Response(
                {
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"email": [_("There is not such user in the system")]}
                }, status=status.HTTP_404_NOT_FOUND
            )

        ValidationCodeSerializer().delete(validation_code.id, DB_USER_PARTNER)
        token = Token.objects.update_or_create(user=user, defaults={"user": user})[0]
        return Response({"token": token.key}, status=status.HTTP_200_OK)
