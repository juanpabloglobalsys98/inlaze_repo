from django.conf import settings
from api_admin.helpers import DB_ADMIN
from api_admin.serializers import AdminSerializer
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.contrib.auth import get_user_model
from django.contrib.auth.hashers import make_password
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

User = get_user_model()


class AdminApi(APIView):
    """
    UserApi View retreive and edit own Staff user data
    """

    def get(self, request):
        """
        Retrieve own staff user data

        Return data
        ---
        - data : `dict`
            the data values corresponding to the requested page,
            defined on UserSerializer

        Return data
        ---
        - message : `str`
            title of error
        - error : `str`
            Description and info about the error
        - data: `dict`
            user data after update
        """
        payload, admin_user = self.get_payload(request)
        if not payload:
            return Response(
                {
                    "message": _("Invalid token"),
                    "error": _("Invalid token or it's expired")
                }, status=status.HTTP_401_UNAUTHORIZED
            )

        serializer = AdminSerializer(admin_user)

        return Response(
            {
                "data": serializer.data,
            }, status=status.HTTP_200_OK
        )

    def patch(self, request):
        """ 
        Update some own data of Staff user

        Body
        ---
        - first_name : `string`
            First name of User (allowed only first and First and Second)
        - last_name : `string`
            Last name of User (allowed only first or First and Second)
        - email : `string`
            email of user, this will used how username
        - password : `string` 
            password without encrypt, on save will be encrypt
        - phone : `string`
            phone or mobile phone number

        Return data
        ---
        - message : `str`
            title of error
        - error : `str`
            Description and info about the error
        - data: `dict`
            user data after update
        """
        payload, admin_user = self.get_payload(request)
        if not payload:
            return Response(
                {
                    "message": _("Invalid token"),
                    "error": _("Invalid token or it's expired")
                }, status=status.HTTP_401_UNAUTHORIZED
            )

        validator = Validator(
            {
                "first_name": {
                    "required": False,
                    "type": "string"
                },
                "last_name": {
                    "required": False,
                    "type": "string"
                },
                "email": {
                    "required": True,
                    "type": "string"
                },
                "password": {
                    "required": False,
                    "type": "string"
                },
                "phone": {
                    "required": False,
                    "type": "string"
                }
            }, error_handler=StandardErrorHandler
        )
        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        new_password = request.data.get("password", None)
        if new_password:
            request.data["password"] = make_password(new_password)
        else:
            # Case when user not send password data
            request.data["password"] = admin_user.user.password

        return Response(status=status.HTTP_200_OK)
