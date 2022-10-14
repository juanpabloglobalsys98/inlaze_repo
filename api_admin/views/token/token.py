from api_admin.paginators import GetTokensAuth
from api_admin.serializers import UserTokenSerializer
from api_partner.helpers import DB_USER_PARTNER
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    request_cfg,
)
from core.models import User
from django.conf import settings
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class TokenShowAPI(APIView, GetTokensAuth):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        request_cfg.is_partner = True
        """ 
            Show tokens to authenticate 

            #Body
           -  email : "str"
                Param to define email to 
           -  full_name : "str"
                Param to define until date return membert report records
           -  lim
           -  offs

        """
        validator = Validator(
            schema={
                "email": {
                    "required": False,
                    "type": "string",
                },
                "full_name": {
                    "required": False,
                    "type": "string",
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                },
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = []
        sort_by = "-date_joined"
        if "sort_by" in request.query_params:
            sort_by = request.query_params.get("sort_by")

        if "email" in request.query_params:
            filters.append(
                Q(email__icontains=request.query_params.get("email")),
            )
        if "full_name" in request.query_params:
            filters.append(
                Q(full_name__icontains=request.query_params.get("full_name")),
            )

        users = User.objects.using(DB_USER_PARTNER).annotate(
            full_name=Concat(
                "first_name",
                Value(" "),
                "second_name",
                Value(" "),
                "last_name",
                Value(" "),
                "second_last_name",
            ),
        ).filter(*filters).order_by(sort_by)
        token_paginator = self.paginate_queryset(
            users,
            request,
            view=self,
        )
        tokenserializer = UserTokenSerializer(token_paginator, many=True)
        return Response(
            data={
                "tokens": tokenserializer.data,
            }, headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        """
            Method that create access token

             #Body
           -  id : "int"
                Param to identify partner to create token access

        """

        request_cfg.is_partner = True

        validator = Validator(
            schema={
                "id": {
                    "required": True,
                    "type": "integer",
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = User.objects.using(DB_USER_PARTNER).filter(Q(id=validator.document.get("id"))).first()

        if not user:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "user": _("User not found"),
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if hasattr(user, "auth_token"):
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "user": _("User already has token"),
                    },
                },
                status=status.HTTP_409_CONFLICT,
            )

        token = Token.objects.db_manager(DB_USER_PARTNER).create(user=user)

        return Response(
            data={
                "token": token.key,
            },
            status=status.HTTP_200_OK,
        )
