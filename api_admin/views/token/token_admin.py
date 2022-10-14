from api_admin.helpers import DB_ADMIN
from api_admin.paginators import GetTokensAuth
from api_admin.serializers import UserTokenSerializer
from api_partner.helpers import DB_USER_PARTNER
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
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
from core.helpers import StandardErrorHandler


class TokenShowAdminAPI(APIView, GetTokensAuth):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        ''' Show tokens to authenticate '''
        validator = Validator({
            'email': {
                'required': False,
                'type': 'string'
            },
            'full_name': {
                'required': False,
                'type': 'string'
            },
            'lim': {
                'required': False,
                'type': 'string'
            },
            'offs': {
                'required': False,
                'type': 'string'
            },
            'sort_by': {
                'required': False,
                'type': 'string'
            }
        })

        if not validator.validate(request.query_params):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        filters = []
        sort_by = "-date_joined"
        if "sort_by" in request.query_params:
            sort_by = request.query_params.get("sort_by")

        if "email" in request.query_params:
            filters.append(Q(email__icontains=request.query_params.get("email")))

        if "full_name" in request.query_params:
            filters.append(Q(full_name__icontains=request.query_params.get("full_name")))

        users = User.objects.using(DB_ADMIN).annotate(
            full_name=Concat(
                "first_name",
                Value(" "),
                "second_name",
                Value(" "),
                "last_name",
                Value(" "),
                "second_last_name",
            )
        ).filter(*filters).order_by(sort_by)

        token_paginator = self.paginate_queryset(users, request, view=self)
        tokenserializer = UserTokenSerializer(token_paginator, many=True)
        return Response({
            "tokens": tokenserializer.data
        }, headers={
            "count": self.count,
            "access-control-expose-headers": "count,next,previous"
        }, status=status.HTTP_200_OK)

    def post(self, request):

        validator = Validator({
            'id': {
                'required': True,
                'type': 'integer'
            }
        })

        if not validator.validate(request.data):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        filters = (
            Q(id=validator.document.get("id")),
        )
        user = User.objects.using(DB_ADMIN).filter(*filters).first()

        if not user:
            return Response({
                "error": settings.BAD_REQUEST_CODE,
                "details": {
                    "admin": _("Admin not found")
                }
            }, status=status.HTTP_400_BAD_REQUEST)

        if hasattr(user, "auth_token"):
            return Response({
                "error": settings.CONFLICT_CODE,
                "details": {
                    "admin": _("Admin already has token")
                }
            }, status=status.HTTP_409_CONFLICT)

        token = Token.objects.db_manager(DB_ADMIN).create(user=user)

        return Response({
            "token": token.key
        }, status=status.HTTP_200_OK)
