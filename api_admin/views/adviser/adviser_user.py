import logging
import sys
import traceback

from api_admin.helpers import DB_ADMIN
from api_admin.paginators import (
    AdminsManagementPaginator,
    AdviserManagementPaginator,
)
from api_admin.serializers import (
    AdminUserSerializer,
    AdviserUserSER,
    PartnerAdviserSER,
    ReferredUserSER,
)
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import Partner
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    request_cfg,
    to_float_0_null,
    to_int,
)
from core.models import (
    Rol,
    User,
)
from core.serializers.user import UserBasicForAdminSerializer
from django.conf import settings
from django.contrib.auth.hashers import make_password
from django.db import transaction
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class PartnerAdviserAPI(APIView):
    """
    Advisers assigned to partners.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Gets all advisers that have partners assigned.
        """
        adviser_pks = [
            p.adviser_id for p in Partner.objects.distinct("adviser_id")
        ]
        advisers = User.objects.using(DB_ADMIN).order_by("pk").filter(
            pk__in=adviser_pks,
        )
        advisers_ser = PartnerAdviserSER(
            instance=advisers,
            many=True,
        )
        return Response(
            data={
                "advisers": advisers_ser.data,
            },
            status=status.HTTP_200_OK,
        )


class AdviserManagementAPI(APIView, AdminsManagementPaginator):

    """
        Class to define structure to manage adviser
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
            Get info from others advisers

            #Body
           -  full_name : "str"
                Param to define fullname to return records
           -  email : "str"
                Param to define email to return records
           -  rol : "int"
                Param to define role id to return records
           -  is_staff : "bool"
                Param to define if is_staff to return records
           -  sort_by : "str"
                Param to sort data
           -  lim : "int"
           -  offs : "int"
        """
        validator = Validator(
            schema={
                "full_name": {
                    "required": False,
                    "type": "string",
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "rol": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "is_staff": {
                    "required": False,
                    "type": "integer",
                    "coerce": bool,
                },
                "sort_by": {
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
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                schema={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        admin = request.user
        filters = [
            ~Q(id=admin.pk),
        ]
        if "full_name" in validator.document:
            filters.append(
                Q(full_name__icontains=validator.document.get("full_name")),
            )

        if "email" in validator.document:
            filters.append(
                Q(email__icontains=validator.document.get("email")),
            )

        if "rol" in validator.document:
            rol = Rol.objects.filter(
                Q(id=validator.document.get("rol")),
            ).first()
            filters.append(
                Q(rol=rol),
            )

        if "is_staff" in validator.document:
            state = validator.document.get("is_staff")
            state = True if state == 1 else False
            filters.append(
                Q(is_staff=state),
            )

        sort_by = "-last_login"
        if "sort_by" in validator.document:
            sort_by = validator.document.get("sort_by")

        user = User.objects.annotate(
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
        user_paginated = self.paginate_queryset(user, request, view=self)
        admin_serializer = AdminUserSerializer(user_paginated, many=True)

        return Response(
            data={
                "admins": admin_serializer.data,
            },
            headers={
                "access-control-expose-headers": "count, next, previous",
                "count": self.count,
            },
        )

    @ transaction.atomic(using=DB_ADMIN, savepoint=True)
    def post(self, request):
        """
            Create new adviser

            #Body
           -  first_name : "str"
                Param to filter by first_name and return records
           -  second_name : "str"
                Param to filter by second_name and return records
           -  last_name : "str"
                Param to define fullname to return records
           -  second_last_name : "str"
                Param to define fullname to return records
           -  email : "str"
                Param to define fullname to return records
           -  password : "str"
                Param to define fullname to return records
           -  phone : "str"
                Param to define fullname to return records
           -  user_type : "int"
                Param to define fullname to return records
           -  rol : "int"
                Param to define fullname to return records
           -  is_staff : "bool"
                Param to define fullname to return records


        """
        validator = Validator(
            schema={
                "first_name": {
                    "required": False,
                    "type": "string"
                },
                "second_name": {
                    "required": False,
                    "type": "string"
                },
                "last_name": {
                    "required": False,
                    "type": "string"
                },
                "second_last_name": {
                    "required": False,
                    "type": "string"
                },
                "email": {
                    "required": True,
                    "type": "string"
                },
                "password": {
                    "required": True,
                    "type": "string"
                },
                "phone": {
                    "required": False,
                    "type": "string"
                },
                "user_type": {
                    "required": False,
                    "type": "integer",
                    "default": 1
                },
                "rol": {
                    "required": False,
                    "type": "integer"
                },
                "is_staff": {
                    "required": False,
                    "type": "boolean",
                    "coerce": bool,
                },
            }
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        sid = transaction.savepoint(using=DB_ADMIN)
        try:
            user_serializer = UserBasicForAdminSerializer(
                data=validator.document,
            )
            if user_serializer.is_valid():
                admin = user_serializer.create(database="admin")
            else:
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "details": user_serializer.errors
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback
            )
            logger.error((
                "Something is wrong when try create a adviser"
                f"check traceback:\n\n{''.join(e)}"
            ))
            return Response(
                data={
                    "message": _("Internal Error"),
                    "error": "Something is wrong when try create a adviser"
                    f"check traceback:\n\n{''.join(e)}"
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(
            data={
                "msg": "Admin created successfully",
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def patch(self, request):
        """
            Update data from adviser

            #Body
            -  id : "str"
                Param to identify
            -  first_name : "str"
                Param to update first_name field first_name
           -  second_name : "str"
                Param to update second_name field second_name
           -  last_name : "str"
                Param to update last_name field last_name
           -  second_last_name : "str"
                Param to update second_last_name field second_last_name
           -  email : "str"
                Param to update email field email
           -  password : "str"
                Param to update password field password
           -  phone : "str"
                Param to update phone field phone
           -  user_type : "int"
                Param to update user_type field user_type
           -  rol : "int"
                Param to update rol field rol
           -  is_staff : "bool"
                Param to update is_staff field is_staff

        """
        validator = Validator(
            schema={
                "id": {
                    "required": True,
                    "type": "integer",
                },
                "first_name": {
                    "required": False,
                    "type": "string",
                },
                "second_name": {
                    "required": False,
                    "type": "string",
                },
                "last_name": {
                    "required": False,
                    "type": "string",
                },
                "second_last_name": {
                    "required": False,
                    "type": "string",
                },
                "phone": {
                    "required": False,
                    "type": "string",
                },
                "user_type": {
                    "required": False,
                    "type": "integer",
                    "default": 1,
                },
                "rol": {
                    "required": False,
                    "type": "integer",
                },
                "is_staff": {
                    "required": False,
                    "type": "boolean",
                    "coerce": bool,
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                    "coerce": bool,
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "password": {
                    "required": False,
                    "type": "string",
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if "password" in validator.document:
            validator.document["password"] = make_password(validator.document["password"])
        try:
            user = User.objects.filter(id=validator.document.get("id")).first()
            user_serializer = UserBasicForAdminSerializer(
                instance=user,
                data=validator.document,
                partial=True,
            )
            if user_serializer.is_valid():
                user_serializer.save()
            else:
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "details": user_serializer.errors,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error((
                "Something is wrong when try create a adviser"
                f"check traceback:\n\n{''.join(e)}"
            ))
            return Response(
                data={
                    "message": _("Internal Error"),
                    "error": "Something is wrong when try create a adviser"
                    f"check traceback:\n\n{''.join(e)}"
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            data={
                "msg": "Admin updated successfully",
            },
            status=status.HTTP_200_OK
        )


class AdviserUserReassing(APIView, AdviserManagementPaginator):

    """
        Class View contain methods to reassing links to partners
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
            Get info from others advisers

            #Body
           -  full_name : "str"
                Param to define fullname to return records
           -  email : "str"
                Param to define email to return records
           -  rol : "int"
                Param to define role id to return records
           -  is_staff : "bool"
                Param to define if is_staff to return records
           -  sort_by : "str"
                Param to sort data
           -  lim : "int"
           -  offs : "int"
        """
        validator = Validator(
            schema={
                "full_name": {
                    "required": False,
                    "type": "string",
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "rol": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "is_staff": {
                    "required": False,
                    "type": "integer",
                    "coerce": bool,
                },
                "sort_by": {
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
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = ~Q(id=request.user.pk)

        if validator.document.get("full_name"):
            query &= Q(full_name__icontains=validator.document.get("full_name"))

        if validator.document.get("email"):
            query &= Q(email__icontains=validator.document.get("email"))

        if "rol" in validator.document:
            rol = Rol.objects.filter(
                Q(id=validator.document.get("rol")),
            ).first()
            query &= Q(rol=rol)

        if "is_staff" in validator.document:
            state = validator.document.get("is_staff")
            state = True if state == 1 else False
            query &= Q(is_staff=state)

        sort_by = "-last_login"
        if "sort_by" in validator.document:
            sort_by = validator.document.get("sort_by")

        user = User.objects.annotate(
            full_name=Concat(
                "first_name",
                Value(" "),
                "second_name",
                Value(" "),
                "last_name",
                Value(" "),
                "second_last_name",
            ),
        ).filter(query).order_by(sort_by)

        user_pag = self.paginate_queryset(
            queryset=user,
            request=request,
            view=self,
        )

        admin_serializer = AdviserUserSER(
            instance=user_pag,
            many=True,
        )
        return Response(
            data={
                "admins": admin_serializer.data,
            },
            headers={
                "access-control-expose-headers": "count, next, previous",
                "count": self.count,
            },
        )

    def patch(self, request):
        """
            Reassing link to partner


            #Body
           -  id_partner : "str"
                Param to identify partner
           -  id_adviser : "str"
                Param to identify adviser


        """

        validator_query = Validator(
            schema={
                "pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
        )

        if not validator_query.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_PARAMS,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = Validator(
            schema={
                "adviser_id": {
                    "required": False,
                    "type": "integer",
                    "nullable": True,
                },
                "fixed_income_adviser_percentage": {
                    "required": False,
                    "type": "float",
                    "nullable": True,
                    "coerce": to_float_0_null,
                },
                "net_revenue_adviser_percentage": {
                    "required": False,
                    "type": "float",
                    "nullable": True,
                    "coerce": to_float_0_null,
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_BODY,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not validator.document:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("not input data for patch"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        pk = validator_query.document.get("pk")
        query = Q(pk=pk)
        partner = Partner.objects.using(DB_USER_PARTNER).filter(query).first()

        pk = validator.document.get("adviser_id")
        query = Q(pk=pk)
        user_adviser = User.objects.using(DB_ADMIN).filter(query).first()

        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "non_fields_errors": [
                            _("Partner dont exists in DB"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if validator.document.get("adviser_id") is not None and user_adviser is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "non_fields_errors": [
                            _("Adviser dont exists in DB"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        adviser = partner.adviser_id

        if adviser is None and not validator.document.get("adviser_id"):
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "pk": [
                            _("You cannot edit percentage with adviser null"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        partner_SER = ReferredUserSER(
            instance=partner,
            data=validator.document,
            partial=True,
        )
        if partner_SER.is_valid():
            partner_SER.save()
            return Response(
                data={},
                status=status.HTTP_204_NO_CONTENT,
            )

        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": partner_SER.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
