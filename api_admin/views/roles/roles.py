import logging
from pickle import FALSE
import sys
import traceback

from api_admin.helpers import DB_ADMIN
from api_admin.serializers import RoleSerializer
from cerberus import Validator
from core.helpers import HavePermissionBasedView
from core.models import Permission, Rol
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class RolesManagementAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        ''' Reuturn all roles in DB '''
        roles = Rol.objects.all()
        rol_serializer = RoleSerializer(roles, many=True)
        return Response({
            "data": rol_serializer.data
        }, status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def post(self, request):
        ''' Create rol '''
        validator = Validator(
            schema={
                "rol": {
                    "required": True,
                    "type": "string"
                },
                'permissions': {
                    'required': True,
                    'type': 'list',
                    'schema': {
                        'type': 'integer',
                    },
                },
            }
        )
        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        sid = transaction.savepoint(using=DB_ADMIN)
        try:
            permissions = Permission.objects.filter(
                Q(id__in=request.data.get("permissions"))).values_list(
                "pk", flat=True)
            rol = Rol(rol=request.data.get("rol"))
            rol.save()
            rol.permissions.add(*permissions)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error((
                "Something is wrong when try create a rol"
                f"check traceback:\n\n{''.join(e)}"
            ))
            return Response(
                data={
                    "message": _("Internal Error"),
                    "error": "Something is wrong when try create a rol"
                    f"check traceback:\n\n{''.join(e)}"
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        return Response(
            data={
                "rol": rol.pk,
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def put(self, request):
        ''' Update role '''
        validator = Validator({
            "id": {
                "required": True,
                "type": "integer"
            },
            "rol": {
                "required": False,
                "type": "string"
            },
            'permissions': {
                'required': False,
                'type': 'list',
                'schema': {
                    'type': 'integer'
                }
            }
        })

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        rol = Rol.objects.filter(Q(pk=request.data.get("id"))).first()

        if not rol:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "rol": [
                        "Rol not found"
                    ]
                }
            }, status=status.HTTP_404_NOT_FOUND)

        sid = transaction.savepoint(using=DB_ADMIN)
        try:
            if "rol" in request.data:
                rol.rol = request.data.get("rol")
            if "permissions" in request.data:
                permissions = Permission.objects.filter(
                    Q(id__in=request.data.get("permissions"))).values_list(
                    "pk", flat=True)
                rol.permissions.clear()
                rol.permissions.add(*permissions)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error((
                "Something is wrong when try create a rol"
                f"check traceback:\n\n{''.join(e)}"
            ))
            return Response({
                "message": _("Internal Error"),
                "error": "Something is wrong when try update a rol"
                         f"check traceback:\n\n{''.join(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        rol.save()
        return Response({
            "msg": "Role updated"
        }, status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def delete(self, request):
        validator = Validator({
            "id": {
                "required": True,
                "type": "integer"
            }
        })

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        rol = Rol.objects.filter(Q(pk=request.data.get("id")))

        if not rol:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "rol": [
                        "Rol not found"
                    ]
                }
            }, status=status.HTTP_404_NOT_FOUND)

        rol.delete()
        return Response({
            "msg": "Role deleted"
        }, status=status.HTTP_200_OK)
