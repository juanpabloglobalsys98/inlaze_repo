from api_admin.serializers import ProfileAdminSerializer
from cerberus import Validator
from core.models import User
from django.conf import settings
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class ProfileAdminAPI(APIView):
    permission_classes = [
        IsAuthenticated
    ]

    def get(self, request):
        """ Return data from admin """
        admin = request.user
        adminSerializer = ProfileAdminSerializer(admin)
        return Response({
            "data": adminSerializer.data
        }, status=status.HTTP_200_OK)

    def patch(self, request):
        """ Update Admin's info """
        validator = Validator({
            'first_name': {
                'required': False,
                'type': 'string'
            },
            'second_name': {
                'required': False,
                'type': 'string'
            },
            'last_name': {
                'required': False,
                'type': 'string'
            },
            'second_last_name': {
                'required': False,
                'type': 'string'
            },
            'phone': {
                'required': False,
                'type': 'string'
            },
            'email': {
                'required': False,
                'type': 'string'
            },
        })
        if not validator.validate(request.data):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        editable_fields = ["phone"]
        admin_request = request.user

        if (
            not admin_request.is_superuser and
            not all(field_i in editable_fields for field_i in validator.document.keys())
        ):
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Invalid fields"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        filters = (Q(id=admin_request.id),)
        User.objects.filter(*filters).update(**request.data)

        return Response(
            data={
                "msg": "User was updated successfully",
            },
            status=status.HTTP_200_OK,
        )


class RolAdminAPI(APIView):

    permission_classes = [
        IsAuthenticated
    ]

    # def get(self, request):
    #     """ Return all roles in DB """
    #     roles = Rol.objects.all()
    #     rolesAdmin = RolAdminSerializer(roles, many=True)
    #     return Response({
    #         "data": rolesAdmin.data
    #     }, status=status.HTTP_200_OK)


class ProfilePasswordAPI(APIView):
    permission_classes = [
        IsAuthenticated
    ]

    def patch(self, request):
        """ Update Admin's password """
        validator = Validator({
            'confirm_password': {
                'required': True,
                'type': 'string'
            },
            'new_password': {
                'required': True,
                'type': 'string'
            }
        })
        if not validator.validate(request.data):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        admin_request = request.user

        if not admin_request.check_password(request.data.get("confirm_password")):
            return Response({
                "error": settings.CONFLICT_CODE,
                "details": {
                    "confirm_password": [
                        "Password does not match"
                    ]
                }
            }, status=status.HTTP_404_NOT_FOUND)

        admin_request.set_password(request.data.get("new_password"))
        admin_request.save()

        return Response({
            "msg": "Password was updated successfully"
        }, status=status.HTTP_200_OK)
