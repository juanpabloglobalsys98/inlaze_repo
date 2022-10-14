from api_admin.paginators.custom_paginators import \
    GetAllBanUnbanCodeReasonPaginator
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.serializers.authentication.ban_unban_code_reason import \
    BanUnbanCodeReasonSerializer
from cerberus import Validator
from core.helpers import HavePermissionBasedView
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class BanUnbanCodeReasonManagementAPI(APIView, GetAllBanUnbanCodeReasonPaginator):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin know about existing ban unban codes reasons in the system
        """

        validator = Validator({
            'offs': {
                'required': False,
                'type': 'integer',
                'coerce': int,
            },
            'lim': {
                'required': False,
                'type': 'integer',
                'coerce': int,
            },
            'is_ban_reason': {
                'required': False,
                'type': 'string',
                'regex': "true|false"
            }
        })

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        is_ban_reason = validator.document.get("is_ban_reason")
        if is_ban_reason:
            is_ban_reason = True if is_ban_reason == "true" else False

        ban_unban_codes_reason = None
        if not is_ban_reason == None:
            ban_unban_codes_reason = BanUnbanCodeReasonSerializer().get_by_is_ban_reason(is_ban_reason, DB_USER_PARTNER)

        if ban_unban_codes_reason == None:
            ban_unban_codes_reason = BanUnbanCodeReasonSerializer().get_all()

        if ban_unban_codes_reason:
            ban_unban_codes_reason = self.paginate_queryset(ban_unban_codes_reason, request, view=self)
            ban_unban_codes_reason = BanUnbanCodeReasonSerializer(instance=ban_unban_codes_reason, many=True)

        return Response(
            data={"ban_unban_codes_reason": ban_unban_codes_reason.data if ban_unban_codes_reason else None},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if ban_unban_codes_reason else None
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates ban unban codes reasons in the system
        """
        validator = Validator({
            'title': {
                'required': True,
                'type': 'string'
            },
            'reason': {
                'required': True,
                'type': 'string'
            },
            'is_ban_reason': {
                'required': True,
                'type': 'boolean'
            },
        })

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        serialized_ban_unban_code_reason = BanUnbanCodeReasonSerializer(data=validator.document)

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        if serialized_ban_unban_code_reason.is_valid():
            serialized_ban_unban_code_reason.create(database=DB_USER_PARTNER)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_ban_unban_code_reason.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates ban unban codes reasons in the system
        """
        validator = Validator({
            'id': {
                'required': True,
                'nullable': True,
                'type': 'integer',
            },
            'title': {
                'required': False,
                'nullable': True,
                'type': 'string'
            },
            'reason': {
                'required': False,
                'nullable': True,
                'type': 'string'
            },
            'is_ban_reason': {
                'required': False,
                'nullable': True,
                'type': 'boolean'
            },
        })

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        ban_unban_code_reason = BanUnbanCodeReasonSerializer().exist(validator.document.get("id"), DB_USER_PARTNER)
        if not ban_unban_code_reason:
            return Response(data={
                "error": settings.NOT_FOUND_CODE,
                "details": {"id": ["There is not such ban unban code reason in the system"]}
            }, status=status.HTTP_404_NOT_FOUND)

        serialized_ban_unban_code_reason = BanUnbanCodeReasonSerializer(
            instance=ban_unban_code_reason, data=validator.document)
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        if serialized_ban_unban_code_reason.is_valid():
            serialized_ban_unban_code_reason.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_ban_unban_code_reason.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)
