from api_admin.paginators.custom_paginators import \
    GetAllInactiveActiveCodeReasonPaginator
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.serializers.authentication.inactive_active_code_reason import \
    InactiveActiveCodeReasonSerializer
from cerberus import Validator
from core.helpers import HavePermissionBasedView
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class ActiveInactiveCodeReasonManagementAPI(APIView, GetAllInactiveActiveCodeReasonPaginator):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Lets an admin know about existing active inactive codes reasons in the system
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
            'is_active_reason': {
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

        is_active_reason = validator.document.get("is_active_reason")
        if is_active_reason:
            is_active_reason = True if is_active_reason == "true" else False

        inactive_active_codes_reason = None
        if not is_active_reason == None:
            inactive_active_codes_reason = InactiveActiveCodeReasonSerializer(
            ).get_by_is_active_reason(is_active_reason, DB_USER_PARTNER)

        if inactive_active_codes_reason == None:
            inactive_active_codes_reason = InactiveActiveCodeReasonSerializer().get_all()

        if inactive_active_codes_reason:
            inactive_active_codes_reason = self.paginate_queryset(inactive_active_codes_reason, request, view=self)
            inactive_active_codes_reason = InactiveActiveCodeReasonSerializer(
                instance=inactive_active_codes_reason, many=True)

        return Response(
            data={"inactive_active_codes_reason": inactive_active_codes_reason.data if inactive_active_codes_reason else None},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if inactive_active_codes_reason else None
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates active inactive codes reasons in the system
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
            'is_active_reason': {
                'required': True,
                'type': 'boolean'
            },
        })

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        serialized_inactive_active_code_reason = InactiveActiveCodeReasonSerializer(data=validator.document)

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        if serialized_inactive_active_code_reason.is_valid():
            serialized_inactive_active_code_reason.create(database=DB_USER_PARTNER)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_inactive_active_code_reason.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates active inactive codes reasons in the system
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
            'is_active_reason': {
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

        inactive_active_code_reason = InactiveActiveCodeReasonSerializer().exist(validator.document.get("id"), DB_USER_PARTNER)
        if not inactive_active_code_reason:
            return Response(data={
                "error": settings.NOT_FOUND_CODE,
                "details": {"id": [_("There is not such inactive/active code reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        serialized_inactive_active_code_reason = InactiveActiveCodeReasonSerializer(
            instance=inactive_active_code_reason, data=validator.document)
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        if serialized_inactive_active_code_reason.is_valid():
            serialized_inactive_active_code_reason.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_inactive_active_code_reason.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)
