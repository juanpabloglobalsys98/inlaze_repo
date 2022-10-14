import logging

from api_admin.helpers import (
    DB_ADMIN,
    CrontabSchedulePaginator,
)
from api_admin.serializers import CrontabScheduleSerializer
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class CrontabScheduleAPI(APIView, CrontabSchedulePaginator):
    """
    """
    permission_classes = (IsAuthenticated, )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates the crontab_schedules task in the system
        """

        validator = Validator(
            {
                "minute": {
                    "required": True,
                    "type": "string"
                },
                "hour": {
                    "required": True,
                    "type": "string"
                },
                "day_of_week": {
                    "required": True,
                    "type": "string"
                },
                "day_of_month": {
                    "required": True,
                    "type": "string"
                },
                "month_of_year": {
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

        sid = transaction.savepoint(using=DB_ADMIN)

        serialized_crontab_schedule = CrontabScheduleSerializer(data=validator.document)

        if serialized_crontab_schedule.is_valid():
            serialized_crontab_schedule.create(DB_ADMIN)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_crontab_schedule.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates the crontab_schedules task in the system
        """

        validator = Validator(
            {
                "id": {
                    "required": True,
                    "type": "integer",
                },
                "minute": {
                    "required": False,
                    "type": "string"
                },
                "hour": {
                    "required": False,
                    "type": "string"
                },
                "day_of_week": {
                    "required": False,
                    "type": "string"
                },
                "day_of_month": {
                    "required": False,
                    "type": "string"
                },
                "month_of_year": {
                    "required": False,
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

        sid = transaction.savepoint(using=DB_ADMIN)
        crontab_schedule = CrontabScheduleSerializer().exist(validator.document.get("id"), DB_ADMIN)
        if not crontab_schedule:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"id": [_("There is not such crontab schedule in the system")]},
                }, status=status.HTTP_404_NOT_FOUND
            )

        serialized_crontab_schedule = CrontabScheduleSerializer(instance=crontab_schedule, data=validator.document)

        if serialized_crontab_schedule.is_valid():
            serialized_crontab_schedule.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_crontab_schedule.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets the crontab_schedules task in the system
        """

        crontab_schedules = CrontabScheduleSerializer().get_all(DB_ADMIN)
        if crontab_schedules:
            crontab_schedules = self.paginate_queryset(crontab_schedules, request, view=self)
            crontab_schedules = CrontabScheduleSerializer(instance=crontab_schedules, many=True)

        return Response(
            data={"crontab_schedules": crontab_schedules.data if crontab_schedules else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if crontab_schedules else None
        )
