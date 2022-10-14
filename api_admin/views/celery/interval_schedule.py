import logging

from api_admin.helpers import (
    DB_ADMIN,
    IntervalSchedulePaginator,
)
from api_admin.serializers import IntervalScheduleSerializer
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


class IntervalScheduleAPI(APIView, IntervalSchedulePaginator):
    """
    """
    permission_classes = (IsAuthenticated, )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates the interval_schedules task in the system
        """

        validator = Validator(
            {
                "every": {
                    "required": True,
                    "type": "integer"
                },
                "period": {
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

        serialized_interval_schedule = IntervalScheduleSerializer(data=validator.document)

        if serialized_interval_schedule.is_valid():
            serialized_interval_schedule.create(DB_ADMIN)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_interval_schedule.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates the interval_schedules task in the system
        """

        validator = Validator(
            {
                "id": {
                    "required": True,
                    "type": "integer",
                },
                "every": {
                    "required": False,
                    "type": "integer"
                },
                "period": {
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
        interval_schedule = IntervalScheduleSerializer().exist(validator.document.get("id"), DB_ADMIN)
        if not interval_schedule:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"id": [_("There is not such interval schedule in the system")]},
                }, status=status.HTTP_404_NOT_FOUND
            )

        serialized_interval_schedule = IntervalScheduleSerializer(instance=interval_schedule, data=validator.document)

        if serialized_interval_schedule.is_valid():
            serialized_interval_schedule.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_interval_schedule.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets the interval_schedules task in the system
        """

        interval_schedules = IntervalScheduleSerializer().get_all(DB_ADMIN)
        if interval_schedules:
            interval_schedules = self.paginate_queryset(interval_schedules, request, view=self)
            interval_schedules = IntervalScheduleSerializer(instance=interval_schedules, many=True)

        return Response(
            data={"interval_schedules": interval_schedules.data if interval_schedules else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if interval_schedules else None
        )
