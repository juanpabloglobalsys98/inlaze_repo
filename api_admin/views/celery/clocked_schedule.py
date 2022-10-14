import logging

from api_admin.helpers import (
    DB_ADMIN,
    ClockedSchedulePaginator,
)
from api_admin.serializers import ClockedScheduleSerializer
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.db import transaction
from django.utils.timezone import (
    datetime,
    make_aware,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class ClockedScheduleAPI(APIView, ClockedSchedulePaginator):

    permission_classes = (IsAuthenticated, )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates one clocked_schedules task in the system
        """

        def to_datetime(s): return make_aware(datetime.strptime(s, "%Y-%m-%d"))
        validator = Validator(
            {
                "clocked_time": {
                    "required": True,
                    "type": "datetime",
                    "coerce": to_datetime
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

        serialized_clocked_schedule = ClockedScheduleSerializer(data=validator.document)

        if serialized_clocked_schedule.is_valid():
            serialized_clocked_schedule.create(DB_ADMIN)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_clocked_schedule.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates one clocked_schedules task in the system
        """

        def to_datetime(s): return make_aware(datetime.strptime(s, "%Y-%m-%d"))
        validator = Validator(
            {
                "id": {
                    "required": True,
                    "type": "integer",
                },
                "clocked_time": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime
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
        clocked_schedule = ClockedScheduleSerializer().exist(validator.document.get("id"), DB_ADMIN)
        if not clocked_schedule:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"id": [_("There is not such clocked schedule in the system")]},
                }, status=status.HTTP_404_NOT_FOUND
            )

        serialized_clocked_schedule = ClockedScheduleSerializer(instance=clocked_schedule, data=validator.document)

        if serialized_clocked_schedule.is_valid():
            serialized_clocked_schedule.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_clocked_schedule.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets the clocked_schedules task in the system
        """
        
        clocked_schedules = ClockedScheduleSerializer().get_all(DB_ADMIN)
        if clocked_schedules:
            clocked_schedules = self.paginate_queryset(clocked_schedules, request, view=self)
            clocked_schedules = ClockedScheduleSerializer(instance=clocked_schedules, many=True)

        return Response(
            data={"clocked_schedules": clocked_schedules.data if clocked_schedules else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if clocked_schedules else None
        )
