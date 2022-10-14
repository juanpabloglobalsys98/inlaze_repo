import inspect
import logging
import re
import sys
import traceback

from api_admin.helpers import (
    DB_ADMIN,
    PeriodicTaskPaginator,
)
from api_admin.serializers import (
    ClockedScheduleSerializer,
    CrontabScheduleSerializer,
    IntervalScheduleSerializer,
    PeriodicTaskBasicSerializer,
    PeriodicTaskDetailsSerializer,
    PeriodicTaskTableSerializer,
)
from api_partner import tasks as partner_tasks
from cerberus import Validator
from core import tasks as core_tasks
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

from core.helpers import HavePermissionBasedView

logger = logging.getLogger(__name__)


class PeriodicTaskAPI(APIView, PeriodicTaskPaginator):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates the periodic task in the system
        """

        def to_datetime(s): return make_aware(datetime.strptime(s, "%Y-%m-%d"))
        general_validator = Validator(
            {
                "name": {
                    "required": True,
                    "type": "string",
                },
                "task": {
                    "required": True,
                    "type": "string",
                },
                "kwargs": {
                    "required": True,
                    "type": "dict",
                },
                "priority": {
                    "required": True,
                    "type": "integer",
                },
                "expire_seconds": {
                    "required": True,
                    "type": "integer",
                },
                "enabled": {
                    "required": True,
                    "type": "boolean",
                },
                "description": {
                    "required": True,
                    "type": "string",
                },

                # clocked schedule parameters
                "clocked_time": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime
                },

                # crontab schedule parameters
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
                },

                # interval schedule parameters
                "every": {
                    "required": False,
                    "type": "integer"
                },
                "period": {
                    "required": False,
                    "type": "string"
                },
            }, error_handler=StandardErrorHandler
        )

        if not general_validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": general_validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        clocked_time = general_validator.document.get("clocked_time")
        minute = general_validator.document.get("minute")
        every = general_validator.document.get("every")
        more_than_one_timer = False

        sid = transaction.savepoint(using=DB_ADMIN)
        if not clocked_time == None:

            if more_than_one_timer:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response(
                    data={
                        "error": settings.ILOGICAL_ACTION,
                        "details": {"non_field_errors": [_("More than one timer, you can only create one timer")]}
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            clocked_time_validator = Validator(
                {
                    # clocked schedule parameters
                    "clocked_time": {
                        "required": True,
                        "type": "datetime",
                        "coerce": to_datetime
                    },
                }, error_handler=StandardErrorHandler
            )

            clocked_time_validator.allow_unknown = True

            if not clocked_time_validator.validate(request.data):
                return Response(
                    {
                        "error": settings.CERBERUS_ERROR_CODE,
                        "details": clocked_time_validator.errors
                    }, status=status.HTTP_400_BAD_REQUEST
                )
            general_validator.document["one_off"] = True
            serialized_clocked_schedule = ClockedScheduleSerializer(data=clocked_time_validator.document)

            if serialized_clocked_schedule.is_valid():
                clocked_schedule = serialized_clocked_schedule.create(serialized_clocked_schedule.validated_data)
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response({
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_clocked_schedule.errors
                }, status=status.HTTP_400_BAD_REQUEST)

            general_validator.document["clocked_id"] = clocked_schedule.id

            more_than_one_timer = True

        if not minute == None:

            if more_than_one_timer:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response(
                    data={
                        "error": settings.ILOGICAL_ACTION,
                        "details": {"non_field_errors": [_("More than one timer, you can only create one timer")]}
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            crontab_schedule_validator = Validator(
                {
                    # crontab schedule parameters
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
                    },
                }, error_handler=StandardErrorHandler
            )

            crontab_schedule_validator.allow_unknown = True
            if not crontab_schedule_validator.validate(request.data):
                return Response(
                    {
                        "error": settings.CERBERUS_ERROR_CODE,
                        "details": crontab_schedule_validator.errors
                    }, status=status.HTTP_400_BAD_REQUEST
                )

            serialized_crontab_schedule = CrontabScheduleSerializer(data=crontab_schedule_validator.document)

            if serialized_crontab_schedule.is_valid():
                crontab = serialized_crontab_schedule.create(serialized_crontab_schedule.validated_data)
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response({
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_crontab_schedule.errors
                }, status=status.HTTP_400_BAD_REQUEST)

            general_validator.document["crontab_id"] = crontab.id
            more_than_one_timer = True

        if not every == None:

            if more_than_one_timer:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response(
                    data={
                        "error": settings.ILOGICAL_ACTION,
                        "details": {"non_field_errors": [_("More than one timer, you can only create one timer")]}
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            interval_schedule_validator = Validator(
                {
                    # interval schedule parameters
                    "every": {
                        "required": True,
                        "type": "integer"
                    },
                    "period": {
                        "required": True,
                        "type": "string"
                    },
                }, error_handler=StandardErrorHandler
            )

            interval_schedule_validator.allow_unknown = True
            if not interval_schedule_validator.validate(request.data):
                return Response(
                    {
                        "error": settings.CERBERUS_ERROR_CODE,
                        "details": interval_schedule_validator.errors
                    }, status=status.HTTP_400_BAD_REQUEST
                )

            serialized_interval_schedule = IntervalScheduleSerializer(data=interval_schedule_validator.document)

            if serialized_interval_schedule.is_valid():
                interval = serialized_interval_schedule.create(serialized_interval_schedule.validated_data)
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response({
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_interval_schedule.errors
                }, status=status.HTTP_400_BAD_REQUEST)

            general_validator.document["interval_id"] = interval.id
            more_than_one_timer = True

        if not more_than_one_timer:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {"non_field_errors": [_("You need to provide at least one timer")]}
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        general_validator.document["kwargs"] = str(general_validator.document.get("kwargs")).replace("'", '"')
        serialized_periodic_task = PeriodicTaskBasicSerializer(data=general_validator.document)

        if serialized_periodic_task.is_valid():
            serialized_periodic_task.create(DB_ADMIN)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_periodic_task.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates the periodic task in the system
        """

        def to_datetime(s): return make_aware(datetime.strptime(s, "%Y-%m-%d"))
        general_validator = Validator(
            {
                "id": {
                    "required": True,
                    "type": "integer",
                },
                "name": {
                    "required": False,
                    "type": "string",
                },
                "kwargs": {
                    "required": False,
                    "type": "dict",
                },
                "priority": {
                    "required": False,
                    "type": "integer",
                },
                "expire_seconds": {
                    "required": False,
                    "type": "integer",
                },
                "enabled": {
                    "required": False,
                    "type": "boolean",
                },
                "description": {
                    "required": False,
                    "type": "string",
                },

                # clocked schedule parameters
                "clocked_time": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime
                },

                # crontab schedule parameters
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
                },

                # interval schedule parameters
                "every": {
                    "required": False,
                    "type": "integer"
                },
                "period": {
                    "required": False,
                    "type": "string"
                },
            }, error_handler=StandardErrorHandler
        )

        if not general_validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": general_validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        old_period_task = PeriodicTaskBasicSerializer().exist(general_validator.document.get("id"), DB_ADMIN)
        if not old_period_task:
            return Response(
                {
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"id": [_("period task not found")]}
                }, status=status.HTTP_404_NOT_FOUND
            )

        sid = transaction.savepoint(using=DB_ADMIN)
        old_interval = old_period_task.interval
        old_crontab = old_period_task.crontab
        old_clocked = old_period_task.clocked

        clocked_time = general_validator.document.get("clocked_time")  # new clocked_time
        minute = general_validator.document.get("minute")  # new contrab
        every = general_validator.document.get("every")  # new interval
        more_than_one_timer = False

        if not clocked_time == None:

            if more_than_one_timer:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response(
                    data={
                        "error": settings.ILOGICAL_ACTION,
                        "details": {"non_field_errors": [_("More than one timer, you can only create one timer")]}
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            clocked_time_validator = Validator(
                {
                    # clocked schedule parameters
                    "clocked_time": {
                        "required": True,
                        "type": "datetime",
                        "coerce": to_datetime
                    },
                }, error_handler=StandardErrorHandler
            )

            clocked_time_validator.allow_unknown = True

            if not clocked_time_validator.validate(request.data):
                return Response(
                    {
                        "error": settings.CERBERUS_ERROR_CODE,
                        "details": clocked_time_validator.errors
                    }, status=status.HTTP_400_BAD_REQUEST
                )

            if not old_interval == None:
                IntervalScheduleSerializer().delete(old_interval.id, DB_ADMIN)
                general_validator.document["interval"] = None

            if not old_crontab == None:
                IntervalScheduleSerializer().delete(old_crontab.id, DB_ADMIN)
                general_validator.document["crontab"] = None

            general_validator.document["one_off"] = True
            clocked_instance = None
            if old_clocked:
                clocked_instance = ClockedScheduleSerializer().exist(old_clocked.id, DB_ADMIN)

            if clocked_instance:
                serialized_clocked_schedule = ClockedScheduleSerializer(
                    instance=clocked_instance, data=clocked_time_validator.document)
            else:
                serialized_clocked_schedule = ClockedScheduleSerializer(data=clocked_time_validator.document)

            if serialized_clocked_schedule.is_valid():
                clocked_schedule = serialized_clocked_schedule.save()
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response({
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_clocked_schedule.errors
                }, status=status.HTTP_400_BAD_REQUEST)

            general_validator.document["clocked"] = clocked_schedule.id
            more_than_one_timer = True

        if not minute == None:

            if more_than_one_timer:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response(
                    data={
                        "error": settings.ILOGICAL_ACTION,
                        "details": {"non_field_errors": [_("More than one timer, you can only create one timer")]}
                    }, status=status.HTTP_400_BAD_REQUEST
                )

            crontab_schedule_validator = Validator(
                {
                    # crontab schedule parameters
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
                    },
                }, error_handler=StandardErrorHandler
            )

            crontab_schedule_validator.allow_unknown = True
            if not crontab_schedule_validator.validate(request.data):
                return Response(
                    {
                        "error": settings.CERBERUS_ERROR_CODE,
                        "details": crontab_schedule_validator.errors
                    }, status=status.HTTP_400_BAD_REQUEST
                )

            if not old_clocked == None:
                ClockedScheduleSerializer().delete(old_clocked.id, DB_ADMIN)
                general_validator.document["one_off"] = False
                general_validator.document["clocked"] = None

            if not old_interval == None:
                IntervalScheduleSerializer().delete(old_interval.id, DB_ADMIN)
                general_validator.document["interval"] = None

            crontab_instance = None
            if old_crontab:
                crontab_instance = CrontabScheduleSerializer().exist(old_crontab.id, DB_ADMIN)

            if crontab_instance:
                serialized_crontab_schedule = CrontabScheduleSerializer(
                    instance=crontab_instance, data=crontab_schedule_validator.document)
            else:
                serialized_crontab_schedule = CrontabScheduleSerializer(data=crontab_schedule_validator.document)

            if serialized_crontab_schedule.is_valid():
                crontab = serialized_crontab_schedule.save()
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response({
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_crontab_schedule.errors
                }, status=status.HTTP_400_BAD_REQUEST)

            general_validator.document["crontab"] = crontab.id
            more_than_one_timer = True

        if not every == None:

            if more_than_one_timer:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response(
                    data={
                        "error": settings.ILOGICAL_ACTION,
                        "details": {"non_field_errors": [_("More than one timer, you can only create one timer")]}
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            interval_schedule_validator = Validator(
                {
                    # interval schedule parameters
                    "every": {
                        "required": True,
                        "type": "integer"
                    },
                    "period": {
                        "required": True,
                        "type": "string"
                    },
                }, error_handler=StandardErrorHandler
            )

            interval_schedule_validator.allow_unknown = True
            if not interval_schedule_validator.validate(request.data):
                return Response(
                    {
                        "error": settings.CERBERUS_ERROR_CODE,
                        "details": interval_schedule_validator.errors
                    }, status=status.HTTP_400_BAD_REQUEST
                )

            if not old_clocked == None:
                ClockedScheduleSerializer().delete(old_clocked.id, DB_ADMIN)
                general_validator.document["clocked"] = None
                general_validator.document["one_off"] = False

            if not old_crontab == None:
                CrontabScheduleSerializer().delete(old_crontab.id, DB_ADMIN)
                general_validator.document["crontab"] = None

            interval_instance = None
            if old_interval:
                interval_instance = IntervalScheduleSerializer().exist(old_interval.id, DB_ADMIN)

            if interval_instance:
                serialized_interval_schedule = IntervalScheduleSerializer(
                    instance=interval_instance, data=interval_schedule_validator.document)
            else:
                serialized_interval_schedule = IntervalScheduleSerializer(data=interval_schedule_validator.document)

            if serialized_interval_schedule.is_valid():
                interval = serialized_interval_schedule.save()
            else:
                transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
                return Response({
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_interval_schedule.errors
                }, status=status.HTTP_400_BAD_REQUEST)

            general_validator.document["interval"] = interval.id
            more_than_one_timer = True

        if not more_than_one_timer:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {"non_field_errors": [_("You need to provide at least one timer")]}},
                status=status.HTTP_400_BAD_REQUEST
            )

        general_validator.document["kwargs"] = str(general_validator.document.get("kwargs")).replace("'", '"')
        serialized_periodic_task = PeriodicTaskBasicSerializer(
            instance=old_period_task, data=general_validator.document)
        if serialized_periodic_task.is_valid():
            serialized_periodic_task.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_ADMIN)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_periodic_task.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_ADMIN)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets the periodic task in the system
        """

        sort_by_regex = "\-?id|\-?name|\-?task|\-?interval|\-?crontab|\-?clocked|\-?priority" + \
            "|\-?expires|\-?expire_seconds|\-?start_time|\-?enabled|\-?last_run_at|\-?total_run_count" + \
            "|\-?date_changed|\-?one_off"

        validator = Validator(
            {
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "regex": sort_by_regex
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        sort_by = validator.document.get("sort_by")
        if not sort_by:
            sort_by = "-id"

        periodic_tasks = PeriodicTaskTableSerializer().get_all(sort_by, DB_ADMIN)
        if periodic_tasks:
            periodic_tasks = self.paginate_queryset(periodic_tasks, request, view=self)
            periodic_tasks = PeriodicTaskTableSerializer(instance=periodic_tasks, many=True)

        return Response(
            data={"periodic_tasks": periodic_tasks.data if periodic_tasks else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if periodic_tasks else None
        )

    def delete(self, request):
        """
        Lets an admin deletes the periodic task in the system
        """
        validator = Validator(
            {
                "id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        periodic_tasks = PeriodicTaskBasicSerializer().exist(validator.document.get("id"), DB_ADMIN)
        if not periodic_tasks:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": _("There is not such periodic task in the system")
                }, status=status.HTTP_404_NOT_FOUND
            )

        PeriodicTaskBasicSerializer().delete(periodic_tasks, DB_ADMIN)
        return Response(status=status.HTTP_200_OK)


class PeriodicTaskDetailsAPI(APIView):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets an admin gets the periodic task details in the system
        """
        validator = Validator(
            {
                "id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int,
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        periodic_task = PeriodicTaskDetailsSerializer().exist(validator.document.get("id"), DB_ADMIN)
        if periodic_task:
            periodic_task = PeriodicTaskDetailsSerializer(instance=periodic_task)

        return Response(
            data={"periodic_task_details": periodic_task.data if periodic_task else []},
            status=status.HTTP_200_OK,
        )


class AdditionalInfoAPI(APIView):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets an admin gets the additional info for periodic task in the system
        """

        try:
            all_partner_tasks = list(filter(lambda task: not re.search("__.+__", task), dir(partner_tasks)))
            tasks = {"data": []}
            for partner_task in all_partner_tasks:
                tasks["data"].append(
                    {
                        "task": eval("partner_tasks." + partner_task + ".name"),
                        "kwargs": list(inspect.signature(eval("partner_tasks." + partner_task)).parameters),
                        "doc": eval("partner_tasks." + partner_task + ".__doc__")
                    }
                )

            all_core_tasks = list(filter(lambda task: not re.search(
                "__.+__|celery_task_failure_email|core.tasks.error_log|error_log", task), dir(core_tasks)))

            for core_task in all_core_tasks:
                tasks["data"].append(
                    {
                        "task": eval("core_tasks." + core_task + ".name"),
                        "kwargs": list(inspect.signature(eval("core_tasks." + core_task)).parameters),
                        "doc": eval("core_tasks." + core_task + ".__doc__")
                    }
                )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                data={
                    "error": settings.CELERY_INTERNAL_ERROR,
                    "details": {"non_field_errors": ["".join(e)]}
                },
                status=status.HTTP_409_CONFLICT
            )

        return Response(data=tasks, status=status.HTTP_200_OK)
