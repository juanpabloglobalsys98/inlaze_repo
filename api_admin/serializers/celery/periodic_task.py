from api_admin.helpers.routers_db import DB_ADMIN
from api_admin.serializers.celery.clocked_schedule import ClockedScheduleSerializer
from api_admin.serializers.celery.crontab_schedule import CrontabScheduleSerializer
from api_admin.serializers.celery.interval_schedule import IntervalScheduleSerializer
from django_celery_beat.models import PeriodicTask
from rest_framework import serializers
from api_partner import tasks as partner_tasks
from core import tasks as core_tasks


class PeriodicTaskSerializer(serializers.ModelSerializer):
    """
    Periodic task serializer with all fields
    """

    class Meta:
        model = PeriodicTask
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return PeriodicTask.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return PeriodicTask.objects.db_manager(database).order_by("-id")

    def delete(self, id, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(title=title).first()


class PeriodicTaskBasicSerializer(serializers.ModelSerializer):
    """
    Periodic task basic serializer with required fields for updating and querying purposes
    """
    clocked_id = serializers.UUIDField(required=False)
    crontab_id = serializers.UUIDField(required=False)
    interval_id = serializers.UUIDField(required=False)
    description = serializers.CharField(required=False)
    enabled = serializers.BooleanField(required=False)
    expire_seconds = serializers.IntegerField(required=False)
    kwargs = serializers.CharField(required=False)
    name = serializers.CharField(required=False)
    one_off = serializers.BooleanField(required=False)
    priority = serializers.IntegerField(required=False)
    task = serializers.CharField(required=False)

    class Meta:
        model = PeriodicTask
        exclude = ("interval", "crontab", "solar", "clocked", "args", "queue", "exchange", "routing_key",
                   "headers", "expires", "start_time", "last_run_at", "total_run_count", "date_changed")

    def create(self, database="default"):
        """
        """
        return PeriodicTask.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return PeriodicTask.objects.db_manager(database).order_by("-id")

    def delete(self, instance, database="default"):
        interval = instance.interval
        crontab = instance.crontab
        clocked = instance.clocked
        if interval:
            IntervalScheduleSerializer().delete(interval.id, DB_ADMIN)
        elif crontab:
            CrontabScheduleSerializer().delete(crontab.id, DB_ADMIN)
        elif clocked:
            ClockedScheduleSerializer().delete(clocked.id, DB_ADMIN)

        return PeriodicTask.objects.db_manager(database).filter(id=instance.id).delete()

    def get_by_title(self, title, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(title=title).first()


class PeriodicTaskTableSerializer(serializers.ModelSerializer):
    """
    Periodic task serializer with required fields for updating and querying purposes
    """
    one_off = serializers.BooleanField(required=False)

    class Meta:
        model = PeriodicTask
        fields = ("id", "name", "task", "interval", "crontab", "clocked", "priority",
                  "expires", "expire_seconds", "start_time", "enabled", "last_run_at",
                  "total_run_count", "date_changed", "one_off")

    def create(self, database="default"):
        """
        """
        return PeriodicTask.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(id=id).first()

    def get_all(self, sort_by, database="default"):
        return PeriodicTask.objects.db_manager(database).order_by("-id").order_by(sort_by)

    def delete(self, id, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(title=title).first()


class PeriodicTaskDetailsSerializer(serializers.ModelSerializer):
    """
    periodic task details serializer with required fields for querying purpose
    """
    
    chronological_type = serializers.SerializerMethodField("get_chronological_type")
    args = serializers.SerializerMethodField("normalize_args")
    kwargs = serializers.SerializerMethodField("normalize_kwargs")
    doc = serializers.SerializerMethodField("get_doc")

    def get_doc(self, periodic_task):
        task = periodic_task.task
        if not task:
            return None

        task_parts = task.split(".")
        if task_parts[0] == "api_partner":
            return eval("partner_tasks." + task_parts[len(task_parts) - 1] + ".__doc__")
        elif task_parts[0] == "core":
            return eval("core_tasks." + task_parts[len(task_parts) - 1] + ".__doc__")
        else:
            return None

    def normalize_args(self, periodic_task):
        return eval(periodic_task.args)

    def normalize_kwargs(self, periodic_task):
        return eval(periodic_task.kwargs)

    def get_chronological_type(self, periodic_task):
        interval = periodic_task.interval
        crontab = periodic_task.crontab
        clocked = periodic_task.clocked

        if not interval == None:
            return IntervalScheduleSerializer(instance=interval).data
        elif not crontab == None:
            return CrontabScheduleSerializer(instance=crontab).data
        elif not clocked == None:
            return ClockedScheduleSerializer(instance=clocked).data
        else:
            return None

    class Meta:
        model = PeriodicTask
        fields = ("id", "name", "task", "interval", "crontab", "solar", "clocked",
                  "args", "kwargs", "queue", "exchange", "routing_key", "headers",
                  "priority", "expires", "expire_seconds", "one_off", "start_time",
                  "enabled", "last_run_at", "total_run_count", "date_changed", "description",
                  "chronological_type", "doc"
                  )

    def create(self, database="default"):
        """
        """
        return PeriodicTask.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(id=id).first()

    def get_all(self, sort_by, database="default"):
        return PeriodicTask.objects.db_manager(database).order_by("-id").order_by(sort_by)

    def delete(self, id, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return PeriodicTask.objects.db_manager(database).filter(title=title).first()
