from api_admin.helpers.routers_db import DB_ADMIN
from django_celery_beat.models import CrontabSchedule
from rest_framework import serializers


class CrontabScheduleSerializer(serializers.ModelSerializer):
    """
    Crontab schedule general serializer with all fields
    """

    timezone = serializers.CharField(required=False)

    class Meta:
        model = CrontabSchedule
        fields = ("timezone", "minute", "hour", "day_of_week", "day_of_month", "month_of_year",)

    def create(self, validated_data):
        """
        """
        return CrontabSchedule.objects.db_manager(DB_ADMIN).create(**validated_data)

    def exist(self, id, database="default"):
        return CrontabSchedule.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return CrontabSchedule.objects.db_manager(database).order_by("-id")

    def delete(self, id, database="default"):
        return CrontabSchedule.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return CrontabSchedule.objects.db_manager(database).filter(title=title).first()
