from api_admin.helpers.routers_db import DB_ADMIN
from django_celery_beat.models import IntervalSchedule
from rest_framework import serializers


class IntervalScheduleSerializer(serializers.ModelSerializer):
    """
    Interval schedule general serializer with all fields
    """

    class Meta:
        model = IntervalSchedule
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return IntervalSchedule.objects.db_manager(DB_ADMIN).create(**validated_data)

    def exist(self, id, database="default"):
        return IntervalSchedule.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return IntervalSchedule.objects.db_manager(database).order_by("-id")

    def delete(self, id, database="default"):
        return IntervalSchedule.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return IntervalSchedule.objects.db_manager(database).filter(title=title).first()
