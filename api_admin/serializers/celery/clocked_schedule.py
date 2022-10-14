from api_admin.helpers.routers_db import DB_ADMIN
from django_celery_beat.models import ClockedSchedule
from rest_framework import serializers


class ClockedScheduleSerializer(serializers.ModelSerializer):
    """
    Clocked schedule serializer with all fields
    """

    class Meta:
        model = ClockedSchedule
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return ClockedSchedule.objects.db_manager(DB_ADMIN).create(**validated_data)

    def exist(self, id, database="default"):
        return ClockedSchedule.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return ClockedSchedule.objects.db_manager(database).order_by("-id")

    def delete(self, id, database="default"):
        return ClockedSchedule.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return ClockedSchedule.objects.db_manager(database).filter(title=title).first()
