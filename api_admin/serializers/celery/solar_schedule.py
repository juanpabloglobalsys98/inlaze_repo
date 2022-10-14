from django_celery_beat.models import SolarSchedule
from rest_framework import serializers


class SolarScheduleSerializer(serializers.ModelSerializer):
    """
    Solar schedule serializer with all fields
    """

    class Meta:
        model = SolarSchedule
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return SolarSchedule.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return SolarSchedule.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return SolarSchedule.objects.db_manager(database).order_by("-id")

    def delete(self, id, database="default"):
        return SolarSchedule.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return SolarSchedule.objects.db_manager(database).filter(title=title).first()
