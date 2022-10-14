from django_celery_results.models import ChordCounter
from rest_framework import serializers


class ChordCounterSerializer(serializers.ModelSerializer):
    """
    Chrod counter general serializer with all fields
    """

    class Meta:
        model = ChordCounter
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return ChordCounter.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return ChordCounter.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return ChordCounter.objects.db_manager(database).order_by("-id")

    def delete(self, id, database="default"):
        return ChordCounter.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return ChordCounter.objects.db_manager(database).filter(title=title).first()
