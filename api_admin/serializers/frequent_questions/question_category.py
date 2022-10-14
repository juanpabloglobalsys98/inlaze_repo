from api_partner.models import QuestionCategory
from rest_framework import serializers


class QuestionCategorySerializer(serializers.ModelSerializer):
    """
    Question category general serializer with all fields
    """

    admin_id = serializers.IntegerField(required=False)
    title = serializers.CharField(required=False)
    icon = serializers.CharField(required=False)
    is_active = serializers.BooleanField(required=False)

    class Meta:
        model = QuestionCategory
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return QuestionCategory.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(id=id).first()

    def get_all(self, sort_by, database="default"):
        return QuestionCategory.objects.db_manager(database).order_by(sort_by)

    def delete(self, id, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(id=id).delete()

    def get_by_title(self, title, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(title=title).first()
