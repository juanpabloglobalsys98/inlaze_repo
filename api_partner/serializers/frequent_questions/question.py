from api_partner.models import Question
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.validators import UniqueTogetherValidator


class QuestionSerializer(serializers.ModelSerializer):
    """
    Question serializer with all fields
    """

    class Meta:
        model = Question
        fields = "__all__"
        validators = [
            UniqueTogetherValidator(
                queryset=Question.objects.all(),
                message=_('Question cannot be repeated in the same category'),
                fields=("category", "description",)
            ),
        ]

    def create(self, database="default"):
        """
        """
        return Question.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return Question.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return Question.objects.db_manager(database).order_by("-id")

    def delete(self, id, database="default"):
        return Question.objects.db_manager(database).filter(id=id).delete()

    def get_by_category(self, category_id, database="default"):
        return Question.objects.db_manager(database).filter(category=category_id)

    def get_commons_by_category(self, category_id, database="default"):
        filters = [Q(is_active=True), Q(is_common=True), Q(category=category_id)]
        return Question.objects.db_manager(database).filter(*filters)


class QuestionBasicSerializer(serializers.ModelSerializer):
    """
    Question basic serializer excluding category for updating purpose
    """

    category_id = serializers.UUIDField(required=False)
    description = serializers.CharField(required=False)
    answer = serializers.CharField(required=False)
    is_active = serializers.BooleanField(required=False)

    class Meta:
        model = Question
        exclude = ("category",)

    def create(self, database="default"):
        """
        """
        return Question.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return Question.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return Question.objects.db_manager(database).order_by("-id")

    def delete(self, id, database="default"):
        return Question.objects.db_manager(database).filter(id=id).delete()
