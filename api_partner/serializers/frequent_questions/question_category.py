from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import QuestionCategory
from api_partner.serializers.frequent_questions.question import (
    QuestionSerializer,
)
from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.validators import UniqueValidator


class QuestionCategorySerializer(serializers.ModelSerializer):
    """
    Question category serializer with all fields
    """

    title = serializers.CharField(validators=[
        UniqueValidator(
            queryset=QuestionCategory.objects.all(),
            message=_("There is another question category with that title")
        ),
    ])

    class Meta:
        model = QuestionCategory
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return QuestionCategory.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(is_active=True).order_by("-id")

    def delete(self, id, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(id=id).delete()


class QuestionCategoryBasicSerializer(serializers.ModelSerializer):
    """
    Question category basic serializer with specific fields for updating and querying purposes
    """
    title = serializers.CharField(validators=[
        UniqueValidator(
            queryset=QuestionCategory.objects.all(),
            message=_("There is another question category with that title")
        ),
    ])

    class Meta:
        model = QuestionCategory
        fields = ("id", "title", "icon",)

    def create(self, validated_data):
        """
        """
        return QuestionCategory.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(is_active=True).order_by("-id")

    def delete(self, id, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(id=id).delete()


class CategoryWithCommonQuestionsSerializer(serializers.ModelSerializer):
    """
    Question category serializer with specific fields for querying and updating purposes
    """

    title = serializers.CharField(validators=[
        UniqueValidator(
            queryset=QuestionCategory.objects.all(),
            message=_("There is another question category with that title")
        ),
    ])
    questions = serializers.SerializerMethodField(method_name="get_questions")

    def get_questions(self, question_category):
        questions = QuestionSerializer().get_commons_by_category(question_category.id, DB_USER_PARTNER)
        return QuestionSerializer(instance=questions, many=True).data

    class Meta:
        model = QuestionCategory
        fields = ("id", "title", "questions",)

    def create(self, validated_data):
        """
        """
        return QuestionCategory.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(is_active=True).order_by("-id")

    def delete(self, id, database="default"):
        return QuestionCategory.objects.db_manager(database).filter(id=id).delete()
