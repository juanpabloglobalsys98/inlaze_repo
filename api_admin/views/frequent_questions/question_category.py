import logging

from api_admin.helpers import Icons
from api_admin.serializers.frequent_questions.question_category import (
    QuestionCategorySerializer,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    GetAllQuestionCategories,
)
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from core.helpers import HavePermissionBasedView

logger = logging.getLogger(__name__)


class QuestionCategoryAPI(APIView, GetAllQuestionCategories):
    """
    """
    permission_classes = (
        IsAuthenticated, 
        HavePermissionBasedView
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates questions categories
        """
        validator = Validator(
            {
                "title": {
                    "required": True,
                    "type": "string",
                },
                "icon": {
                    "required": True,
                    "type": "integer",
                },
                "is_active": {
                    "required": True,
                    "type": "boolean",
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        icon = validator.document.get("icon")
        if icon and (icon < 0 or icon > len(Icons.labels)-1):
            return Response(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {"icon": [_("Unrecognized icon")]}
                }, status=status.HTTP_400_BAD_REQUEST
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        question_category = QuestionCategorySerializer().get_by_title(validator.document.get("title"))
        if question_category:
            return Response(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {"title": [_("There is a question category with that name")]}
                }, status=status.HTTP_400_BAD_REQUEST
            )

        serialized_question_category = QuestionCategorySerializer(data=validator.document)

        if serialized_question_category.is_valid():
            serialized_question_category.create(DB_USER_PARTNER)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_question_category.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates questions categories
        """
        validator = Validator(
            {
                "question_category_id": {
                    "required": True,
                    "type": "integer",
                },
                "title": {
                    "required": False,
                    "type": "string",
                },
                "icon": {
                    "required": False,
                    "type": "integer",
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        icon = validator.document.get("icon")
        if icon and (icon < 0 or icon > len(Icons.labels)-1):
            return Response(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {"icon": [_("Unrecognized icon")]}
                }, status=status.HTTP_400_BAD_REQUEST
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        question_category = QuestionCategorySerializer().exist(validator.document.get("question_category_id"), DB_USER_PARTNER)
        if not question_category:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"question_category_id": [_("There is not such question category in the system")]},
                }, status=status.HTTP_404_NOT_FOUND
            )

        serialized_question_category = QuestionCategorySerializer(instance=question_category, data=validator.document)

        if serialized_question_category.is_valid():
            serialized_question_category.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_question_category.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets all questions categories
        """
        sort_by_regex = "\-?id|\-?title|\-?icon|\-?is_active"

        validator = Validator(
            {
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "regex": sort_by_regex
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        sort_by = validator.document.get("sort_by")
        if not sort_by:
            sort_by = "-id"

        question_categories = QuestionCategorySerializer().get_all(sort_by, DB_USER_PARTNER)
        if question_categories:
            question_categories = self.paginate_queryset(question_categories, request, view=self)
            question_categories = QuestionCategorySerializer(instance=question_categories, many=True)

        return Response(
            data={"question_categories": question_categories.data if question_categories else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if question_categories else None
        )
