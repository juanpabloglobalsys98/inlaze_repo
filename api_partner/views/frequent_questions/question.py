import logging

from api_partner.helpers import (
    DB_USER_PARTNER,
    GetAllQuestion,
)
from api_partner.helpers.permissions import (
    IsNotBanned,
    IsNotToBeVerified,
)
from api_partner.serializers.frequent_questions import QuestionSerializer
from api_partner.serializers.frequent_questions.question_category import (
    CategoryWithCommonQuestionsSerializer,
)
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class QuestionAPI(APIView, GetAllQuestion):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified
    )

    def get(self, request):
        """
        Lets to get questions by category
        """
        validator = Validator(
            {
                "category_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int,
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        questions = QuestionSerializer().get_by_category(validator.document.get("category_id"), DB_USER_PARTNER)
        if questions:
            questions = self.paginate_queryset(questions, request, view=self)
            questions = QuestionSerializer(instance=questions, many=True)

        return Response(
            data={"questions": questions.data if questions else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if questions else None
        )


class CommonQuestionsAPI(APIView, GetAllQuestion):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified,
    )

    def get(self, request):
        """
        Lets to get most common questions with their categories
        """

        category_with_common_questions = CategoryWithCommonQuestionsSerializer().get_all(DB_USER_PARTNER)
        if category_with_common_questions:
            category_with_common_questions = self.paginate_queryset(category_with_common_questions, request, view=self)
            category_with_common_questions = CategoryWithCommonQuestionsSerializer(
                instance=category_with_common_questions, many=True)

        return Response(
            data={"categories": category_with_common_questions.data if category_with_common_questions else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if category_with_common_questions else None
        )
