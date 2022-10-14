import logging

from api_partner.helpers import (
    DB_USER_PARTNER,
    GetAllQuestionCategories,
)
from api_partner.helpers.permissions import (
    IsNotBanned,
    IsNotOnLogUpPhase2A,
    IsNotOnLogUpPhase2B,
    IsNotOnLogUpPhase2C,
    IsBasicInfoValid,
    IsEmailValid,
    IsNotToBeVerified,
)
from api_partner.serializers.frequent_questions.question_category import (
    QuestionCategoryBasicSerializer,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class QuestionCategoryAPI(APIView, GetAllQuestionCategories):
    """
    """
    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsBasicInfoValid,
        IsEmailValid,
        IsNotToBeVerified
    )

    def get(self, request):
        """
        Let to get all questions categories
        """

        question_categories = QuestionCategoryBasicSerializer().get_all(DB_USER_PARTNER)
        if question_categories:
            question_categories = self.paginate_queryset(question_categories, request, view=self)
            question_categories = QuestionCategoryBasicSerializer(instance=question_categories, many=True)

        return Response(
            data={"partners": question_categories.data if question_categories else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if question_categories else None
        )
