import logging

from api_admin.serializers.frequent_questions import (
    QuestionCategorySerializer,
    QuestionSerializer,
)
from api_admin.serializers.frequent_questions.question import (
    QuestionBasicSerializer,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    GetAllQuestion,
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

logger = logging.getLogger(__name__)


class QuestionAPI(APIView, GetAllQuestion):

    permission_classes = (IsAuthenticated, )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates a question
        """
        validator = Validator(
            {
                "category_id": {
                    "required": True,
                    "type": "integer",
                    "rename": "category"
                },
                "description": {
                    "required": True,
                    "type": "string",
                },
                "answer": {
                    "required": True,
                    "type": "string",
                },
                "is_common": {
                    "required": True,
                    "type": "boolean",
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data, normalize=False):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        validator.normalized(request.data)

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        question_category = QuestionCategorySerializer().exist(validator.document.get("category"), DB_USER_PARTNER)
        if not question_category:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"category_id": [_("There is not such question category in the system")]},
                }, status=status.HTTP_404_NOT_FOUND
            )

        serialized_question = QuestionSerializer(data=validator.document)

        if serialized_question.is_valid():
            serialized_question.create(DB_USER_PARTNER)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_question.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates a question
        """
        validator = Validator(
            {
                "question_id": {
                    "required": True,
                    "type": "integer",
                },
                "category_id": {
                    "required": False,
                    "type": "integer",
                },
                "description": {
                    "required": False,
                    "type": "string",
                },
                "answer": {
                    "required": False,
                    "type": "string",
                },
                "is_common": {
                    "required": False,
                    "type": "boolean",
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

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        question = QuestionBasicSerializer().exist(validator.document.get("question_id"), DB_USER_PARTNER)
        if not question:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"question_id": [_("There is not such question in the system")]},
                }, status=status.HTTP_404_NOT_FOUND
            )

        category_id = validator.document.get("category_id")
        if category_id:
            question_category = QuestionCategorySerializer().exist(category_id, DB_USER_PARTNER)
            if not question_category:
                transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
                return Response(
                    data={
                        "error": settings.NOT_FOUND_CODE,
                        "details": {"category_id": [_("There is not such question category in the system")]},
                    }, status=status.HTTP_404_NOT_FOUND
                )

        serialized_question = QuestionBasicSerializer(instance=question, data=validator.document)

        if serialized_question.is_valid():
            serialized_question.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_question.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets all questions by category
        """

        validator = Validator(
            {
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
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

    def delete(self, request):
        """
        Lets an admin deletes a question
        """
        validator = Validator(
            {
                "id": {
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
        question_id = validator.document.get("id")
        question = QuestionSerializer().exist(question_id, DB_USER_PARTNER)

        if not question:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": _("There is not such question in the system")
                }, status=status.HTTP_404_NOT_FOUND
            )

        question = QuestionSerializer().delete(question_id, DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)
