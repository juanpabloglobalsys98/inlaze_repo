import logging

from api_partner.helpers import (
    DB_USER_PARTNER,
    GetAllFeedback,
)
from api_partner.helpers.permissions import (
    IsNotBanned,
    IsNotOnLogUpPhase2A,
    IsNotOnLogUpPhase2B,
    IsNotOnLogUpPhase2C,
    IsNotToBeVerified,
    IsBasicInfoValid,
    IsEmailValid,
)
from api_partner.serializers.frequent_questions.partner_feedback import (
    PartnerFeedbackSerializer,
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


class PartnerFeedbackAPI(APIView, GetAllFeedback):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsBasicInfoValid,
        IsEmailValid,
        # IsNotOnLogUpPhase2A,
        # IsNotOnLogUpPhase2B,
        # IsNotOnLogUpPhase2C,
        IsNotToBeVerified
    )

    def post(self, request):
        """
            Saves user feedback about if an answer for a specific question is usefull or not
        """
        validator = Validator(
            {
                "question": {
                    "required": True,
                    "type": "integer",
                },
                "calification": {
                    "required": True,
                    "type": "number",
                },
                "feedback": {
                    "required": True,
                    "type": "string",
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

        validator.document["partner"] = request.user.id
        serialized_partner_feed_back = PartnerFeedbackSerializer(data=validator.document)
        if serialized_partner_feed_back.is_valid():
            serialized_partner_feed_back.create(DB_USER_PARTNER)
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner_feed_back.errors
                }, status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Gets user feedback about if an answer for a specific question is usefull or not
        """

        partner_feedback = PartnerFeedbackSerializer().get_all(DB_USER_PARTNER)
        if partner_feedback:
            partner_feedback = self.paginate_queryset(partner_feedback, request, view=self)
            partner_feedback = PartnerFeedbackSerializer(instance=partner_feedback, many=True)

        return Response(
            data={"partners_feedback": partner_feedback.data if partner_feedback else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if partner_feedback else None
        )
