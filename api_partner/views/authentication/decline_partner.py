from api_partner.helpers.permissions import IsNotBanned
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.serializers.authentication import (
    RegistrationFeedbackBankSerializer,
    RegistrationFeedbackBasicInfoSerializer,
    RegistrationFeedbackDocumentsSerializer,
)
from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class DeclinePartnerPhase2AAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
    )

    def get(self, request):
        """
        Get the reason and an array with rejected data why a user's basic info was declined by an adviser 
        """
        partner = request.user.partner
        feedback_basic_info = RegistrationFeedbackBasicInfoSerializer().get_latest(partner.user_id, DB_USER_PARTNER)
        if not feedback_basic_info:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"non_field_errors": [_("There is not feedback in basic info for that user in the system")]}
                }, status=status.HTTP_404_NOT_FOUND
            )

        feedback_basic_info._error_fields = eval(feedback_basic_info.error_fields)
        feedback_basic_info.error_fields = None
        serialized_feedback_basic_info = RegistrationFeedbackBasicInfoSerializer(instance=feedback_basic_info)

        return Response(
            data={"feedback_basic_info": serialized_feedback_basic_info.data},
            status=status.HTTP_200_OK
        )


class DeclinePartnerPhase2BAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
    )

    def get(self, request):
        """
        Get the reason and an array with rejected data why a user's bank info was declined by an adviser
        """

        partner = request.user.partner
        feedback_bank = RegistrationFeedbackBankSerializer().get_latest(partner.user_id, DB_USER_PARTNER)
        if not feedback_bank:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"non_field_errors": [_("There is not feedback in bank for that user in the system")]}
                }, status=status.HTTP_404_NOT_FOUND
            )

        feedback_bank._error_fields = eval(feedback_bank.error_fields)
        feedback_bank.error_fields = None
        serialized_feedback_basic_info = RegistrationFeedbackBankSerializer(instance=feedback_bank)

        return Response(
            data={"feedback_bank": serialized_feedback_basic_info.data},
            status=status.HTTP_200_OK
        )


class DeclinePartnerPhase2CAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
    )

    def get(self, request):
        """
        Get the reason and an array with rejected data why a user's documents was declined by an adviser
        """

        partner = request.user.partner
        feedback_documents = RegistrationFeedbackDocumentsSerializer().get_latest(partner.user_id, DB_USER_PARTNER)

        if not feedback_documents:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"non_field_errors": [_("There is not feedback in documents for that user in the system")]}
                }, status=status.HTTP_404_NOT_FOUND
            )

        feedback_documents._error_fields = eval(feedback_documents.error_fields)
        feedback_documents.error_fields = None
        serialized_feedback_documents = RegistrationFeedbackDocumentsSerializer(instance=feedback_documents)

        return Response(
            data={"feedback_documents": serialized_feedback_documents.data},
            status=status.HTTP_200_OK
        )
