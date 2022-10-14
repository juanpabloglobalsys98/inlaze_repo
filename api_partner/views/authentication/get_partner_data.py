from api_partner.helpers import DB_USER_PARTNER
from api_partner.helpers.permissions import IsNotBanned
from api_partner.serializers.authentication.additional_info import (
    RequiredAdditionalInfoSerializer,
)
from api_partner.serializers.authentication.bank_account import (
    BankAccountRequiredInfoSerializer,
)
from api_partner.serializers.authentication.company import (
    CompanyRequiredInfoSerializer,
)
from api_partner.serializers.authentication.documents_company import (
    RequiredDocumentsCompanySerializer,
)
from api_partner.serializers.authentication.documents_partner import (
    RequiredDocumentsPartnerSER,
)
from api_partner.serializers.authentication.partner import (
    PartnerSerializer,
    PartnerStatusSER,
)
from core.serializers.user import UserRequiredInfoSerializer
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class GetPartnerDataPhase2AAPI(APIView):

    permission_classes = (IsAuthenticated, IsNotBanned,)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def get(self, request):
        """
        Get user's basic information considering their session
        """

        user = request.user
        user.prefix, user.phone = user.phone.split(" ") if user.phone else " ".split(" ")
        partner = PartnerSerializer.get_basic_data(None, user.id, DB_USER_PARTNER)
        user = UserRequiredInfoSerializer(instance=user)
        additional_info = partner.additionalinfo if hasattr(partner, 'additionalinfo') else None
        additional_info = RequiredAdditionalInfoSerializer(instance=additional_info)
        company = partner.company if hasattr(partner, 'company') else None
        company = CompanyRequiredInfoSerializer(instance=company)
        partner_status = PartnerStatusSER(instance=partner)

        return Response({
            "user": user.data if user else None,
            "partner_status": partner_status.data if partner else None,
            "additional_info": additional_info.data if additional_info else None,
            "company": company.data if company else None
        }, status=status.HTTP_200_OK)


class GetPartnerDataPhase2BAPI(APIView):

    permission_classes = (IsAuthenticated, IsNotBanned,)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def get(self, request):
        """
        Get user's bank information considering their session
        """

        partner = PartnerSerializer.get_bank_data(None, request.user.id, DB_USER_PARTNER)
        partner_status = PartnerStatusSER(instance=partner)
        bank_account = partner.bankaccount if hasattr(partner,  'bankaccount') else None
        bank_account = BankAccountRequiredInfoSerializer(instance=bank_account)

        return Response({
            "bank_account": bank_account.data if bank_account else None,
            "partner_status": partner_status.data,
        }, status=status.HTTP_200_OK)


class GetPartnerDataPhase2CAPI(APIView):

    permission_classes = (IsAuthenticated, IsNotBanned,)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def get(self, request):
        """
        Get user's documentation considering their session
        """

        partner = PartnerSerializer.get_documents_data(None, request.user.id, DB_USER_PARTNER)
        company = partner.company if hasattr(partner, 'company') else None
        documents_company = partner.company.documents_company if company and hasattr(
            company, 'documents_company') else None
        documents_company = RequiredDocumentsCompanySerializer(instance=documents_company)
        documents_partner = partner.documents_partner if hasattr(partner, 'documents_partner') else None
        documents_partner = RequiredDocumentsPartnerSER(instance=documents_partner)

        partner_status = PartnerStatusSER(instance=partner)
        return Response(
            {
                "docs_company": documents_company.data if documents_company else None,
                "docs_partner": documents_partner.data if documents_partner else None,
                "partner_status": partner_status.data,
            },
            status=status.HTTP_200_OK)


class GetAllPartnerDataAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
    )

    def get(self, request):
        """
        Get all user's information considering their session
        """
        user = request.user
        user.prefix, user.phone = user.phone.split(" ") if user.phone else " ".split(" ")
        partner = PartnerSerializer.get_all_partner_data(None, user.id, DB_USER_PARTNER)

        serialized_user = UserRequiredInfoSerializer(instance=user)
        additional_info = partner.additionalinfo if hasattr(partner, 'additionalinfo') else None
        additional_info = RequiredAdditionalInfoSerializer(instance=additional_info)
        bank_account = partner.bankaccount if hasattr(partner, 'bankaccount') else None
        bank_account = BankAccountRequiredInfoSerializer(instance=bank_account)
        documents_partner = partner.documents_partner if hasattr(partner, 'documents_partner') else None
        documents_partner = RequiredDocumentsPartnerSER(instance=documents_partner)
        company = partner.company if hasattr(partner, 'company') else None
        documents_company = company.documents_company if company and hasattr(company, 'documents_company') else None
        company = CompanyRequiredInfoSerializer(instance=company)
        documents_company = RequiredDocumentsCompanySerializer(instance=documents_company)

        partner_status = PartnerStatusSER(instance=partner)
        return Response({
            "user": serialized_user.data,
            "additional_info": additional_info.data if additional_info else None,
            "company": company.data if company else None,
            "bank_account": bank_account.data if bank_account else None,
            "docs_partner": documents_partner.data if documents_partner else None,
            "docs_company": documents_company.data if documents_company else None,
            "partner_status": partner_status.data,
        }, status=status.HTTP_200_OK)
