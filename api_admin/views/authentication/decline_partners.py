import logging
import sys
import traceback

from api_admin.helpers import DB_ADMIN
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import Partner
from api_partner.serializers import (
    PartnerStatusSER,
    RegistrationFeedbackBankSerializer,
    RegistrationFeedbackBasicInfoSerializer,
    RegistrationFeedbackDocumentsSerializer,
)
from cerberus import Validator
from core.helpers import (
    EmailThread,
    request_cfg,
)
from core.helpers.check_permissions import HavePermissionBasedView
from core.serializers import UserBasicSerializer
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class DeclinePartnerPhase2AAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Lets an adviser gets the reason of declined partner basic info in the system
        """

        validator = Validator(
            schema={
                'user_id': {
                    'required': True,
                    'type': 'integer',
                    'coerce': int,
                },
            },
        )

        if not validator.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        adviser = request.user
        partner = PartnerStatusSER().exist(validator.document.get("user_id"), DB_USER_PARTNER)
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        if adviser.is_superuser:
            feedback_basic_info = RegistrationFeedbackBasicInfoSerializer().get_latest(partner.user_id, DB_USER_PARTNER)
        else:
            feedback_basic_info = RegistrationFeedbackBasicInfoSerializer(
            ).get_by_partner_and_admin(partner.user_id, adviser.id, DB_USER_PARTNER)

        if not feedback_basic_info:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not feedback with basic info for that user in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        serialized_feedback_basic_info = RegistrationFeedbackBasicInfoSerializer(instance=feedback_basic_info)

        return Response(
            data={
                "feedback_basic_info": serialized_feedback_basic_info.data,
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an adviser annotate partner documents as declined in the system
        """
        validator = Validator(
            schema={
                'user_id': {
                    'required': True,
                    'type': 'integer',
                    'rename': 'partner',
                },
                'error_fields': {
                    'required': True,
                    'type': 'list',
                },
                'description': {
                    'required': False,
                    'type': 'string',
                },
            },
        )

        if not validator.validate(
            document=request.data,
            normalize=False,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.normalized(
            document=request.data,
        )

        adviser = request.user
        partner = PartnerStatusSER().exist(validator.document.get("partner"), DB_USER_PARTNER)
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Prevent execution if partner not have status to_be_verified
        if (
            not partner.status in (
                Partner.Status.TO_BE_VERIFIED,
                Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED,
            )
        ):
            return Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner not have the expected state TO BE VERIFIED"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get basic info phase status previous to temp modification
        old_basic_info_status = partner.basic_info_status

        validator.document["error_fields"] = str(validator.document.get("error_fields"))
        validator.document["adviser_id"] = adviser.id

        # Initial Registration case
        if (partner.status == Partner.Status.TO_BE_VERIFIED):
            validator.document["basic_info_status"] = Partner.ValidationStatus.REJECTED

            # Temp setup bank status for re-calculate account state
            partner.basic_info_status = Partner.ValidationStatus.REJECTED
        # Case after initial registration with skipped data
        elif (partner.status == Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED):
            validator.document["basic_info_status"] = Partner.ValidationStatus.SKIPPED_REJECTED

            # Temp setup bank status for re-calculate account state
            partner.basic_info_status = Partner.ValidationStatus.SKIPPED_REJECTED

        # Setup routing for Partner on Users
        request_cfg.is_partner = True
        # Get new status with temp change
        validator.document["status"] = partner.get_status()

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        feedback_basic_info = RegistrationFeedbackBasicInfoSerializer(
        ).get_by_partner(partner.user_id, DB_USER_PARTNER)
        if feedback_basic_info:
            serialized_feedback_basic_info = RegistrationFeedbackBasicInfoSerializer(
                instance=feedback_basic_info, data=validator.document)
        else:
            serialized_feedback_basic_info = RegistrationFeedbackBasicInfoSerializer(data=validator.document)

        if serialized_feedback_basic_info.is_valid():
            serialized_feedback_basic_info.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_feedback_basic_info.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Prevent re-send email after a previous reject
        if (
            old_basic_info_status in (
                Partner.ValidationStatus.REJECTED,
                Partner.ValidationStatus.SKIPPED_REJECTED,
            )
        ):
            transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
            return Response(
                status=status.HTTP_200_OK,
            )

        serialized_partner = PartnerStatusSER(instance=partner, data=validator.document)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        # partner_user = partner.user
        # adviser = UserBasicSerializer.exist(None, partner.adviser_id, DB_ADMIN)
        # partner_full_name = partner_user.get_full_name()
        # if adviser:
        #     adviser_full_name = adviser.get_full_name()
        #     adviser_email = adviser.email
        #     adviser_phone = adviser.phone
        # else:
        #     adviser_full_name = ""
        #     adviser_email = ""
        #     adviser_phone = ""

        # sending emails
        # try:
        #     EmailThread(
        #         html="decline_alert.html",
        #         email=partner.user.email,
        #         subject=_("[account_data_rejection] Account data rejection"),
        #         data={
        #             "ADVISER_FULL_NAME": adviser_full_name,
        #             "ADVISER_EMAIL": adviser_email,
        #             "ADVISER_PHONE": adviser_phone,
        #             "PARTNER_FULL_NAME": partner_full_name,
        #             "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
        #             "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,
        #             "BETENLACE_LOGIN": settings.BETENLACE_LOGIN,
        #             "COMPANY_URL": settings.COMPANY_URL,
        #             "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
        #             "ADVISER_INFO": _("Adviser contact"),
        #             "CUSTOMER_MESSAGE": _("To activate your account and enjoy all the benefits offered by Betenlace you need to adjust the fields specified by your advisor in the registration form."),
        #             "GREETING": _("Hi"),
        #             "LOGIN_MESSAGE": _("Login Inlazz"),
        #             "CUSTOMER_SERVICE_PART_1": _("Customer service"),
        #             "CUSTOMER_SERVICE_PART_2": _("Monday to Friday"),
        #             "CUSTOMER_SERVICE_PART_3": _("Colombia (UTC-5)"),
        #             "CUSTOMER_SERVICE_PART_4": _("Saturdays"),
        #             "CUSTOMER_SERVICE_PART_5": _("Colombia (UTC-5)"),
        #             "FOOTER_MESSAGE_PART_1": _("Best regards,"),
        #             "FOOTER_MESSAGE_PART_2": _("Inlazz team"),
        #             "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
        #             "CUSTOMER_SERVICE": _("Customer service."),
        #             "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
        #             "FOOTER_MESSAGE_PART_5": _("Access here"),
        #         }).start()
        # except Exception as e:
        #     exc_type, exc_value, exc_traceback = sys.exc_info()
        #     e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        #     logger.critical("".join(e))
        #     return Response(
        #         data={
        #             "error": settings.INTERNAL_SERVER_ERROR,
        #             "details": {
        #                 "non_field_errors": [
        #                     _("Server error"),
        #                 ],
        #             },
        #         },
        #         status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #     )

        return Response(status=status.HTTP_200_OK)


class DeclinePartnerPhase2BAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Lets an adviser gets the reason of declined partner bank info in the system
        """

        validator = Validator(
            {
                'user_id': {
                    'required': True,
                    'type': 'integer',
                    'coerce': int,
                },
            },
        )

        if not validator.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        adviser = request.user
        partner = PartnerStatusSER().exist(validator.document.get("user_id"), DB_USER_PARTNER)
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        if adviser.is_superuser:
            feedback_bank = RegistrationFeedbackBankSerializer().get_latest(partner.user_id, DB_USER_PARTNER)
        else:
            feedback_bank = RegistrationFeedbackBankSerializer().get_by_partner_and_admin(partner.user_id, adviser.id, DB_USER_PARTNER)

        if not feedback_bank:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not feedback with bank for that user in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        serialized_feedback_basic_info = RegistrationFeedbackBankSerializer(instance=feedback_bank)

        return Response(
            data={"feedback_bank": serialized_feedback_basic_info.data},
            status=status.HTTP_200_OK
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an adviser annotate partner bank info as declined in the system
        """
        validator = Validator(
            {
                'user_id': {
                    'required': True,
                    'type': 'integer',
                    'rename': 'partner',
                },
                'error_fields': {
                    'required': True,
                    'type': 'list',
                },
                'description': {
                    'required': False,
                    'type': 'string',
                },
            },
        )

        if not validator.validate(
            document=request.data,
            normalize=False,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.normalized(
            document=request.data,
        )

        adviser = request.user
        partner = PartnerStatusSER().exist(validator.document.get("partner"), DB_USER_PARTNER)
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Prevent execution if partner account not have status to_be_verified
        if (
            not partner.status in (
                Partner.Status.TO_BE_VERIFIED,
                Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED,
            )
        ):
            return Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner not have the expected state TO BE VERIFIED"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get bank phase status previous to temp modification
        old_bank_status = partner.bank_status

        validator.document["error_fields"] = str(validator.document.get("error_fields"))
        validator.document["adviser_id"] = adviser.id

        # Get bank phase status previous to temp modification
        if (partner.status == Partner.Status.TO_BE_VERIFIED):
            validator.document["bank_status"] = Partner.ValidationStatus.REJECTED

            # Temp setup bank status for re-calculate account state
            partner.bank_status = Partner.ValidationStatus.REJECTED
        # Case after initial registration with skipped data
        elif (partner.status == Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED):
            validator.document["bank_status"] = Partner.ValidationStatus.SKIPPED_REJECTED

            # Temp setup bank status for re-calculate account state
            partner.bank_status = Partner.ValidationStatus.SKIPPED_REJECTED

        # Setup routing for Partner on Users
        request_cfg.is_partner = True

        # Get new status with temp change
        validator.document["status"] = partner.get_status()

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        feedback_bank = RegistrationFeedbackBankSerializer().get_by_partner(partner.user_id, DB_USER_PARTNER)
        if feedback_bank:
            serialized_feedback_bank = RegistrationFeedbackBankSerializer(
                instance=feedback_bank, data=validator.document)
        else:
            serialized_feedback_bank = RegistrationFeedbackBankSerializer(data=validator.document)

        if serialized_feedback_bank.is_valid():
            serialized_feedback_bank.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_feedback_bank.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Prevent re-send email after a previous reject
        if (
            old_bank_status in (
                Partner.ValidationStatus.REJECTED,
                Partner.ValidationStatus.SKIPPED_REJECTED,
            )
        ):
            transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
            return Response(
                status=status.HTTP_200_OK,
            )

        serialized_partner = PartnerStatusSER(instance=partner, data=validator.document)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        # partner_user = partner.user
        # adviser = UserBasicSerializer.exist(None, partner.adviser_id, DB_ADMIN)
        # partner_full_name = partner_user.get_full_name()
        # if adviser:
        #     adviser_full_name = adviser.get_full_name()
        #     adviser_email = adviser.email
        #     adviser_phone = adviser.phone
        # else:
        #     adviser_full_name = ""
        #     adviser_email = ""
        #     adviser_phone = ""

        # sending emails
        # try:
        #     EmailThread(
        #         html="decline_alert.html",
        #         email=partner.user.email,
        #         subject=_("[account_data_rejection] Account data rejection"),
        #         data={
        #             "ADVISER_FULL_NAME": adviser_full_name,
        #             "ADVISER_EMAIL": adviser_email,
        #             "ADVISER_PHONE": adviser_phone,
        #             "PARTNER_FULL_NAME": partner_full_name,
        #             "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
        #             "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,
        #             "BETENLACE_LOGIN": settings.BETENLACE_LOGIN,
        #             "COMPANY_URL": settings.COMPANY_URL,
        #             "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
        #             "ADVISER_INFO": _("Adviser contact"),
        #             "CUSTOMER_MESSAGE": _("To activate your account and enjoy all the benefits offered by Betenlace you need to adjust the fields specified by your advisor in the registration form."),
        #             "GREETING": _("Hi"),
        #             "LOGIN_MESSAGE": _("Login Inlazz"),
        #             "CUSTOMER_SERVICE_PART_1": _("Customer service"),
        #             "CUSTOMER_SERVICE_PART_2": _("Monday to Friday"),
        #             "CUSTOMER_SERVICE_PART_3": _("Colombia (UTC-5)"),
        #             "CUSTOMER_SERVICE_PART_4": _("Saturdays"),
        #             "CUSTOMER_SERVICE_PART_5": _("Colombia (UTC-5)"),
        #             "FOOTER_MESSAGE_PART_1": _("Best regards,"),
        #             "FOOTER_MESSAGE_PART_2": _("Inlazz team"),
        #             "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
        #             "CUSTOMER_SERVICE": _("Customer service."),
        #             "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
        #             "FOOTER_MESSAGE_PART_5": _("Access here"),
        #         }
        #     ).start()
        # except Exception as e:
        #     exc_type, exc_value, exc_traceback = sys.exc_info()
        #     e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        #     logger.critical("".join(e))
        #     return Response(
        #         data={
        #             "error": settings.INTERNAL_SERVER_ERROR,
        #             "details": {
        #                 "non_field_errors": [
        #                     _("Server error"),
        #                 ],
        #             },
        #         },
        #         status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #     )

        return Response(
            status=status.HTTP_200_OK,
        )


class DeclinePartnerPhase2CAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Lets an adviser gets the reason of declined partner's document in the system
        """

        validator = Validator(
            schema={
                'user_id': {
                    'required': True,
                    'type': 'integer',
                    'coerce': int,
                },
            },
        )

        if not validator.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        adviser = request.user
        partner = PartnerStatusSER().exist(validator.document.get("user_id"), DB_USER_PARTNER)
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        if adviser.is_superuser:
            feedback_documents = RegistrationFeedbackDocumentsSerializer().get_latest(partner.user_id, DB_USER_PARTNER)
        else:
            feedback_documents = RegistrationFeedbackDocumentsSerializer(
            ).get_by_partner_and_admin(partner.user_id, adviser.id, DB_USER_PARTNER)

        if not feedback_documents:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not feedback with documents for that user in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        serialized_feedback_documents = RegistrationFeedbackDocumentsSerializer(instance=feedback_documents)

        return Response(
            data={
                "feedback_documents": serialized_feedback_documents.data,
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an adviser annotate partner documents as declined in the system
        """
        validator = Validator(
            {
                'user_id': {
                    'required': True,
                    'type': 'integer',
                    'rename': 'partner',
                },
                'error_fields': {
                    'required': True,
                    'type': 'list',
                },
                'description': {
                    'required': False,
                    'type': 'string',
                },
            },
        )

        if not validator.validate(
            document=request.data,
            normalize=False,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST)

        validator.normalized(
            document=request.data,
        )

        adviser = request.user
        partner = PartnerStatusSER().exist(validator.document.get("partner"), DB_USER_PARTNER)
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Prevent execution if partner account not have status to_be_verified
        if (
            not partner.status in (
                Partner.Status.TO_BE_VERIFIED,
                Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED,
            )
        ):
            return Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner not have the expected state TO BE VERIFIED"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.document["error_fields"] = str(validator.document.get("error_fields"))
        validator.document["adviser_id"] = adviser.id

        # Get documents phase status previous to temp modification
        old_documents_status = partner.documents_status

        # Initial Registration case
        if (partner.status == Partner.Status.TO_BE_VERIFIED):
            validator.document["documents_status"] = Partner.ValidationStatus.REJECTED

            # Temp setup document status for re-calculate account state
            partner.documents_status = Partner.ValidationStatus.REJECTED
        # Case after initial registration with skipped data
        elif (partner.status == Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED):
            validator.document["documents_status"] = Partner.ValidationStatus.SKIPPED_REJECTED

            # Temp setup document status for re-calculate account state
            partner.documents_status = Partner.ValidationStatus.SKIPPED_REJECTED

        # Setup routing for Partner on Users
        request_cfg.is_partner = True

        # Get new status with temp change
        validator.document["status"] = partner.get_status()

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        feedback_documents = RegistrationFeedbackDocumentsSerializer().get_by_partner(partner.user_id, DB_USER_PARTNER)
        if feedback_documents:
            serialized_feedback_documents = RegistrationFeedbackDocumentsSerializer(
                instance=feedback_documents, data=validator.document)
        else:
            serialized_feedback_documents = RegistrationFeedbackDocumentsSerializer(data=validator.document)

        if serialized_feedback_documents.is_valid():
            serialized_feedback_documents.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_feedback_documents.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Prevent re-send email after a previous reject
        if (
            old_documents_status in (
                Partner.ValidationStatus.REJECTED,
                Partner.ValidationStatus.SKIPPED_REJECTED,
            )
        ):
            transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
            return Response(status=status.HTTP_200_OK)

        serialized_partner = PartnerStatusSER(instance=partner, data=validator.document)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        # partner_user = partner.user
        # adviser = UserBasicSerializer.exist(None, partner.adviser_id, DB_ADMIN)
        # partner_full_name = partner_user.get_full_name()
        # if adviser:
        #     adviser_full_name = adviser.get_full_name()
        #     adviser_email = adviser.email
        #     adviser_phone = adviser.phone
        # else:
        #     adviser_full_name = ""
        #     adviser_email = ""
        #     adviser_phone = ""

        # sending emails
        # try:
        #     EmailThread(
        #         html="decline_alert.html",
        #         email=partner.user.email,
        #         subject=_("[account_data_rejection] Account data rejection"),
        #         data={
        #             "ADVISER_FULL_NAME": adviser_full_name,
        #             "ADVISER_EMAIL": adviser_email,
        #             "ADVISER_PHONE": adviser_phone,
        #             "PARTNER_FULL_NAME": partner_full_name,
        #             "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
        #             "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,
        #             "BETENLACE_LOGIN": settings.BETENLACE_LOGIN,
        #             "COMPANY_URL": settings.COMPANY_URL,
        #             "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
        #             "ADVISER_INFO": _("Adviser contact"),
        #             "CUSTOMER_MESSAGE": _("To activate your account and enjoy all the benefits offered by Betenlace you need to adjust the fields specified by your advisor in the registration form."),
        #             "GREETING": _("Hi"),
        #             "LOGIN_MESSAGE": _("Login Inlazz"),
        #             "CUSTOMER_SERVICE_PART_1": _("Customer service"),
        #             "CUSTOMER_SERVICE_PART_2": _("Monday to Friday"),
        #             "CUSTOMER_SERVICE_PART_3": _("Colombia (UTC-5)"),
        #             "CUSTOMER_SERVICE_PART_4": _("Saturdays"),
        #             "CUSTOMER_SERVICE_PART_5": _("Colombia (UTC-5)"),
        #             "FOOTER_MESSAGE_PART_1": _("Best regards,"),
        #             "FOOTER_MESSAGE_PART_2": _("Inlazz team"),
        #             "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
        #             "CUSTOMER_SERVICE": _("Customer service."),
        #             "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
        #             "FOOTER_MESSAGE_PART_5": _("Access here"),
        #         }
        #     ).start()
        # except Exception as e:
        #     exc_type, exc_value, exc_traceback = sys.exc_info()
        #     e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        #     logger.critical("".join(e))
        #     return Response(
        #         {
        #             "error": settings.INTERNAL_SERVER_ERROR,
        #             "details": {
        #                 "non_field_errors": [
        #                     _("Server error"),
        #                 ],
        #             },
        #         },
        #         status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #     )

        return Response(
            status=status.HTTP_200_OK,
        )
