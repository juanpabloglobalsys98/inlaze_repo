import logging
import sys
import traceback

from api_partner.helpers import (
    DB_USER_PARTNER,
    HasLevel,
    IsBasicInfoValid,
    IsEmailValid,
    IsTerms,
    PartnerStatusCHO,
)
from api_partner.models import (
    AdditionalInfo,
    Partner,
    PartnerBankAccount,
    PartnerBankValidationRequest,
    PartnerInfoValidationRequest,
)
from api_partner.serializers import (
    AdditionalInfoSerializer,
    DynamicPartnerSER,
    PartnerBankAccountSER,
    PartnerBankValidationRequestSER,
    PartnerInfoValidationRequestREADSER,
    PartnerInfoValidationRequestSER,
)
from cerberus import Validator
from core.helpers import (
    CountryAll,
    PartnerFilesNamesErrorHandler,
    StandardErrorHandler,
    ValidatorFile,
    bad_request_response,
    copy_s3_file,
    create_validator,
    normalize_capitalize,
    obj_not_found_response,
    str_extra_space_remove,
    to_int,
    validate_validator,
)
from core.serializers import UserRequiredInfoSerializer
from core.tasks import chat_logger
from django.conf import settings
from django.db import transaction
from django.db.models import (
    F,
    Q,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class ProfileInfoAPI(APIView):

    permission_classes = (
        IsAuthenticated,
    )

    def post(self, request):
        """
        Retrieves partner data according to their pk.
        """

        validator = Validator(
            schema={
                "partner_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        partner_pk = validator.document.get("partner_pk")
        partner = Partner.objects.filter(pk=partner_pk).first()
        if partner is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "partner_pk": [
                            _("Partner not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user_ser = UserRequiredInfoSerializer(instance=partner.user)

        return Response(
            data={
                "user": user_ser.data,
            },
            status=status.HTTP_200_OK,
        )


class PartnerInfoValidationAPI(APIView):
    """
    Handles info validation requests.
    """

    permission_classes = (
        IsAuthenticated,
        IsTerms,
        HasLevel,
    )

    def get(self, request):
        """
        Retrieves user's basic info and their last info validation request,
        according to their basic info status.
        """
        partner: Partner = request.user.partner
        user_ser = UserRequiredInfoSerializer(instance=request.user)
        partner_ser = DynamicPartnerSER(
            instance=partner,
            fields=(
                "basic_info_status",
                "partner_docs",
            ),
        )

        additional_info_ser = None
        if hasattr(partner, "additionalinfo"):
            partner.additionalinfo
            additional_info_ser = AdditionalInfoSerializer(instance=partner.additionalinfo)

        # According to the basic info status, get the appropriate request
        info_request = PartnerInfoValidationRequest.objects.filter(
            partner=partner,
            status=partner.basic_info_status,
        ).order_by("-created_at").first()
        info_request_ser = None
        if info_request:
            info_request_ser = PartnerInfoValidationRequestREADSER(
                instance=info_request,
            )

        return Response(
            data={
                "user": user_ser.data,
                "partner": partner_ser.data,
                "additional_info": additional_info_ser.data if additional_info_ser else None,
                "info_request": info_request_ser.data if info_request_ser else None,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        """
        Create a new info validation request for the current logged in user.
        """
        validator = ValidatorFile(
            schema={
                "name": {
                    "required": True,
                    "coerce": normalize_capitalize,
                    "type": "string",
                },
                "surname": {
                    "required": True,
                    "coerce": normalize_capitalize,
                    "type": "string",
                },
                "country": {
                    "required": True,
                    "type": "string",
                    "allowed": CountryAll.values,
                },
                "phone": {
                    "required": False,
                    "type": "string",
                },
                "id_type": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerInfoValidationRequest.IdType.values,
                },
                "id_number": {
                    "required": True,
                    "type": "string",
                },
                "document_id_front": {
                    "required": False,
                    "type": "file",
                },
                "document_id_back": {
                    "required": False,
                    "type": "file",
                },
                "selfie": {
                    "required": False,
                    "type": "file",
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filenames_validator = Validator(
            schema={
                "document_id_front_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|webp|JPEG|PDF|PNG|JPG|WEBP|)+",
                    "nullable": True,
                },
                "document_id_back_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|webp|JPEG|PDF|PNG|JPG|WEBP|)+",
                    "nullable": True,
                },
                "selfie_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|webp|JPEG|PDF|PNG|JPG|WEBP|)+",
                    "nullable": True,
                },
            },
            error_handler=PartnerFilesNamesErrorHandler,
        )

        document_id_front_file = validator.document.get("document_id_front")
        document_id_back_file = validator.document.get("document_id_back")
        selfie_file = validator.document.get("selfie")

        # Take each file's name to perform file type validation
        filenames = {
            "document_id_front_name": document_id_front_file.name if document_id_front_file else None,
            "document_id_back_name": document_id_back_file.name if document_id_back_file else None,
            "selfie_name": selfie_file.name if selfie_file else None,
        }
        # Validate data from cerberus and save in files_names dict
        if not filenames_validator.validate(filenames):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": filenames_validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner: Partner = request.user.partner
        all_required = True
        docs = (document_id_front_file, document_id_back_file, selfie_file)
        # Check if partner has an info validation with requested status
        if partner.validation_requests.filter(
            status=PartnerStatusCHO.REQUESTED,
        ).first():
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "code": [
                            _("Info validation request already submitted"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        # Check if any document is missing
        elif not all(docs):
            all_required = False

            if not (rejected_validation := partner.validation_requests.filter(
                status=PartnerStatusCHO.REJECTED,
            ).last()) or partner.basic_info_status != PartnerStatusCHO.REJECTED:
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "detail": {
                            "non_field_errors": [
                                _("Must have a rejected status and a rejected validation request if any document is missing"),
                            ]
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

        name = validator.document.get("name", "").split()
        surname = validator.document.get("surname", "").split()
        current_country = validator.document.get("country")
        id_type = validator.document.get("id_type")
        id_number = validator.document.get("id_number")

        query = Q(identification=id_number, identification_type=id_type)
        if AdditionalInfo.objects.filter(query).exists():
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "identification": [
                            _("this identification number already exists"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # If field input splitted value length is 2, then save each value in their
        # corresponding field. Will put everything in the first field with any other length
        first_name = second_name = last_name = second_last_name = ""
        if len(name) == 2:
            first_name, second_name = name
        else:
            first_name = " ".join(name)
        if len(surname) == 2:
            last_name, second_last_name = surname
        else:
            last_name = " ".join(surname)

        partner_info_validation = PartnerInfoValidationRequest.objects.create(
            partner=partner,
        )

        try:
            if all_required:
                # Create a file for each filefield, passing its attribute's name as a string
                partner_info_validation.create_file(document_id_front_file, "document_id_front_file")
                partner_info_validation.create_file(document_id_back_file, "document_id_back_file")
                partner_info_validation.create_file(selfie_file, "selfie_file")
            else:
                # Check if each doc was uploaded, otherwise copy the doc from the rejected validation
                to_path = partner_info_validation.get_destination_path()

                if document_id_front_file:
                    partner_info_validation.create_file(document_id_front_file, "document_id_front_file")
                else:
                    partner_info_validation.document_id_front_file = copy_s3_file(
                        source_file=rejected_validation.document_id_front_file,
                        to_path=to_path
                    )

                if document_id_back_file:
                    partner_info_validation.create_file(document_id_back_file, "document_id_back_file")
                else:
                    partner_info_validation.document_id_back_file = copy_s3_file(
                        source_file=rejected_validation.document_id_back_file,
                        to_path=to_path
                    )

                if selfie_file:
                    partner_info_validation.create_file(selfie_file, "selfie_file")
                else:
                    partner_info_validation.selfie_file = copy_s3_file(
                        source_file=rejected_validation.selfie_file,
                        to_path=to_path
                    )

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))

            partner_info_validation.delete()

            return Response(
                data={
                    "error": settings.ERROR_SAVING_PARTNER_DOCUMENTS,
                    "details": {
                        "non_field_errors": [
                            "".join(str(exc_value)),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        partner_info_validation_ser = PartnerInfoValidationRequestSER(
            instance=partner_info_validation,
            data={
                "first_name": first_name,
                "second_name": second_name,
                "last_name": last_name,
                "second_last_name": second_last_name,
                "current_country": current_country,
                "id_type": id_type,
                "id_number": id_number,
                "status": PartnerStatusCHO.REQUESTED,
            },
            partial=True,
        )
        if not partner_info_validation_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_info_validation_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner.basic_info_status = PartnerStatusCHO.REQUESTED
        with transaction.atomic(using=DB_USER_PARTNER):
            partner_info_validation_ser.save()
            partner.save()

        chat_logger.apply_async(
            kwargs={
                "msg": (
                    f"Partner {partner.user.get_full_name()} - {partner.user.email} "
                    "has uploaded basic information."
                ),
                "msg_url": settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION,
            },
        )
        return Response(status=status.HTTP_201_CREATED)


class PartnerBankValidationAPI(APIView):
    """
    Handles bank info validation requests.
    """
    permission_classes = (
        IsAuthenticated,
        IsBasicInfoValid,
        IsEmailValid,
        IsTerms,
        HasLevel,
    )

    def get(self, request):
        """
        Retrieves user's bank info and bank requests.
        """
        # Get partner's active bank accounts
        query = Q(partner_id=request.user.id, is_active=True)
        partner_bank_account = PartnerBankAccount.objects.filter(query)
        accounts_ser = PartnerBankAccountSER(
            instance=partner_bank_account,
            fields=(
                "pk",
                "billing_country",
                "billing_city",
                "billing_address",
                "bank_name",
                "account_type",
                "account_number",
                "swift_code",
                "company_name",
                "company_reg_number",
                "is_active",
                "is_primary",
            ),
            many=True,
        )

        # Get partner's requested bank validations
        query = Q(partner_id=request.user.id) & Q(status=PartnerStatusCHO.REQUESTED)
        partner_bank_account_request = PartnerBankValidationRequest.objects.filter(query)
        requests_ser = PartnerBankValidationRequestSER(
            instance=partner_bank_account_request,
            fields=(
                "pk",
                "billing_country",
                "billing_city",
                "billing_address",
                "bank_name",
                "account_type",
                "account_number",
                "swift_code",
                "company_name",
                "company_reg_number",
                "is_active",
                "status",
            ),
            many=True,
        )

        return Response(
            data={
                "accounts": accounts_ser.data,
                "requests": requests_ser.data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        """
        Create bank validation requests for the current logged in user.
        """
        validator = Validator(
            schema={
                "billing_country": {
                    "required": True,
                    "type": "string",
                    "allowed": CountryAll.values,
                },
                "billing_city": {
                    "required": True,
                    "type": "string",
                    "empty": False,
                    "coerce": normalize_capitalize,
                },
                "billing_address": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "bank_name": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "account_type": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerBankValidationRequest.AccountType.values,
                },
                "account_number": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "swift_code": {
                    "required": False,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (res := PartnerBankAccount.verify_account_type(validator.document)):
            return res

        account_type = validator.document.get("account_type")
        partner: Partner = request.user.partner

        bank_request_exists = partner.bank_validation_requests.filter(
            status=PartnerStatusCHO.REQUESTED,
        ).exists()
        bank_accounts_count = partner.bank_accounts.count()
        # Check if partner already has one requested bank validation
        if bank_request_exists:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("You already have a pending bank validation request"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        # Check if having an additional bank account would go over the current accounts limit
        elif bank_accounts_count + 1 > settings.BANK_ACCOUNTS_LIMIT:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Bank accounts limit reached"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_bank_validation = PartnerBankValidationRequest.objects.create(
            partner=partner,
        )

        partner_bank_validation_ser = PartnerBankValidationRequestSER(
            instance=partner_bank_validation,
            data={
                "partner": partner,
                "billing_country": validator.document.get("billing_country"),
                "billing_city": validator.document.get("billing_city"),
                "billing_address": validator.document.get("billing_address"),
                "bank_name": validator.document.get("bank_name"),
                "account_type": account_type,
                "account_number": validator.document.get("account_number"),
                "swift_code": validator.document.get("swift_code", ""),
                "status": PartnerStatusCHO.REQUESTED,
            },
            partial=True,
        )
        if not partner_bank_validation_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_bank_validation_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # If partner has a primary bank account, change their secondary account status,
        # else it's their first (primary) account status that should be changed
        has_primary = partner.bank_accounts.filter(is_primary=True).exists()
        if has_primary:
            partner.secondary_bank_status = PartnerStatusCHO.REQUESTED
        else:
            partner.bank_status = PartnerStatusCHO.REQUESTED

        with transaction.atomic(using=DB_USER_PARTNER):
            partner_bank_validation_ser.save()
            partner.save()

        chat_logger.apply_async(
            kwargs={
                "msg": (
                    f"Partner {partner.user.get_full_name()} - {partner.user.email} "
                    "has uploaded banking information."
                ),
                "msg_url": settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION,
            },
        )
        return Response(status=status.HTTP_201_CREATED)

    def patch(self, request):
        """
        Allows a partner to set a bank account as their primary one.
        """
        schema = {
            "pk": {
                "required": True,
                "type": "integer",
                "coerce": to_int,
            },
        }
        validator = create_validator(schema)
        if (res := validate_validator(validator, request.data)):
            return res

        partner = request.user.partner
        bank_account: PartnerBankAccount = PartnerBankAccount.objects.filter(
            pk=validator.document.get("pk"),
            partner=partner,
        ).first()

        if bank_account is None:
            return obj_not_found_response(PartnerBankAccount)
        elif bank_account.is_primary:
            return bad_request_response(
                detail={
                    "non_field_errors": [
                        _("This bank account is already your primary account"),
                    ],
                },
            )

        # Get the primary account and change its is_primary field
        # and set the current account as the primary one
        primary_account = partner.bank_accounts.filter(is_primary=True).first()
        if primary_account:
            primary_account.is_primary = False
        bank_account.is_primary = True

        with transaction.atomic(using=DB_USER_PARTNER):
            bank_account.save()
            if primary_account:
                primary_account.save()

        return Response(status=status.HTTP_200_OK)

    def delete(self, request):
        """
        Allows a partner to delete non primary bank accounts.
        """
        schema = {
            "pk": {
                "required": True,
                "type": "integer",
                "coerce": to_int,
            },

        }
        validator = create_validator(schema)
        if (res := validate_validator(validator, request.query_params)):
            return res

        bank_pk = validator.document.get("pk")
        partner: Partner = request.user.partner
        bank_account: PartnerBankAccount = PartnerBankAccount.objects.filter(
            pk=bank_pk,
            partner=partner,
        ).first()

        if bank_account is None:
            return obj_not_found_response(PartnerBankAccount)
        elif bank_account.is_primary:
            return bad_request_response(
                detail={
                    "non_field_errors": [
                        _("You can't delete your primary bank account"),
                    ],
                },
            )

        bank_account.is_active = False
        with transaction.atomic(using=DB_USER_PARTNER):
            bank_account.save()
            # Check if partner has active bank accounts that are not primary
            query = Q(partner=partner, is_active=True) & ~Q(is_primary=True)
            has_secondary_accounts = PartnerBankAccount.objects.filter(query).exists()
            if not has_secondary_accounts:
                # Partner has no secondary accounts, therefore secondary status is None
                partner.secondary_bank_status = None
                partner.save()

        return Response(status=status.HTTP_200_OK)


class CompanyBankValidationAPI(APIView):
    """
    Handles bank info validation requests for partners that are companies.
    """
    permission_classes = (
        IsAuthenticated,
        IsBasicInfoValid,
        IsEmailValid,
        IsTerms,
        HasLevel,
    )

    def post(self, request):
        """
        Create bank validation request for companies. More fields are required
        for this type of request.
        """
        validator = ValidatorFile(
            schema={
                "billing_country": {
                    "required": True,
                    "type": "string",
                    "allowed": CountryAll.values,
                },
                "billing_city": {
                    "required": True,
                    "type": "string",
                    "empty": False,
                    "coerce": normalize_capitalize,
                },
                "billing_address": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "bank_name": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "account_type": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerBankValidationRequest.AccountType.values,
                },
                "account_number": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "swift_code": {
                    "required": False,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "company_name": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "company_reg_number": {
                    "required": False,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (res := PartnerBankAccount.verify_account_type(validator.document)):
            return res

        account_type = validator.document.get("account_type")

        partner: Partner = request.user.partner
        bank_request_exists = partner.bank_validation_requests.filter(
            status=PartnerStatusCHO.REQUESTED,
        ).exists()
        bank_accounts_count = partner.bank_accounts.count()
        if bank_request_exists:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("You already have a pending bank validation request"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif bank_accounts_count + 1 > settings.BANK_ACCOUNTS_LIMIT:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Bank info validation request limit reached"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_bank_validation = PartnerBankValidationRequest.objects.create(
            partner=partner,
        )

        partner_bank_validation_ser = PartnerBankValidationRequestSER(
            instance=partner_bank_validation,
            data={
                "billing_country": validator.document.get("billing_country"),
                "billing_city": validator.document.get("billing_city"),
                "billing_address": validator.document.get("billing_address"),
                "bank_name": validator.document.get("bank_name"),
                "account_type": account_type,
                "account_number": validator.document.get("account_number"),
                "swift_code": validator.document.get("swift_code", ""),
                "is_company": True,
                "company_name": validator.document.get("company_name"),
                "company_reg_number": validator.document.get("company_reg_number"),
                "status": PartnerStatusCHO.REQUESTED,
            },
            partial=True,
        )
        if not partner_bank_validation_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_bank_validation_ser.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        has_primary = partner.bank_accounts.filter(is_primary=True).exists()
        if has_primary:
            partner.secondary_bank_status = PartnerStatusCHO.REQUESTED
        else:
            partner.bank_status = PartnerStatusCHO.REQUESTED

        with transaction.atomic(using=DB_USER_PARTNER):
            partner_bank_validation_ser.save()
            partner.save()

        chat_logger.apply_async(
            kwargs={
                "msg": (
                    f"Partner {partner.user.get_full_name()} - {partner.user.email} "
                    "has uploaded banking information."
                ),
                "msg_url": settings.CHAT_WEBHOOK_PARTNERS_REGISTRATION,
            },
        )
        return Response(status=status.HTTP_201_CREATED)
