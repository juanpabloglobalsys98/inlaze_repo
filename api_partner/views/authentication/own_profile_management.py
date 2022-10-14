from api_partner.helpers import (
    DB_USER_PARTNER,
    IsActive,
    IsFullRegisterSkipData,
    IsNotBanned,
    IsTerms,
)
from api_partner.models import (
    AdditionalInfo,
    BankAccount,
    DocumentCompany,
    DocumentPartner,
    Partner,
)
from api_partner.serializers import (
    BankAccountBasicSerializer,
    DocumentsCompanySerializer,
    DocumentsPartnerSerializer,
)
from cerberus import Validator
from core.helpers import (
    PartnerFilesNamesErrorHandler,
    StandardErrorHandler,
    ValidatorFile,
    str_extra_space_remove,
    to_int,
)
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class OwnProfileManagementPhase2BAPI(APIView):
    """
    Partner manage own data about their Bank info this is about Phase 2B, 
    partner only can use when have data status diferrent status of Skipped case
    (except Skipped to be verified or skipped accepted)

    ## Defined methods
    ### patch
    Edit data about bank like of partner of current session: 
    - bank_name : `string`
        Name of bank that the partner want to recive payments
    - account_number : `string`
        account number of bank account where partner want to recieve payments,
        allow any character On Paypal case is email
    - account_type : `int`
        Determine if account is Saving or Checking, this apply for Colombia,
        other countries that not manage this bank types will have null value
    - swift_code : `string`
        Swif code of bank account of partner, this is required only for 
        contries different to Colombia, if partner want billing on Colombia 
        Bank account this value will null.
    - request.user : `int`
        User of current session, this will based for edit bank info
    """
    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsTerms,
    )

    @transaction.atomic(using=DB_USER_PARTNER)
    def patch(self, request):
        """
        Partner edit own Bank info, this 
        """

        partner = request.user.partner

        # Check if partner can modify own Bank info
        if (
            not (
                partner.bank_status in (
                Partner.ValidationStatus.SKIPPED,
                Partner.ValidationStatus.SKIPPED_UPLOADED,
                Partner.ValidationStatus.SKIPPED_REJECTED,
                Partner.ValidationStatus.SKIPPED_FIXED,
                )
            )
        ):
            return Response(
                data={
                    "error": settings.FORBIDDEN,
                    "details": {
                        "non_field_errors": [
                            _("You have a bad bank validation status"),
                        ],
                    },
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        validator = Validator(
            schema={
                "bank_name": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "account_number": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "account_type": {
                    "required": False,
                    "nullable": True,
                    "type": "integer",
                    "coerce": to_int,
                    "default": None,
                    "allowed": BankAccount.AccounType.values,
                },
                "swift_code": {
                    "required": False,
                    "nullable": True,
                    "type": "string",
                    "default": None,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        bank_account = BankAccount.objects.filter(pk=request.user.pk).first()

        if bank_account is None:
            return Response(
                data={
                    "error": settings.RELATION_MISSING_404,
                    "details": {
                        "non_field_errors": [
                            _("Missing relation on your bank account info, contact an Adviser!"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        bank_account_ser = BankAccountBasicSerializer(
            instance=bank_account,
            data=validator.document,
            partial=True,
        )

        if bank_account_ser.is_valid():
            bank_account_ser.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": bank_account_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (partner.bank_status == Partner.ValidationStatus.SKIPPED):
            partner.bank_status = Partner.ValidationStatus.SKIPPED_UPLOADED

            # Save changes
            partner.status = partner.get_status()
            partner.save()
        elif (partner.bank_status == Partner.ValidationStatus.SKIPPED_REJECTED):
            partner.bank_status = Partner.ValidationStatus.SKIPPED_FIXED

            # Save changes
            partner.status = partner.get_status()
            partner.save()

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(
            status=status.HTTP_200_OK,
        )


class OwnProfileManagementPhase2CAPI(APIView):
    """
    Partner manage own data about their Documents info this is about Phase 2C, 
    partner only can use when have data status diferrent status of Skipped case
    (except Skipped to be verified or skipped accepted)

    ## Defined methods
    ### patch
    Edit data about documents like of partner of current session: 
    - bank_certification_file : `file`
        File that certificates the bank account exist, in some cases indicates 
        the person that is the owner (most cases partner)
    - identification_file : `file`
        File that identifies the partner
    - rut_file : `file`
        File that identifies a Company on Colombia, another countries will 
        have this value null. For Partner type person will null
    - exist_legal_repr_file : `file`
        File that certificates the existence of Company in Colombia is 
        Camara y comercio another countries have other documents. For Partner 
        type person will null 
    - request.user : `int`
        User of current session, this will based for edit bank info
    """
    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsTerms,
    )

    def patch(self, request):
        """
        Partner edit own Documents info
        """

        partner = request.user.partner

        # Check if partner can modify own Documents info
        if (
            not (
                partner.documents_status in (
                Partner.ValidationStatus.SKIPPED,
                Partner.ValidationStatus.SKIPPED_UPLOADED,
                Partner.ValidationStatus.SKIPPED_REJECTED,
                Partner.ValidationStatus.SKIPPED_FIXED,
                )
            )
        ):
            return Response(
                data={
                    "error": settings.FORBIDDEN,
                    "details": {
                        "non_field_errors": [
                            _("You have a bad bank validation status"),
                        ],
                    },
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        validator = ValidatorFile(
            schema={
                "bank_certification_file": {
                    "required": False,
                    "type": "file",
                },
                "identification_file": {
                    "required": False,
                    "type": "file",
                },
                "rut_file": {
                    "required": False,
                    "type": "file",
                },
                "exist_legal_repr_file": {
                    "required": False,
                    "type": "file",
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        files_names_validator = Validator(
            schema={
                "bank_certification_file": {
                    "required": False,
                    "type": "string",
                    "regex": "(?i).+\.(pdf|png|jpg|jpeg|webp)",
                },
                "identification_file": {
                    "required": False,
                    "type": "string",
                    "regex": "(?i).+\.(pdf|png|jpg|jpeg|webp)",
                },
                "rut_file": {
                    "required": False,
                    "type": "string",
                    "regex": "(?i).+\.(pdf|png|jpg|jpeg|webp)",
                },
                "exist_legal_repr_file": {
                    "required": False,
                    "type": "string",
                    "regex": "(?i).+\.(pdf|png|jpg|jpeg|webp)",
                },
            }, error_handler=PartnerFilesNamesErrorHandler
        )

        # Get file names
        files_names = {key_i: validator.document.get(key_i).name for key_i in validator.document}

        if not files_names_validator.validate(files_names):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": files_names_validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        # Get documents
        bank_certification_file = validator.document.get("bank_certification_file")
        identification_file = validator.document.get("identification_file")
        rut_file = validator.document.get("rut_file")
        exist_legal_repr_file = validator.document.get("exist_legal_repr_file")

        # Everithing null
        if not bank_certification_file and not identification_file and not rut_file and not exist_legal_repr_file:
            return Response(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("You are sending everything empty, you need to send at least one file"),
                        ],
                    },
                }, status=status.HTTP_400_BAD_REQUEST,
            )

        person_type = partner.additionalinfo.person_type

        documents_person = DocumentPartner.objects.db_manager(DB_USER_PARTNER).filter(pk=partner.pk).first()
        if not documents_person:
            return Response(
                data={
                    "error": settings.RELATION_MISSING_404,
                    "details": {
                        "non_field_errors": [
                            _("Missing relation on your document info for person, contact an Adviser!"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Only if all REQUIRED documents has not null status can change in
        # another case still on same status
        can_change_status = False

        if (person_type == AdditionalInfo.PersonType.PERSON):
            can_change_status = (
                # identification file uploaded or alredy saved
                (bool(identification_file) or bool(documents_person.identification_file)) and
                # bank certification file uploaded or alredy saved
                (bool(bank_certification_file) or bool(documents_person.bank_certification_file))
            )
        elif (person_type == AdditionalInfo.PersonType.COMPANY):
            documents_company = DocumentCompany.objects.db_manager(DB_USER_PARTNER).filter(pk=partner.pk).first()
            if not documents_company:
                return Response(
                    data={
                        "error": settings.RELATION_MISSING_404,
                        "details": {
                            "non_field_errors": [
                                _("Missing relation on your document info for company, contact an Adviser!"),
                            ],
                        },
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            can_change_status = (
                # identification file uploaded or alredy saved
                (bool(identification_file) or bool(documents_person.identification_file)) and
                # bank certification file uploaded or alredy saved
                (bool(bank_certification_file) or bool(documents_person.bank_certification_file)) and
                # Exist legal representation for company
                (bool(exist_legal_repr_file) or bool(documents_company.exist_legal_repr_file))
            )

            # Temporaly save data method - Special case Company
            # Rut file
            # Create case
            if not bool(documents_company.rut_file) and rut_file:
                DocumentsCompanySerializer().create_file("rut_file", documents_company, rut_file)
            # Update case
            elif documents_company.rut_file and rut_file:
                DocumentsCompanySerializer().update_file("rut_file", documents_company, rut_file)
            # Exist legal Representation
            # Create case
            if not bool(documents_company.exist_legal_repr_file) and exist_legal_repr_file:
                DocumentsCompanySerializer().create_file("exist_legal_repr_file", documents_company, exist_legal_repr_file)
            # Update case
            elif documents_company.exist_legal_repr_file and exist_legal_repr_file:
                DocumentsCompanySerializer().update_file("exist_legal_repr_file", documents_company, exist_legal_repr_file)

        # Temporaly save data method - Both cases
        # Identification type
        # Create case
        if not bool(documents_person.identification_file) and identification_file:
            DocumentsPartnerSerializer().create_file("identification", documents_person, identification_file)
        # Update case
        elif documents_person.identification_file and identification_file:
            DocumentsPartnerSerializer().update_file("identification", documents_person, identification_file)

        # Bank certification
        # Create case
        if not bool(documents_person.bank_certification_file) and bank_certification_file:
            DocumentsPartnerSerializer().create_file("bank_certification", documents_person, bank_certification_file)
        # Update case
        elif documents_person.bank_certification_file and bank_certification_file:
            DocumentsPartnerSerializer().update_file("bank_certification", documents_person, bank_certification_file)

        if (partner.documents_status == Partner.ValidationStatus.SKIPPED and can_change_status):
            partner.documents_status = Partner.ValidationStatus.SKIPPED_UPLOADED

            # Save changes
            partner.status = partner.get_status()
            partner.save()
        elif (partner.documents_status == Partner.ValidationStatus.SKIPPED_REJECTED and can_change_status):
            partner.documents_status = Partner.ValidationStatus.SKIPPED_FIXED

            # Save changes
            partner.status = partner.get_status()
            partner.save()

        return Response(
            status=status.HTTP_200_OK,
        )
