import logging
import re
import sys
import traceback

from api_admin.helpers import (
    DB_ADMIN,
    PartnersPaginator,
    get_message_from_code_reason,
    report_visualization_limit,
)
from api_admin.models import (
    CodeReason,
    SearchPartnerLimit,
    TranslateMessage,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    NormalizePartnerRegInfo,
)
from api_partner.models import (
    AdditionalInfo,
    BanUnbanReason,
    DocumentPartner,
    Partner,
    SocialChannel,
)
from api_partner.serializers import (
    BankAccountBasicSerializer,
    BanUnbanCodeReasonSerializer,
    BanUnbanReasonBasicSerializer,
    BanUnbanReasonSER,
    BanUnbanReasonSerializer,
    DocumentsPartnerSerializer,
    GeneralPartnerSER,
    InactiveActiveCodeReasonSerializer,
    InactiveHistoryBasicSerializer,
    InactiveHistorySerializer,
    PartnerSerializer,
    PartnersForAdvisersSerializer,
    PartnersGeneralAdviserSearchSER,
    PartnerStatusSER,
    RequiredAdditionalInfoSerializer,
    RequiredDocumentsPartnerSER,
    ValidationCodeRegisterBasicSerializer,
)
from cerberus import Validator
from core.helpers import (
    EmailThread,
    HavePermissionBasedView,
    StandardErrorHandler,
    ValidatorFile,
    normalize,
    normalize_capitalize,
    request_cfg,
    send_ban_unban_email,
    str_split_to_list,
    to_bool,
    to_datetime_from,
    to_datetime_to,
    to_int,
)
from core.models import User
from core.serializers import (
    UserBasicSerializer,
    UserRequiredInfoSerializer,
)
from django.conf import settings
from django.db import transaction
from django.db.models import (
    F,
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.db.models.query_utils import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class PartnersGeneralAPI(APIView, PartnersPaginator):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin retrieves all partners in the system or the partners that meets the filter
        criteria
        """

        validator = Validator(
            schema={
                "level": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "was_linked": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "identification": {
                    "required": False,
                    "type": "string",
                },
                "identification_type": {
                    "required": False,
                    "type": "string",
                },
                "full_name": {
                    "required": False,
                    "type": "string",
                },
                "date_joined_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime_from,
                },
                "date_joined_to": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime_to,
                },
                "adviser_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "status": {
                    "required": False,
                    "type": "list",
                    "coerce": str_split_to_list,
                    "schema": {
                        "type": "integer",
                        "coerce": to_int,
                    },
                },
                "channel_url": {
                    "required": False,
                    "type": "string",
                },
                "channel_type": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "country": {
                    "required": False,
                    "type": "string",
                },
                "phone": {
                    "required": False,
                    "type": "string",
                },
                "pk": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "referred_by_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "is_banned": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "default": "pk",
                    "allowed": (
                        GeneralPartnerSER.Meta.fields +
                        tuple(["-"+i for i in GeneralPartnerSER.Meta.fields])
                    ),
                },
            }, error_handler=StandardErrorHandler,
        )

        if not validator.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        name_view = self.get_view_name().lower()
        name_method = request.method.lower()
        codename = f"{name_view}-{name_method}"

        # Make report visualization fields if is_superuser so can be show all fields
        if request.user.is_superuser:
            report_visualization = GeneralPartnerSER.Meta.fields

        # show just fields that the user's role has assigned
        else:
            report_visualization = report_visualization_limit(
                admin=request.user,
                permission_codename=codename,
            )
            if (not report_visualization):
                return Response(
                    data={
                        "error": settings.FORBIDDEN_NOT_ALLOWED,
                        "details": {
                            "non_field_errors": [
                                _("This user does not has permission to visualization"),
                            ],
                        },
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )

        order_by = validator.document.get("sort_by")
        order_by = (
            F(order_by[1:]).desc(nulls_last=True)
            if "-" == order_by[0]
            else
            F(order_by).asc(nulls_first=True)
        )

        name_view = self.get_view_name().lower()
        name_method = request.method.lower()
        codename = f"{name_view}-{name_method}"

        query = Q(rol=request.user.rol) & Q(codename=codename)
        searchpartnerlimit = SearchPartnerLimit.objects.using(DB_ADMIN).filter(query).first()

        # filters
        query = Q()
        if (
            (
                not searchpartnerlimit or
                searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED
            ) and
                not request.user.is_superuser
        ):
            query &= (Q(adviser_id=request.user.pk))

        # Force default DB routes to Partner
        request_cfg.is_partner = True
        if "level" in validator.document:
            query &= (Q(level=validator.document.get("level")))

        if validator.document.get("email"):
            query &= (Q(user__email__icontains=validator.document.get("email")))

        if "was_linked" in validator.document:
            query &= (Q(was_linked=validator.document.get("was_linked")))

        if "identification" in validator.document:
            query &= (Q(additionalinfo__identification=validator.document.get("identification")))

        if "identification_type" in validator.document:
            query &= (Q(additionalinfo__identification_type=validator.document.get("identification_type")))

        if validator.document.get("full_name"):
            query &= (Q(partner_full_name__icontains=validator.document.get("full_name")))

        if (
            (date_joined_from := validator.document.get("date_joined_from")) and
            (date_joined_to := validator.document.get("date_joined_to"))
        ):
            query &= (Q(user__date_joined__range=[date_joined_from, date_joined_to]))

        if validator.document.get("adviser_id"):
            query &= (Q(adviser_id=validator.document.get("adviser_id")))

        if "status" in validator.document:
            query &= (Q(status__in=validator.document.get("status")))

        if validator.document.get("country"):
            query &= (Q(additionalinfo__country=validator.document.get("country")))

        if validator.document.get("phone"):
            query &= (Q(user__phone__icontains=validator.document.get("phone")))

        if validator.document.get("pk"):
            query &= (Q(pk=validator.document.get("pk")))

        if validator.document.get("referred_by_id"):
            query &= (Q(referred_by_id=validator.document.get("referred_by_id")))

        if "is_active" in validator.document:
            query &= (Q(user__is_active=validator.document.get("is_active")))

        if "is_banned" in validator.document:
            query &= (Q(user__is_banned=validator.document.get("is_banned")))

        social_query = Q()
        if validator.document.get("channel_url"):
            social_query &= (Q(url=validator.document.get("channel_url")))
        if validator.document.get("channel_type"):
            social_query &= (Q(type_channel=validator.document.get("channel_type")))

        social_channel = SocialChannel.objects.filter(
            social_query
        ).distinct(
            "partner",
        ).values_list(
            "partner_id",
            flat=True,
        )
        if validator.document.get("channel_url"):
            query &= (Q(user_id__in=social_channel))
        if validator.document.get("channel_type"):
            query &= (Q(user_id__in=social_channel))

        partners = Partner.objects.db_manager(DB_USER_PARTNER).annotate(
            partner_full_name=Concat(
                "user__first_name", Value(" "),
                "user__second_name", Value(" "),
                "user__last_name", Value(" "),
                "user__second_last_name",
            ),
            referred_full_name=Concat(
                "referred_by__user__first_name", Value(" "),
                "referred_by__user__second_name", Value(" "),
                "referred_by__user__last_name", Value(" "),
                "referred_by__user__second_last_name",
            ),
            identification=F("additionalinfo__identification"),
            identification_type=F("additionalinfo__identification_type"),
            country=F("additionalinfo__country"),
            email=F("user__email"),
            phone=F("user__phone"),
            date_joined=F("user__date_joined"),
            last_login=F("user__last_login"),
            is_active=F("user__is_active"),
            is_banned=F("user__is_banned"),
        ).filter(
            query,
        ).order_by(
            order_by,
        )

        if partners:
            partners = self.paginate_queryset(partners, request, view=self)
            ban_unban_reasons = BanUnbanReason.objects.filter(
                partner_id__in=set(p.pk for p in partners),
            ).order_by(
                "partner_id",
                "-created_at",
            ).distinct(
                "partner_id",
            )
            advisers_pk = set(
                [p.adviser_id for p in partners]
                + [b.adviser_id for b in ban_unban_reasons]
            )
            advisers = User.objects.using(DB_ADMIN).annotate(
                full_name=Concat(
                    "first_name", Value(" "),
                    "second_name", Value(" "),
                    "last_name", Value(" "),
                    "second_last_name",
                ),
            ).filter(pk__in=advisers_pk).only(
                "pk",
                "first_name",
                "second_name",
                "last_name",
                "second_last_name",
            )

            code_reasons = CodeReason.objects.filter(
                type_code=CodeReason.Type.PARTNER_BAN,
            )
            partners = GeneralPartnerSER(
                instance=partners,
                many=True,
                partial=True,
                context={
                    "advisers": advisers,
                    "ban_unban_reasons": ban_unban_reasons,
                    "code_reasons": code_reasons,
                    "permissions": report_visualization,
                }
            )

        return Response(
            data={
                "partners": partners.data if partners else [],
            },
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link(),
            } if partners else None,
        )


class PartnersToValidateAPI(APIView, PartnersPaginator):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets an admin retrieves all partners that have to be verified by an adviser
        """

        validator = Validator(
            {
                "email": {
                    "required": False,
                    "type": "string"
                },
                "was_linked": {
                    "required": False,
                    "type": "string",
                    "regex": "True|False"
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "regex": "\-?user_id|\-?was_linked|\-?email"
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        # set default sort by
        sort_by = request.query_params.get("sort_by")
        if sort_by:
            if re.search("\-?email", sort_by):
                sort_by = "user__" + sort_by if sort_by.find("-") == -1 else "-user__" + sort_by[1:]
        else:
            sort_by = "-user_id"

        # filters
        email = request.query_params.get("email")
        was_linked = request.query_params.get("was_linked")

        filters = []
        if email:
            filters.append(Q(user__email=email))
        if was_linked:
            filters.append(Q(was_linked=was_linked))

        partners = PartnersForAdvisersSerializer().get_partners(filters=filters, order_by=sort_by, database=DB_USER_PARTNER)
        if partners:
            partners = self.paginate_queryset(partners, request, view=self)
            partners = PartnersForAdvisersSerializer(instance=partners, many=True)

        return Response(
            data={"partners": partners.data if partners else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if partners else None
        )


class PartnerDataDetailAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin gets the partner's data
        """
        validator = Validator(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        user = UserRequiredInfoSerializer().exist(validator.document.get("user_id"), DB_USER_PARTNER)
        if not user:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        user.prefix, user.phone = user.phone.split(" ") if user.phone else " ".split(" ")
        partner = PartnerSerializer.get_all_partner_data(None, user.id, DB_USER_PARTNER)

        serialized_user = UserRequiredInfoSerializer(instance=user, partial=True)

        additional_info = partner.additionalinfo if hasattr(partner, 'additionalinfo') else None
        if(additional_info is not None):
            additional_info = RequiredAdditionalInfoSerializer(instance=additional_info, partial=True)

        documents_partner = partner.documents_partner if hasattr(partner, 'documents_partner') else None
        if(documents_partner is not None):
            documents_partner = RequiredDocumentsPartnerSER(instance=documents_partner, partial=True)

        return Response({
            "user": serialized_user.data,
            "additional_info": additional_info.data if additional_info else None,
            "docs_partner": documents_partner.data if documents_partner else None,
        }, status=status.HTTP_200_OK)


class PartnersPhase1(APIView, PartnersPaginator):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets an admin gets the partner on phase 1
        """
        validator = Validator(
            {
                "email": {
                    "required": False,
                    "type": "string",
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "message": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        email = validator.document.get("email")
        filters = []
        if email:
            filters.append(Q(email=email))

        validation_codes = ValidationCodeRegisterBasicSerializer().get_all(filters=filters, database=DB_USER_PARTNER)
        if validation_codes:
            validation_codes = self.paginate_queryset(validation_codes, request, view=self)
            validation_codes = ValidationCodeRegisterBasicSerializer(instance=validation_codes, many=True)

        return Response(
            data={"partners": validation_codes.data if validation_codes else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if validation_codes else None
        )


class PartnerUpdateAdditionalInfoAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates the partner's basic info
        """

        request_cfg.is_partner = True
        validator = Validator(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "first_name": {
                    "required": False,
                    "type": "string",
                    "empty": True,
                    "coerce": normalize_capitalize,
                },
                "last_name": {
                    "required": False,
                    "type": "string",
                    "empty": True,
                    "coerce": normalize_capitalize,
                },
                "identification": {
                    "required": False,
                    "type": "string",
                    "nullable": True,
                },
                "identification_type": {
                    "required": False,
                    "type": "integer",
                    "nullable": True,
                    "coerce": to_int,
                },
                "prefix": {
                    "required": False,
                    "type": "string"
                },
                "phone": {
                    "required": False,
                    "type": "string",
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "country": {
                    "required": False,
                    "type": "string"
                },
                "company_id": {
                    "required": False,
                    "type": "string"
                },
                "social_reason": {
                    "required": False,
                    "type": "string",
                    "coerce": normalize,
                }
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        # user = UserRequiredInfoSerializer.exist(None, validator.document.get("user_id"), DB_USER_PARTNER)
        filters = (
            Q(id=validator.document.get("user_id")),
        )
        user = User.objects.db_manager(DB_USER_PARTNER).filter(*filters).first()
        if not user:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "user_id": [
                        _("There is not such user in the system")
                    ],
                },
            },
                status=status.HTTP_404_NOT_FOUND,
            )

        name = validator.document.pop("first_name", "").split()
        surname = validator.document.pop("last_name", "").split()

        first_name = second_name = last_name = second_last_name = ""
        if name:
            if len(name) == 2:
                first_name, second_name = name
            else:
                first_name = " ".join(name)
        if surname:
            if len(surname) == 2:
                last_name, second_last_name = surname
            else:
                last_name = " ".join(surname)

        serialized_user = UserRequiredInfoSerializer(
            instance=user,
            data={
                "first_name": first_name.title(),
                "second_name": second_name.title(),
                "last_name": last_name.title(),
                "second_last_name": second_last_name.title(),
            },
            partial=True,
        )
        if serialized_user.is_valid():
            serialized_user.save()

        # NormalizePartnerRegInfo.normalize_additinal_info(None, validator.document)
        email = validator.document.get("email")
        existing_user = UserRequiredInfoSerializer().get_by_email(email, DB_USER_PARTNER)
        if existing_user:
            return Response({
                "error": settings.USER_ALREADY_EXIST,
                "details": {"email": [_("That email was already taken by another user")]}
            }, status=status.HTTP_400_BAD_REQUEST)

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        serialized_user = UserRequiredInfoSerializer(
            instance=user,
            data=validator.document,
            partial=True,
        )
        if serialized_user.is_valid():
            serialized_user.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_user.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        validator.document["user"] = user.id
        partner = user.partner
        serialized_partner = PartnerSerializer(instance=partner, data=validator.document)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_partner.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        additional_info = partner.additionalinfo
        validator.document["partner"] = partner.user_id

        serialized_additional_info = RequiredAdditionalInfoSerializer(
            instance=additional_info,
            data=validator.document,
            partial=True,
        )
        if serialized_additional_info.is_valid():
            serialized_additional_info.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_additional_info.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)


class PartnerUpdateBankAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates the partner's bank info
        """
        request_cfg.is_partner = True
        validator = Validator(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "bank_name": {
                    "required": False,
                    "type": "string"
                },
                "account_number": {
                    "required": False,
                    "type": "string"
                },
                "account_type": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                },
                "swift_code": {
                    "required": False,
                    "type": "string"
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        NormalizePartnerRegInfo.normalize_bank_info(None, validator.document)
        user = UserRequiredInfoSerializer.exist(None, validator.document.get("user_id"), DB_USER_PARTNER)
        if not user:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        partner = user.partner
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        validator.document["partner"] = user.id
        bank_account = partner.bankaccount
        validator.document["swift_code"] = validator.document.get("swift_code")
        serialized_bank_account = BankAccountBasicSerializer(instance=bank_account, data=validator.document)

        if serialized_bank_account.is_valid():
            serialized_bank_account.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_bank_account.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)


class PartnerUpdateDocumentsAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def patch(self, request):
        """
        Lets an admin updates the partner's documents
        """
        request_cfg.is_partner = True
        validator = ValidatorFile(
            schema={
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "document_id_front_file": {
                    "required": False,
                    "type": "file"
                },
                "document_id_back_file": {
                    "required": False,
                    "type": "file"
                },
                "selfie_file": {
                    "required": False,
                    "type": "file"
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

        files_names_validator = ValidatorFile(
            schema={
                "document_id_front_file_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|JPEG|PDF|PNG|JPG)+",
                },
                "document_id_back_file_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|JPEG|PDF|PNG|JPG)+",
                },
                "selfie_file_name": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(pdf|png|jpg|jpeg|JPEG|PDF|PNG|JPG)+",
                },
            },
            error_handler=StandardErrorHandler,
        )

        files_names = {}

        document_id_front_file = request.data.get("document_id_front_file")
        if document_id_front_file:
            files_names["document_id_front_file_name"] = document_id_front_file.name

        document_id_back_file = request.data.get("document_id_back_file")
        if document_id_back_file:
            files_names["document_id_back_file_name"] = document_id_back_file.name

        selfie_file = request.data.get("selfie_file")
        if selfie_file:
            files_names["selfie_file_name"] = selfie_file.name

        if not any((document_id_front_file, document_id_front_file, selfie_file)):
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Must upload at least one image"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not files_names_validator.validate(files_names):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": files_names_validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = UserBasicSerializer().exist(validator.document.get("user_id"), DB_USER_PARTNER)
        if not user:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        document_id_front_file = validator.document.get("document_id_front_file")
        document_id_back_file = validator.document.get("document_id_back_file")
        selfie_file = validator.document.get("selfie_file")

        partner = user.partner
        partner_documents = DocumentsPartnerSerializer().exist(partner.user_id, DB_USER_PARTNER)
        if not hasattr(partner, "documents_partner"):
            partner.documents_partner = DocumentPartner(partner=partner)
            partner_documents = partner.documents_partner

        person_type = partner.additionalinfo.person_type
        validator.document["user"] = user.id
        validator.document["company"] = validator.document["partner"] = partner.user_id

        try:
            if not bool(partner_documents.document_id_front_file) and document_id_front_file:
                DocumentsPartnerSerializer().create_file(
                    "document_id_front_file",
                    partner_documents,
                    document_id_front_file,
                )
            elif partner_documents.document_id_front_file and document_id_front_file:
                DocumentsPartnerSerializer().update_file(
                    "document_id_front_file",
                    partner_documents,
                    document_id_front_file,
                )

            if not bool(partner_documents.document_id_back_file) and document_id_back_file:
                DocumentsPartnerSerializer().create_file(
                    "document_id_back_file",
                    partner_documents,
                    document_id_back_file,
                )
            elif partner_documents.document_id_back_file and document_id_back_file:
                DocumentsPartnerSerializer().update_file(
                    "document_id_back_file",
                    partner_documents,
                    document_id_back_file,
                )

            if not bool(partner_documents.selfie_file) and selfie_file:
                DocumentsPartnerSerializer().create_file(
                    "selfie_file",
                    partner_documents,
                    selfie_file,
                )
            elif partner_documents.selfie_file and selfie_file:
                DocumentsPartnerSerializer().update_file(
                    "selfie_file",
                    partner_documents,
                    selfie_file,
                )

        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))

            if partner_documents.document_id_front_file and document_id_front_file:
                partner_documents.delete_document_id_front_file()

            if partner_documents.document_id_back_file and document_id_back_file:
                partner_documents.delete_document_id_back_file()

            if partner_documents.selfie_file and selfie_file:
                partner_documents.delete_selfie_file()

            return Response(
                data={
                    "error": settings.ERROR_SAVING_PARTNER_DOCUMENTS,
                    "details": {"non_field_errors": ["".join(str(exc_value))]}
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        partner_documents = DocumentsPartnerSerializer().exist(partner.user_id, DB_USER_PARTNER)

        # checking files bad integrity
        bad_files = []

        if not bool(partner_documents.document_id_front_file) and document_id_front_file:
            bad_files.append("document_id_front_file")

        if not bool(partner_documents.document_id_back_file) and document_id_back_file:
            bad_files.append("document_id_back_file")

        if not bool(partner_documents.selfie_file) and selfie_file:
            bad_files.append("selfie_file")

        if person_type == AdditionalInfo.PersonType.PERSON:
            if len(bad_files):
                return Response(
                    data={
                        "error": settings.BAD_FILES_INTEGRITY,
                        "details": {"bad_files": [bad_files]}
                    }, status=status.HTTP_400_BAD_REQUEST
                )

        return Response(status=status.HTTP_200_OK)


class PartnerBanAPI(APIView):
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def post(self, request):
        """
        Creates a ban/unban reason when setting a user's ban status
        """
        request_cfg.is_partner = True
        validator = Validator(
            schema={
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int,
                },
                "code_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "is_banned": {
                    "required": True,
                    "type": "boolean",
                    "coerce": to_bool,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = Partner.objects.using(DB_USER_PARTNER).filter(
            user=validator.document.get("user_id"),
        ).first()
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "user_id": [
                            _("There is not such user in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        is_banned = validator.document.get("is_banned")
        if partner.user.is_banned == is_banned:
            return Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("User already has that ban status"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        type = CodeReason.Type.PARTNER_BAN if is_banned else CodeReason.Type.PARTNER_UNBAN
        code_reason = CodeReason.objects.filter(
            pk=validator.document.get("code_id"),
            type_code=type,
        ).first()
        if code_reason is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "code_id": [
                            _("Code reason not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        message = get_message_from_code_reason(code_reason, partner.user.language)
        if message is None:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("No message found"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        ban_unban_reason_ser = BanUnbanReasonSER(
            data={
                "partner": partner,
                "code_reason_id": code_reason.pk,
                "adviser_id": request.user.pk,
            },
        )
        if not ban_unban_reason_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": ban_unban_reason_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = partner.user
        user.is_banned = is_banned
        with transaction.atomic(using=DB_USER_PARTNER):
            ban_unban_reason_ser.save()
            user.save()

        send_ban_unban_email(
            user=user,
            message=message.message,
            request=request,
        )

        return Response(status=status.HTTP_200_OK)

    def patch(self, request):
        """
        Lets an admin updates a partner ban reason
        """
        validator = ValidatorFile(
            schema={
                "ban_unban_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "code_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            }, error_handler=StandardErrorHandler,
        )
        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        admin = request.user
        validator.document["adviser_id"] = admin.id

        ban_unban_id = validator.document.get("ban_unban_id")
        ban_unban_reason = BanUnbanReason.objects.filter(pk=ban_unban_id).first()
        if ban_unban_reason is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "ban_unban_id": [
                            _("There is not such ban/unban reason in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        code_id = validator.document.get("code_id")
        new_code_reason = CodeReason.objects.filter(pk=code_id).first()
        if new_code_reason is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "code_id": [
                            _("Code reason not found"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        code_reason = CodeReason.objects.filter(
            pk=ban_unban_reason.code_reason_id,
        ).first()
        if code_reason is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Code reason not found"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )
        elif code_reason.type_code != new_code_reason.type_code:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "type_code": [
                            _("Code reasons' types don't match"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif code_reason == new_code_reason:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("Code reason is the same"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        ban_unban_reason_ser = BanUnbanReasonSER(
            instance=ban_unban_reason,
            data={
                "code_reason_id": new_code_reason.pk,
                "adviser_id": request.user.pk,
            },
            partial=True,
        )
        if not ban_unban_reason_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": ban_unban_reason_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        ban_unban_reason_ser.save()
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets a partner ban reason
        """
        validator = Validator(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner = Partner.objects.using(DB_USER_PARTNER).filter(
            user=validator.document.get("user_id"),
        ).first()
        if not partner:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        ban_unban_reason = BanUnbanReasonSerializer().get_by_created_at_and_is_ban_reason(
            validator.document.get("user_id"), DB_USER_PARTNER)
        if not ban_unban_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There are not ban/unban reasons for that user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        serialized_ban_unban_reason_basic = BanUnbanReasonSerializer(instance=ban_unban_reason)
        return Response(data={"ban_unban_reason": serialized_ban_unban_reason_basic.data}, status=status.HTTP_200_OK)


class PartnerUnbanAPI(APIView):
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin unban a partner
        """

        admin = request.user
        request_cfg.is_partner = True
        validator = ValidatorFile(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "ban_unban_code_reason": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        validator.document["adviser_id"] = admin.id
        partner = Partner.objects.using(DB_USER_PARTNER).filter(
            user=validator.document.get("user_id"),
        ).first()
        if not partner:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if not partner.user.is_banned == True:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"user_id": [_("You are trying to unban a user that is not banned")]}
            })

        validator.document["partner"] = partner.user_id
        ban_unban_code_reason = validator.document.get("ban_unban_code_reason")
        ban_unban_code_reason = BanUnbanCodeReasonSerializer().exist(ban_unban_code_reason, DB_USER_PARTNER)
        if not ban_unban_code_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"ban_unban_code_reason": [_("There is not such ban/unban code reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if ban_unban_code_reason.is_ban_reason:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"ban_unban_code_reason": [_("You cannot use a ban code when you are trying to unban")]}
            }, status=status.HTTP_400_BAD_REQUEST)

        serialized_ban_unban_reason = BanUnbanReasonSerializer(data=validator.document)

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        if serialized_ban_unban_reason.is_valid():
            serialized_ban_unban_reason.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_ban_unban_reason.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner_user = User.objects.db_manager(using=DB_USER_PARTNER).filter(id=partner.user_id).first()
        validator.document["is_banned"] = False
        serialized_partner_user = UserBasicSerializer(instance=partner_user, data=validator.document)
        if serialized_partner_user.is_valid():
            serialized_partner_user.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_partner_user.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)

        # sending email
        partner_full_name = partner_user.first_name + " " + partner_user.second_name + " " + partner_user.last_name + " " + partner_user.second_last_name

        try:
            EmailThread(
                html="unban_alert.html",
                email=partner_user.email,
                subject=_("[account_unbanned] Your account was unbanned!"),
                data={
                    "PARTNER_FULL_NAME": partner_full_name,
                    "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
                    "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,
                    "BETENLACE_LOGIN": settings.BETENLACE_LOGIN,
                    "COMPANY_URL": settings.COMPANY_URL,
                    "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
                    "GREETING": _("Hi"),
                    "CUSTOMER_MESSAGE": _("Your account has been unbanned. If you have any question contact your assigned advisor"),
                    "LOGIN_MESSAGE": _("Login Inlazz"),
                    "CUSTOMER_SERVICE_PART_1": _("Customer service"),
                    "CUSTOMER_SERVICE_PART_2": _("Monday to Friday"),
                    "CUSTOMER_SERVICE_PART_3": _("Colombia (UTC-5)"),
                    "CUSTOMER_SERVICE_PART_4": _("Saturdays"),
                    "CUSTOMER_SERVICE_PART_5": _("Colombia (UTC-5)"),
                    "FOOTER_MESSAGE_PART_1": _("Best regards,"),
                    "FOOTER_MESSAGE_PART_2": _("Inlazz team"),
                    "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
                    "CUSTOMER_SERVICE": _("Customer service."),
                    "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
                    "FOOTER_MESSAGE_PART_5": _("Access here"),
                }).start()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                {
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {"non_field_errors": ["".join(str(exc_value))]}
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates a partner unban reason
        """
        validator = ValidatorFile(
            {
                "ban_unban_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "ban_unban_code_reason": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        admin = request.user
        validator.document["adviser_id"] = admin.id

        ban_unban_reason = BanUnbanReasonBasicSerializer().get_by_id(validator.document.get("ban_unban_id"), DB_USER_PARTNER)
        if not ban_unban_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"ban_unban_id": _("There is not such ban/unban reason in the system")}
            }, status=status.HTTP_404_NOT_FOUND)

        ban_unban_code_reason = validator.document.get("ban_unban_code_reason")
        ban_unban_code_reason = BanUnbanCodeReasonSerializer().exist(ban_unban_code_reason, DB_USER_PARTNER)
        if not ban_unban_code_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"ban_unban_code_reason": [_("There is not such ban/unban code reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if ban_unban_code_reason.is_ban_reason:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"ban_unban_code_reason": [_("You cannot use a ban code when you are trying to unban")]}
            }, status=status.HTTP_400_BAD_REQUEST)

        serialized_ban_unban_reason = BanUnbanReasonBasicSerializer(instance=ban_unban_reason, data=validator.document)
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        if serialized_ban_unban_reason.is_valid():
            serialized_ban_unban_reason.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_ban_unban_reason.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets a partner unban reason
        """

        validator = Validator(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner = Partner.objects.using(DB_USER_PARTNER).filter(
            user=validator.document.get("user_id"),
        ).first()

        if not partner:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        ban_unban_reason = BanUnbanReasonSerializer().get_by_create_at_and_is_not_ban_reason(
            partner, DB_USER_PARTNER)

        if not ban_unban_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": _[("There are not ban/unban reasons for that user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        serialized_ban_unban_reason_basic = BanUnbanReasonSerializer(instance=ban_unban_reason)
        return Response(data={"ban_unban_reason": serialized_ban_unban_reason_basic.data}, status=status.HTTP_200_OK)


class PartnerActiveAPI(APIView):
    permission_classes = (IsAuthenticated, )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin activate a partner account
        """
        admin = request.user
        request_cfg.is_partner = True
        validator = ValidatorFile(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "active_inactive_code_reason": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        validator.document["adviser_id"] = admin.id
        partner = Partner.objects.using(DB_USER_PARTNER).select_related(
            "user",
        ).filter(
            user=validator.document.get("user_id"),
        ).first()
        if not partner:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if not partner.user.is_active == False:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"user_id": [_("You are trying to active a user that is already activated")]}
            })

        active_inactive_code_reason = validator.document.get("active_inactive_code_reason")
        active_inactive_code_reason = InactiveActiveCodeReasonSerializer().exist(active_inactive_code_reason, DB_USER_PARTNER)
        if not active_inactive_code_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"active_inactive_code_reason": [_("There is not such Active/inactive code reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if not active_inactive_code_reason.is_active_reason:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"active_inactive_code_reason": [_("You cannot use an inactive code when you are trying to activate")]}
            }, status=status.HTTP_400_BAD_REQUEST)

        validator.document["partner"] = partner.user_id
        serialized_inactive_history = InactiveHistorySerializer(data=validator.document)

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        if serialized_inactive_history.is_valid():
            serialized_inactive_history.create(database=DB_USER_PARTNER)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_inactive_history.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner_user = User.objects.db_manager(using=DB_USER_PARTNER).filter(id=partner.user_id).first()
        validator.document["is_active"] = True
        serialized_partner_user = UserBasicSerializer(instance=partner_user, data=validator.document)
        if serialized_partner_user.is_valid():
            serialized_partner_user.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_partner_user.errors
            })

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)

        # sending email
        partner_user = partner.user
        partner_full_name = partner_user.first_name + " " + partner_user.second_name + " " + partner_user.last_name + " " + partner_user.second_last_name

        try:

            EmailThread(
                html="activated_alert.html",
                email=partner_user.email,
                subject=_("[activated_account] Your account was activated!"),
                data={
                    "PARTNER_FULL_NAME": partner_full_name,
                    "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
                    "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,
                    "BETENLACE_LOGIN": settings.BETENLACE_LOGIN,
                    "COMPANY_URL": settings.COMPANY_URL,
                    "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
                    "GREETING": _("Hi"),
                    "LOGIN_MESSAGE": _("Login Inlazz"),
                    "CUSTOMER_MESSAGE": _("This account has been activated."),
                    "CUSTOMER_SERVICE_PART_1": _("Customer service"),
                    "CUSTOMER_SERVICE_PART_2": _("Monday to Friday"),
                    "CUSTOMER_SERVICE_PART_3": _("Colombia (UTC-5)"),
                    "CUSTOMER_SERVICE_PART_4": _("Saturdays"),
                    "CUSTOMER_SERVICE_PART_5": _("Colombia (UTC-5)"),
                    "FOOTER_MESSAGE_PART_1": _("Best regards,"),
                    "FOOTER_MESSAGE_PART_2": _("Inlazz team"),
                    "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
                    "CUSTOMER_SERVICE": _("Customer service."),
                    "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
                    "FOOTER_MESSAGE_PART_5": _("Access here"),
                }).start()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                {
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {"non_field_errors": ["".join(str(exc_value))]}
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates a partner account activation reason
        """
        validator = ValidatorFile(
            {
                "inactive_history_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "active_inactive_code_reason": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        admin = request.user
        validator.document["adviser_id"] = admin.id

        inactive_history = InactiveHistoryBasicSerializer().get_by_id(
            validator.document.get("inactive_history_id"), DB_USER_PARTNER)
        if not inactive_history:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"inactive_history_id": [_("There is not such Active/inactive reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        active_inactive_code_reason = validator.document.get("active_inactive_code_reason")
        active_inactive_code_reason = InactiveActiveCodeReasonSerializer().exist(active_inactive_code_reason, DB_USER_PARTNER)
        if not active_inactive_code_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"active_inactive_code_reason": [_("There is not such Active/inactive code reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if not active_inactive_code_reason.is_active_reason:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"active_inactive_code_reason": [_("You cannot use an inactive code when you are trying to activate")]}
            }, status=status.HTTP_400_BAD_REQUEST)

        serialized_inactive_history = InactiveHistoryBasicSerializer(
            instance=inactive_history, data=validator.document)
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        if serialized_inactive_history.is_valid():
            serialized_inactive_history.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_inactive_history.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets a partner account activation reason
        """
        validator = Validator(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner = Partner.objects.using(DB_USER_PARTNER).filter(
            user=validator.document.get("user_id"),
        ).first()

        if not partner:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        inactive_history = InactiveHistorySerializer().get_by_created_at_and_is_active_reason(
            validator.document.get("user_id"), DB_USER_PARTNER)
        if not inactive_history:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There are not Active/inactive history for that user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        serialized_inactive_history_basic = InactiveHistorySerializer(instance=inactive_history)
        return Response(data={"inactive_history": serialized_inactive_history_basic.data}, status=status.HTTP_200_OK)


class PartnerInactiveAPI(APIView):
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin deactivate a partner account
        """
        admin = request.user
        request_cfg.is_partner = True
        validator = ValidatorFile(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "active_inactive_code_reason": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        validator.document["adviser_id"] = admin.id
        partner = Partner.objects.using(DB_USER_PARTNER).select_related(
            "user"
        ).filter(
            user=validator.document.get("user_id"),
        ).first()
        if not partner:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if partner.user.is_active == False:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"user_id": [_("You are trying to inactivate a user that is already inactive")]}
            })

        validator.document["partner"] = partner.user_id
        active_inactive_code_reason = validator.document.get("active_inactive_code_reason")
        active_inactive_code_reason = InactiveActiveCodeReasonSerializer().exist(active_inactive_code_reason, DB_USER_PARTNER)
        if not active_inactive_code_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"active_inactive_code_reason": [_("There is not such active/inactive code reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if active_inactive_code_reason.is_active_reason:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"active_inactive_code_reason": [_("You cannot use an active code when you are trying to inactive")]}
            }, status=status.HTTP_400_BAD_REQUEST)

        serialized_inactive_history = InactiveHistorySerializer(data=validator.document)

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        if serialized_inactive_history.is_valid():
            serialized_inactive_history.create(database=DB_USER_PARTNER)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_inactive_history.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner_user = User.objects.db_manager(using=DB_USER_PARTNER).filter(id=partner.user_id).first()
        validator.document["is_active"] = False

        serialized_partner_user = UserBasicSerializer(instance=partner_user, data=validator.document)
        if serialized_partner_user.is_valid():
            serialized_partner_user.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_partner_user.errors
            })

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)

        # sending email
        partner_full_name = partner_user.first_name + " " + partner_user.second_name + " " + partner_user.last_name + " " + partner_user.second_last_name

        try:
            EmailThread(
                html="deactivated_alert.html",
                email=partner_user.email,
                subject=_("[inactivated_account] Your account was innactivated"),
                data={
                    "PARTNER_FULL_NAME": partner_full_name,
                    "TEMPLATE_HEADER_LOGO": settings.TEMPLATE_HEADER_LOGO,
                    "TEMPLATE_FOOTER_LOGO": settings.TEMPLATE_FOOTER_LOGO,
                    "BETENLACE_LOGIN": settings.BETENLACE_LOGIN,
                    "COMPANY_URL": settings.COMPANY_URL,
                    "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
                    "GREETING": _("Hi"),
                    "CUTOMER_MESSAGE": _("This account is not longer part of Betenlace."),
                    "CUSTOMER_SERVICE_PART_1": _("Customer service"),
                    "CUSTOMER_SERVICE_PART_2": _("Monday to Friday"),
                    "CUSTOMER_SERVICE_PART_3": _("Colombia (UTC-5)"),
                    "CUSTOMER_SERVICE_PART_4": _("Saturdays"),
                    "CUSTOMER_SERVICE_PART_5": _("Colombia (UTC-5)"),
                    "FOOTER_MESSAGE_PART_1": _("Best regards,"),
                    "FOOTER_MESSAGE_PART_2": _("Inlazz team"),
                    "FOOTER_MESSAGE_PART_3": _("*Do not reply this email. If you have any question contact our"),
                    "CUSTOMER_SERVICE": _("Customer service."),
                    "FOOTER_MESSAGE_PART_4": _("For more information about your account"),
                    "FOOTER_MESSAGE_PART_5": _("Access here"),
                }).start()
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                {
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {"non_field_errors": ["".join(str(exc_value))]}
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates a partner account deactivation reason
        """
        validator = ValidatorFile(
            {
                "inactive_history_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "active_inactive_code_reason": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        admin = request.user
        validator.document["adviser_id"] = admin.id

        inactive_history = InactiveHistoryBasicSerializer().get_by_id(
            validator.document.get("inactive_history_id"), DB_USER_PARTNER)
        if not inactive_history:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"inactive_history_id": [_("There is not such active/inactive reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        active_inactive_code_reason = validator.document.get("active_inactive_code_reason")
        active_inactive_code_reason = InactiveActiveCodeReasonSerializer().exist(active_inactive_code_reason, DB_USER_PARTNER)
        if not active_inactive_code_reason:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"active_inactive_code_reason": [_("There is not such active/inactive code reason in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        if active_inactive_code_reason.is_active_reason:
            return Response({
                "error": settings.ILOGICAL_ACTION,
                "details": {"active_inactive_code_reason": [_("You cannot use an active code when you are trying to inactive")]}
            }, status=status.HTTP_400_BAD_REQUEST)

        serialized_inactive_history = InactiveHistoryBasicSerializer(
            instance=inactive_history, data=validator.document)
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        if serialized_inactive_history.is_valid():
            serialized_inactive_history.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": serialized_inactive_history.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    def get(self, request):
        """
        Lets an admin gets a partner account deactivation reason
        """

        validator = Validator(
            {
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner = Partner.objects.using(DB_USER_PARTNER).filter(
            user=validator.document.get("user_id"),
        ).first()
        if not partner:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There is not such user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        inactive_history = InactiveHistorySerializer().get_by_create_at_and_is_not_active_reason(
            partner, DB_USER_PARTNER)

        if not inactive_history:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"user_id": [_("There are not active/inactive reasons for that user in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        serialized_ban_uninactive_history = InactiveHistorySerializer(instance=inactive_history)
        return Response(data={"inactive_history": serialized_ban_uninactive_history.data}, status=status.HTTP_200_OK)


class PartnerGeneralAdviserSearchAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
            Returning partners related to adviser
        """
        validator = Validator(
            schema={
                'adviser_id': {
                    'required': False,
                    'type': 'string',
                },
                'full_name': {
                    'required': False,
                    'type': 'string',
                },
                'email': {
                    'required': False,
                    'type': 'string',
                },
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q()

        if validator.document.get("adviser_id"):
            query &= Q(pk=request.query_params.get("adviser_id"))

        if validator.document.get("full_name"):
            query &= Q(full_name__icontains=request.query_params.get("full_name"))

        if validator.document.get("email"):
            query &= Q(email__icontains=request.query_params.get("email"))

        advisers = User.objects.annotate(
            full_name=Concat(
                "first_name",
                Value(" "),
                "second_name",
                Value(" "),
                "last_name",
                Value(" "),
                "second_last_name",
            ),
        ).using(DB_ADMIN).filter(query)[:5]

        member_report_ser = PartnersGeneralAdviserSearchSER(
            instance=advisers,
            many=True,
        )

        return Response(
            data={
                "count": advisers.count(),
                "advisers": member_report_ser.data
            },
            status=status.HTTP_200_OK,
        )
