import logging

from api_admin.helpers import (
    DB_ADMIN,
    get_message_from_code_reason,
)
from api_admin.helpers.partner_accum_history import create_history
from api_admin.models import (
    CodeReason,
    LevelPercentageBase,
    PartnerLevelHistory,
    SearchPartnerLimit,
)
from api_admin.serializers import (
    PartnerLevelHistorySER,
    PartnerLevelVerifyCustomSER,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerLevelCHO,
    PartnerStatusCHO,
)
from api_partner.helpers.choices.partner_link_accum_status import PartnerAccumUpdateReasonCHO
from api_partner.models import (
    AdditionalInfo,
    DocumentPartner,
    FxPartner,
    Partner,
    PartnerBankAccount,
    PartnerBankValidationRequest,
    PartnerInfoValidationRequest,
    PartnerLevelRequest,
    PartnerLinkAccumulated,
    SocialChannel,
    WithdrawalPartnerMoney,
)
from api_partner.serializers import (
    DynamicPartnerSER,
    PartnerBankValidationRequestSER,
    PartnerBankValidationRequestUserSER,
    PartnerInfoValidationRequestREADSER,
    PartnerInfoValidationRequestUserSER,
    PartnerLevelRequestSER,
    PartnerLevelRequestUserSER,
    PartnerSerializer,
    SocialChannelRequestSER,
)
from cerberus import Validator
from core.helpers import (
    CountryAll,
    HavePermissionBasedView,
    StandardErrorHandler,
    get_codename,
    request_cfg,
    send_change_level_response_email,
    send_validation_response_email,
    to_bool,
    to_int,
    to_lower,
)
from core.models import User
from core.paginators import DefaultPAG
from django.conf import settings
from django.db import transaction
from django.db.models import (
    F,
    Q,
    Value,
    Prefetch,
)
from django.db.models.functions import Concat
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class PartnerRequestsAPI(APIView, DefaultPAG):
    """
    Filters any partner's level, basic info and bank requests.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def post(self, request):
        from api_admin.views import PartnersGeneralAPI

        validator_query = Validator(
            schema={
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "pk",
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator_query.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = Validator(
            schema={
                "filter": {
                    "required": False,
                    "type": "dict",
                    "schema": {
                        "pk": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                        },
                        "name__icontains": {
                            "required": False,
                            "type": "string",
                            "coerce": to_lower,
                        },
                        "email__icontains": {
                            "required": False,
                            "type": "string",
                            "coerce": to_lower,
                        },
                        "country": {
                            "required": False,
                            "type": "string",
                            "allowed": CountryAll.values,
                        },
                        "phone": {
                            "required": False,
                            "type": "string",
                        },
                        "level": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                            "allowed": PartnerLevelCHO.values,
                        },
                        "is_banned": {
                            "required": False,
                            "type": "boolean",
                            "coerce": to_bool,
                        },
                    },
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

        requested = PartnerStatusCHO.REQUESTED
        query = (
            Q(basic_info_status=requested)
            | Q(bank_status=requested)
            | Q(level_status=requested)
            | Q(secondary_bank_status=requested)
        )
        codename = get_codename(class_name=PartnersGeneralAPI)
        # Check if user is adviser or the partner itself, to limit the query
        if SearchPartnerLimit.has_limit(request.user, codename):
            query &= Q(partner__adviser_id=request.user.pk)

        if validator.document.get("filter") is not None:
            query &= Q(**validator.document.get("filter"))

        order_by = validator_query.document.get("order_by")
        request_cfg.is_partner = True

        basic_info = PartnerInfoValidationRequest.objects.only(
            "pk",
            "created_at",
            "partner_id",
        ).order_by(
            "partner_id",
            "-created_at",
        ).distinct(
            "partner_id",
        )

        bank_info = PartnerBankValidationRequest.objects.only(
            "pk",
            "created_at",
            "partner_id",
        ).order_by(
            "partner_id",
            "-created_at",
        ).distinct(
            "partner_id",
        )

        level_info = PartnerLevelRequest.objects.only(
            "pk",
            "created_at",
            "partner_id"
        ).order_by(
            "partner_id",
            "-created_at",
        ).distinct(
            "partner_id",
        )

        partners = Partner.objects.prefetch_related(
            Prefetch(
                lookup="validation_requests",
                queryset=basic_info,
                to_attr="last_basic_validation",
            ),
            Prefetch(
                lookup="bank_validation_requests",
                queryset=bank_info,
                to_attr="last_bank_validation",
            ),
            Prefetch(
                lookup="partnerlevelrequest_set",
                queryset=level_info,
                to_attr="last_level_validation",
            ),
        ).select_related(
            "user",
        ).annotate(
            name=Concat(
                F("user__first_name"),
                F("user__second_name"),
                F("user__last_name"),
                F("user__second_last_name"),
            ),
            id=F("user_id"),
            email=F("user__email"),
            phone=F("user__phone"),
            country=F("additionalinfo__country"),
            partner_level=F("level"),
            is_banned=F("user__is_banned"),
        ).filter(query).order_by(order_by)

        partners_pag = self.paginate_queryset(
            queryset=partners,
            request=request,
            view=self,
        )

        partners_ser = DynamicPartnerSER(
            instance=partners_pag,
            fields=(
                "id",
                "full_name",
                "email",
                "phone",
                "country",
                "status",
                "partner_level",
                "level_status",
                "basic_info_status",
                "bank_status",
                "secondary_bank_status",
                "is_banned",
                "is_active",
                "last_basic_validation",
                "last_bank_validation",
                "last_level_validation",
            ),
            many=True,
        )

        return Response(
            data={
                "partners": partners_ser.data,
            },
            headers={
                "count": self.count,
                "next": self.get_next_link(),
                "previous": self.get_previous_link(),
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )


class PartnerLevelRequestAPI(APIView, DefaultPAG):
    """
    Manage partner level requests.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Returns the last prime level request for a certain partner.
        """
        from api_admin.views import PartnersGeneralAPI

        validator = Validator(
            schema={
                "partner_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = (
            Q(partner_id=validator.document.get("partner_id"))
            & Q(status=PartnerStatusCHO.REQUESTED)
        )
        codename = get_codename(class_name=PartnersGeneralAPI)
        if SearchPartnerLimit.has_limit(request.user, codename):
            query &= Q(partner__adviser_id=request.user.pk)

        level_request = PartnerLevelRequest.objects.filter(query).order_by("created_at").last()

        if level_request is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Level request not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        level_request_ser = PartnerLevelRequestSER(
            instance=level_request,
        )
        return Response(
            data={
                "level_request": level_request_ser.data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        """
        Filters partner level requests.
        """
        from api_admin.views import PartnersGeneralAPI

        validator_query = Validator(
            schema={
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "-created_at",
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator_query.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = Validator(
            schema={
                "filter": {
                    "required": False,
                    "type": "dict",
                    "schema": {
                        "partner_id": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                        },
                        "name__icontains": {
                            "required": False,
                            "type": "string",
                            "coerce": to_lower,
                        },
                        "email__icontains": {
                            "required": False,
                            "type": "string",
                            "coerce": to_lower,
                        },
                        "country": {
                            "required": False,
                            "type": "string",
                            "allowed": CountryAll.values,
                        },
                        "phone": {
                            "required": False,
                            "type": "string",
                        },
                        "level": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                            "allowed": PartnerLevelCHO.values,
                        },
                        "status": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                            "allowed": PartnerStatusCHO.values,
                        },
                        "is_banned": {
                            "required": False,
                            "type": "boolean",
                            "coerce": to_bool,
                        },
                    },
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

        query = Q()
        codename = get_codename(class_name=PartnersGeneralAPI)
        # Check if user is adviser or the partner itself, to limit the query
        if SearchPartnerLimit.has_limit(request.user, codename):
            query &= Q(partner__adviser_id=request.user.pk)

        if validator.document.get("filter") is not None:
            query &= Q(**validator.document.get("filter"))

        order_by = validator_query.document.get("order_by")
        request_cfg.is_partner = True
        partner_level_requests = PartnerLevelRequest.objects.annotate(
            name=Concat(
                F("partner__user__first_name"),
                F("partner__user__second_name"),
                F("partner__user__last_name"),
                F("partner__user__second_last_name"),
            ),
            email=F("partner__user__email"),
            phone=F("partner__user__phone"),
            country=F("partner__additionalinfo__country"),
            partner_level=F("partner__level"),
            is_banned=F("partner__user__is_banned"),
            is_active=F("partner__user__is_active"),
        ).filter(query).order_by(order_by)

        admin = request.user

        partner_level_requests_pag = self.paginate_queryset(
            queryset=partner_level_requests,
            request=request,
            view=self,
        )

        partner_level_requests_ser = PartnerLevelRequestUserSER(
            instance=partner_level_requests_pag,
            many=True,
            context={
                "admin": admin,
            },
        )

        return Response(
            data={
                "requests": partner_level_requests_ser.data,
            },
            headers={
                "count": self.count,
                "next": self.get_next_link(),
                "previous": self.get_previous_link(),
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request):
        """
        Accept or reject a single partner level request.
        """
        validator = Validator(
            schema={
                "level_request_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "status": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerStatusCHO.allowed_status_request(),
                },
                "code_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "partner_accum": {
                    "type": "list",
                    "required": True,
                    "schema": {
                        "type": "dict",
                        "schema": {
                            "pk": {
                                "required": True,
                                "type": "integer",
                            },
                            "percentage_cpa": {
                                "required": True,
                                "type": "float",
                            },
                        },
                    },
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

        request_pk = validator.document.get("level_request_pk")
        new_status = validator.document.get("status")
        code_pk = validator.document.get("code_pk")
        type_code = None
        if new_status == PartnerStatusCHO.ACCEPTED:
            type_code = CodeReason.Type.PARTNER_LVL_ACCEPT
        elif new_status == PartnerStatusCHO.REJECTED:
            type_code = CodeReason.Type.PARTNER_LVL_REJECT

        code_reason = CodeReason.objects.filter(
            pk=code_pk,
            type_code=type_code,
        ).first()
        if code_reason is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "code_pk": [
                            _("Code reason not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_level_request = PartnerLevelRequest.objects.filter(pk=request_pk).first()
        if partner_level_request is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "level_request_pk": [
                            _("Partner level request not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif partner_level_request.status != PartnerStatusCHO.REQUESTED:
            msg = _("Level request status is not {}")
            msg = msg.format(PartnerStatusCHO.REQUESTED.label)
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "status": [
                            msg,
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif partner_level_request.level == partner_level_request.partner.level:
            # Partner has same the level as the one requested, reject their request.
            partner_level_request.partner.level_status = PartnerStatusCHO.REJECTED
            partner_level_request.partner.alerts["level"] = True
            partner_level_request_ser = PartnerLevelRequestSER(
                instance=partner_level_request,
                data={
                    "status": PartnerStatusCHO.REJECTED,
                    "answered_at": timezone.now(),
                },
                partial=True,
            )
            if not partner_level_request_ser.is_valid():
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "detail": partner_level_request_ser.errors,
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            partner_level_request_ser.save()
            msg = _("Partner level is already {} - {}")
            msg = msg.format(
                partner_level_request.level,
                PartnerLevelCHO(partner_level_request.level).label,
            )
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "level": [
                            msg,
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = partner_level_request.partner
        request_cfg.is_partner = True
        message = get_message_from_code_reason(code_reason, partner.user.language)
        if message is None:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("No message found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        level_history_ser = accumulated_links = None
        channels_to_create = []
        channels_to_update = []
        if new_status == PartnerStatusCHO.ACCEPTED:
            partner.alerts["level"] = True
            accumulated_links = partner.partnerlinkaccumulated_to_partner.all()
            custom_accumulated_links = accumulated_links.filter(
                is_percentage_custom=True,
            )

            previous_level = partner.level
            partner_accum = validator.document.get("partner_accum")
            custom_accumulated_links_pks = set(custom_accumulated_links.values_list("pk", flat=True))
            partner_accum_pks = {d.get("pk") for d in partner_accum}
            # Check that the sets of PKs (from request and partner) don't differ
            if custom_accumulated_links_pks != partner_accum_pks:
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "detail": {
                            "partner_accum": [
                                _("Custom accumulated links differ from the ones provided"),
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            level_p = LevelPercentageBase.objects.order_by("created_at").last()
            if level_p is None:
                msg = _("Level percentage is not defined")
                logger.critical(msg)
                return Response(
                    data={
                        "error": settings.INTERNAL_SERVER_ERROR,
                        "detail": {
                            "non_field_errors": [
                                msg,
                            ],
                        }
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )
            level_percentage = level_p.percentages.get(str(partner_level_request.level))
            for accumulated_links_i in accumulated_links:
                create_history(
                    instance=accumulated_links_i,
                    update_reason=PartnerAccumUpdateReasonCHO.ADVISER_CHANGE_PARTNER_LEVEL,
                    adviser=request.user.id,
                )

            for link in accumulated_links:
                campaign_percentage = link.campaign.default_percentage
                if link.is_percentage_custom:
                    # Get a new percentage if it was provided, will set it to the default value otherwise
                    new_cpa = next(d.get("percentage_cpa") for d in partner_accum if d.get("pk") == link.pk)
                    if new_cpa == 0 or new_cpa == campaign_percentage * level_percentage:
                        link.percentage_cpa = campaign_percentage * (level_percentage)
                        link.is_percentage_custom = False
                    else:
                        link.percentage_cpa = campaign_percentage * (new_cpa)
                        link.is_percentage_custom = True
                else:
                    link.percentage_cpa = campaign_percentage * level_percentage
                link.partner_level = partner_level_request.level

            # Accept channel requests and update channels if they already exist
            channel_requests = partner_level_request.channels.all()
            current_channels = partner.channels.all()
            for channel in channel_requests:
                # Get a partner channel by its url, update it if it's found, else create a new one
                ch_to_update = next(
                    (ch for ch in current_channels if ch.url == channel.url),
                    None,
                )
                if ch_to_update:
                    ch_to_update.name = channel.name
                    ch_to_update.type_channel = channel.type
                    ch_to_update.deleted_at = None
                    channels_to_update.append(ch_to_update)
                else:
                    channels_to_create.append(
                        SocialChannel(
                            partner=partner,
                            name=channel.name,
                            url=channel.url,
                            type_channel=channel.type,
                        ),
                    )

            new_level = PartnerLevelCHO.PRIME
            partner.alerts["level"] = True
            partner.level_status = PartnerStatusCHO.ACCEPTED
            data = {
                "level": new_level,
            }

            # Create level change record
            level_history_ser = PartnerLevelHistorySER(
                data={
                    "partner_id": partner.pk,
                    "admin": request.user,
                    "previous_level": previous_level,
                    "new_level": new_level,
                },
            )
            if not level_history_ser.is_valid():
                return Response(
                    data={
                        "error": settings.SERIALIZER_ERROR_CODE,
                        "detail": level_history_ser.errors,
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )
        elif new_status == PartnerStatusCHO.REJECTED:
            partner.level_status = PartnerStatusCHO.REJECTED
            partner.alerts["level"] = True
            data = {
                "last_rejected_level_at": timezone.now(),
            }
        else:
            logger.critical(f"{new_level} is not a valid level")
            return Response(status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        partner_ser = PartnerSerializer(
            instance=partner,
            data=data,
            partial=True,
        )
        if not partner_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        partner_level_request_ser = PartnerLevelRequestSER(
            instance=partner_level_request,
            data={
                "status": new_status,
                "answered_at": timezone.now(),
            },
            partial=True,
        )
        if not partner_level_request_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_level_request_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic(using=DB_USER_PARTNER):
            with transaction.atomic(using=DB_ADMIN):
                partner_level_request_ser.save()
                partner_ser.save()
                if accumulated_links:
                    PartnerLinkAccumulated.objects.bulk_update(
                        objs=accumulated_links,
                        fields=(
                            "percentage_cpa",
                            "partner_level",
                            "is_percentage_custom",
                        ),
                    )
                if level_history_ser:
                    SocialChannel.objects.bulk_create(
                        objs=channels_to_create,
                    )
                    SocialChannel.objects.bulk_update(
                        objs=channels_to_update,
                        fields=(
                            "name",
                            "type_channel",
                            "deleted_at",
                        ),
                    )
                    level_history_ser.save()

        send_validation_response_email(
            user=partner.user,
            new_status=new_status,
            request_type="level",
            message=message.message,
            request=request,
        )
        return Response(status=status.HTTP_204_NO_CONTENT)


class VerifyCustomPercentageAPI(APIView):
    """
    Checks if partner has custom percentage before changing their level.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        validator = Validator(
            schema={
                "pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "level": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerLevelCHO.values,
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        pk = validator.document.get("pk")
        partner = Partner.objects.filter(pk=pk).first()
        if partner is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "pk": [
                            _("Partner not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        level_percentage = LevelPercentageBase.objects.order_by("created_at").last()
        if level_percentage is None:
            msg = _("Level percentage is not defined")
            logger.critical(msg)
            return Response(
                data={
                    "error": settings.INTERNAL_SERVER_ERROR,
                    "detail": {
                        "non_field_errors": [
                            msg,
                        ],
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        current_level = str(partner.level)
        new_level = str(validator.document.get("level"))
        current_level_percentage = level_percentage.percentages.get(current_level)
        new_level_percentage = level_percentage.percentages.get(new_level)

        if not current_level_percentage or not new_level_percentage:
            msg = (
                f"Level percentage is missing some values, or has invalid values: {level_percentage.percentages}, "
                f"Partner: {partner}, current level: {current_level}, new level: {new_level}"
            )
            logger.critical(msg)
            return Response(
                data={
                    "error": settings.INTERNAL_SERVER_ERROR,
                    "detail": {
                        "non_field_errors": [
                            msg,
                        ],
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        accumulated_links = partner.partnerlinkaccumulated_to_partner.filter(
            is_assigned=True,
            is_percentage_custom=True,
        ).only("id", "percentage_cpa").annotate(
            campaign_title=Concat(
                "campaign__bookmaker__name",
                Value(" "),
                "campaign__title",
            ),
            current_default_percentage=(
                F("campaign__default_percentage") * current_level_percentage
            ),
            new_default_percentage=(
                F("campaign__default_percentage") * new_level_percentage
            ),
            campaign_fixed_income_unitary=F("campaign__fixed_income_unitary"),
            campaign_currency_fixed_income=F("campaign__currency_fixed_income"),
        )

        fx_partner = FxPartner.objects.all().order_by("-created_at").first()

        accumulated_links_ser = PartnerLevelVerifyCustomSER(
            accumulated_links,
            many=True,
            context={
                "fx_partner": fx_partner,
            },
        )

        return Response(
            data={
                "mod_percentages_by_accum": accumulated_links_ser.data,
            },
            status=status.HTTP_200_OK,
        )


class SocialChannelRequestAPI(APIView):
    """
    Lists the social channel requests for a single partner level request id.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        validator = Validator(
            schema={
                "level_request_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        level_request_pk = validator.document.get("level_request_pk")
        level_request = PartnerLevelRequest.objects.filter(pk=level_request_pk).first()
        if level_request is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "pk": [
                            _("Partner level request not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        social_channel_ser = SocialChannelRequestSER(
            instance=level_request.channels.all(),
            many=True,
        )

        return Response(
            data={
                "channels": social_channel_ser.data,
            },
            status=status.HTTP_200_OK,
        )


class ChangePartnerLevelAPI(APIView):
    """
    Allow admins to change a partner's level.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def patch(self, request):
        """
        Change partner level.
        """
        validator = Validator(
            schema={
                "partner_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "level": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerLevelCHO.values,
                },
                "code_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "partner_accum": {
                    "type": "list",
                    "required": True,
                    "schema": {
                        "type": "dict",
                        "schema": {
                            "pk": {
                                "required": True,
                                "type": "integer",
                            },
                            "percentage_cpa": {
                                "required": True,
                                "type": "float",
                                "nullable": True,
                            },
                        },
                    },
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

        partner_pk = validator.document.get("partner_pk")
        new_level = validator.document.get("level")
        code_pk = validator.document.get("code_pk")

        code_reason = CodeReason.objects.filter(
            pk=code_pk,
            type_code=CodeReason.Type.PARTNER_LVL_CHANGE,
        ).first()
        if code_reason is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "code_pk": [
                            _("Code reason not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner: Partner = Partner.objects.filter(pk=partner_pk).first()

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
        elif partner.level == new_level:
            msg = _("Partner level is already {} - {}")
            msg = msg.format(PartnerLevelCHO(partner.level).label, partner.level)
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "level": [
                            msg,
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        request_cfg.is_partner = True
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
        previous_level = partner.level
        accumulated_links = partner.partnerlinkaccumulated_to_partner.all()
        custom_accumulated_links = accumulated_links.filter(
            is_percentage_custom=True,
        )
        for accumulated_links_i in accumulated_links:
            create_history(
                instance=accumulated_links_i,
                update_reason=PartnerAccumUpdateReasonCHO.ADVISER_CHANGE_PARTNER_LEVEL,
                adviser=request.user.id,
            )
        # Update partner accumulated links
        partner_accum = validator.document.get("partner_accum")
        custom_accumulated_links_pks = set(custom_accumulated_links.values_list("pk", flat=True))
        partner_accum_pks = {d.get("pk") for d in partner_accum}
        if custom_accumulated_links_pks != partner_accum_pks:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "partner_accum": [
                            _("Custom accumulated links differ from the ones provided")
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        level_p = LevelPercentageBase.objects.order_by("created_at").last()
        if level_p is None:
            msg = _("Level percentage is not defined")
            logger.critical(msg)
            return Response(
                data={
                    "error": settings.INTERNAL_SERVER_ERROR,
                    "detail": {
                        "non_field_errors": [
                            msg,
                        ],
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        level_percentage = level_p.percentages.get(str(new_level))

        for link in accumulated_links:
            campaign_percentage = link.campaign.default_percentage
            if link.is_percentage_custom:
                new_cpa = next(d.get("percentage_cpa") for d in partner_accum if d.get("pk") == link.pk)
                if new_cpa == 0 or new_cpa == campaign_percentage * level_percentage:
                    link.percentage_cpa = campaign_percentage * (level_percentage)
                    link.is_percentage_custom = False
                else:
                    link.percentage_cpa = campaign_percentage * (new_cpa)
                    link.is_percentage_custom = True
            else:
                link.percentage_cpa = campaign_percentage * level_percentage
            link.partner_level = new_level

        # Soft delete the partner's channels
        current_channels = []
        if new_level == PartnerLevelCHO.BASIC:
            partner.alerts["level"] = True
            partner.level_status = PartnerStatusCHO.REJECTED
            current_channels = partner.channels.all()
            now = timezone.now()
            for channel in current_channels:
                channel.deleted_at = now

        partner_ser = PartnerSerializer(
            instance=partner,
            data={
                "level": new_level,
            },
            partial=True,
        )
        if not partner_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Create level change record
        level_history_ser = PartnerLevelHistorySER(
            data={
                "partner_id": partner_ser.instance.pk,
                "admin": request.user,
                "previous_level": previous_level,
                "new_level": new_level,
                "changed_by": PartnerLevelHistory.ChangedBy.ADMIN,
            },
        )
        if not level_history_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": level_history_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic(using=DB_USER_PARTNER):
            with transaction.atomic(using=DB_ADMIN):
                partner_ser.save()
                level_history_ser.save()
                SocialChannel.objects.bulk_update(
                    objs=current_channels,
                    fields=(
                        "deleted_at",
                    ),
                )
                PartnerLinkAccumulated.objects.bulk_update(
                    objs=accumulated_links,
                    fields=(
                        "percentage_cpa",
                        "partner_level",
                        "is_percentage_custom",
                    ),
                )

        send_change_level_response_email(
            user=partner.user,
            new_level=new_level,
            message=message.message,
            request=request,
        )
        return Response(status=status.HTTP_204_NO_CONTENT)


class BasicInfoValidationAPI(APIView, DefaultPAG):
    """
    Manage partner basic info validation requests.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Returns the last requested info validation for a certain partner.
        """
        from api_admin.views import PartnersGeneralAPI

        validator = Validator(
            schema={
                "partner_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q(partner_id=validator.document.get("partner_id")) & Q(status=PartnerStatusCHO.REQUESTED)
        codename = get_codename(class_name=PartnersGeneralAPI)
        if SearchPartnerLimit.has_limit(request.user, codename):
            query &= Q(partner__adviser_id=request.user.pk)

        info_request = PartnerInfoValidationRequest.objects.filter(query).order_by("created_at").last()

        if info_request is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Partner info validation request not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        info_request_ser = PartnerInfoValidationRequestREADSER(
            instance=info_request,
        )
        return Response(
            data={
                "info_request": info_request_ser.data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        """
        Filters partner basic info validation requests.
        """
        from api_admin.views import PartnersGeneralAPI

        validator_query = Validator(
            schema={
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "-created_at",
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator_query.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = Validator(
            schema={
                "filter": {
                    "required": False,
                    "type": "dict",
                    "schema": {
                        "partner_id": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                        },
                        "name__icontains": {
                            "required": False,
                            "type": "string",
                            "coerce": to_lower,
                        },
                        "email__icontains": {
                            "required": False,
                            "type": "string",
                            "coerce": to_lower,
                        },
                        "country": {
                            "required": False,
                            "type": "string",
                            "allowed": CountryAll.values,
                        },
                        "phone": {
                            "required": False,
                            "type": "string",
                        },
                        "partner__level": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                            "allowed": PartnerLevelCHO.values,
                        },
                        "status": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                            "allowed": PartnerStatusCHO.values,
                        },
                        "is_banned": {
                            "required": False,
                            "type": "boolean",
                            "coerce": to_bool,
                        },
                    },
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

        query = Q()
        codename = get_codename(class_name=PartnersGeneralAPI)
        if SearchPartnerLimit.has_limit(request.user, codename):
            query &= Q(partner__adviser_id=request.user.pk)

        if validator.document.get("filter") is not None:
            query &= Q(**validator.document.get("filter"))

        order_by = validator_query.document.get("order_by")
        request_cfg.is_partner = True
        partner_info_requests = PartnerInfoValidationRequest.objects.annotate(
            name=Concat(
                F("partner__user__first_name"),
                F("partner__user__second_name"),
                F("partner__user__last_name"),
                F("partner__user__second_last_name"),
            ),
            email=F("partner__user__email"),
            phone=F("partner__user__phone"),
            country=F("partner__additionalinfo__country"),
            partner_level=F("partner__level"),
            is_banned=F("partner__user__is_banned"),
            is_active=F("partner__user__is_active"),
        ).filter(query).order_by(order_by)

        partner_info_requests_pag = self.paginate_queryset(
            queryset=partner_info_requests,
            request=request,
            view=self,
        )

        partner_info_requests_ser = PartnerInfoValidationRequestUserSER(
            instance=partner_info_requests_pag,
            many=True,
        )

        return Response(
            data={
                "requests": partner_info_requests_ser.data,
            },
            headers={
                "count": self.count,
                "next": self.get_next_link(),
                "previous": self.get_previous_link(),
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request):
        """
        Accept or reject a single partner info validation request.
        """
        validator = Validator(
            schema={
                "info_request_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "status": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerStatusCHO.allowed_status_request(),
                },
                "error_fields": {
                    "required": False,
                    "type": "list",
                    "allowed": PartnerInfoValidationRequest.ErrorField.values,
                },
                "code_pk": {
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

        request_pk = validator.document.get("info_request_pk")
        new_status = validator.document.get("status")
        error_fields = validator.document.get("error_fields", "")
        # Check that error fields was provided if the partner request is rejected
        if (is_reject := new_status == PartnerStatusCHO.REJECTED) and not error_fields:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "error_fields": [
                            _("Required field"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        code_pk = validator.document.get("code_pk")
        type_code = None
        if new_status == PartnerStatusCHO.ACCEPTED:
            type_code = CodeReason.Type.PARTNER_BASIC_INFO_REQUEST_ACCEPT
        elif new_status == PartnerStatusCHO.REJECTED:
            type_code = CodeReason.Type.PARTNER_BASIC_INFO_REQUEST_REJECT

        code_reason = CodeReason.objects.filter(
            pk=code_pk,
            type_code=type_code,
        ).first()
        if code_reason is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "code_pk": [
                            _("Code reason not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_info_request: PartnerInfoValidationRequest = PartnerInfoValidationRequest.objects.using(
            DB_USER_PARTNER,
        ).filter(pk=request_pk,).first()
        if partner_info_request is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "info_request_pk": [
                            _("Partner info validation request not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif partner_info_request.status != PartnerStatusCHO.REQUESTED:
            msg = _("Info validation request status is not {}")
            msg = msg.format(PartnerStatusCHO.REQUESTED.label)
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "status": [
                            msg,
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner: Partner = partner_info_request.partner
        request_cfg.is_partner = True
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

        user = info = docs = None
        if new_status == PartnerStatusCHO.ACCEPTED:
            partner.basic_info_status = PartnerStatusCHO.ACCEPTED
            partner.alerts["basic_info"] = True

            user: User = partner.user
            user.first_name = partner_info_request.first_name
            user.second_name = partner_info_request.second_name
            user.last_name = partner_info_request.last_name
            user.second_last_name = partner_info_request.second_last_name

            if not hasattr(partner, "additionalinfo"):
                partner.additionalinfo = AdditionalInfo(partner=partner)

            info: AdditionalInfo = partner.additionalinfo
            info.country = partner_info_request.current_country
            info.identification_type = partner_info_request.id_type
            info.identification = partner_info_request.id_number

            query = Q(identification_type=info.identification_type, identification=info.identification)
            if AdditionalInfo.objects.filter(query).exclude(partner=partner).exists():
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

            if not hasattr(partner, "documents_partner"):
                partner.documents_partner = DocumentPartner(partner=partner)

            docs: DocumentPartner = partner.documents_partner
            docs.copy_document_files(partner_info_request)

        elif new_status == PartnerStatusCHO.REJECTED:
            partner.basic_info_status = PartnerStatusCHO.REJECTED
            partner.alerts["basic_info"] = True

        partner_info_request_ser = PartnerInfoValidationRequestUserSER(
            instance=partner_info_request,
            data={
                "adviser_id": request.user.id,
                "status": new_status,
                "answered_at": timezone.now(),
                "error_fields": str(error_fields) if is_reject else "[]",
                "code_id": code_pk,
            },
            partial=True,
        )
        if not partner_info_request_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_info_request_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic(using=DB_USER_PARTNER):
            partner_info_request_ser.save()
            partner.save()
            if user and info and docs:
                user.save()
                info.save()
                docs.save()

        send_validation_response_email(
            user=partner.user,
            new_status=new_status,
            request_type="basic",
            message=message.message,
            request=request,
        )
        return Response(status=status.HTTP_204_NO_CONTENT)


class BankInfoValidationAPI(APIView, DefaultPAG):
    """
    Manage partner bank info validation requests.
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Returns the last requested bank info validation for a certain partner.
        """
        from api_admin.views import PartnersGeneralAPI

        validator = Validator(
            schema={
                "partner_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q(partner_id=validator.document.get("partner_id")) & Q(status=PartnerStatusCHO.REQUESTED)
        codename = get_codename(class_name=PartnersGeneralAPI)
        if SearchPartnerLimit.has_limit(request.user, codename):
            query &= Q(partner__adviser_id=request.user.pk)

        bank_request = PartnerBankValidationRequest.objects.filter(query).order_by("created_at").last()

        if bank_request is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Partner bank validation request not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        bank_request_ser = PartnerBankValidationRequestSER(
            instance=bank_request,
        )
        return Response(
            data={
                "bank_request": bank_request_ser.data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        """
        Filters partner bank validation requests.
        """
        from api_admin.views import PartnersGeneralAPI

        validator_query = Validator(
            schema={
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "-created_at",
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator_query.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = Validator(
            schema={
                "filter": {
                    "required": False,
                    "type": "dict",
                    "schema": {
                        "partner_id": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                        },
                        "name__icontains": {
                            "required": False,
                            "type": "string",
                            "coerce": to_lower,
                        },
                        "email__icontains": {
                            "required": False,
                            "type": "string",
                            "coerce": to_lower,
                        },
                        "country": {
                            "required": False,
                            "type": "string",
                            "allowed": CountryAll.values,
                        },
                        "phone": {
                            "required": False,
                            "type": "string",
                        },
                        "partner__level": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                            "allowed": PartnerLevelCHO.values,
                        },
                        "status": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                            "allowed": PartnerStatusCHO.values,
                        },
                        "is_banned": {
                            "required": False,
                            "type": "boolean",
                            "coerce": to_bool,
                        }
                    },
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

        query = Q()
        codename = get_codename(class_name=PartnersGeneralAPI)
        if SearchPartnerLimit.has_limit(request.user, codename):
            query &= Q(partner__adviser_id=request.user.pk)

        if validator.document.get("filter") is not None:
            query &= Q(**validator.document.get("filter"))

        order_by = validator_query.document.get("order_by")
        request_cfg.is_partner = True
        partner_bank_requests = PartnerBankValidationRequest.objects.annotate(
            name=Concat(
                F("partner__user__first_name"),
                F("partner__user__second_name"),
                F("partner__user__last_name"),
                F("partner__user__second_last_name"),
            ),
            email=F("partner__user__email"),
            phone=F("partner__user__phone"),
            country=F("partner__additionalinfo__country"),
            partner_level=F("partner__level"),
            is_banned=F("partner__user__is_banned"),
            is_active=F("partner__user__is_active"),
        ).filter(query).order_by(order_by)

        partner_bank_requests_pag = self.paginate_queryset(
            queryset=partner_bank_requests,
            request=request,
            view=self,
        )

        partner_bank_requests_ser = PartnerBankValidationRequestUserSER(
            instance=partner_bank_requests_pag,
            many=True,
        )

        return Response(
            data={
                "requests": partner_bank_requests_ser.data,
            },
            headers={
                "count": self.count,
                "next": self.get_next_link(),
                "previous": self.get_previous_link(),
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request):
        """
        Accept or reject a single partner bank info validation request.
        """
        validator = Validator(
            schema={
                "bank_request_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "status": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerStatusCHO.allowed_status_request(),
                },
                "error_fields": {
                    "required": False,
                    "type": "list",
                    "allowed": PartnerBankValidationRequest.ErrorField.values,
                },
                "code_pk": {
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

        request_pk = validator.document.get("bank_request_pk")
        new_status = validator.document.get("status")
        error_fields = validator.document.get("error_fields", "")

        code_pk = validator.document.get("code_pk")
        type_code = None
        if new_status == PartnerStatusCHO.ACCEPTED:
            type_code = CodeReason.Type.PARTNER_BILLING_INFO_REQUEST_ACCEPT
        elif new_status == PartnerStatusCHO.REJECTED:
            type_code = CodeReason.Type.PARTNER_BILLING_INFO_REQUEST_REJECT

        code_reason = CodeReason.objects.filter(
            pk=code_pk,
            type_code=type_code,
        ).first()
        if code_reason is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "code_pk": [
                            _("Code reason not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_bank_request: PartnerBankValidationRequest = PartnerBankValidationRequest.objects.using(
            DB_USER_PARTNER,
        ).filter(pk=request_pk,).first()
        if partner_bank_request is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "bank_request_pk": [
                            _("Partner info validation request not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif partner_bank_request.status != PartnerStatusCHO.REQUESTED:
            msg = _("Bank validation request status is not {}")
            msg = msg.format(PartnerStatusCHO.REQUESTED.label)
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "status": [
                            msg,
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner: Partner = partner_bank_request.partner
        request_cfg.is_partner = True
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

        bank_account = None
        has_primary = partner.bank_accounts.filter(is_primary=True).exists()
        if new_status == PartnerStatusCHO.ACCEPTED:
            # Check if having an additional bank account would go over the current accounts limit
            if partner.bank_accounts.count() + 1 > settings.BANK_ACCOUNTS_LIMIT:
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

            # Take bank validation data and create a new bank account
            request_cfg.is_partner = True
            bank_account: PartnerBankAccount = PartnerBankAccount.objects.create(
                partner=partner,
                billing_country=partner_bank_request.billing_country,
                billing_city=partner_bank_request.billing_city,
                billing_address=partner_bank_request.billing_address,
                bank_name=partner_bank_request.bank_name,
                account_type=partner_bank_request.account_type,
                account_number=partner_bank_request.account_number,
                swift_code=partner_bank_request.swift_code,
                is_primary=not has_primary,
                is_company=partner_bank_request.is_company,
                company_name=partner_bank_request.company_name,
                company_reg_number=partner_bank_request.company_reg_number,
            )

        if not has_primary:
            partner.bank_status = new_status
            partner.alerts["bank"] = True
        else:
            # Modify secondary account status if partner already has a primary account
            partner.secondary_bank_status = new_status
            partner.alerts["secondary_bank"] = True

        partner_bank_request_ser = PartnerBankValidationRequestUserSER(
            instance=partner_bank_request,
            data={
                "adviser_id": request.user.id,
                "status": new_status,
                "answered_at": timezone.now(),
                "error_fields": "[]",
                "code_id": code_pk,
            },
            partial=True,
        )
        if not partner_bank_request_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_bank_request_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        withdrawals = []
        if bank_account and bank_account.is_primary:
            # Update withdrawals that have a null bank
            query = Q(partner=partner) & Q(bank_account=None)
            withdrawals = WithdrawalPartnerMoney.objects.filter(query)
            for withdrawal in withdrawals:
                withdrawal.bank_account = bank_account

        with transaction.atomic(using=DB_USER_PARTNER):
            partner_bank_request_ser.save()
            partner.save()
            if bank_account:
                bank_account.save()
                WithdrawalPartnerMoney.objects.bulk_update(
                    objs=withdrawals,
                    fields=(
                        "bank_account",
                    ),
                )

        send_validation_response_email(
            user=partner.user,
            new_status=new_status,
            request_type="bank",
            message=message.message,
            request=request,
        )
        return Response(status=status.HTTP_204_NO_CONTENT)
