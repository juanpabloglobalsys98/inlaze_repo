import logging

from api_admin.helpers import get_message_from_code_reason
from api_admin.models import (
    CodeReason,
    TranslateMessage,
)
from api_admin.serializers import (
    CreateCodeSER,
    CreateMsgSER,
    GetMsgSER,
    PartnerLanguageSER,
    PartnerMessageSER,
)
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models.authentication.partner import User
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    LanguagesCHO,
    StandardErrorHandler,
    request_cfg,
    to_int,
)
from core.helpers.cerberus_gen_coerce_functions import to_bool
from core.paginators import DefaultPAG
from django.conf import settings
from django.db.models import (
    F,
    Max,
    Q,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class CodeReasonFilterAPI(APIView, DefaultPAG):
    def post(self, request):
        """
        search a code reason with a filter works like a get
        """
        validator_query = Validator(
            schema={
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
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "created_at",
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
                    "required": True,
                    "type": "dict",
                    "default": {},
                    "schema": {
                        "pk": {
                            "required": False,
                            "type": "integer",
                            "coerce": to_int,
                        },
                        "code_int": {
                            "required": False,
                            "type": "integer",
                        },
                        "code": {
                            "required": False,
                            "type": "string",
                        },
                        "type_code": {
                            "required": False,
                            "type": "string",
                        },
                        "title__icontains": {
                            "required": False,
                            "type": "string",
                        },
                        "is_active": {
                            "type": "boolean"
                        },
                        "created_at": {
                            "required": False,
                            "type": "string",
                        },
                        "update_at": {
                            "required": False,
                            "type": "string",
                        },
                    }
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        order_by = validator_query.document.get("order_by")

        query = Q(**validator.document.get("filter"))

        code_message = CodeReason.objects.filter(query).values(
            "pk",
            "code",
            "type_code",
            "title",
            "is_active",
            "created_at",
            "updated_at",
        ).order_by(order_by)

        if not code_message:
            return Response(
                data={},
            )

        user_paginated = self.paginate_queryset(
            queryset=code_message,
            request=request,
            view=self,
        )

        code_ser = CreateCodeSER(
            instance=user_paginated,
            many=True,
            partial=True
        )

        return Response(
            data={
                "codes": code_ser.data,
            },
            headers={
                "count": self.count,
                "next": self.get_next_link(),
                "previous": self.get_previous_link(),
                "access-control-expose-headers": "count,next,previous"
            },
            status=status.HTTP_200_OK,
        )


class CodeReasonAPI(APIView, DefaultPAG):

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def post(self, request):
        """
        Create a code Reason, using a dict as data for the serializer, must create the dict
        """
        validator = Validator(
            schema={
                "title": {
                    "required": False,
                    "type": "string",
                },
                "type_code": {
                    "required": False,
                    "type": "string",
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_BODY,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not validator.document:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("not input data for post"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        data_code = {}

        type_code = validator.document.get("type_code")
        query = Q(type_code=type_code)

        current_number = CodeReason.objects.filter(query).values(
            "type_code").annotate(max_code=Max('code_int'))

        if current_number.exists():
            code_int = current_number[0]['max_code']
        else:
            code_int = 0

        number_code = str(code_int+1)

        """
        current number is the max number in code int, and this number is using to
        create de code that follow the next syntaxis, type+"-"+number_code
        example: ban_reason-1
        """

        code_name = type_code+"-"+number_code
        data_code["code"] = code_name
        data_code["code_int"] = code_int+1
        data_code["title"] = validator.document.get("title")
        data_code["type_code"] = validator.document.get("type_code")
        data_code["is_active"] = validator.document.get("is_active")

        """
        create a new dictionary with the necesary information, and then is used as data
        for the serializer
        """

        message_serializer = CreateCodeSER(
            data=data_code,
        )

        if message_serializer.is_valid():
            message_serializer.save()
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )

        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": CreateCodeSER.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

    def patch(self, request):
        """
        edit a Code reason previously created by the post method in this class
        """
        validator_query = Validator(
            schema={
                "pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
        )

        if not validator_query.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_PARAMS,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = Validator(
            schema={
                "title": {
                    "required": False,
                    "type": "string",
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                },
            },
        )

        """
        only can edit the title, and is active
        """

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_BODY,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not validator.document:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("not input data for patch"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        pk = validator_query.document.get("pk")
        query = Q(pk=pk)

        code = CodeReason.objects.filter(query).first()
        if code is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "id": [
                            _("Code reason doesnt exist"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )
        code_SER = CreateCodeSER(
            instance=code,
            data=validator.document,
            partial=True,
        )
        if code_SER.is_valid():
            code_SER.save()
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": code_SER.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )


class TranslateMessageAPI(APIView, DefaultPAG):
    """
    this view is a management for the messages
    """
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
        shows all the message that are already created.
        """
        validator = Validator(
            schema={
                "code_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "code_id",
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
            }
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        order_by = validator.document.get("order_by")

        query = Q(code_id=validator.document.get("code_id"))

        message_by_code = TranslateMessage.objects.filter(query).annotate(
            type_code=F("code__type_code"),
            title=F("code__title"),
            code_name=F("code__code"),
        ).order_by(order_by)

        if not message_by_code:
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )

        user_paginated = self.paginate_queryset(
            queryset=message_by_code,
            request=request,
            view=self,
        )

        message_ser = GetMsgSER(
            instance=user_paginated,
            many=True,
            partial=True
        )

        return Response(
            data={
                "admins": message_ser.data,
            },
            headers={
                "access-control-expose-headers": "count, next, previous",
                "count": self.count,
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request):
        """
        edit the content of the message, only the text
        """
        validator_query = Validator(
            schema={
                "pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
        )

        if not validator_query.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_PARAMS,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = Validator(
            schema={
                "message": {
                    "required": False,
                    "type": "string",
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool
                }
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_BODY,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not validator.document:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("not input data for patch"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        pk = validator_query.document.get("pk")
        query = Q(pk=pk)

        msg = TranslateMessage.objects.filter(query).first()
        if msg is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "id": [
                            _("message doesnt exist"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )
        msg_SER = CreateMsgSER(
            instance=msg,
            data=validator.document,
            partial=True,
        )
        if msg_SER.is_valid():
            msg_SER.save()
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": msg_SER.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

    def post(self, request):
        """
            Just create a new message, associated with code_id that is a FK with
            Code reasons
        """
        validator = Validator(
            schema={
                "language": {
                    "required": True,
                    "type": "string",
                    "allowed": LanguagesCHO.values,
                },
                "message": {
                    "required": True,
                    "type": "string",
                },
                "code_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "is_active": {
                    "required": True,
                    "type": "boolean",
                    "coerce": to_bool,
                },
            },
        )
        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_BODY,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not validator.document:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("not input data for post"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        language = validator.document.get("language")
        code_id = validator.document.get("code_id")
        message = validator.document.get("message")
        is_active = validator.document.get("is_active")

        query = Q(language=language, code_id=code_id)
        verify = TranslateMessage.objects.filter(query).first()

        if verify:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("already exist a message with this language in this code reason"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if len(language) > 3:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("not valid language for post"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        """
        verify if the language is valid with the standar in langaugesCHO(helper)
        """
        new_message = TranslateMessage.objects.create(
            language=language,
            code_id=code_id,
            message=message,
            is_active=is_active,
        )

        message_serializer = CreateMsgSER(
            instance=new_message,
            data=validator.document,
            partial=True
        )

        if message_serializer.is_valid():
            message_serializer.save()
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )

        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": message_serializer.errors,
                },
                status=status.HTTP_400_BAD_REQUEST
            )


class TranslateMessagePartnerAPI(APIView):
    """
    this point return the message, lenguage message, and partner's lenguage
    """


# add partner id to the filter.
    def post(self, request):
        validator = Validator(
            schema={
                "filter_code": {
                    "required": True,
                    "type": "dict",
                    "schema": {
                        "code": {
                            "required": True,
                            "type": "string",
                        },
                    }
                },
                "id": {
                    "required": True,
                    "type": "integer",
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q(**validator.document.get("filter_code"))
        code_message = CodeReason.objects.filter(query).first()
        query = Q(pk=validator.document.get("id"))
        language_partner = User.objects.using(DB_USER_PARTNER).filter(query).values(
            "language",
            "id",
        ).first()

        if not language_partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        message_partner = get_message_from_code_reason(code_message, language_partner['language'])
        if message_partner is None:
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

        message_ser = PartnerMessageSER(
            instance=message_partner,
            partial=True
        )

        partner_ser = PartnerLanguageSER(
            instance=language_partner,
            partial=True
        )

        return Response(
            data={
                "translate_message": message_ser.data,
                "partner": partner_ser.data,
            },

            status=status.HTTP_200_OK,
        )
