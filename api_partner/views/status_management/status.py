import logging
from multiprocessing import context

from api_admin.views.admin import User
from api_partner.helpers import (
    DB_USER_PARTNER,
    IsNotBanned,
    IsTerms,
)
from api_partner.models import (
    Partner,
    SocialChannel,
)
from api_partner.serializers import (
    PartnerSerializer,
    PartnerStatusManagementSER,
    languageSER,
)
from cerberus import Validator
from core.helpers import to_bool
from django.conf import settings
from django.db.models import (
    F,
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class StatusPartnerAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        IsNotBanned,
        IsTerms,
    ]

    """
        class view for list all  and alerts about the partner in session.
    """

    def get(self, request):
        query = Q(user_id=request.user.id)
        partner_status = Partner.objects.annotate(
            email=F("user__email"),
            language=F("user__language"),
            full_name=Concat(
                "user__first_name",
                Value(" "),
                "user__second_name",
                Value(" "),
                "user__last_name",
                Value(" "),
                "user__second_last_name",
            )
        ).filter(query).only(
            "basic_info_status",
            "bank_status",
            "documents_status",
            "level_status",
            "level",
            "alerts",
            "is_email_valid",
        ).first()

        social = SocialChannel.objects.filter(partner_id=request.user.id).first()

        partner_status_ser = PartnerStatusManagementSER(
            instance=partner_status,
            context={
                "social": social
            }
        )

        return Response(
            data={
                "partner_status": partner_status_ser.data
            }, status=status.HTTP_200_OK,
        )

    def patch(self, request):
        validator = Validator(
            schema={
                "alerts": {
                    "required": True,
                    "type": "dict",
                    "schema": {
                        "level": {
                            "required": False,
                            "type": "boolean",
                            "coerce": to_bool,
                        },
                        "bank": {
                            "required": False,
                            "type": "boolean",
                            "coerce": to_bool,
                        },
                        "basic_info": {
                            "required": False,
                            "type": "boolean",
                            "coerce": to_bool,
                        },
                    },
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

        query = Q(user_id=request.user.id)
        partner_status = Partner.objects.filter(query).only("alerts").first()

        if partner_status is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "id": [
                            _("partner doesnt exist"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        alerts = validator.document.get("alerts")
        partner_status_ser = PartnerSerializer(
            instance=partner_status,
            data={
                "alerts": partner_status.alerts | alerts,
            },
            partial=True,
        )

        if partner_status_ser.is_valid():
            partner_status_ser.save()
            return Response(
                data={
                    "partner_status": partner_status_ser.data
                },
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": partner_status_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )


class LanguagePartnerAPI(APIView):
    permission_classes = [
        IsAuthenticated,
    ]

    def get(self, request):
        query = Q(id=request.user.pk)
        partner_status = User.objects.filter(query).values(
            "language",
        ).first()

        partner_status_ser = languageSER(
            instance=partner_status,
        )

        return Response(
            data={
                "partner_status": partner_status_ser.data
            }, status=status.HTTP_200_OK,
        )

    def patch(self, request):
        validator = Validator(
            schema={
                "language": {
                    "required": False,
                    "type": "string",
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

        query = Q(pk=request.user.pk)
        language_partner = User.objects.filter(query).only(
            "language"
        ).first()

        partner_ser = languageSER(
            instance=language_partner,
            data=validator.document,
        )

        if partner_ser.is_valid():
            partner_ser.save()
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )

        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": partner_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST
            )
