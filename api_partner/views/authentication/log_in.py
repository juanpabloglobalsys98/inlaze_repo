import logging

from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import (
    Partner,
    SocialChannel,
)
from api_partner.serializers import (
    PartnerStatusManagementSER,
    PartnerStatusSER,
)
from cerberus import Validator
from core.helpers import (
    StandardErrorHandler,
    to_lower,
)
from core.models import (
    Permission,
    User,
)
from django.conf import settings
from django.contrib.auth import authenticate
from django.contrib.auth.models import update_last_login
from django.db import transaction
from django.db.models import (
    F,
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class LogInAPI(APIView):

    @transaction.atomic(using=DB_USER_PARTNER)
    def post(self, request):
        """
        Let a user login to their account.
        """
        validator = Validator(
            schema={
                "email": {
                    "required": False,
                    "type": "string",
                    "coerce": to_lower,
                    "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
                },
                "phone": {
                    "required": False,
                    "type": "string",
                },
                "password": {
                    "required": True,
                    "type": "string",
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

        if "email" not in validator.document and "phone" not in validator.document:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Email or phone are necessary"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        query = Q()
        if "email" in validator.document:
            query &= Q(email=validator.document.get("email"))
        elif "phone" in validator.document:
            query &= Q(phone=validator.document.get("phone"))

        user = User.objects.using(DB_USER_PARTNER).filter(query).first()
        if user is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "user": [
                            _("Email does not exist"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        user = authenticate(
            username=user.email,
            password=request.data.get("password"),
        )
        if not user:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Invalid password"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        update_last_login(None, user)

        if not user.is_active:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.INACTIVE_USER_CODE,
                    "detail": {
                        "email": [
                            _("Your account is not activated, contact your adviser"),
                        ],
                    },
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        if user.is_banned:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.BANNED_USER_CODE,
                    "detail": {
                        "email": [
                            _("Your account is banned, contact your adviser"),
                        ],
                    },
                },
                status=status.HTTP_403_FORBIDDEN,
            )

        token = Token.objects.update_or_create(
            user=user,
            defaults={
                "user": user,
            },
        )[0]

        partner = user.partner
        partner_status = PartnerStatusSER(instance=partner)
        partner_country = None
        alert_to_upload_data = False

        if hasattr(partner, "additionalinfo"):
            partner_country = partner.additionalinfo.country

        if partner.status != Partner.Status.VALIDATED:
            query = Q(cpa_count__gt=0)
            cpa_count = partner.partnerlinkaccumulated_to_partner.filter(query).only("pk").first()
            alert_to_upload_data = cpa_count is not None

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)

        return Response(
            data={
                "partner_status": partner_status.data,
                "partner_full_name": user.get_full_name(),
                "partner_country": partner_country,
                "partner_level": partner.level,
                "partner_terms": partner.is_terms,
                "alert_to_upload_data": alert_to_upload_data,
                "token": token.key,
            },
            status=status.HTTP_200_OK,
        )


class LogInDetailsAPI(APIView):

    def get(self, request):
        validator_query = Validator(
            schema={
                "email": {
                    "required": True,
                    "type": "string",
                    "coerce": to_lower,
                    "regex": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
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

        email = validator_query.document.get("email")
        user = User.objects.filter(email=email).first()
        if user is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "email": [
                            _("Email does not exist"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        return Response(
            data={
                "email": self.mask_email(user.email),
                "phone": self.mask_phone(user.phone),
            },
            status=status.HTTP_200_OK,
        )

    def mask_email(self, email):
        email, domain = email.split("@")
        domain, tld = domain.split(".")
        return f"{email[0]}{'*' * (len(email) - 2)}{email[-1]}@{domain[0]}{'*' * (len(domain) - 1)}.{tld}"

    def mask_phone(self, phone):
        return f"{'*' * 8}{phone[-2:]}"
