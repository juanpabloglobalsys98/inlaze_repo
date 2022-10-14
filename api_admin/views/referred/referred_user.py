import logging

from api_admin.helpers import DB_ADMIN
from api_admin.paginators import ReferredManagementPaginator
from api_admin.serializers import ReferredUserSER
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import Partner
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    request_cfg,
    to_float_0_null,
    to_int,
)
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


class ReferredManagementAPI(APIView, ReferredManagementPaginator):
    """
        Class to define structure to manage referred
    """

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):

        validator = Validator(
            schema={
                "pk": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "full_name": {
                    "required": False,
                    "type": "string",
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "pk",
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        query = Q()
        if "pk" in validator.document:
            query &= Q(pk=validator.document.get("pk"))

        if "full_name" in validator.document:
            query &= Q(full_name__icontains=validator.document.get("full_name"))

        if "email" in validator.document:
            query &= Q(email__icontains=validator.document.get("email"))

        order_by = validator.document.get("order_by")

        # Force default DB routes to Partner
        request_cfg.is_partner = True

        user = Partner.objects.using(DB_USER_PARTNER).annotate(
            full_name=Concat(
                "user__first_name",
                Value(" "),
                "user__second_name",
                Value(" "),
                "user__last_name",
                Value(" "),
                "user__second_last_name",
            ),
            email=F("user__email"),
        ).filter(query).order_by(order_by)

        user_paginated = self.paginate_queryset(
            queryset=user,
            request=request,
            view=self,
        )

        referred_user_ser = ReferredUserSER(
            instance=user_paginated,
            many=True,
            partial=True,
        )

        return Response(
            data={
                "admins": referred_user_ser.data,
            },
            headers={
                "access-control-expose-headers": "count, next, previous",
                "count": self.count,
            },
        )

    def patch(self, request):
        request_cfg.is_partner = True

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
                "referred_by_id": {
                    "required": False,
                    "type": "integer",
                    "nullable": True,
                },
                "fixed_income_referred_percentage": {
                    "required": False,
                    "type": "float",
                    "nullable": True,
                    "coerce": to_float_0_null,
                },
                "net_revenue_referred_percentage": {
                    "required": False,
                    "type": "float",
                    "nullable": True,
                    "coerce": to_float_0_null,
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
                            _("not input data for patch"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        pk = validator_query.document.get("pk")
        query = Q(pk=pk)

        partner = Partner.objects.filter(query).first()
        if partner is None:
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

        referred = partner.referred_by_id

        if referred is None and not validator.document.get("referred_by_id"):
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "pk": [
                            _("You cannot edit percentage with referred null"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )
        """
        validate if referred id exist for the assign moment
        """

        partner_SER = ReferredUserSER(
            instance=partner,
            data=validator.document,
            partial=True,
        )
        if partner_SER.is_valid():
            partner_SER.save()
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": partner_SER.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
