import logging

from api_admin.helpers import (
    DB_ADMIN,
    DefaultPAG,
    create_history,
)
from api_admin.models import LevelPercentageBase
from api_admin.serializers import LevelPercentageSER
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerAccumUpdateReasonCHO,
    PartnerLevelCHO,
)
from api_partner.models import PartnerLinkAccumulated
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    to_datetime_from,
    to_int,
)
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.db.models.query_utils import Q
from django.utils.timezone import (
    datetime,
    make_aware,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class LevelPercentageBaseAPI(APIView, DefaultPAG):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):

        validator_query = Validator(
            schema={
                'created_at': {
                    'required': False,
                    'type': 'datetime',
                    "coerce": to_datetime_from,
                },
                'created_by_id': {
                    'required': False,
                    'type': 'integer',
                    "coerce": to_int,
                },
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
        )

        if not validator_query.validate(request.query_params):
            return Response(
                schema={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        order_by = validator_query.document.get("order_by")

        level = LevelPercentageBase.objects.all().order_by(order_by)

        user_paginated = self.paginate_queryset(
            queryset=level,
            request=request,
            view=self,
        )

        level_percentage = LevelPercentageSER(
            instance=user_paginated,
            many=True,

        )

        return Response(
            data={
                "level": level_percentage.data,
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request):

        validator = Validator(
            schema={
                "percentages": {
                    "required": False,
                    "type": "dict",
                    "schema": {
                            str(key): {
                                "required": False,
                                "type": "float",
                            }
                        for key in PartnerLevelCHO.values
                    },
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
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

        percentages = validator.document.get("percentages")
        old_percentages = LevelPercentageBase.objects.all().order_by("-created_at").first()
        percentages = old_percentages.percentages | percentages

        query = Q(is_percentage_custom=False, partner_level__in=list(map(int, percentages.keys())))

        partner_accum = PartnerLinkAccumulated.objects.select_related(
            "campaign",
        ).only(
            "campaign__default_percentage",
            "percentage_cpa",
            "pk",
            "partner_level",
        ).filter(query)
        """
        get all partner link accumulated with percentage, pk, partner_level
        """
        update_partner_accum = []
        for partner_accum_i in partner_accum:
            partner_accum_i.percentage_cpa = (
                percentages.get(str(partner_accum_i.partner_level)) *
                partner_accum_i.campaign.default_percentage
            )
            update_partner_accum.append(partner_accum_i)

        data = {
            "percentages": percentages,
            "created_by": request.user
        }

        level_SER = LevelPercentageSER(
            data=data,
        )

        if not level_SER.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": level_SER.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        with transaction.atomic(using=DB_USER_PARTNER):
            with transaction.atomic(using=DB_ADMIN):
                if(update_partner_accum):
                    PartnerLinkAccumulated.objects.bulk_update(
                        objs=update_partner_accum,
                        fields=(
                            "percentage_cpa",
                        ),
                        batch_size=999,
                    )
                level_SER.save()

        query = Q(is_percentage_custom=False)

        partner_accum = PartnerLinkAccumulated.objects.select_related(
            "campaign",
        ).filter(query)

        for partner_accum_i in partner_accum:
            create_history(
                instance=partner_accum_i,
                update_reason=PartnerAccumUpdateReasonCHO.CHANGE_LEVEL_PERCENTAGE,
                adviser=request.user.id,
            )

        return Response(
            data={},
            status=status.HTTP_204_NO_CONTENT,
        )
