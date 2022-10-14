import logging

from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import Partner
from api_partner.serializers import PartnerStatusSER
from cerberus import Validator

from core.models import (
    User,
)
from django.conf import settings
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class LogInDataAPI(APIView):

    def get(self, request):
        query = Q(id=request.user.id)

        user = User.objects.using(DB_USER_PARTNER).filter(query).first()
        if user is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "user": [
                            _("There is not such user"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

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

        return Response(
            data={
                "partner_status": partner_status.data,
                "partner_full_name": user.get_full_name(),
                "partner_country": partner_country,
                "partner_level": partner.level,
                "partner_terms": partner.is_terms,
                "alert_to_upload_data": alert_to_upload_data,
            },
            status=status.HTTP_200_OK,
        )
