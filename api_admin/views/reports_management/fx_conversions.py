import logging

from api_partner.models import FxPartner
from api_admin.serializers import FxPartnerCurrentFullSer
from cerberus import Validator
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from django.conf import settings

logger = logging.getLogger(__name__)


class FxPartnerCurrentFullConversionAPI(APIView):
    permission_classes = (
        IsAuthenticated,
    )

    def get(self, request):
        """ Get report fx conversions from values according to 
        `CurrencyFixedIncome.values` enumerator this build a dictionary like

        >>> {
            "fx_eur_local": 1000,
            "fx_cop_local": 1000,
            "fx_usd_local": 1000,
        }

        1000 is an example value
        """
        validator = Validator(
            schema={},
        )

        if not validator.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get current fx_partner
        fx_partner = FxPartner.objects.all().order_by("-created_at").first()

        if (not fx_partner):
            return Response(
                data={
                    "error": settings.ERROR_FX_NOT_IN_DB,
                    "details": _("Fx rate not any in DB"),
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        fx_partner_ser = FxPartnerCurrentFullSer(
            instance=fx_partner,
        )

        FxPartnerCurrentFullSer

        return Response(
            data={
                "fx_partner": fx_partner_ser.data,
            },
            status=status.HTTP_200_OK,
        )
