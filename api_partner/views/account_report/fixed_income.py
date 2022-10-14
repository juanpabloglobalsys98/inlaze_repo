from api_partner.helpers import IsTerms
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.serializers.reports_management.partner_accumulated import (
    FixedCurrencyIncomeSerializer,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class FixedCurrencyIncomeAPI(APIView):
    """
    """
    permission_classes = (
        IsAuthenticated,
        IsTerms,
    )

    def get(self, request):
        """
        """
        partner = request.user.id
        fixed_currency = FixedCurrencyIncomeSerializer().partner_fixed_currency_income(partner, DB_USER_PARTNER)
        return Response(
            data={"fixed_income": fixed_currency if fixed_currency else []},
            status=status.HTTP_200_OK
        )
