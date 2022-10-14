from api_partner.helpers import (
    IsNotBanned,
    IsNotToBeVerified,
    IsEmailValid,
    IsBasicInfoValid,
    IsBankInfoValid,
)
from api_partner.models import PartnerLinkAccumulated
from api_partner.serializers import TotalFixedSerializer
from django.db.models import F, Q, Sum
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class TotalFixedIncomeAPI(APIView):

    permission_classes = [
        IsAuthenticated,
        IsNotBanned,
        IsEmailValid,
        IsBasicInfoValid,
        IsBankInfoValid,
        IsNotToBeVerified
    ]

    def get(self, request):
        user = request.user
        filters = [Q(partner=user.partner)]
        partnerlinkaccumulated = PartnerLinkAccumulated.objects.filter(*filters).values(
            "partner"
        ).annotate(
            total_fixed=Sum("fixed_income_local"),
            currency=F("currency_local")
        )
        totalfixedserializer = TotalFixedSerializer(partnerlinkaccumulated, many=True)
        return Response({
            "data": totalfixedserializer.data
        }, status=status.HTTP_200_OK)
