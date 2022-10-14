from api_admin.serializers import (
    MinWithdrawalPartnerMoneySerializer,
    PercentageFXSerializer,
    TaxFXSerializer,
)
from api_partner.helpers import (
    GetHistorialFXTax,
    PartnerLevelCHO,
)
from api_partner.models import (
    FxPartner,
    FxPartnerPercentage,
    MinWithdrawalPartnerMoney,
)
from cerberus import Validator
from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class TaxFXAPI(APIView, GetHistorialFXTax):

    permission_classes = [IsAuthenticated]

    def get(self, request):
        validator = Validator({
            'lim': {
                'required': False,
                'type': 'string'
            },
            'offs': {
                'required': False,
                'type': 'string'
            }
        })

        if not validator.validate(request.query_params):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        fxpartners = FxPartner.objects.all().order_by('-created_at')
        fxpartners = self.paginate_queryset(
            fxpartners, request, view=self
        )
        fxparnerts_serializer = TaxFXSerializer(fxpartners, many=True)
        return Response({
            "historial": fxparnerts_serializer.data
        }, headers={
            "count": self.count,
            "access-control-expose-headers": "count,next,previous"
        }, status=status.HTTP_200_OK)


class PercentageFX(APIView, GetHistorialFXTax):

    """ Show percentage fx """
    permission_classes = [IsAuthenticated]

    def get(self, request):

        fxpartnerpercent = FxPartnerPercentage.objects.all().order_by('-created_at')
        fxpartners = self.paginate_queryset(
            fxpartnerpercent, request, view=self
        )
        percentagefx = PercentageFXSerializer(fxpartners, many=True)
        return Response({
            "percentage": percentagefx.data
        }, status=status.HTTP_200_OK)

    """ Create percentage fx """

    def post(self, request):
        validator = Validator({
            'percentage_fx': {
                'required': True,
                'type': 'float'
            }
        })

        if not validator.validate(request.data):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        FxPartnerPercentage.objects.create(**request.data)

        return Response({
            "msg": "Created ok"
        }, status=status.HTTP_200_OK)
