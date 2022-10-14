from api_partner.models import (
    FxPartner,
    FxPartnerPercentage,
    MinWithdrawalPartnerMoney,
)
from rest_framework import serializers


class TaxFXSerializer(serializers.ModelSerializer):

    class Meta:
        model = FxPartner
        fields = (
            "fx_eur_cop",
            "fx_eur_mxn",
            "fx_eur_usd",
            "fx_eur_brl",
            "fx_eur_pen",
            "fx_usd_cop",
            "fx_usd_mxn",
            "fx_usd_eur",
            "fx_usd_brl",
            "fx_usd_pen",
            "fx_cop_usd",
            "fx_cop_mxn",
            "fx_cop_eur",
            "fx_cop_brl",
            "fx_cop_pen"
        )


class PercentageFXSerializer(serializers.ModelSerializer):

    class Meta:
        model = FxPartnerPercentage
        fields = (
            "percentage_fx",
            "created_at"
        )


class MinWithdrawalPartnerMoneySerializer(serializers.ModelSerializer):

    class Meta:
        model = MinWithdrawalPartnerMoney
        fields = (
            "min_usd_by_level",
            "created_by",
            "created_at",
        )


class FxPartnerToUSD(serializers.ModelSerializer):
    class Meta:
        model = FxPartner
        fields = (
            "fx_eur_usd",
            "fx_cop_usd",
            "fx_mxn_usd",
            "fx_gbp_usd",
            "fx_pen_usd",
            "fx_clp_usd",
            "fx_percentage",
        )
