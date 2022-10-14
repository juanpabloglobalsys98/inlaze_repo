from core.helpers import calc_fx
from rest_framework import serializers
from api_partner.models import PartnerLinkAccumulated


class PartnerLevelVerifyCustomSER(serializers.ModelSerializer):
    campaign_title = serializers.CharField()
    current_default_percentage = serializers.FloatField()
    new_default_percentage = serializers.FloatField()
    campaign_fixed_income_unitary = serializers.FloatField()
    campaign_currency_fixed_income = serializers.CharField()
    fx_book_local = serializers.SerializerMethodField("get_fx_book_local")

    class Meta:
        model = PartnerLinkAccumulated
        fields = [
            "id",
            "campaign_title",
            "percentage_cpa",
            "current_default_percentage",
            "new_default_percentage",
            "campaign_fixed_income_unitary",
            "campaign_currency_fixed_income",
            "fx_book_local"
        ]

    def get_fx_book_local(self, obj):
        fx_partner = self.context.get("fx_partner")
        currency_from_str = self.get_currency_fixed_income(obj).lower()
        partner_currency_str = "usd"
        fx_book_local = calc_fx(
            fx_partner=fx_partner,
            currency_from_str=currency_from_str,
            partner_currency_str=partner_currency_str,
        )

        return fx_book_local

    def get_currency_fixed_income(self, obj):
        return obj.campaign.currency_fixed_income
