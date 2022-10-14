from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import WithdrawalPartnerMoneyAccum
from api_partner.serializers.fx_rate.fx_rate import (
    FxPartnerForAdviserSer,
    FxPartnerSerializer,
)
from core.helpers import CurrencyPartner
from django.db.models import Q
from rest_framework import serializers


class WithdrawalPartnerMoneyAccumSerializer(serializers.ModelSerializer):
    """
    Withdrawal partner money accumulated serializer with all fields
    """

    fx_partner = serializers.SerializerMethodField("get_fx_partner")

    def get_fx_partner(self, bill_details):
        return FxPartnerSerializer(instance=bill_details.fx_partner).data

    class Meta:
        model = WithdrawalPartnerMoneyAccum
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return WithdrawalPartnerMoneyAccum.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def get_bill_details(self, id, database="default"):
        filters = [Q(withdrawal_partner_money=id)]
        return WithdrawalPartnerMoneyAccum.objects.db_manager(database).filter(*filters)

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoneyAccum.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return WithdrawalPartnerMoneyAccum.objects.db_manager(database).filter(id=id).delete()


class WithdrawalPartnerMoneyAccumForPartnerSerializer(serializers.ModelSerializer):
    """
    Withdrawal partner money accumulated serializer with specific fields for updating and querying purposes
    """
    fx_partner = serializers.SerializerMethodField("get_fx_partner")
    fixed_income_usd_sum = serializers.SerializerMethodField("get_fixed_income_usd_sum")

    def get_fx_partner(self, bill_details):
        currency_partner = bill_details.withdrawal_partner_money.currency_local
        if (currency_partner == CurrencyPartner.USD):
            return 1.0

        fx_partner = bill_details.fx_partner

        return eval(f"fx_partner.fx_usd_{currency_partner.lower()}") * bill_details.fx_percentage

    def get_fixed_income_usd_sum(self, bill_details):
        currency_partner = bill_details.withdrawal_partner_money.currency_local
        fx_percentage = bill_details.fx_percentage
        if(currency_partner == CurrencyPartner.USD):
            # Currency local of partner does not implicate intermediate change
            return bill_details.fixed_income_local
        return (
            bill_details.fixed_income_usd +
            bill_details.fixed_income_eur_usd +
            bill_details.fixed_income_cop_usd +
            bill_details.fixed_income_mxn_usd +
            bill_details.fixed_income_gbp_usd +
            bill_details.fixed_income_pen_usd
        )

    class Meta:
        model = WithdrawalPartnerMoneyAccum
        fields = (
            "cpa_count",
            "accum_at",
            "fixed_income_usd_sum",
            "fixed_income_local",
            "currency_local",
            "fx_partner",
        )

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoneyAccum.objects.db_manager(database).filter(id=id).first()


class WithdrawalPartnerMoneyAccumForAdviserSer(serializers.ModelSerializer):
    """
    Withdrawal partner money accumulated serializer with specific fields for updating and querying purposes
    """
    fx_conversions = serializers.SerializerMethodField("get_fx_conversions")

    def get_fx_conversions(self, bill_details):
        fx_partner = bill_details.fx_partner
        currency_partner = bill_details.withdrawal_partner_money.currency_local

        if (currency_partner == CurrencyPartner.USD):
            fx_usd_partner = 1.0
        else:
            fx_usd_partner = eval(f"fx_partner.fx_usd_{currency_partner.lower()}")

        data_result = FxPartnerForAdviserSer(instance=fx_partner).data
        data_result["fx_usd_partner"] = fx_usd_partner

        return data_result

    class Meta:
        model = WithdrawalPartnerMoneyAccum
        fields = (
            "cpa_count",
            "accum_at",
            "fixed_income_usd",
            "fixed_income_eur",
            "fixed_income_cop",
            "fixed_income_mxn",
            "fixed_income_gbp",
            "fixed_income_pen",
            "fixed_income_eur_usd",
            "fixed_income_cop_usd",
            "fixed_income_mxn_usd",
            "fixed_income_gbp_usd",
            "fixed_income_pen_usd",
            "fx_conversions",
            "fx_percentage",
            "fixed_income_local",
            "currency_local",
            "partner_level",
        )

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoneyAccum.objects.db_manager(database).filter(id=id).first()
