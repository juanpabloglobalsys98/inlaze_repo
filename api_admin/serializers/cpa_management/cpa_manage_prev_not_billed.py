import re

from api_partner.models import (
    BetenlaceDailyReport,
    BetenlaceCPA,
    FxPartner,
)
from rest_framework import serializers


class CpaManageDailyNotBilledSer(serializers.ModelSerializer):
    partner_full_name = serializers.SerializerMethodField("get_partner_full_name")
    partner_email = serializers.CharField()

    deposit_partner = serializers.FloatField()

    registered_count_partner = serializers.IntegerField()
    first_deposit_count_partner = serializers.IntegerField()
    wagering_count_partner = serializers.IntegerField()
    currency_local = serializers.CharField()

    fx_book_local = serializers.FloatField()
    fx_book_net_revenue_local = serializers.FloatField()
    percentage_cpa = serializers.FloatField()

    cpa_partner = serializers.IntegerField()

    fixed_income_partner = serializers.FloatField()
    fixed_income_partner_unitary = serializers.FloatField()
    fixed_income_partner_local = serializers.FloatField()
    fixed_income_partner_unitary_local = serializers.FloatField()

    tracker = serializers.FloatField()
    tracker_deposit = serializers.FloatField()
    tracker_registered_count = serializers.FloatField()
    tracker_first_deposit_count = serializers.FloatField()
    tracker_wagering_count = serializers.FloatField()

    adviser_id = serializers.IntegerField()
    fixed_income_adviser = serializers.FloatField()
    net_revenue_adviser = serializers.FloatField()
    fixed_income_adviser_local = serializers.FloatField()
    net_revenue_adviser_local = serializers.FloatField()
    fixed_income_adviser_percentage = serializers.FloatField()
    net_revenue_adviser_percentage = serializers.FloatField()

    referred_by_id = serializers.IntegerField()
    fixed_income_referred_local = serializers.FloatField()
    net_revenue_referred_local = serializers.FloatField()
    fixed_income_referred_percentage = serializers.FloatField()
    net_revenue_referred_percentage = serializers.FloatField()

    class Meta:
        model = BetenlaceDailyReport
        fields = (
            "id",

            "partner_full_name",
            "partner_email",

            "currency_condition",

            "deposit",
            "stake",
            "net_revenue",
            "revenue_share",

            "registered_count",
            "first_deposit_count",
            "wagering_count",

            "deposit_partner",

            "registered_count_partner",
            "first_deposit_count_partner",
            "wagering_count_partner",

            "currency_local",

            "fx_book_local",
            "fx_book_net_revenue_local",
            "percentage_cpa",

            "cpa_count",
            "cpa_partner",

            "currency_fixed_income",

            "fixed_income",
            "fixed_income_unitary",

            "fixed_income_partner",
            "fixed_income_partner_unitary",
            "fixed_income_partner_local",
            "fixed_income_partner_unitary_local",

            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",

            "adviser_id",
            "fixed_income_adviser",
            "net_revenue_adviser",

            "fixed_income_adviser_local",
            "net_revenue_adviser_local",

            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",

            "referred_by_id",
            "fixed_income_referred_local",
            "net_revenue_referred_local",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",

            "created_at",
        )

    def get_partner_full_name(self, obj):
        if (obj is not None and obj.partner_full_name is not None):
            full_name = re.sub('\s+', ' ', obj.partner_full_name)
            return full_name.strip()


class CpaManageLinksNotBilledSer(serializers.ModelSerializer):
    betenlace_dailies = CpaManageDailyNotBilledSer(many=True)

    campaign_title = serializers.CharField()
    prom_code = serializers.CharField()

    fixed_income_unitary = serializers.FloatField()

    currency_condition = serializers.CharField()
    currency_fixed_income = serializers.CharField()

    partner_full_name = serializers.SerializerMethodField("get_partner_full_name")
    partner_email = serializers.CharField()

    class Meta:
        model = BetenlaceCPA
        fields = (
            "pk",
            "betenlace_dailies",

            "campaign_title",
            "prom_code",

            "currency_condition",
            "currency_fixed_income",

            "fixed_income_unitary",

            "partner_full_name",
            "partner_email",
        )

    def get_partner_full_name(self, obj):
        if (obj is not None and obj.partner_full_name is not None):
            full_name = re.sub('\s+', ' ', obj.partner_full_name)
            return full_name.strip()
