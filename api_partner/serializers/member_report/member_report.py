from rest_framework import serializers
from api_partner.models import PartnerLinkDailyReport


class MemberReportSer(serializers.ModelSerializer):
    campaign_title = serializers.CharField()
    deposit_usd = serializers.FloatField()
    click_count = serializers.IntegerField()

    class Meta:
        model = PartnerLinkDailyReport
        fields = (
            "campaign_title",
            "deposit_usd",
            "click_count",
            "cpa_count",
            "first_deposit_count",
            "registered_count",
            "currency_local",
            "fixed_income_local",
            "wagering_count",
            "created_at",
            "fixed_income_referred_local",
            "net_revenue_referred_local",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",
        )


class MemberReportGroupedSer(serializers.Serializer):
    campaign_title = serializers.CharField(required=False)
    deposit_usd = serializers.FloatField()
    fixed_income_local = serializers.FloatField()
    click_count = serializers.IntegerField()
    cpa_count = serializers.IntegerField()
    currency_local = serializers.CharField()
    created_at__month = serializers.IntegerField(required=False)
    created_at__year = serializers.IntegerField(required=False)
    first_deposit_count = serializers.IntegerField()
    registered_count = serializers.IntegerField()
    wagering_count = serializers.IntegerField()


class MemberReportConsolidatedSer(serializers.Serializer):
    deposit_usd = serializers.FloatField()
    fixed_income_local = serializers.FloatField()
    click_count = serializers.IntegerField()
    cpa_count = serializers.IntegerField()
    currency_local = serializers.CharField()
    first_deposit_count = serializers.IntegerField()
    registered_count = serializers.IntegerField()
    wagering_count = serializers.IntegerField()


class MemberReportGroupedReferredSer(serializers.Serializer):
    campaign_title = serializers.CharField(required=False)
    deposit_usd = serializers.FloatField()
    fixed_income_local = serializers.FloatField()
    click_count = serializers.IntegerField()
    cpa_count = serializers.IntegerField()
    currency_local = serializers.CharField()
    created_at__month = serializers.IntegerField(required=False)
    created_at__year = serializers.IntegerField(required=False)
    first_deposit_count = serializers.IntegerField()
    registered_count = serializers.IntegerField()
    wagering_count = serializers.IntegerField()

    fixed_income_referred_local = serializers.FloatField()
    net_revenue_referred_local = serializers.FloatField()

    class Meta:
        model = PartnerLinkDailyReport
        fields = (
            "referred_by_id",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",
        )
