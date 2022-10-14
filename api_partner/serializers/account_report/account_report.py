from rest_framework import serializers
from api_partner.models import AccountReport


class AccountReportSer(serializers.ModelSerializer):
    campaign_title = serializers.CharField()
    prom_code = serializers.CharField()
    prom_code = serializers.CharField()

    class Meta:
        model = AccountReport
        fields = (
            "campaign_title",
            "prom_code",
            "punter_id",
            "registered_at",
            "cpa_partner",
            "currency_condition",
            "cpa_at",
            "created_at"
        )


class AccountReportTotalCountSer(serializers.Serializer):
    currency_fixed_income = serializers.CharField()
    cpa_partner__sum = serializers.IntegerField()
