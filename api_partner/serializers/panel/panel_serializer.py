from api_partner.models import PartnerLinkDailyReport
from rest_framework import serializers


class PanelPartnerSerializer(serializers.Serializer):
    registered_count = serializers.IntegerField()
    first_deposit_count = serializers.IntegerField()
    deposit = serializers.FloatField()
    cpa_count = serializers.IntegerField()
    fixed_income = serializers.FloatField()
    # fixed_income_unitary = serializers.FloatField()
    # currency_local = serializers.CharField()
    created_at = serializers.CharField()
