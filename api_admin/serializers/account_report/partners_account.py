from api_partner.models import (
    AccountReport,
    Partner,
)
from rest_framework import serializers


class PartnerAccountSerializer(serializers.ModelSerializer):

    first_name = serializers.SerializerMethodField("get_first_name")
    last_name = serializers.SerializerMethodField("get_last_name")
    identification = serializers.SerializerMethodField("get_identification")
    identification_type = serializers.SerializerMethodField("get_identification_type")

    class Meta:
        model = Partner
        fields = (
            "user_id",
            "first_name",
            "last_name",
            "identification",
            "identification_type"
        )

    def get_first_name(self, obj):
        return obj.user.first_name

    def get_last_name(self, obj):
        return obj.user.last_name

    def get_identification(self, obj):
        return obj.additionalinfo.identification

    def get_identification_type(self, obj):
        return obj.additionalinfo.identification_type


class PartnerSerializer(serializers.ModelSerializer):
    email = serializers.SerializerMethodField("get_email")
    first_name = serializers.SerializerMethodField("get_first_name")
    last_name = serializers.SerializerMethodField("get_last_name")

    class Meta:
        model = Partner
        fields = (
            "user_id",
            "first_name",
            "last_name",
            "email"
        )

    def get_email(self, obj):
        return obj.user.email

    def get_first_name(self, obj):
        return obj.user.first_name

    def get_last_name(self, obj):
        return obj.user.last_name


class AcountReportAdminSerializers(serializers.ModelSerializer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        elements_remove = set(self.fields.keys()) - set(self.context.get("permissions"))
        for i in elements_remove:
            self.fields.pop(i)

    campaign_title = serializers.CharField()
    partner_name = serializers.CharField()
    prom_code = serializers.CharField()

    class Meta:
        model = AccountReport
        fields = (
            "campaign_title",
            "punter_id",
            "deposit",
            "stake",
            "net_revenue",
            "fixed_income",
            "revenue_share",
            "cpa_betenlace",
            "cpa_partner",
            "cpa_at",
            "created_at",
            "updated_at",
            "registered_at",
            "first_deposit_at",
            "prom_code",
            "partner_name",
            "currency_condition",
            "currency_fixed_income",
        )


class AccountReportTotalCount(serializers.Serializer):
    currency_fixed_income = serializers.CharField()
    deposit__sum = serializers.FloatField()
    stake__sum = serializers.FloatField()
    cpa_partner__sum = serializers.IntegerField()
    cpa_betenlace__sum = serializers.IntegerField()
