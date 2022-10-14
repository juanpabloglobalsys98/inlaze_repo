from api_partner.models import (
    HistoricalPartnerLinkAccum,
    PartnerLinkAccumulated,
)
from rest_framework import serializers


class PartnerLinkAccumAdditionalBasicSer(serializers.ModelSerializer):
    identification_number = serializers.CharField(
        source="partner.additionalinfo.identification",
        allow_null=True,
        read_only=True,
    )
    identification_type = serializers.IntegerField(
        source="partner.additionalinfo.identification_type",
        allow_null=True,
        read_only=True,
    )
    email = serializers.CharField(
        source="partner.user.email",
        allow_null=True,
        read_only=True,
    )
    first_name = serializers.CharField(
        source="partner.user.first_name",
        allow_null=True,
        read_only=True,
    )
    second_name = serializers.CharField(
        source="partner.user.second_name",
        allow_null=True,
        read_only=True,
    )
    last_name = serializers.CharField(
        source="partner.user.last_name",
        allow_null=True,
        read_only=True,
    )
    second_last_name = serializers.CharField(
        source="partner.user.second_last_name",
        allow_null=True,
        read_only=True,
    )

    class Meta:
        model = PartnerLinkAccumulated
        fields = (
            "identification_number",
            "identification_type",
            "email",
            "first_name",
            "last_name",
            "second_name",
            "second_last_name",
        )


class PartnerLinkAccumManageLinkSer(serializers.ModelSerializer):
    class Meta:
        model = PartnerLinkAccumulated
        fields = (
            "prom_code",
            "percentage_cpa",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",
        )

    def validate_percentage_cpa(self, value):
        level = self.context.get("level")
        level_percentage = level.percentages.get(str(self.instance.partner_level))
        percentage = self.instance.campaign.default_percentage * (
            level_percentage
        )
        if value == 0 or value == percentage:
            self.instance.is_percentage_custom = False
            return percentage
        self.instance.is_percentage_custom = True
        return value


class PartnerLinkAccumHistoricalSER(serializers.ModelSerializer):
    class Meta:
        model = HistoricalPartnerLinkAccum
        fields = (
            "partner_link_accum",
            "prom_code",
            "link",
            "is_assigned",
            "percentage_cpa",
            "is_percentage_custom",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",
            "status",
            "partner_level",
            "assigned_at",
        )


class PartnerLinkAccumSER(serializers.ModelSerializer):
    campaign_title = serializers.CharField()

    class Meta:
        model = PartnerLinkAccumulated
        fields = (
            "pk",
            "campaign",
            "campaign_title",
            "prom_code",
            "is_assigned",
            "cpa_count",
            "fixed_income",
            "currency_fixed_income",
            "fixed_income_local",
            "currency_local",
            "percentage_cpa",
            "is_percentage_custom",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",
            "status",
            "partner_level",
            "assigned_at",
            "updated_at",
        )
