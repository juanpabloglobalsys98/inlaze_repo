import ast

from api_partner.models import (
    Campaign,
    HistoricalCampaign,
)
from rest_framework import serializers


class CampaignSer(serializers.ModelSerializer):
    """ Serializer to campaign """
    campaign_title = serializers.CharField()
    countries = serializers.SerializerMethodField("get_countries")

    class Meta:
        model = Campaign
        fields = (
            "id",
            "bookmaker",
            "campaign_title",
            "deposit_condition",
            "stake_condition",
            "lose_condition",
            "deposit_condition_campaign_only",
            "stake_condition_campaign_only",
            "lose_condition_campaign_only",
            "currency_condition_campaign_only",
            "currency_condition",
            "status",
            "default_percentage",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",
            "temperature",
            "countries",
            "cpa_limit",
            "fixed_income_unitary",
            "currency_fixed_income",
            "fixed_income_updated_at",
            "last_inactive_at",
        )

    def get_countries(self, obj):
        return ast.literal_eval(obj.countries)


class CampaignBasicSerializer(serializers.ModelSerializer):
    class Meta:
        model = Campaign
        fileds = (
            "id",
            "bookmaker",
            "title"
        )


class CampaignManageSer(serializers.ModelSerializer):
    def to_internal_value(self, data):
        # Cast list to string for Charfield Case
        if isinstance(data.get("countries"), list):
            data["countries"] = str(data.get("countries"))
        return super().to_internal_value(data)

    class Meta:
        model = Campaign
        fields = (
            "bookmaker",
            "title",
            "deposit_condition",
            "stake_condition",
            "lose_condition",
            "deposit_condition_campaign_only",
            "stake_condition_campaign_only",
            "lose_condition_campaign_only",
            "currency_condition_campaign_only",
            "currency_condition",
            "countries",
            "fixed_income_unitary",
            "currency_fixed_income",
            "status",
            "default_percentage",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",
        )


class CampaignAccountReportSerializer(serializers.ModelSerializer):
    """ Serializer to Campaign report """
    name = serializers.CharField()

    class Meta:
        model = Campaign
        fields = (
            "id",
            "name"
        )


class CampaignUserSerializer(serializers.ModelSerializer):
    """ Campaign Users """

    class Meta:
        model = Campaign


class CampaignBasicSer(serializers.ModelSerializer):
    """ Campaign Users """
    campaign_title = serializers.CharField()

    class Meta:
        model = Campaign
        fields = (
            "id",
            "campaign_title",
        )


class HistoricalCampaignSER(serializers.ModelSerializer):
    """ Serializer to campaign """
    countries = serializers.SerializerMethodField("get_countries")
    campaign_title = serializers.CharField()

    class Meta:
        model = HistoricalCampaign
        fields = (
            "id",
            "campaign",
            "modified_by_id",
            "bookmaker",
            "title",
            "deposit_condition",
            "stake_condition",
            "lose_condition",
            "deposit_condition_campaign_only",
            "stake_condition_campaign_only",
            "lose_condition_campaign_only",
            "currency_condition_campaign_only",
            "currency_condition",
            "status",
            "fixed_income_unitary",
            "currency_fixed_income",
            "default_percentage",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",
            "temperature",
            "countries",
            "cpa_limit",
            "created_at",
            "campaign_title",
        )

    def get_countries(self, obj):
        if obj.countries:
            return ast.literal_eval(obj.countries)
        return obj.countries
