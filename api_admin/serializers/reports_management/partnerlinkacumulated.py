from rest_framework import serializers
from api_partner.models import PartnerLinkAccumulated


class PartnerLinkAccumulatedAdminSerializer(serializers.ModelSerializer):

    campaign = serializers.SerializerMethodField("get_campaign")
    campaign_id = serializers.SerializerMethodField("get_campaign_id")

    class Meta:
        model = PartnerLinkAccumulated
        fields = (
            "id",
            "campaign_id",
            "prom_code",
            "is_assigned",
            "campaign",
            "status",
            "assigned_at",
        )

    def get_campaign(self, obj):
        return f"{obj.campaign.bookmaker.name} {obj.campaign.title}"

    def get_campaign_id(self, obj):
        return obj.campaign.id
