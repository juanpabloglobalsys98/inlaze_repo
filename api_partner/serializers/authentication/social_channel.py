from rest_framework import serializers
from api_partner.models import SocialChannel


class SocialChannelSER(serializers.ModelSerializer):
    name = serializers.CharField()
    url = serializers.URLField()

    class Meta:
        model = SocialChannel
        fields = (
            "pk",
            "partner_id",
            "name",
            "url",
            "type_channel",
            "is_active",
        )


class SocialChannelToPartnerSER(serializers.ModelSerializer):
    name = serializers.CharField()
    url = serializers.URLField()

    class Meta:
        model = SocialChannel
        fields = (
            "partner_id",
            "name",
            "url",
            "type_channel",
            "is_active",
        )
