from api_partner.models import SocialChannelRequest
from rest_framework import serializers


class SocialChannelRequestSER(serializers.ModelSerializer):

    class Meta:
        model = SocialChannelRequest
        fields = (
            "id",
            "name",
            "url",
            "type",
            "partner_level_request",
        )
