from rest_framework import serializers
from api_partner.models import Partner


class PartnerTermsSer(serializers.ModelSerializer):
    class Meta:
        model = Partner
        fields = (
            "is_terms",
            "terms_at",
            "is_notify_campaign",
            "is_notify_notice",
        )
