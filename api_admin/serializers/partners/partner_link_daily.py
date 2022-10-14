from rest_framework import serializers
from api_partner.models import PartnerLinkDailyReport

class PartnerLinkDilySerializer(serializers.ModelSerializer):
    
    class Meta:
        model = PartnerLinkDailyReport
        fiels = (
            "cpa_count",
            "last_cpa_at",
            "click_count",
            "registered_count",
        )
