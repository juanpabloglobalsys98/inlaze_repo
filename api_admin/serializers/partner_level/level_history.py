from rest_framework import serializers
from api_admin.models import PartnerLevelHistory


class PartnerLevelHistorySER(serializers.ModelSerializer):

    class Meta:
        model = PartnerLevelHistory
        fields = (
            "partner_id",
            "admin",
            "previous_level",
            "new_level",
            "changed_by",
            "created_at",
        )
