from api_admin.models import LevelPercentageBase
from rest_framework import serializers


class LevelPercentageSER(serializers.ModelSerializer):
    class Meta:
        model = LevelPercentageBase
        fields = (
            'percentages',
            'created_by',
            'created_at',
        )
