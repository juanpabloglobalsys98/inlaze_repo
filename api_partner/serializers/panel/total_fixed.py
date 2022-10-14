from rest_framework import serializers

class TotalFixedSerializer(serializers.Serializer):
    total_fixed = serializers.FloatField()
    currency = serializers.CharField()