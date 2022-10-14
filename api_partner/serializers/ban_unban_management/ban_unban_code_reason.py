from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import BanUnbanCodeReason
from rest_framework import serializers


class BanUnbanCodeReasonSerializer(serializers.ModelSerializer):
    """
    Ban unban code reason serializer with all fields
    """

    class Meta:
        model = BanUnbanCodeReason
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return BanUnbanCodeReason.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return BanUnbanCodeReason.objects.db_manager(database).filter(
            id=id).first()

    def delete(self, id, database="default"):
        return BanUnbanCodeReason.objects.db_manager(database).filter(
            id=id).delete()
