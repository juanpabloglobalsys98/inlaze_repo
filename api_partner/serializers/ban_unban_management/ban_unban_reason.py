from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import BanUnbanReason
from rest_framework import serializers


class BanUnbanReasonSerializer(serializers.ModelSerializer):
    """
    Ban unban reason serializer with all fields
    """

    class Meta:
        model = BanUnbanReason
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return BanUnbanReason.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return BanUnbanReason.objects.db_manager(database).filter(
            id=id).first()

    def delete(self, id, database="default"):
        return BanUnbanReason.objects.db_manager(database).filter(
            id=id).delete()
