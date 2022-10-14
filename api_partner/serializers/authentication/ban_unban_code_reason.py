from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import BanUnbanCodeReason
from django.db.models import Q
from rest_framework import serializers


class BanUnbanCodeReasonSerializer(serializers.ModelSerializer):
    """
    Ban unban code reason general serializer with all fields
    """

    title = serializers.CharField(required=False)
    reason = serializers.CharField(required=False)
    is_ban_reason = serializers.BooleanField(required=False)

    class Meta:
        model = BanUnbanCodeReason
        fields = "__all__"

    def create(self, database="default"):
        return BanUnbanCodeReason.objects.db_manager(database).create(**self.validated_data)

    def get_all(self):
        """
        """
        return BanUnbanCodeReason.objects.db_manager(DB_USER_PARTNER).all()

    def get_by_is_ban_reason(self, is_ban_reason, database="default"):
        filters = [Q(is_ban_reason=is_ban_reason)]
        return BanUnbanCodeReason.objects.db_manager(database).filter(*filters)

    def exist(self, id, database="default"):
        return BanUnbanCodeReason.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return BanUnbanCodeReason.objects.db_manager(database).filter(id=id).delete()
