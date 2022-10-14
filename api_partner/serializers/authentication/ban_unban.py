from api_partner.models import BanUnbanReason
from django.db.models import Q
from rest_framework import serializers


class BanUnbanReasonSER(serializers.ModelSerializer):
    code_reason_id = serializers.IntegerField()
    adviser_id = serializers.IntegerField()

    class Meta:
        model = BanUnbanReason
        fields = (
            "partner",
            "code_reason_id",
            "adviser_id",
        )


class BanUnbanReasonSerializer(serializers.ModelSerializer):
    """
    Ban unban reason general serializer with all fields
    """

    class Meta:
        model = BanUnbanReason
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return BanUnbanReason.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return BanUnbanReason.objects.db_manager(database).filter(id=id).first()

    def get_by_id(self, id, database="default"):
        return BanUnbanReason.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return BanUnbanReason.objects.db_manager(database).filter(id=id).delete()

    def get_by_created_at_and_is_ban_reason(self, partner_id, database):
        try:
            filters = [Q(partner=partner_id), Q(ban_unban_code_reason__is_ban_reason=True)]
            return BanUnbanReason.objects.db_manager(database).select_related("ban_unban_code_reason").filter(
                *filters).latest('created_at')
        except:
            return None

    def get_by_create_at_and_is_not_ban_reason(self, partner_id, database):
        try:
            filters = [Q(partner=partner_id), Q(ban_unban_code_reason__is_ban_reason=False)]
            return BanUnbanReason.objects.db_manager(database).select_related("ban_unban_code_reason").filter(
                *filters).latest('created_at')
        except:
            return None


class BanUnbanReasonBasicSerializer(serializers.ModelSerializer):
    """
    Ban unban reason serializer for updating purpose
    """

    adviser_id = serializers.IntegerField(required=False)

    class Meta:
        model = BanUnbanReason
        fields = ("adviser_id", "ban_unban_code_reason")

    def exist(self, id, database="default"):
        return BanUnbanReason.objects.db_manager(database).filter(id=id).first()

    def get_by_id(self, id, database="default"):
        return BanUnbanReason.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return BanUnbanReason.objects.db_manager(database).filter(id=id).delete()
