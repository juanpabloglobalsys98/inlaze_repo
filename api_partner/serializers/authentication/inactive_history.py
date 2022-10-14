from api_partner.models import InactiveHistory
from django.db.models import Q
from rest_framework import serializers


class InactiveHistorySerializer(serializers.ModelSerializer):
    """
    Inactive history general serializer with all fields
    """

    class Meta:
        model = InactiveHistory
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return InactiveHistory.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return InactiveHistory.objects.db_manager(database).filter(id=id).first()

    def get_by_id(self, id, database="default"):
        return InactiveHistory.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return InactiveHistory.objects.db_manager(database).filter(id=id).delete()

    def get_by_created_at_and_is_active_reason(self, partner_id, database):
        try:
            filters = [Q(partner=partner_id), Q(active_inactive_code_reason__is_active_reason=True)]
            return InactiveHistory.objects.db_manager(database).select_related("active_inactive_code_reason").filter(
                *filters).latest('created_at')
        except:
            return None

    def get_by_create_at_and_is_not_active_reason(self, partner_id, database):
        filters = [Q(partner=partner_id), Q(active_inactive_code_reason__is_active_reason=False)]
        try:
            return InactiveHistory.objects.db_manager(database).select_related("active_inactive_code_reason").filter(
                *filters).latest('created_at')
        except:
            return None


class InactiveHistoryBasicSerializer(serializers.ModelSerializer):
    """
    Inactive history serializer with specific fields for updating and querying purposes
    """

    adviser_id = serializers.IntegerField(required=False)

    class Meta:
        model = InactiveHistory
        fields = ("adviser_id", "active_inactive_code_reason")

    def exist(self, id, database="default"):
        return InactiveHistory.objects.db_manager(database).filter(id=id).first()

    def get_by_id(self, id, database="default"):
        return InactiveHistory.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return InactiveHistory.objects.db_manager(database).filter(id=id).delete()
