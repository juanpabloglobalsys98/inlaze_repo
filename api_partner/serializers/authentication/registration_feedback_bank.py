from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import RegistrationFeedbackBank
from django.db.models import Q
from rest_framework import serializers


class RegistrationFeedbackBankSerializer(serializers.ModelSerializer):
    """
    Registration feedback bank general serializer with all fields
    """

    _error_fields = serializers.ListField(required=False, allow_null=True)

    class Meta:
        model = RegistrationFeedbackBank
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return RegistrationFeedbackBank.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return RegistrationFeedbackBank.objects.db_manager(database).filter(id=id).first()

    def get_by_partner_and_admin(self, user_id, adviser_id, database="default"):
        try:
            filters = [Q(partner=user_id), Q(adviser_id=adviser_id)]
            return RegistrationFeedbackBank.objects.db_manager(database).filter(*filters).latest("created_at")
        except:
            return None

    def get_by_partner(self, user_id, database="default"):
        try:
            filters = [Q(partner=user_id)]
            return RegistrationFeedbackBank.objects.db_manager(database).filter(*filters).latest("created_at")
        except:
            return None

    def get_latest(self, user_id, database="default"):
        try:
            filters = [Q(partner=user_id)]
            return RegistrationFeedbackBank.objects.db_manager(database).filter(*filters).latest("created_at")
        except:
            return None

    def delete(self, id, database="default"):
        return RegistrationFeedbackBank.objects.db_manager(database).filter(id=id).delete()
