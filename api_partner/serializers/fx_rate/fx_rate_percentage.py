from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models.payment_management import FxPartnerPercentage, fx_partner_percentage
from rest_framework import serializers


class FxPartnerPercentageSerializer(serializers.ModelSerializer):
    """
    fx partner percentage serializer with all fields
    """

    class Meta:
        model = FxPartnerPercentage
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return FxPartnerPercentage.objects.db_manager(DB_USER_PARTNER).create(**self.validated_data)

    def exist(self, id, database="default"):
        return FxPartnerPercentage.objects.db_manager(database).filter(user=id).first()

    def get_fx_percentage(self, filters, sort_by, database="default"):
        return FxPartnerPercentage.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def get_latest(self, database="default"):
        try:
            return FxPartnerPercentage.objects.db_manager(database).latest('updated_at')
        except:
            return None

    def delete(self, id, database="default"):
        return FxPartnerPercentage.objects.db_manager(database).filter(user=id).delete()
