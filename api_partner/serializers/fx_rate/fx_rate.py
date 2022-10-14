from api_partner.models import FxPartner
from rest_framework import serializers


class FxPartnerSerializer(serializers.ModelSerializer):
    """
    fx partner serializer with all fields
    """

    class Meta:
        model = FxPartner
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return FxPartner.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return FxPartner.objects.db_manager(database).filter(user=id).first()

    def get_fx(self, filters, sort_by, database="default"):
        return FxPartner.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def delete(self, id, database="default"):
        return FxPartner.objects.db_manager(database).filter(user=id).delete()


class FxPartnerForAdviserSer(serializers.ModelSerializer):
    """
    fx partner serializer with field of usd transition conversions EUR and COP
    """

    class Meta:
        model = FxPartner
        fields = (
            "fx_eur_usd",
            "fx_cop_usd",
            "fx_mxn_usd",
            "fx_gbp_usd",
            "fx_pen_usd",
        )
