from rest_framework import serializers
from api_partner.models import FxPartner


class FxPartnerCurrentFullSer(serializers.ModelSerializer):
    class Meta:
        model = FxPartner
        fields = "__all__"
