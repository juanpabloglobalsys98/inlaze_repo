import re

from api_partner.models import (
    PartnerBankValidationRequest,
    PartnerInfoValidationRequest,
    PartnerLevelRequest,
)
from rest_framework import serializers


class PartnerLevelRequestSER(serializers.ModelSerializer):

    class Meta:
        model = PartnerLevelRequest
        fields = "__all__"


class PartnerUserSER(serializers.ModelSerializer):
    """
    Base partner serializer with its user info.
    """
    full_name = serializers.SerializerMethodField("get_full_name")
    email = serializers.CharField()
    phone = serializers.CharField()
    country = serializers.CharField()
    partner_level = serializers.IntegerField()

    def get_full_name(self, obj):
        return obj.partner.user.get_full_name()


class PartnerLevelRequestUserSER(PartnerUserSER):
    """
    Apart from the level request info, process additional info about its user.
    """
    adviser_id = serializers.SerializerMethodField("get_adviser_id")
    is_banned = serializers.BooleanField()
    is_active = serializers.BooleanField()

    class Meta:
        model = PartnerLevelRequest
        fields = "__all__"

    def get_adviser_id(self, obj):
        admin = self.context.get("admin")
        return admin.id


class PartnerInfoValidationRequestUserSER(PartnerUserSER):
    """
    Apart from the basic info validation request info, process additional info about its user.
    """
    is_banned = serializers.BooleanField()
    is_active = serializers.BooleanField()

    class Meta:
        model = PartnerInfoValidationRequest
        fields = "__all__"


class PartnerBankValidationRequestUserSER(PartnerUserSER):
    """
    Apart from the bank info validation request info, process additional info about its user.
    """
    is_banned = serializers.BooleanField()
    is_active = serializers.BooleanField()

    class Meta:
        model = PartnerBankValidationRequest
        fields = "__all__"
