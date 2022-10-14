import re

from api_partner.models import Partner
from django.contrib.auth import get_user_model
from rest_framework import serializers

User = get_user_model()


class PartnerStatusManagementSER(serializers.ModelSerializer):
    full_name = serializers.SerializerMethodField("get_full_name")
    social_channel = serializers.SerializerMethodField("get_social_channel")
    email = serializers.EmailField()
    language = serializers.CharField()

    class Meta:
        model = Partner
        fields = (
            "user_id",
            "full_name",
            "basic_info_status",
            "bank_status",
            "documents_status",
            "level_status",
            "level",
            "alerts",
            "is_email_valid",
            "email",
            "language",
            "social_channel",
        )

    def get_full_name(self, obj):
        if (obj is not None and obj.full_name is not None):
            full_name = re.sub('\s+', ' ', obj.full_name)
            return full_name.strip()

    def get_social_channel(self, obj):
        if obj.level == 1:
            social = self.context.get("social")
            if social:
                return social.type_channel
        return None


class languageSER(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = (
            "language",
        )
