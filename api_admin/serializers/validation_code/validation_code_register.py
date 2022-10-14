import imp
import re

from api_admin.helpers.routers_db import DB_ADMIN
from api_partner.models import ValidationCodeRegister
from core.models import User
from rest_framework import serializers


class ValidationCodeRegisterSerializer(serializers.ModelSerializer):

    adviser = serializers.SerializerMethodField("get_adviser")
    full_name = serializers.SerializerMethodField("get_full_name")

    class Meta:
        model = ValidationCodeRegister
        fields = (
            "code",
            "full_name",
            "email",
            "phone",
            "adviser",
            "expiration",
            "attempts",
            "created_at",
        )

    def get_adviser(self, obj):
        if obj.adviser_id:
            user_admin = next(
                filter(
                    lambda adviser: adviser.id == obj.adviser_id, self.context.get("adviser_users")
                ),
                None,
            )
            full_name = '%s %s %s %s' % (
                user_admin.first_name,
                user_admin.second_name,
                user_admin.last_name,
                user_admin.second_last_name,
            )
            full_name = re.sub('\s+', ' ', full_name)
            return full_name.strip()
        return None

    def get_full_name(self, obj):
        full_name = '%s %s %s %s' % (obj.first_name, obj.second_name, obj.last_name, obj.second_last_name)
        full_name = re.sub('\s+', ' ', full_name)
        return full_name.strip()
