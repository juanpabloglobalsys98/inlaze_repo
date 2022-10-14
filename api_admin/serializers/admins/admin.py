import re
from rest_framework import serializers
from core.models import User


class AdminUserSerializer(serializers.ModelSerializer):

    full_name = serializers.SerializerMethodField("get_full_name")

    class Meta:
        model = User
        fields = (
            "id",
            "full_name",
            "first_name",
            "second_name",
            "last_name",
            "second_last_name",
            "phone",
            "email",
            "rol",
            "is_staff",
            "last_login",
            "date_joined",
            "is_active",
        )

    def get_full_name(self, obj):
        return obj.get_full_name()


class AdviserUserSER(serializers.ModelSerializer):

    full_name = serializers.SerializerMethodField("get_full_name")

    class Meta:
        model = User
        fields = (
            "pk",
            "full_name",
            "email",
            "rol",
            "is_staff",
        )

    def get_full_name(self, obj):
        if (obj is not None and obj.full_name is not None):
            full_name = re.sub('\s+', ' ', obj.full_name)
            return full_name.strip()


class PartnerAdviserSER(serializers.ModelSerializer):
    full_name = serializers.SerializerMethodField("get_full_name")

    class Meta:
        model = User
        fields = (
            "pk",
            "full_name",
        )

    def get_full_name(self, user):
        return user.get_full_name()
