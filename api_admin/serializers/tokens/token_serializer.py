from rest_framework import serializers
from rest_framework.authtoken.models import Token
from core.models import User


class UserTokenSerializer(serializers.ModelSerializer):

    key = serializers.SerializerMethodField("get_key")
    name = serializers.SerializerMethodField("get_name")

    class Meta:
        model = User
        fields = (
            "id",
            "key",
            "name",
            "email",
            "date_joined",
            "last_login",
        )

    def get_key(self, obj):
        if hasattr(obj, "auth_token"):
            return obj.auth_token.key
        return None

    def get_name(self, obj):
        return obj.get_full_name()
