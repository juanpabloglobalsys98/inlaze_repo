from core.models import User
from rest_framework import serializers


class ProfileAdminSerializer(serializers.ModelSerializer):

    class Meta:
        model = User
        fields = (
            "first_name",
            "second_name",
            "last_name",
            "second_last_name",
            "phone",
            "email",
            "is_superuser",
        )
