from core.models import Permission
from rest_framework import serializers


class PermissionsSerializer(serializers.ModelSerializer):

    class Meta:
        model = Permission
        fields = (
            "id",
            "codename",
            "section",
            "name",
            "action",
            "description",
        )
