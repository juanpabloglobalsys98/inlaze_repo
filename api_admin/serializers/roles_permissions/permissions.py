from rest_framework import serializers
from core.models import Permission

class PermissionSerializer(serializers.ModelSerializer):

    class Meta:
        model = Permission
        fields = (
            "id",
            "codename",
            "name",
            "description",
            "action",
            "section"
        )