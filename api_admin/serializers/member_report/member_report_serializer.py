from rest_framework import serializers
from core.models import User


class MemberReportAdviserSer(serializers.ModelSerializer):

    full_name = serializers.SerializerMethodField('get_full_name')

    class Meta:
        model = User
        fields = (
            "id",
            "full_name",
            "email",
        )

    def get_full_name(self, obj):
        return obj.get_full_name()
