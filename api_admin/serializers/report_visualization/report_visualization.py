import ast

from api_admin.models import ReportVisualization
from rest_framework import serializers


class ReportVisualizationSerializer(serializers.ModelSerializer):

    values_can_view = serializers.SerializerMethodField("get_values_can_view")
    permission_codename = serializers.SerializerMethodField("get_permission")

    class Meta:
        model = ReportVisualization
        fields = (
            "rol",
            "permission_codename",
            "values_can_view",
        )

    def get_values_can_view(self, obj):
        return ast.literal_eval(obj.values_can_view)

    def get_permission(self, obj):
        return obj.permission.codename
