from api_admin.serializers import (
    ReportVisualizationSerializer,
)
from core.models import Rol
from rest_framework import serializers

from . import PermissionSerializer


class RoleSerializer(serializers.ModelSerializer):

    permissions = PermissionSerializer(read_only=True, many=True)
    visualization = serializers.SerializerMethodField("get_visualization")
    searchpartnerlimit = serializers.SerializerMethodField("get_searchpartnerlimit")

    class Meta:
        model = Rol
        fields = (
            "id",
            "rol",
            "permissions",
            "visualization",
            "searchpartnerlimit",
        )

    def get_visualization(self, obj):
        report_visualization = obj.report_visualization_rol.all()
        return ReportVisualizationSerializer(report_visualization, many=True).data

    def get_searchpartnerlimit(self, obj):
        from api_admin.serializers import (
            SearchSerializer,
        )
        searchpartnerlimit = obj.search_limit_rol.all()
        return SearchSerializer(searchpartnerlimit, many=True).data
