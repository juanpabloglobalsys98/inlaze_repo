from api_admin.models import SearchPartnerLimit
from rest_framework import serializers


class SearchSerializer(serializers.ModelSerializer):

    class Meta:
        model = SearchPartnerLimit
        fields = (
            "id",
            "rol",
            "codename",
            "search_type",
        )