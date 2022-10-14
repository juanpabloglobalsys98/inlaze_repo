from api_admin.models import Admin
from core.models import User
from django.db.models import Q
from rest_framework import serializers


class AdminSerializer(serializers.ModelSerializer):
    """
    Admin general serializer with all fields
    """
    
    deactivated_at = serializers.CharField(required=False, allow_null=True)

    class Meta:
        model = Admin
        fields = "__all__"

    def create(self, database="admin"):
        """
        """
        return Admin.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="admin"):
        return Admin.objects.db_manager(database).filter(user=id).first()

    def delete(self, id, database="admin"):
        return Admin.objects.db_manager(database).filter(user=id).delete()

    def available_advisers(self, database="admin"):
        filters = [Q(is_active=True,is_staff=True)]
        return User.objects.db_manager(database).filter(*filters)
