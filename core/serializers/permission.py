from django.contrib.auth.models import Permission
from django.db.models import Q
from rest_framework import serializers

class PermissionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Permission
        fields = "__all__"

    def create(self, using="default"):
        """
        Create and return a new `Permission` instance, given the validated data.
        """
        return Permission.objects.db_manager(using).create(**self.validated_data)

    def exist(self, Permission_id, using="default"):
        """
        ¿?
        """
        filters = [Q(id=Permission_id)]
        return Permission.objects.using(using).filter(*filters).first()

class PermissionBasicSerializer(serializers.ModelSerializer):
    class Meta:
        model = Permission
        fields = ("id", "name",)

    def create(self, using="default"):
        """
        Create and return a new `Permission` instance, given the validated data.
        """
        return Permission.objects.db_manager(using).create(**self.validated_data)

    def exist(self, Permission_id, using="default"):
        """
        ¿?
        """
        filters = [Q(id=Permission_id)]
        return Permission.objects.using(using).filter(*filters).first()