from django.contrib.auth.models import Group
from django.db.models import Q
from rest_framework import serializers
from api_admin.helpers import DB_ADMIN

class GroupListSerializer(serializers.ListSerializer):
    def create(self, validated_data):
        return [
            self.child.create(attrs, using=DB_ADMIN) for attrs in validated_data
        ]

class GroupSerializer(serializers.ModelSerializer):

    is_active = serializers.BooleanField(source="groupextra.is_active")

    class Meta:
        model = Group
        exclude = ('permissions',)
        list_serializer_class = GroupListSerializer

    def create(self, data, using="default"):
        """
        Create and return a new `Group` instance, given the validated data.
        """
        return Group.objects.db_manager(using).create(**data)

    def create(self, using="default"):
        return Group.objects.db_manager(using).create(**self.validated_data)

    def bulk_create(self, data, using="default"):
        return Group.objects.db_manager(using).bulk_create(data)

    def exist(self, id, using="default"):
        """
        ¿?
        """
        filters = [Q(id=id)]
        return Group.objects.using(using).filter(*filters).first()

class GroupBasicSerializer(serializers.ModelSerializer):

    class Meta:
        model = Group
        exclude = ('permissions',)

    def create(self, using="default"):
        return Group.objects.db_manager(using).create(**self.validated_data)
    
    def exist(self, group_id, using="default"):
        """
        ¿?
        """
        filters = [Q(id=group_id)]
        return Group.objects.using(using).filter(*filters).first()

class AllGroupsWitPermissionsSerializer(serializers.ModelSerializer):

    is_active = serializers.BooleanField(source="groupextra.is_active")

    class Meta:
        model = Group
        fields = "__all__"
