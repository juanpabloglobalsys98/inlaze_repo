from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import OwnCompany
from rest_framework import serializers


class OwnCompanySerializer(serializers.ModelSerializer):
    """
    Own company serializer with all fields
    """

    class Meta:
        model = OwnCompany
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return OwnCompany.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def create_logo(self, instance, logo):
        instance.save_file(logo)

    def update_logo(self, instance, logo):
        instance.delete_file()
        instance.save_file(logo)

    def get_latest(self, database="default"):
        try:
            return OwnCompany.objects.db_manager(database).latest('created_at')
        except:
            return None

    def get_all(self, database="default"):
        return OwnCompany.objects.db_manager(database).all()

    def get_by_id(self, id, database="default"):
        return OwnCompany.objects.db_manager(database).filter(id=id).first()

    def get_by_ids(self, filters, database="default"):
        return OwnCompany.objects.db_manager(database).filter(*filters)

    def exist(self, id, database="default"):
        return OwnCompany.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return OwnCompany.objects.db_manager(database).filter(id=id).delete()


class OwnCompanyUpdateSerializer(serializers.ModelSerializer):
    """
    Own company update serializer excluding logo for updating purpose
    """

    class Meta:
        model = OwnCompany
        exclude = ("logo",)

    def create(self, validated_data):
        """
        """
        return OwnCompany.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def create_logo(self, instance, logo):
        instance.save_file(logo)

    def update_logo(self, instance, logo):
        instance.delete_file()
        instance.save_file(logo)

    def get_latest(self, database="default"):
        try:
            return OwnCompany.objects.db_manager(database).latest('created_at')
        except:
            return None

    def get_all(self, database="default"):
        return OwnCompany.objects.db_manager(database).all()

    def get_by_id(self, id, database="default"):
        return OwnCompany.objects.db_manager(database).filter(id=id).first()

    def get_by_ids(self, filters, database="default"):
        return OwnCompany.objects.db_manager(database).filter(*filters)

    def exist(self, id, database="default"):
        return OwnCompany.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return OwnCompany.objects.db_manager(database).filter(id=id).delete()


class OwnCompanyPartnerSerializer(serializers.ModelSerializer):
    """
    Own company serializer with all fields
    """

    class Meta:
        model = OwnCompany
        fields = ("name", "nit", "city", "address", "phone",)
