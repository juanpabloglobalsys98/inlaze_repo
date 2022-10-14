from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import DocumentCompany
from rest_framework import serializers


class DocumentsCompanySerializer(serializers.ModelSerializer):
    """
    Documents company general serializer with all fields
    """

    exist_legal_repr_file = serializers.FileField(
        required=False, allow_null=True)
    rut_file = serializers.FileField(required=False, allow_null=True)

    class Meta:
        model = DocumentCompany
        fields = "__all__"

    def create(self, validated_data):
        return DocumentCompany.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def create_file(self, file_type, instance, file):
        if file_type == "rut_file":
            instance.create_rut_file(file)
            instance.save()
        else:
            instance.create_exist_legal_repr_file(file)
            instance.save()

    def update_file(self, file_type, instance, file):
        if file_type == "rut_file":
            instance.delete_rut_file()
            instance.update_rut_file(file)
            instance.save()
        else:
            instance.delete_exist_legal_repr_file()
            instance.update_exist_legal_repr_file(file)
            instance.save()

    def exist(self, id, database="default"):
        return DocumentCompany.objects.db_manager(database).filter(company=id).first()

    def delete(self, id, database="default"):
        return DocumentCompany.objects.db_manager(database).filter(
            company=id).delete()


class RequiredDocumentsCompanySerializer(serializers.ModelSerializer):
    """
    Documents company serializer with specific fields for querying purpose 
    """

    class Meta:
        model = DocumentCompany
        fields = ("rut_file", "exist_legal_repr_file",)

    def exist(self, id, database="default"):
        return DocumentCompany.objects.db_manager(database).filter(
            company=id).first()
