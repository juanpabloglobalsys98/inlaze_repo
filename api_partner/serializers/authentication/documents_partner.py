from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models.authentication.document_partner import DocumentPartner
from rest_framework import serializers


class DocumentsPartnerSerializer(serializers.ModelSerializer):
    """
    Documents partner general serializer with all fields
    """

    document_id_front_file = serializers.FileField(required=False, allow_null=True)
    document_id_back_file = serializers.FileField(required=False, allow_null=True)
    selfie_file = serializers.FileField(required=False, allow_null=True)

    class Meta:
        model = DocumentPartner
        fields = "__all__"

    def create(self, validated_data):
        return DocumentPartner.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def create_file(self, file_type, instance, file):

        if file_type == "document_id_front_file":
            instance.create_document_id_front_file(file)
            instance.save()
        elif file_type == "document_id_back_file":
            instance.create_document_id_back_file(file)
            instance.save()
        else:
            instance.create_selfie_file(file)
            instance.save()

    def update_file(self, file_type, instance, file):

        if file_type == "document_id_front_file":
            instance.delete_document_id_front_file()
            instance.update_document_id_front_file(file)
            instance.save()
        elif file_type == "document_id_back_file":
            instance.delete_document_id_back_file()
            instance.update_document_id_back_file(file)
            instance.save()
        else:
            instance.delete_selfie_file()
            instance.update_selfie_file(file)
            instance.save()

    def exist(self, id, database="default"):
        return DocumentPartner.objects.db_manager(database).filter(
            partner=id).first()

    def get_by_partner(self, user_id, database="default"):
        return DocumentPartner.objects.db_manager(database).filter(partner=user_id).first()

    def delete(self, id, database="default"):
        return DocumentPartner.objects.db_manager(database).filter(
            partner=id).delete()


class RequiredDocumentsPartnerSER(serializers.ModelSerializer):
    """
    Documents partner serializer with specific fields for querying purpose
    """

    class Meta:
        model = DocumentPartner
        fields = (
            "document_id_front_file",
            "document_id_back_file",
            "selfie_file",
        )

    def create(self, validated_data):
        return DocumentPartner.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def create_file(self, file_type, instance, file):
        if file_type == "document_id_front_file":
            instance.create_document_id_front_file(file)
            instance.save()
        elif file_type == "document_id_back_file":
            instance.create_document_id_back_file(file)
            instance.save()
        else:
            instance.create_selfie_file(file)
            instance.save()

    def update_file(self, file_type, instance, file):
        if file_type == "document_id_front_file":
            instance.delete_document_id_front_file()
            instance.update_document_id_front_file(file)
            instance.save()
        elif file_type == "document_id_back_file":
            instance.delete_document_id_back_file()
            instance.update_document_id_back_file(file)
            instance.save()
        else:
            instance.delete_selfie_file()
            instance.update_selfie_file(file)
            instance.save()

    def exist(self, id, database="default"):
        return DocumentPartner.objects.db_manager(database).filter(
            partner=id).first()

    def get_by_partner(self, user_id, database="default"):
        return DocumentPartner.objects.db_manager(database).filter(partner=user_id).first()

    def delete(self, id, database="default"):
        return DocumentPartner.objects.db_manager(database).filter(
            partner=id).delete()
