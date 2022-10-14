import ast
from rest_framework import serializers
from api_partner.models import PartnerInfoValidationRequest


class PartnerInfoValidationRequestSER(serializers.ModelSerializer):

    class Meta:
        model = PartnerInfoValidationRequest
        fields = (
            "partner",
            "first_name",
            "second_name",
            "last_name",
            "second_last_name",
            "current_country",
            "id_type",
            "id_number",
            "status",
            "error_fields",
            "answered_at",
            "created_at",
            "document_id_front_file",
            "document_id_back_file",
            "selfie_file",
        )


class PartnerInfoValidationRequestREADSER(serializers.ModelSerializer):

    error_fields_read = serializers.SerializerMethodField("get_error_fields_read")

    class Meta:
        model = PartnerInfoValidationRequest
        fields = (
            "pk",
            "partner",
            "first_name",
            "second_name",
            "last_name",
            "second_last_name",
            "current_country",
            "id_type",
            "id_number",
            "status",
            "error_fields",
            "error_fields_read",
            "answered_at",
            "created_at",
            "document_id_front_file",
            "document_id_back_file",
            "selfie_file",
        )

    def get_error_fields_read(self, obj):
        if obj.error_fields:
            return ast.literal_eval(obj.error_fields)
        return None
