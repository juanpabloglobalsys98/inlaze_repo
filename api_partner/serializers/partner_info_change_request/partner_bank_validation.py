from api_partner.models import PartnerBankValidationRequest
from core.serializers import DynamicFieldsModelSerializer


class PartnerBankValidationRequestSER(DynamicFieldsModelSerializer):

    class Meta:
        model = PartnerBankValidationRequest
        fields = (
            "pk",
            "partner",
            "adviser_id",
            "billing_country",
            "billing_city",
            "billing_address",
            "bank_name",
            "account_type",
            "account_number",
            "swift_code",
            "is_company",
            "company_name",
            "company_reg_number",
            "status",
            "error_fields",
            "answered_at",
            "created_at",
        )
