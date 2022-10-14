from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import (
    BankAccount,
    PartnerBankAccount,
)
from core.serializers import DynamicFieldsModelSerializer
from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.validators import UniqueTogetherValidator


class BankAccountSerializer(serializers.ModelSerializer):
    """
    Bank account general serializer with all fields
    """

    class Meta:
        model = BankAccount
        fields = "__all__"
        validators = [
            UniqueTogetherValidator(
                queryset=BankAccount.objects.all(),
                message=_('There is a partner with that account type, account number and swift code in the system'),
                fields=("account_number", "account_type", "swift_code",)
            ),
        ]

    def create(self, validated_data):
        """
        """
        return BankAccount.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return BankAccount.objects.db_manager(database).filter(
            partner=id).first()

    def delete(self, id, database="default"):
        return BankAccount.objects.db_manager(database).filter(
            partner=id).delete()


class BankAccountBasicSerializer(serializers.ModelSerializer):
    """
    Bank account serializer with specific fields for updating purpose
    """

    bank_name = serializers.CharField(required=False, allow_null=True)
    account_number = serializers.CharField(required=False, allow_null=True)
    account_type = serializers.IntegerField(required=False, allow_null=True)
    swift_code = serializers.CharField(required=False, allow_null=True)

    class Meta:
        model = BankAccount
        fields = (
            "partner",
            "bank_name",
            "account_number",
            "account_type",
            "swift_code",
        )

        validators = [
            UniqueTogetherValidator(
                queryset=BankAccount.objects.all(),
                message=_('There is a partner with that account type, account number and swift code in the system'),
                fields=(
                    "bank_name",
                    "account_number",
                    "account_type",
                    "swift_code",
                ),
            ),
            UniqueTogetherValidator(
                queryset=BankAccount.objects.all(),
                message=_('There is a partner with that account type, account number and swift code in the system'),
                fields=(
                    "bank_name",
                    "account_number",
                    "account_type",
                ),
            ),
            UniqueTogetherValidator(
                queryset=BankAccount.objects.all(),
                message=_('There is a partner with that account number in the system'),
                fields=(
                    "bank_name",
                    "account_number",
                ),
            ),
        ]

    def create(self, validated_data):
        return BankAccount.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return BankAccount.objects.db_manager(database).filter(partner=id).first()

    def delete(self, id, database="default"):
        return BankAccount.objects.db_manager(database).filter(partner=id).delete()


class BankAccountRequiredInfoSerializer(serializers.ModelSerializer):
    """
    Bank account serializer with specific fields for querying purpose
    """

    class Meta:
        model = BankAccount
        fields = ("bank_name", "account_number", "account_type",
                  "swift_code",)

    def exist(self, id, database="default"):
        return BankAccount.objects.db_manager(database).filter(
            partner=id).first()


class PartnerBankAccountSER(DynamicFieldsModelSerializer):

    class Meta:
        model = PartnerBankAccount
        fields = (
            "pk",
            "partner",
            "billing_country",
            "billing_address",
            "billing_city",
            "bank_name",
            "account_type",
            "account_number",
            "swift_code",
            "updated_at",
            "is_primary",
            "is_company",
            "is_active",
            "company_name",
            "company_reg_number",
        )


class PartnerBankFilesSER(serializers.ModelSerializer):

    class Meta:
        model = PartnerBankAccount
        fields = (
            "pk",
            "partner",
            "billing_country",
            "billing_address",
            "bank_name",
            "account_type",
            "account_number",
            "swift_code",
            "updated_at",
            "is_primary",
            "is_company",
            "company_name",
            "company_reg_number",
        )
