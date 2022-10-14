from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import Company
from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.validators import UniqueTogetherValidator


class CompanySerializer(serializers.ModelSerializer):
    """
    Company general serializer with all fields
    """

    class Meta:
        model = Company
        fields = "__all__"
        validators = [
            UniqueTogetherValidator(
                queryset=Company.objects.all(),
                message=_('There is a partner with that company name and identification'),
                fields=("company_id", "social_reason",)
            ),
        ]

    def create(self, validated_data):
        """
        """
        return Company.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return Company.objects.db_manager(database).filter(partner=id).first()

    def delete(self, id, database="default"):
        return Company.objects.db_manager(database).filter(partner=id).delete()


class CompanyBasicSerializer(serializers.ModelSerializer):
    """
    Company serializer with specific fields for updating purpose
    """

    company_id = serializers.CharField(required=False)
    social_reason = serializers.CharField(required=False)

    class Meta:
        model = Company
        fields = ("partner", "company_id", "social_reason",)

    def create(self, validated_data):
        """
        """
        return Company.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return Company.objects.db_manager(database).filter(partner=id).first()

    def delete(self, id, database="default"):
        return Company.objects.db_manager(database).filter(partner=id).delete()


class CompanyRequiredInfoSerializer(serializers.ModelSerializer):
    """
    Company serializer with specific fields for querying purpose
    """

    class Meta:
        model = Company
        fields = ("social_reason", "company_id", )
        validators = [
            UniqueTogetherValidator(
                queryset=Company.objects.all(),
                message=_('There is a partner with that company name and identification'),
                fields=("company_id", "social_reason",)
            ),
        ]

    def create(self, validated_data):
        """
        """
        return Company.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return Company.objects.db_manager(database).filter(partner=id).first()

    def delete(self, id, database="default"):
        return Company.objects.db_manager(database).filter(partner=id).delete()
