from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import ValidationCodeRegister
from django.db.models.query_utils import Q
from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.validators import UniqueValidator


class ValidationCodeRegisterSerializer(serializers.ModelSerializer):
    """
    Validation code register serializer with all fields
    """

    password = serializers.CharField(required=False, allow_null=True)
    adviser_id = serializers.IntegerField(required=False, allow_null=True)
    expiration = serializers.DateTimeField(required=False, allow_null=True)
    phone = serializers.CharField(required=False, allow_null=True)
    code = serializers.CharField(required=False)
    email = serializers.EmailField(required=False)
    attempts = serializers.IntegerField(required=False)

    class Meta:
        model = ValidationCodeRegister
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return ValidationCodeRegister.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return ValidationCodeRegister.objects.db_manager(database).filter(
            id=id).first()

    def get_by_email(self, email, database="default"):
        return ValidationCodeRegister.objects.db_manager(database).filter(
            email=email).first()

    def get_by_code(self, code, database="default"):
        return ValidationCodeRegister.objects.db_manager(database).filter(
            code=code).first()

    def get_by_email_code(self, code, email, database="default"):
        filters = [Q(code=code, email=email)]
        return ValidationCodeRegister.objects.db_manager(database).filter(
            *filters).first()

    def get_all(self, filters, database="default"):
        return ValidationCodeRegister.objects.db_manager(database).filter(
            *filters).all()

    def delete(self, id, database="default"):
        return ValidationCodeRegister.objects.db_manager(database).filter(
            id=id).delete()


class ValidationCodeRegisterBasicSerializer(serializers.ModelSerializer):
    """
    Validation code register serializer with specific fields for updating and querying purposes
    """

    email = serializers.EmailField(required=False, validators=[
        UniqueValidator(
            queryset=ValidationCodeRegister.objects.all(),
            message=_("That email was already taken")
        ),
    ])

    class Meta:
        model = ValidationCodeRegister
        fields = ("email", "phone", "adviser_id", "expiration")

    def get_all(self, filters, database="default"):
        return ValidationCodeRegister.objects.db_manager(database).filter(
            *filters).all()
