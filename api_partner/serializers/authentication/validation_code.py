from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import ValidationCode
from django.db.models.query_utils import Q
from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.validators import (
    UniqueTogetherValidator,
    UniqueValidator,
)


class ValidationCodeSerializer(serializers.ModelSerializer):
    """
    Validation code serializer with all fields
    """

    email = serializers.EmailField(required=False, validators=[
        UniqueValidator(
            queryset=ValidationCode.objects.all(),
            message=_("That email was already taken")
        ),
    ])
    code = serializers.CharField(required=False)
    expiration = serializers.DateTimeField(required=False)
    attempts = serializers.IntegerField(required=False)

    class Meta:
        model = ValidationCode
        fields = "__all__"
        validators = [
            UniqueTogetherValidator(
                queryset=ValidationCode.objects.all(),
                message=_('The email cannot have one validation code with the same code'),
                fields=("email", "code",)
            ),
        ]

    def create(self, validated_data):
        """
        """
        return ValidationCode.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return ValidationCode.objects.db_manager(database).filter(id=id).first()

    def get_by_code(self, code, database="default"):
        return ValidationCode.objects.db_manager(database).filter(code=code).first()

    def get_by_email(self, email, database="default"):
        return ValidationCode.objects.db_manager(database).filter(
            email=email).first()

    def get_by_email_code(self, code, email, database="default"):
        filters = [Q(code=code, email=email)]
        return ValidationCode.objects.db_manager(database).filter(*filters).first()

    def get_all(self, filters, database="default"):
        return ValidationCode.objects.db_manager(database).filter(*filters).all()

    def delete(self, id, database="default"):
        return ValidationCode.objects.db_manager(database).filter(id=id).delete()


class ValidationCodePhase1BSer(serializers.ModelSerializer):

    class Meta:
        model = ValidationCode
        fields = ("__all__")
