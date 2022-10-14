from api_admin.helpers import DB_ADMIN
from api_admin.models import ValidationCode
from django.db.models.query_utils import Q
from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.validators import (
    UniqueTogetherValidator,
    UniqueValidator,
)


class ValidationCodeSer(serializers.ModelSerializer):
    """
    Validation code serializer with all fields
    """

    email = serializers.EmailField(
        required=True,
        validators=[
            UniqueValidator(
                queryset=ValidationCode.objects.all(),
                message=_("That email was already taken"),
            ),
        ],
    )

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
