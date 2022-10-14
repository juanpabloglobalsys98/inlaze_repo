from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import AdditionalInfo
from django.utils.translation import gettext as _
from rest_framework import serializers
from rest_framework.validators import (
    UniqueTogetherValidator,
    UniqueValidator,
)


class AdditionalInfoSerializer(serializers.ModelSerializer):
    """
    Additional information general serializer with all fields
    """

    channel_url = serializers.URLField(validators=[
        UniqueValidator(
            queryset=AdditionalInfo.objects.all(),
            message=_("That link is associated with other partner")
        ),
    ])

    class Meta:
        model = AdditionalInfo
        fields = "__all__"
        validators = [
            UniqueTogetherValidator(
                queryset=AdditionalInfo.objects.all(),
                message=_('There is a partner with that identification type and identification number'),
                fields=("identification", "identification_type",)
            ),
        ]

    def create(self, validated_data):
        """
        """
        return AdditionalInfo.objects.db_manager(DB_USER_PARTNER).create(
            **validated_data)

    def exist(self, id, database="default"):
        return AdditionalInfo.objects.db_manager(database).filter(
            partner=id).first()

    def delete(self, id, database="default"):
        return AdditionalInfo.objects.db_manager(database).filter(
            partner=id).delete()


class RequiredAdditionalInfoSerializer(serializers.ModelSerializer):
    """
    Additional information serializer with all required fields to deliver necessary information
    """

    person_type = serializers.IntegerField(required=False)
    identification = serializers.CharField(required=False, allow_null=True)
    identification_type = serializers.IntegerField(required=False)
    country = serializers.CharField(required=False)
    person_type = serializers.IntegerField(required=False)

    class Meta:
        model = AdditionalInfo
        fields = (
            "identification",
            "identification_type",
            "country",
            "person_type",
        )
        validators = [
            UniqueTogetherValidator(
                queryset=AdditionalInfo.objects.all(),
                message=_('There is a partner with that identification type and identification number'),
                fields=("identification", "identification_type",)
            ),
        ]

    def exist(self, id, database="default"):
        return AdditionalInfo.objects.db_manager(database).filter(
            partner=id).first()
