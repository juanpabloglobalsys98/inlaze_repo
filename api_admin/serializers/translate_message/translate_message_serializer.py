from api_admin.models import (
    CodeReason,
    TranslateMessage,
)
from api_partner.models.authentication.partner import User
from rest_framework import serializers


class CreateCodeSER(serializers.ModelSerializer):
    class Meta:
        model = CodeReason
        fields = (
            "pk",
            "code",
            "code_int",
            "title",
            "type_code",
            "is_active",
            "created_at",
            "updated_at",
        )


class CreateMsgSER(serializers.ModelSerializer):

    code_name = serializers.CharField()

    class Meta:
        model = TranslateMessage
        fields = (
            "pk",
            "language",
            "message",
            "code_name",
            "is_active",
            "created_at",
            "updated_at",
        )


class GetMsgSER(serializers.ModelSerializer):
    code_id = serializers.IntegerField()
    type_code = serializers.CharField()
    title = serializers.CharField()
    code_name = serializers.CharField()

    class Meta:
        model = TranslateMessage
        fields = (
            "pk",
            "language",
            "message",
            "code_id",
            "type_code",
            "title",
            "code_name",
            "is_active",
            "created_at",
            "updated_at",
        )


class PartnerMessageSER(serializers.ModelSerializer):

    class Meta:
        model = TranslateMessage
        fields = (
            "pk",
            "language",
            "message",
        )


class PartnerLanguageSER(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = (
            "language",
        )
