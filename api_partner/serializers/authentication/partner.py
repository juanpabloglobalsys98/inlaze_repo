import re
from xmlrpc.client import Boolean

from api_admin.helpers import DB_ADMIN
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import (
    DocumentPartner,
    Partner,
)
from api_partner.models.authentication.partner_request import PartnerLevelRequest
from api_partner.models.partner_info_change_request.partner_bank_validation import PartnerBankValidationRequest
from api_partner.models.partner_info_change_request.partner_info_validation import PartnerInfoValidationRequest
from core.models import User
from core.serializers import DynamicFieldsModelSerializer
from django.db.models import Q
from django.db.models.expressions import Value
from django.db.models.functions import Concat
from rest_framework import serializers


class DateBasicRequestSER(serializers.ModelSerializer):
    class Meta:
        model = PartnerInfoValidationRequest
        fields = (
            "pk",
            "created_at",
        )


class DateBankRequestSER(serializers.ModelSerializer):
    class Meta:
        model = PartnerBankValidationRequest
        fields = (
            "pk",
            "created_at",
        )


class DateLevelRequestSER(serializers.ModelSerializer):
    class Meta:
        model = PartnerLevelRequest
        fields = (
            "pk",
            "created_at",
        )


class DynamicPartnerSER(DynamicFieldsModelSerializer):
    """
    A dynamic serializer that returns the specified fields.
    Should be called with a fields arg, listing the required fields.
    """
    id = serializers.IntegerField()
    full_name = serializers.SerializerMethodField("get_full_name")
    phone = serializers.CharField()
    country = serializers.CharField()
    email = serializers.EmailField()
    partner_level = serializers.IntegerField()
    partner_docs = serializers.SerializerMethodField("get_partner_docs")
    is_banned = serializers.BooleanField()
    is_active = serializers.SerializerMethodField("get_is_active")
    last_basic_validation = DateBasicRequestSER(many=True)
    last_bank_validation = DateBankRequestSER(many=True)
    last_level_validation = DateLevelRequestSER(many=True)

    class Meta:
        model = Partner
        fields = "__all__"

    def get_full_name(self, obj):
        return obj.user.get_full_name()

    def get_partner_docs(self, obj):
        if hasattr(obj, "documents_partner"):
            return PartnerDocumentSER(
                instance=obj.documents_partner,
            ).data

        return None

    def get_is_banned(self, obj):
        return obj.user.is_banned

    def get_is_active(self, obj):
        return obj.user.is_active


class PartnerDocumentSER(serializers.ModelSerializer):
    class Meta:
        model = DocumentPartner
        fields = (
            "document_id_front_file",
            "document_id_back_file",
            "selfie_file",
        )


class PartnerSerializer(serializers.ModelSerializer):
    """
    Partner general serializer with all fields
    """

    basic_info_status = serializers.IntegerField(required=False, allow_null=True)
    bank_status = serializers.IntegerField(required=False, allow_null=True)
    documents_status = serializers.IntegerField(required=False, allow_null=True)
    adviser_id = serializers.IntegerField(required=False, allow_null=True)
    is_enterprise = serializers.BooleanField(required=False, allow_null=True)
    was_linked = serializers.BooleanField(required=False, allow_null=True)
    agreement = serializers.BooleanField(required=False, allow_null=True)
    full_registered_at = serializers.DateTimeField(required=False, allow_null=True)
    is_email_valid = serializers.BooleanField(required=False)
    is_phone_valid = serializers.BooleanField(required=False)
    is_notify_campaign = serializers.BooleanField(required=False)
    is_notify_notice = serializers.BooleanField(required=False)
    terms_at = serializers.DateTimeField(required=False)

    class Meta:
        model = Partner
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return Partner.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return Partner.objects.db_manager(database).filter(user=id).first()

    def get_basic_data(self, id, database="database"):
        return Partner.objects.db_manager(database).filter(user=id).select_related("additionalinfo", "company")[0]

    def get_bank_data(self, id, database="database"):
        return Partner.objects.db_manager(database).filter(user=id).select_related("bankaccount")[0]

    def get_documents_data(self, id, database="database"):
        return Partner.objects.db_manager(database).filter(user=id).select_related(
            "company", "company__documents_company", "documents_partner")[0]

    def get_all_partner_data(self, id, database="database"):
        return Partner.objects.db_manager(database).filter(user=id).select_related(
            "additionalinfo", "company", "bankaccount", "company__documents_company", "documents_partner"
        )[0]

    def delete(self, id, database="default"):
        return Partner.objects.db_manager(database).filter(user=id).delete()

    def partners_non_linked(self, database="default"):
        filters = [Q(was_linked=False)]
        return Partner.objects.db_manager(database).filter(*filters).count()


class PartnerStatusSER(serializers.ModelSerializer):
    """
    Partner status serializer with status fields and full_registered_at for querying purpose
    """

    status = serializers.IntegerField(required=False)
    basic_info_status = serializers.IntegerField(required=False)
    bank_status = serializers.IntegerField(required=False)
    documents_status = serializers.IntegerField(required=False)
    full_registered_at = serializers.DateTimeField(required=False, allow_null=True)
    language = serializers.SerializerMethodField("get_language")

    class Meta:
        model = Partner
        fields = (
            "user_id",
            "status",
            "basic_info_status",
            "bank_status",
            "documents_status",
            "full_registered_at",
            "is_email_valid",
            "is_phone_valid",
            "is_notify_campaign",
            "is_notify_notice",
            "is_terms",
            "level_status",
            "alerts",
            "level",
            "language"
        )

    def create(self, validated_data):
        """
        """
        return Partner.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def get_language(self, id, database="default"):
        """
        """
        partner = Partner.objects.db_manager(database).filter(user=id).first()
        return partner.user.language

    def exist(self, id, database="default"):
        return Partner.objects.db_manager(database).filter(user=id).first()


class PartnersForAdvisersSerializer(serializers.ModelSerializer):
    """
    Partner for adviser serializer with required fields for querying purpose
    """
    email = serializers.EmailField(source="user.email")
    phone = serializers.CharField(source="user.phone")

    class Meta:
        model = Partner
        fields = ("user", "was_linked", "email", "phone", "basic_info_status",
                  "bank_status", "documents_status", "status")

    def by_adviser(self, adviser_id, order_by, database="default"):
        filters = [Q(adviser_id=adviser_id)]
        return Partner.objects.db_manager(database).filter(*filters).all().order_by(order_by)

    def by_adviser_actives(self, adviser_id, database="default"):
        filters = [Q(adviser_id=adviser_id, user__is_active=True)]
        return Partner.objects.db_manager(database).select_related("user").filter(*filters).all()

    def get_all(self, order_by, database="default"):
        return Partner.objects.db_manager(database).all().order_by(order_by)

    def get_all_actives(self, database="default"):
        return Partner.objects.db_manager(database).select_related("user").filter(user__is_active=True).all()

    def byuser(self, id, database="default"):
        return Partner.objects.db_manager(database).filter(user=id).first()

    def by_was_linked(self, was_linked, order_by, database="default"):
        filters = [Q(was_linked=was_linked)]
        return Partner.objects.db_manager(database).filter(*filters).all().order_by(order_by)

    def get_partners(self, filters, order_by, database="default"):
        return Partner.objects.db_manager(database).select_related(
            "user", "additionalinfo").filter(*filters).order_by(order_by)


class GeneralPartnerSER(serializers.ModelSerializer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        elements_remove = set(self.fields.keys()) - set(self.context.get("permissions"))
        for i in elements_remove:
            self.fields.pop(i)
    """
    General partner serializer with required fields for querying purpose
    """
    partner_full_name = serializers.SerializerMethodField(method_name="get_partner_full_name")
    adviser_full_name = serializers.SerializerMethodField(method_name="get_adviser_full_name")
    referred_full_name = serializers.SerializerMethodField(method_name="get_referred_full_name")
    ban_details = serializers.SerializerMethodField(method_name="get_ban_details")
    level = serializers.IntegerField()
    email = serializers.CharField()
    identification = serializers.CharField()
    identification_type = serializers.IntegerField()
    phone = serializers.CharField()
    country = serializers.CharField()
    date_joined = serializers.DateTimeField()
    # channel_type = serializers.IntegerField()
    # channel_url = serializers.CharField()
    last_login = serializers.CharField()
    is_active = serializers.BooleanField()
    is_banned = serializers.BooleanField()

    class Meta:
        model = Partner
        fields = (
            "pk",
            "level",
            "partner_full_name",
            "email",
            "identification",
            "identification_type",
            "phone",
            "country",
            "was_linked",
            "date_joined",
            "full_registered_at",
            "status",
            "is_enterprise",
            "agreement",
            "basic_info_status",
            "bank_status",
            "documents_status",
            "adviser_id",
            "adviser_full_name",
            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",
            "referred_by",
            "referred_full_name",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",
            "last_login",
            "is_active",
            "is_banned",
            "ban_details",
        )

    def get_partner_full_name(self, obj):
        full_name = obj.partner_full_name
        full_name = re.sub('\s+', ' ', full_name)
        return full_name.strip()

    def get_adviser_full_name(self, partner):
        advisers = self.context.get("advisers")

        adviser = next(
            filter(
                lambda adviser_i: adviser_i.pk == partner.adviser_id,
                advisers,
            ),
            None,
        )
        if (adviser is not None):
           return adviser.get_full_name()
        return None

    def get_referred_full_name(self, obj):
        full_name = obj.referred_full_name
        full_name = re.sub('\s+', ' ', full_name)
        return full_name.strip()

    def get_partners(self, filters, order_by, database="default"):

        return Partner.objects.db_manager(database).select_related(
            "user", "additionalinfo").annotate(
            full_name=Concat(
                'user__first_name', Value(" "),
                'user__second_name', Value(" "),
                'user__last_name', Value(" "),
                'user__second_last_name')).filter(*filters).order_by(order_by)

    def get_ban_details(self, partner):
        if not partner.user.is_banned:
            return None

        ban_unban_reasons = self.context.get("ban_unban_reasons")
        ban_unban_reason = next(
            (b for b in ban_unban_reasons if b.partner == partner),
            None,
        )
        if ban_unban_reason is None:
            return None

        code_reasons = self.context.get("code_reasons")
        code_reason = next(
            (c for c in code_reasons if c.pk == ban_unban_reason.code_reason_id),
            None,
        )

        advisers = self.context.get("advisers")
        adviser = next(
            (a for a in advisers if a.pk == ban_unban_reason.adviser_id),
            None,
        )
        return {
            "reason": code_reason.title if code_reason else None,
            "date": ban_unban_reason.created_at if ban_unban_reason else None,
            "adviser": adviser.get_full_name() if adviser else None,
        }


class PartnerBillingDetailSerializer(serializers.ModelSerializer):
    """
    Partner billing details serializer with required fields for querying purpose
    """

    full_name = serializers.SerializerMethodField(method_name="get_full_name")
    email = serializers.CharField(source="user.email")
    identification_number = serializers.CharField(source="additionalinfo.identification")
    identification_type = serializers.IntegerField(source="additionalinfo.identification_type")

    def get_full_name(self, partner):
        user = partner.user
        return user.first_name + " " + user.second_name + " " + user.last_name + " " + user.second_last_name

    class Meta:
        model = Partner
        fields = ("user_id", "full_name", "email", "identification_number", "identification_type")

    def get_partners(self, filters, database="default"):
        return Partner.objects.db_manager(database).select_related(
            "user", "additionalinfo").annotate(
            full_name=Concat(
                'user__first_name', Value(" "),
                'user__second_name', Value(" "),
                'user__last_name', Value(" "),
                'user__second_last_name')).filter(*filters)


class PartnerLogUpSerializer(serializers.ModelSerializer):

    email = serializers.CharField(source="user.email")
    phone = serializers.CharField(source="user.phone")

    class Meta:
        model = Partner
        fields = (
            "is_email_valid",
            "is_phone_valid",
            "status",
            "email",
            "phone",
        )


class PartnersGeneralAdviserSearchSER(serializers.ModelSerializer):

    full_name = serializers.SerializerMethodField('get_full_name')

    class Meta:
        model = User
        fields = (
            "id",
            "full_name",
            "email",
        )

    def get_partner_full_name(self, obj):
        if (obj is not None and obj.partner_full_name is not None):
            full_name = re.sub('\s+', ' ', obj.partner_full_name)
            return full_name.strip()
