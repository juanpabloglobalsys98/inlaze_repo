from api_admin.serializers.partners.partner_link_accumulated import (
    PartnerLinkAccumAdditionalBasicSer,
)
from api_partner.models import (
    Link,
    Partner,
)
from core.helpers import calc_fx
from django.utils.translation import gettext as _
from rest_framework import serializers


class LinkTableSer(serializers.ModelSerializer):
    """ serializer to link """

    bookmaker_name = serializers.CharField()
    campaign_title = serializers.CharField()
    information_partner = PartnerLinkAccumAdditionalBasicSer(
        source="partner_link_accumulated",
        allow_null=True,
    )
    percentage_cpa = serializers.FloatField(
        source="partner_link_accumulated.percentage_cpa",
        allow_null=True,
    )
    tracker = serializers.FloatField(
        source="partner_link_accumulated.tracker",
        allow_null=True,
    )
    tracker_deposit = serializers.FloatField(
        source="partner_link_accumulated.tracker_deposit",
        allow_null=True,
    )
    tracker_registered_count = serializers.FloatField(
        source="partner_link_accumulated.tracker_registered_count",
        allow_null=True,
    )
    tracker_first_deposit_count = serializers.FloatField(
        source="partner_link_accumulated.tracker_first_deposit_count",
        allow_null=True,
    )
    tracker_wagering_count = serializers.FloatField(
        source="partner_link_accumulated.tracker_wagering_count",
        allow_null=True,
    )
    assigned_at = serializers.DateTimeField(
        source="partner_link_accumulated.assigned_at",
        allow_null=True,
    )
    clicks = serializers.SerializerMethodField("get_clicks")
    last_click_at = serializers.SerializerMethodField("get_last_click_at")
    cpa_count = serializers.IntegerField(
        source="partner_link_accumulated.cpa_count",
        allow_null=True,
    )
    last_cpa_at = serializers.SerializerMethodField("get_last_cpa_at")
    fixed_income_unitary = serializers.FloatField(source="campaign.fixed_income_unitary")
    currency_fixed_income = serializers.CharField(source="campaign.currency_fixed_income")
    partner_level = serializers.IntegerField(
        source="partner_link_accumulated.partner_level",
        allow_null=True,
    )
    is_percentage_custom = serializers.BooleanField(
        source="partner_link_accumulated.is_percentage_custom",
        allow_null=True,
    )

    fx_book_local = serializers.SerializerMethodField("get_fx_book_local")
    default_percentage = serializers.SerializerMethodField("get_default_percentage")

    class Meta:
        model = Link
        fields = (
            "id",
            "partner_level",
            "bookmaker_name",
            "campaign_title",
            "default_percentage",
            "prom_code",
            "status",
            "url",
            "information_partner",
            "percentage_cpa",
            "is_percentage_custom",
            "fixed_income_unitary",
            "currency_fixed_income",
            "fx_book_local",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",
            "created_at",
            "assigned_at",
            "clicks",
            "last_click_at",
            "cpa_count",
            "last_cpa_at",
        )

    def get_fx_book_local(self, obj):
        fx_partner = self.context.get("fx_partner")
        currency_from_str = self.get_currency_fixed_income(obj).lower()
        partner_currency_str = "usd"
        fx_book_local = calc_fx(
            fx_partner=fx_partner,
            currency_from_str=currency_from_str,
            partner_currency_str=partner_currency_str,
        )

        return fx_book_local

    def get_default_percentage(self, obj):
        level_percentages = self.context.get("percentages")
        if obj.partner_link_accumulated is None:
            return None
        else:
            default_percentage = (
                level_percentages.percentages.get(str(obj.partner_link_accumulated.partner_level))
                * obj.partner_link_accumulated.campaign.default_percentage
            )
            return default_percentage

    def get_bookmaker(self, obj):
        return obj.campaign.bookmaker.name

    def get_campaign(self, obj):
        return f"{obj.campaign.bookmaker.name} {obj.campaign.title}"

    def get_information(self, obj):
        if obj.partner_link_accumulated:
            identification_number = \
                obj.partner_link_accumulated.partner.additionalinfo.identification

            identification_type = \
                obj.partner_link_accumulated.partner.additionalinfo.identification_type

            email = obj.partner_link_accumulated.partner._user.email

            first_name = obj.partner_link_accumulated.partner._user.first_name
            last_name = obj.partner_link_accumulated.partner._user.last_name

            return {
                "identification_number": identification_number,
                "identification_type": identification_type,
                "email": email,
                "first_name": first_name,
                "last_name": last_name
            }
        return None

    def get_percentagecpa(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.percentage_cpa
        return None

    def get_tracker(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.tracker
        return None

    def get_assigned_at(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.assigned_at
        return None

    def get_clicks(self, obj):
        return None

    def get_last_click_at(self, obj):
        return None

    def get_cpa_count(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.cpa_count
        return None

    def get_last_cpa_at(self, obj):
        return None

    def get_fixed_income_unitary(self, obj):
        return obj.campaign.fixed_income_unitary

    def get_currency_fixed_income(self, obj):
        return obj.campaign.currency_fixed_income


class ParnertAssignSer(serializers.ModelSerializer):
    """ serializer to partner """

    identification_number = serializers.CharField()
    identification_type = serializers.IntegerField()
    email = serializers.CharField()
    email = serializers.CharField()
    full_name = serializers.CharField()

    class Meta:
        model = Partner
        fields = (
            "user_id",
            "identification_number",
            "identification_type",
            "email",
            "full_name"
        )


class LinkSpecificSerializer(serializers.ModelSerializer):

    assigned_at = serializers.SerializerMethodField("get_assigned_at")
    click_count = serializers.SerializerMethodField("get_click_count")
    last_click_at = serializers.SerializerMethodField("get_last_click_at")
    cpa_count = serializers.SerializerMethodField("get_cpa_count")
    last_cpa_at = serializers.SerializerMethodField("get_last_cpa_at")

    class Meta:
        model = Link
        fields = (
            "created_at",
            "assigned_at",
            "click_count",
            "last_click_at",
            "cpa_count",
            "last_cpa_at",
        )

    def validate_if_partner(self, obj):
        if obj.partner_link_accumulated:
            return True
        return False

    def get_assigned_at(self, obj):
        if self.validate_if_partner(obj):
            return obj.partner_link_accumulated.assigned_at
        return None

    def get_click_count(self, obj):
        return 0

    def get_last_click_at(self, obj):
        return None

    def get_cpa_count(self, obj):
        if self.validate_if_partner(obj):
            return obj.partner_link_accumulated.cpa_count
        return None

    def get_last_cpa_at(self, obj):
        return None


class LinkAdviserPartnerSerializer(serializers.ModelSerializer):

    tracker = serializers.SerializerMethodField("get_tracker")
    fixed_income_campaign = serializers.SerializerMethodField("get_fixed_income_campaign")
    percentage_cpa = serializers.SerializerMethodField("get_percentage_cpa")
    partner_name = serializers.SerializerMethodField("get_partner_name")
    assigned_at = serializers.SerializerMethodField("get_assigned_at")
    click_count = serializers.SerializerMethodField("get_click_count")
    last_click_at = serializers.SerializerMethodField("get_last_click_at")
    cpa_count = serializers.SerializerMethodField("get_cpa_count")
    last_cpa_at = serializers.SerializerMethodField("get_last_cpa_at")

    class Meta:
        model = Link
        fields = (
            "url",
            "tracker",
            "fixed_income_campaign",
            "percentage_cpa",
            "prom_code",
            "status",
            "partner_name",
            "created_at",
            "assigned_at",
            "click_count",
            "last_click_at",
            "cpa_count",
            "last_cpa_at",
        )

    def get_tracker(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.tracker
        return None

    def get_fixed_income_campaign(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.fixed_income
        return None

    def get_percentage_cpa(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.percentage_cpa
        return None

    def get_partner_name(self, obj):
        if obj.partner_link_accumulated:
            return f"{obj.partner_link_accumulated.partner._user.first_name} {obj.partner_link_accumulated.partner._user.last_name}"
        return None

    def get_assigned_at(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.assigned_at
        return None

    def get_click_count(self, obj):
        return None

    def get_last_click_at(self, obj):
        return None

    def get_cpa_count(self, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.cpa_count
        return None

    def get_last_cpa_at(self, obj):
        return None


class LinkUpdateSer(serializers.ModelSerializer):
    class Meta:
        model = Link
        fields = (
            "status",
            "prom_code",
        )

    def validate_status(self, value):
        if (self.instance.status == Link.Status.ASSIGNED):
            raise serializers.ValidationError(_("The link has an user assigned"))
        return value
