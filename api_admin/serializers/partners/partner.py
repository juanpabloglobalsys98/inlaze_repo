from rest_framework import serializers
from api_partner.models import Partner

PHASE1 = 0
PHASE2 = 1
PHASE3 = 2
TO_VALIDATE = 3
FULL_REGISTER = 4
INACTIVE = 5
BANNED = 5


class PartnerFromAdviserSerializer(serializers.ModelSerializer):

    partner_full_name = serializers.SerializerMethodField("get_full_name")
    email = serializers.SerializerMethodField("get_email")
    country = serializers.SerializerMethodField("get_country")
    phone = serializers.SerializerMethodField("get_phone")
    channel_type = serializers.SerializerMethodField("get_channeltype")
    channel_url = serializers.SerializerMethodField("get_channelurl")
    date_joined = serializers.SerializerMethodField("get_date_joined")
    last_login = serializers.SerializerMethodField("get_last_login")
    last_cpa_at = serializers.SerializerMethodField("get_last_cpa_at")
    status = serializers.SerializerMethodField("get_status")

    class Meta:
        model = Partner
        fields = (
            "user_id",
            "partner_full_name",
            "email",
            "country",
            "phone",
            "channel_type",
            "channel_url",
            "date_joined",
            "last_login",
            "last_cpa_at",
            "status",
        )

    def get_full_name(self, obj):
        return f"{obj._user.first_name} {obj._user.last_name}"

    def get_email(self, obj):
        return obj.email

    def get_country(self, obj):
        return obj.additionalinfo.country

    def get_phone(self, obj):
        return obj._user.phone

    def get_phone(self, obj):
        return obj._user.phone

    def get_channeltype(self, obj):
        return obj.additionalinfo.channel_type

    def get_channelurl(self, obj):
        return obj.additionalinfo.channel_url

    def get_channelurl(self, obj):
        return obj.additionalinfo.channel_url

    def get_date_joined(self, obj):
        return obj._user.date_joined

    def get_last_login(self, obj):
        return None

    def get_last_cpa_at(self, obj):
        return None

    def get_email(self, obj):
        return obj._user.email

    def get_status(self, obj):
        return obj.status


class PartnerMemeberReportSerializer(serializers.ModelSerializer):

    identification_number = serializers.SerializerMethodField("get_identification")
    identification_type = serializers.SerializerMethodField("get_identification_type")
    email = serializers.SerializerMethodField('get_email')
    full_name = serializers.SerializerMethodField('get_full_name')

    class Meta:
        model = Partner
        fields = (
            "user_id",
            "identification_number",
            "identification_type",
            "email",
            "full_name"
        )

    def user_id(slef, obj):
        return obj._user.id

    def get_first_name(slef, obj):
        return obj._user.first_name

    def get_last_name(slef, obj):
        return obj._user.last_name

    def get_identification(slef, obj):
        if obj.additionalinfo:
            return obj.additionalinfo.identification

    def get_identification_type(slef, obj):
        if obj.additionalinfo:
            return obj.additionalinfo.identification_type

    def get_full_name(self, obj):
        return f"{obj._user.first_name} {obj._user.last_name}"

    def get_email(self, obj):
        return obj._user.email


class PartnerInRelationshipCampaign(serializers.ModelSerializer):
    from api_admin.serializers.reports_management.partnerlinkacumulated import PartnerLinkAccumulatedAdminSerializer

    name = serializers.CharField()
    email = serializers.CharField()
    campaigns_relationship = PartnerLinkAccumulatedAdminSerializer(source="partnerlinkaccumulated", many=True)
    identification_type = serializers.IntegerField()
    identification = serializers.CharField()

    class Meta:
        model = Partner
        fields = (
            "user_id",
            "email",
            "name",
            "campaigns_relationship",
            "campaign_status",
            "identification_type",
            "identification",
        )


class PartnerViewCampaignSerializer(serializers.ModelSerializer):
    campaign_status = serializers.IntegerField()

    class Meta:
        model = Partner
        fields = (
            "campaign_status",
        )
