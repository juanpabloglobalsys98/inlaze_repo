from time import time
from api_partner.models import (
    BetenlaceDailyReport,
    Link,
    PartnerLinkDailyReport,
)
from core.helpers import timezone_customer
from django.db.models import Q
from django.utils.timezone import datetime, timedelta
from rest_framework import serializers
from core.helpers import timezone_customer


class ClicksManagementSerializer(serializers.ModelSerializer):
    """ Serializer to clicks from admin """
    campaign = serializers.SerializerMethodField("get_campaign")
    cpa_partner = serializers.SerializerMethodField("get_cpa_partner")
    cpa_betenlace = serializers.SerializerMethodField("get_cpa_betenlace")
    registered_count = serializers.SerializerMethodField("get_registered_count")
    registered_count_partner = serializers.SerializerMethodField("get_registered_count_partner")
    email = serializers.SerializerMethodField("get_email")
    first_deposit_count = serializers.SerializerMethodField("get_first_deposit")
    first_deposit_count_partner = serializers.SerializerMethodField("get_first_deposit_partner")
    wagering_count = serializers.SerializerMethodField("get_wagering_count")
    wagering_count_partner = serializers.SerializerMethodField("get_wagering_count_partner")
    revenue_share = serializers.SerializerMethodField("get_shared_revenue")
    stake = serializers.SerializerMethodField("get_stake")
    deposit = serializers.SerializerMethodField("get_deposit")
    deposit_partner = serializers.SerializerMethodField("get_deposit_partner")
    net_revenue = serializers.SerializerMethodField("get_net_revenue")
    status = serializers.SerializerMethodField("get_status")
    created_at = serializers.SerializerMethodField("get_created_at")

    class Meta:
        model = Link
        fields = (
            "id",
            "campaign",
            "prom_code",
            "cpa_partner",
            "cpa_betenlace",
            "created_at",
            "registered_count",
            "registered_count_partner",
            "email",
            "deposit",
            "first_deposit_count_partner",
            "stake",
            "revenue_share",
            "first_deposit_count",
            "wagering_count_partner",
            "wagering_count",
            "deposit_partner",
            "status",
            "net_revenue"
        )

    def get_campaign(self, obj):
        return f"{obj.campaign.bookmaker.name} {obj.campaign.title}"

    def get_cpa_partner(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        if obj.partner_link_accumulated:
            partner_daily = PartnerLinkDailyReport.objects.filter(
                Q(partner_link_accumulated=obj.partner_link_accumulated),
                Q(created_at=today)
            ).first()
            if partner_daily:
                return partner_daily.cpa_count
        return None

    def get_cpa_betenlace(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.cpa_count
        return None

    def get_registered_count(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.registered_count
        return None

    def get_registered_count_partner(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if hasattr(betenlacedailyreport, "partnerlinkdailyreport"):
            return betenlacedailyreport.partnerlinkdailyreport.registered_count
        return None

    def get_email(slef, obj):
        if obj.partner_link_accumulated:
            return obj.partner_link_accumulated.partner.user.email
        return None

    def get_betdaily_id(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.id
        return None

    def get_partnerdaily_id(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        if obj.partner_link_accumulated:
            partner_daily = PartnerLinkDailyReport.objects.filter(
                Q(partner_link_accumulated=obj.partner_link_accumulated),
                Q(created_at=today)
            ).first()
            if partner_daily:
                return partner_daily.id
        return None

    def get_first_deposit(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.first_deposit_count
        return None

    def get_first_deposit_partner(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if hasattr(betenlacedailyreport, "partnerlinkdailyreport"):
            return betenlacedailyreport.partnerlinkdailyreport.first_deposit_count
        return None

    def get_wagering_count(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.wagering_count
        return None

    def get_wagering_count_partner(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if hasattr(betenlacedailyreport, "partnerlinkdailyreport"):
            return betenlacedailyreport.partnerlinkdailyreport.wagering_count
        return None

    def get_shared_revenue(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.revenue_share
        return None

    def get_stake(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.stake
        return None

    def get_deposit(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.deposit
        return None

    def get_deposit_partner(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if hasattr(betenlacedailyreport, "partnerlinkdailyreport"):
            return betenlacedailyreport.partnerlinkdailyreport.deposit
        return None

    def get_net_revenue(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        betenlacedailyreport = BetenlaceDailyReport.objects.filter(
            Q(betenlace_cpa__link=obj),
            Q(created_at=today)
        ).first()
        if betenlacedailyreport:
            return betenlacedailyreport.net_revenue
        return None

    def get_created_at(self, obj):
        today = (timezone_customer(datetime.now()) - timedelta(days=1)).date()
        return today

    def get_status(self, obj):
        return obj.campaign.status
