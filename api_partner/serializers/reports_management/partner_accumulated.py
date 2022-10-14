from rest_framework import serializers
from api_partner.models import PartnerLinkAccumulated
from django.db.models import Q
from django.db.models import Sum


class CampaignAccountSer(serializers.ModelSerializer):

    id_campaign = serializers.IntegerField()
    name = serializers.CharField()

    class Meta:
        model = PartnerLinkAccumulated
        fields = (
            "id_campaign",
            "name",
        )


class FixedCurrencyIncomeSerializer(serializers.ModelSerializer):

    class Meta:
        model = PartnerLinkAccumulated
        fields = ("currency_local",)

    def partner_fixed_currency_income(self, partner, database="default"):
        filters = [Q(partner=partner)]
        query_set = PartnerLinkAccumulated.objects.db_manager(database).filter(*filters)
        if query_set:
            return {"currency_local": query_set[0].currency_local, "total": query_set.aggregate(total=Sum('fixed_income_local')).get("total")}
        return None


class PartnerLinkAccumulatedBasicSerializer(serializers.ModelSerializer):
    """
    Partner link accumulated basic serializer with specific fields for querying purpose
    """

    class Meta:
        model = PartnerLinkAccumulated
        fields = ("id", "partner", "campaing",)

    def get_by_partner_and_campaign(self, partner, campaign, database="default"):
        filters = [Q(partner=partner), Q(campaign=campaign)]
        return PartnerLinkAccumulated.objects.db_manager(database).select_related("link_to_partner_link_accumulated"
                                                                                  ).filter(*filters)

    def get_by_campaign(self, campaign, database="default"):
        return PartnerLinkAccumulated.objects.db_manager(database).filter(campaign=campaign).select_related(
            "link_to_partner_link_accumulated").order_by("partner")


class PartnerLinkAccumulatedSerializer(serializers.ModelSerializer):

    campaign = serializers.SerializerMethodField("get_campaign")
    campaign_id = serializers.SerializerMethodField("get_campaign_id")

    class Meta:
        model = PartnerLinkAccumulated
        fields = (
            "id",
            "campaign_id",
            "prom_code",
            "is_assigned",
            "campaign",
            "status",
            "assigned_at",
        )

    def get_campaign(self, obj):
        return f"{obj.campaign.bookmaker.name} {obj.campaign.title}"

    def get_campaign_id(self, obj):
        return obj.campaign.id
