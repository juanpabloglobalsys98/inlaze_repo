from api_partner.helpers import PartnerAccumStatusCHO
from api_partner.models import (
    Campaign,
    PartnerLinkAccumulated,
)
from core.helpers import CurrencyAll
from rest_framework import serializers


class CampaignPartnerSerializer(serializers.ModelSerializer):
    bookmaker_name = serializers.CharField()
    campaign_name = serializers.CharField()
    image_url = serializers.SerializerMethodField("get_image_url")
    countries = serializers.SerializerMethodField("get_countries")
    prom_code = serializers.SerializerMethodField("get_prom_code")
    fixed_income_unitary = serializers.SerializerMethodField("get_fixed_income_unitary")
    currency_fixed_income_partner = serializers.SerializerMethodField("get_currency_fixed_income_partner")
    date_to_assign = serializers.SerializerMethodField("get_date_assign")
    deposit_dollar = serializers.SerializerMethodField("get_deposit_dollar")
    status_partner = serializers.SerializerMethodField("get_status_partner")

    class Meta:
        model = Campaign
        fields = (
            "id",
            "prom_code",
            "bookmaker_name",
            "campaign_name",
            "image_url",
            "countries",
            "fixed_income_unitary",
            "currency_fixed_income_partner",
            "currency_condition_campaign_only",
            "status",
            "status_partner",
            "temperature",
            "date_to_assign",
            "deposit_condition_campaign_only",
            "stake_condition_campaign_only",
            "lose_condition_campaign_only",
            "last_inactive_at",
            "deposit_dollar",
            "has_links",
        )

    def get_title(self, obj):
        return f"{obj.bookmaker.name} {obj.title}"

    def get_countries(self, obj):
        return eval(obj.countries)

    def get_image_url(self, obj):
        return obj.bookmaker.image.url

    def get_prom_code(self, obj):
        link = obj.link if hasattr(obj, 'link') else None
        if link:
            return link[0].prom_code
        return None

    def get_fixed_income_unitary(self, obj):
        partner_acumulated = (
            obj.partner_link_accumulated
            if hasattr(obj, 'partner_link_accumulated')
            else
            None
        )
        level = self.context.get("level")
        partner = self.context.get("partner")

        if(partner_acumulated):
            level_percentage = level.get(str(partner_acumulated[0].partner_level))
            # prefetch related of certain user show a list for each campaign
            # with a single value
            return (obj.fixed_income_unitary_usd*partner_acumulated[0].percentage_cpa)

        level_percentage = level.get(str(partner.level))
        return obj.fixed_income_unitary_usd*(obj.default_percentage*level_percentage)

    def get_currency_fixed_income_partner(self, obj):
        return CurrencyAll.USD

    def get_date_assign(self, obj):
        partner_acumulated = obj.partner_link_accumulated if hasattr(obj, 'partner_link_accumulated') else None
        if(partner_acumulated):
            # prefetch related of certain user show a list for each campaign
            # with a single value
            return partner_acumulated[0].assigned_at
        return None

    def get_deposit_dollar(self, obj):
        fx_partner_percentage = self.context.get("fx_partner_percentage")
        fx_partner = self.context.get("fx_partner")
        currency_condition = obj.currency_condition.lower()
        if currency_condition != "usd":
            try:
                fx_book_partner = eval(f"fx_partner.fx_{currency_condition}_usd") * fx_partner_percentage
            except Exception as e:
                pass
        else:
            fx_book_partner = 1

        return obj.deposit_condition * fx_book_partner

    def get_status_partner(self, obj):
        if not obj.partner_link_accumulated:
            return PartnerAccumStatusCHO.BY_CAMPAIGN
        return obj.partner_link_accumulated[0].status


class CampaignPartnerBasicSER(serializers.ModelSerializer):

    campaign_title = serializers.CharField()

    class Meta:
        model = Campaign
        fields = (
            "campaign_title",
            "id",
            "status",
        )
