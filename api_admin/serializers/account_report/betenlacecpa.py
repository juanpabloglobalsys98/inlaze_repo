from rest_framework import serializers
from api_partner.models import BetenlaceCPA
from core.models import User
from api_admin.helpers import DB_ADMIN
from django.db.models import Sum

class BetenlacecpaSerializer(serializers.ModelSerializer):

    partner_data = serializers.SerializerMethodField("get_partner")
    campaign_data = serializers.SerializerMethodField("get_campaign")


    class Meta:
        model = BetenlaceCPA
        fields = (
            "partner_data",
            "campaign_data"
        )

    def get_partner(self,obj):
        if obj.link.partner_link_accumulated:
            partner = obj.link.partner_link_accumulated.partner
            user = partner._user
            adviser = User.objects.using(DB_ADMIN).filter(
                id = partner.adviser_id
            ).first()
            if adviser:
                adviser = f"{adviser.first_name} {adviser.last_name}"
            else: adviser =  None
            return {
                "partner_id": user.id,
                "partner_full_name": f"{user.first_name} {user.last_name}",
                "phone":user.phone,
                "identification_number":partner.additionalinfo.identification,
                "identification_type":partner.additionalinfo.identification_type,
                "email":user.email,
                "adviser_full_name":adviser
            }
        return None
    
    def get_campaign(slef,obj):
        return {
            "campaign_title":\
                f"{obj.link.campaign.bookmaker.name} {obj.link.campaign.title}",
            "prom_code":obj.link.prom_code,
            "cpa_count":obj.Betenlacedailyreport_to_BetenlaceCPA.all().aggregate(Sum('cpa_count')).get("cpa_count__sum"),
            "registered_count":obj.Betenlacedailyreport_to_BetenlaceCPA.all().aggregate(
                Sum('registered_count')).get("registered_count__sum"),
        }

