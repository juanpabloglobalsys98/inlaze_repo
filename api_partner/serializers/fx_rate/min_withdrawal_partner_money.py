from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import MinWithdrawalPartnerMoney
from api_partner.models.payment_management import min_withdrawal_partner_money
from rest_framework import serializers


class MinWithdrawalPartnerMoneySerializer(serializers.ModelSerializer):
    """
    Min withdrawal partner money serializer with all fields
    """

    class Meta:
        model = MinWithdrawalPartnerMoney
        fields = (
            "min_usd_by_level",
            "created_by",
            "created_at",
        )

    def create(self, validated_data):
        """
        """
        return MinWithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return MinWithdrawalPartnerMoney.objects.db_manager(database).filter(user=id).first()

    def min_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return MinWithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def get_latest(self, database="default"):
        try:
            return MinWithdrawalPartnerMoney.objects.db_manager(database).latest('created_at')
        except:
            return None

    def delete(self, id, database="default"):
        return MinWithdrawalPartnerMoney.objects.db_manager(database).filter(user=id).delete()
