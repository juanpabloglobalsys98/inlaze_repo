from core.helpers import (
    CurrencyCondition,
    CurrencyFixedIncome,
)
from django.db import models


class BetenlaceCPA(models.Model):
    """
    BetenlaceCPA acummulate the CPA data getted for links (every entry 
    have data of certain link) on current month. At start of every month 
    the the data on that entries will restart to default values
    """
    link = models.OneToOneField(to='api_partner.Link', on_delete=models.CASCADE, primary_key=True)

    deposit = models.FloatField(default=0)
    stake = models.FloatField(default=0)

    currency_condition = models.CharField(max_length=3)

    fixed_income = models.FloatField(default=0)
    net_revenue = models.FloatField(default=0)
    revenue_share = models.FloatField(default=0)

    currency_fixed_income = models.CharField(max_length=3)

    registered_count = models.IntegerField(default=0)
    cpa_count = models.IntegerField(default=0)
    first_deposit_count = models.IntegerField(default=0)
    wagering_count = models.IntegerField(default=0)

    updated_at = models.DateField(auto_now=True)

    class Meta:
        verbose_name = "Betenlace CPA"
        verbose_name_plural = "Betenlace CPAS"

    def __str__(self):
        return f"{self.link} CPA count: {self.cpa_count}"
