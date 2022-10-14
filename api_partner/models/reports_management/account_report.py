from core.helpers import (
    CurrencyCondition,
    CurrencyFixedIncome,
)
from django.db import models
from django.utils import timezone


class AccountReport(models.Model):
    link = models.ForeignKey(to="api_partner.Link", on_delete=models.CASCADE, related_name='accountreport_to_link')
    partner_link_accumulated = models.ForeignKey(
        to="api_partner.PartnerLinkAccumulated", on_delete=models.SET_NULL,
        related_name='accountreport_to_partner_link_accumulated', null=True, default=None)

    punter_id = models.CharField(max_length=50)

    deposit = models.FloatField(default=0)
    stake = models.FloatField(default=0)

    currency_condition = models.CharField(max_length=3)

    fixed_income = models.FloatField(default=0)
    net_revenue = models.FloatField(default=0)
    revenue_share = models.FloatField(default=0)
    revenue_share_cpa = models.FloatField(default=0)

    currency_fixed_income = models.CharField(max_length=3)

    cpa_betenlace = models.IntegerField(default=0)
    cpa_partner = models.IntegerField(default=0)

    cpa_at = models.DateField(null=True, default=None)
    registered_at = models.DateField(null=True, default=None)
    first_deposit_at = models.DateField(null=True, default=None)

    created_at = models.DateField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Account report"
        verbose_name_plural = "Account reports"
        unique_together = ("link", "punter_id",)

    def __str__(self):
        return f"{self.id} - {self.link.campaign.title}"
