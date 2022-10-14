from django.db import models
from django.utils import timezone


class AccountDailyReport(models.Model):
    account_report = models.ForeignKey(
        to="api_partner.AccountReport",
        on_delete=models.CASCADE,
    )
    deposit = models.FloatField(default=0)
    stake = models.FloatField(default=0)

    currency_condition = models.CharField(max_length=3)

    fixed_income = models.FloatField(default=0)
    net_revenue = models.FloatField(default=0)
    revenue_share = models.FloatField(default=0)
    revenue_share_cpa = models.FloatField(default=0)

    currency_fixed_income = models.CharField(max_length=3)

    is_cpa_betenlace = models.BooleanField(default=False)
    is_cpa_partner = models.BooleanField(default=False)
    is_first_deposit_count = models.BooleanField(default=False)

    created_at = models.DateField(default=timezone.now)

    def __str__(self):
        return f"{self.pk} - {self.account_report}"
