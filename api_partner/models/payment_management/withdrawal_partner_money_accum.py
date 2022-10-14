from core.helpers import CurrencyPartner
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext as _


class WithdrawalPartnerMoneyAccum(models.Model):
    """
    """
    withdrawal_partner_money = models.ForeignKey(
        to="api_partner.WithdrawalPartnerMoney", on_delete=models.CASCADE,
        related_name="withdrawal_partner_money_accum_set")

    cpa_count = models.IntegerField()
    accum_at = models.DateField(default=timezone.now)

    fixed_income_usd = models.FloatField(default=0)

    fixed_income_eur = models.FloatField(default=0)
    fixed_income_eur_usd = models.FloatField(default=0)

    fixed_income_cop = models.FloatField(default=0)
    fixed_income_cop_usd = models.FloatField(default=0)

    fixed_income_mxn = models.FloatField(default=0)
    fixed_income_mxn_usd = models.FloatField(default=0)

    fixed_income_gbp = models.FloatField(default=0)
    fixed_income_gbp_usd = models.FloatField(default=0)

    fixed_income_pen = models.FloatField(default=0)
    fixed_income_pen_usd = models.FloatField(default=0)

    fx_partner = models.ForeignKey(to="api_partner.FxPartner", on_delete=models.CASCADE, related_name="fxpartner")
    fx_percentage = models.FloatField(default=1.0)

    fixed_income_local = models.FloatField()
    currency_local = models.CharField(max_length=3)
    partner_level = models.SmallIntegerField(null=True, default=None)

    class Meta:
        verbose_name = "Withdrawal Partner Accum"
        verbose_name_plural = "Withdrawals partner Accum"

    def __str__(self):
        return f"{self.withdrawal_partner_money} - accum count {self.cpa_count}"
