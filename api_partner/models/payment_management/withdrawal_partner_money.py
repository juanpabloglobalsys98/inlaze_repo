import re

from core.helpers import (
    CountryAll,
    CurrencyFixedIncome,
    CurrencyPartner,
)
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext as _


class WithdrawalPartnerMoney(models.Model):
    """
    """
    partner = models.ForeignKey(to="api_partner.Partner", on_delete=models.CASCADE,
                                related_name="withdrawal_partner_money_accum_set")
    own_company = models.ForeignKey(to="api_partner.OwnCompany", on_delete=models.CASCADE,
                                    related_name="WithdrawalPartner_to_ownCompany")

    bank_account = models.ForeignKey(
        to="api_partner.PartnerBankAccount",
        on_delete=models.PROTECT,
        related_name="withdrawals",
        null=True,
        default=None,
    )
    first_name = models.CharField(max_length=150, blank=True)
    second_name = models.CharField(max_length=150, blank=True)
    last_name = models.CharField(max_length=150, blank=True)
    second_last_name = models.CharField(max_length=150, blank=True)

    email = models.EmailField()
    phone = models.CharField(max_length=50, null=True, default=None)
    country = models.CharField(max_length=3, null=True, default=None)
    city = models.CharField(max_length=50, null=True, default=None)
    address = models.CharField(max_length=255, null=True, default=None)

    identification = models.CharField(max_length=60, null=True, default=True)
    identification_type = models.IntegerField(null=True, default=True)

    billed_from_at = models.DateField(default=timezone.now)
    billed_to_at = models.DateField(default=timezone.now)

    cpa_count = models.IntegerField(default=0)

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

    fixed_income_local = models.FloatField(default=0)
    bill_rate = models.FloatField(default=0)
    currency_local = models.CharField(max_length=3)
    bill_bonus = models.FloatField(default=0)

    class Status(models.IntegerChoices):
        """
        It defines the kind of users in the program
        """
        NOT_READY = 0
        """
        Indicates that the invoice to partner is not yet eligible for payment.
        """
        TO_PAY = 1
        PAYED = 2
        REJECTED = 3
        NO_INFO = 4
    status = models.IntegerField(default=Status.NOT_READY)
    payment_at = models.DateTimeField(null=True, default=None)
    created_at = models.DateTimeField(default=timezone.now)

    def get_full_name(self):
        """
        Return the first_name plus the last_name, with a space in between.
        """
        full_name = '%s %s %s %s' % (self.first_name, self.second_name, self.last_name, self.second_last_name)
        full_name = re.sub('\s+', ' ', full_name)
        return full_name.strip()

    class Meta:
        verbose_name = "Withdrawal Partner"
        verbose_name_plural = "Withdrawals partner"

    def __str__(self):
        return f"{self.partner.user.get_full_name()} - email: {self.email} - max count {self.cpa_count}"
