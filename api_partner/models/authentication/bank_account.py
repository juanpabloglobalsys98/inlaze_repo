from core.helpers import (
    CountryAll,
    bad_request_response,
)
from django.db import models
from django.utils.translation import gettext as _


class PartnerBankAccount(models.Model):
    """
    Partner's bank account details.
    """
    partner = models.ForeignKey(
        to="api_partner.Partner",
        related_name="bank_accounts",
        on_delete=models.CASCADE,
    )
    billing_country = models.CharField(max_length=3)
    billing_city = models.CharField(max_length=255)
    billing_address = models.CharField(max_length=255)
    bank_name = models.CharField(max_length=150)

    class AccountType(models.IntegerChoices):
        SAVINGS = 0, _("Savings account")
        IBAN = 1, _("IBAN")
        PAYPAL = 2, _("PayPal")
        CHECKING = 3, _("Checking account")
    account_type = models.SmallIntegerField(null=True, default=None)

    account_number = models.CharField(max_length=100)
    swift_code = models.CharField(max_length=100, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_primary = models.BooleanField(default=False)
    is_company = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    company_name = models.CharField(max_length=255, blank=True)
    company_reg_number = models.CharField(max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Bank account"
        verbose_name_plural = "Bank accounts"

    def __str__(self):
        return f"Account name: {self.bank_name} - Account type: {self.account_type}"

    @classmethod
    def verify_account_type(cls, document):
        country = document.get("billing_country")
        account_type = document.get("account_type")
        swift_code = document.get("swift_code")

        field = msg = ""
        if account_type == cls.AccountType.IBAN and not swift_code:
            field = "swift_code"
            msg = _("Swift code is required for IBAN accounts")

        if country == CountryAll.COLOMBIA:
            if account_type == cls.AccountType.PAYPAL:
                field = "account_type"
                msg = _("{} not available for {}")
                msg = msg.format(cls.AccountType.PAYPAL.label, CountryAll.COLOMBIA.label)
        else:
            if account_type in (cls.AccountType.SAVINGS, cls.AccountType.CHECKING):
                field = "account_type"
                msg = _("Only {} and {} are allowed")
                msg = msg.format(cls.AccountType.IBAN.label, cls.AccountType.PAYPAL.label)

        if msg:
            return bad_request_response(
                detail={
                    field: [
                        msg
                    ],
                },
            )


class BankAccount(models.Model):
    """
    """
    partner = models.OneToOneField("api_partner.Partner", on_delete=models.CASCADE, primary_key=True)

    bank_name = models.CharField(max_length=150, null=True, default=None)
    account_number = models.CharField(max_length=100, null=True, default=None)

    class AccounType(models.IntegerChoices):
        SAVING_ACC = 0
        CHECKING_ACC = 1
    account_type = models.SmallIntegerField(null=True, default=None)

    swift_code = models.CharField(max_length=100, null=True, default=None)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Bank account"
        verbose_name_plural = "Bank accounts"
        unique_together = ("account_number", "account_type", "swift_code")

    def __str__(self):
        return f"Account name: {self.bank_name} - Account type: {self.account_type}"
