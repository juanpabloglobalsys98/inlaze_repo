from api_partner.helpers import PartnerStatusCHO
from django.db import models
from django.utils.translation import gettext as _


class PartnerBankValidationRequest(models.Model):
    """
    Contains partner bank info to be validated.
    """
    partner = models.ForeignKey(
        to="api_partner.Partner",
        related_name="bank_validation_requests",
        on_delete=models.CASCADE,
    )
    adviser_id = models.BigIntegerField(null=True)
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
    is_company = models.BooleanField(default=False)
    company_name = models.CharField(max_length=255, blank=True)
    company_reg_number = models.CharField(max_length=255, blank=True)
    status = models.SmallIntegerField(default=PartnerStatusCHO.REQUESTED)
    code_id = models.IntegerField(default=0)

    class ErrorField(models.IntegerChoices):
        BILLING_COUNTRY = 0, _("Billing country")
        BILLING_CITY = 1, _("Billing City")
        BILLING_ADDRESS = 2, _("Billing address")
        BANK_NAME = 3, _("Bank name")
        ACCOUNT_TYPE = 4, _("Account type")
        ACCOUNT_NUMBER = 5, _("Account number")
        SWIFT_CODE = 6, _("Swift code")
        COMPANY_NAME = 7, _("Company name")
        COMPANY_REG_NUMBER = 8, _("Company Register Number")
    error_fields = models.CharField(max_length=255, default="[]")

    answered_at = models.DateTimeField(null=True, default=None)
    created_at = models.DateTimeField(auto_now_add=True)
