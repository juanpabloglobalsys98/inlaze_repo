from django.db import models
from django.utils.translation import gettext as _


class BankChangeRequest(models.Model):
    """
    """
    bank_account = models.OneToOneField("api_partner.BankAccount", on_delete=models.CASCADE)

    bank_name = models.CharField(max_length=150)
    account_number = models.CharField(max_length=100)

    class AccounType(models.IntegerChoices):
        SAVING_ACC = 0
        CHECKING_ACC = 1
    account_type = models.SmallIntegerField(null=True, default=None)
    swift_code = models.CharField(max_length=100, null=True)
    created_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Bank change request"
        verbose_name_plural = "Bank change requests"
        unique_together = ("account_number", "account_type", "swift_code")

    def __str__(self):
        return f"New bank: {self.name} - account type: {self.account_type}"
