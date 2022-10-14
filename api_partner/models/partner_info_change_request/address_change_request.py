from core.helpers import CountryPartner
from django.db import models
from django.utils.translation import gettext as _


class AddressChangeRequest(models.Model):
    """
    """
    additional_info = models.OneToOneField("api_partner.AdditionalInfo", on_delete=models.CASCADE)

    country = models.CharField(max_length=3)
    fiscal_address = models.CharField(max_length=255)
    city = models.CharField(max_length=100)

    is_accepted = models.BooleanField(default=None, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Address change request"
        verbose_name_plural = "Address change requests"

    def __str__(self):
        return f"New address: {self.fiscal_address} \n city: {self.city}"
