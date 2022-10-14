from api_partner.helpers import DB_USER_PARTNER
from core.helpers import (
    CurrencyPartner,
    IdentificationType,
)
from django.db import models
from django.db.models import Q
from django.utils.translation import gettext as _


class WithdrawalHistory(models.Model):
    """
    """
    adviser = models.ForeignKey(to="api_admin.Admin", on_delete=models.CASCADE)
    own_company_info_id = models.BigIntegerField()

    @property
    def own_company_info(self):
        """
        Get own company info on User DB
        """
        from api_partner.models import OwnCompany
        filters = [Q(id=self.own_company_info_id)]
        return OwnCompany.objects.using(DB_USER_PARTNER).filter(*filters).first()

    first_name = models.CharField(_('first name'), max_length=150, blank=True)
    second_name = models.CharField(_('second name'), max_length=150, blank=True)
    last_name = models.CharField(_('last name'), max_length=150, blank=True)
    second_last_name = models.CharField(_('second last name'), max_length=150, blank=True)
    identification = models.CharField(max_length=60)
    identification_type = models.SmallIntegerField()
    withdrawal_at = models.DateField(auto_now_add=True)

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
    status = models.SmallIntegerField(default=Status.NOT_READY)
    value = models.FloatField()
    currency = models.CharField(max_length=3)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Withdrawal history"
        verbose_name_plural = "Withdrawal histories"

    def __str__(self):
        return f"Full name: {self.adviser.user.full_name()} -  value: {self.value}"
