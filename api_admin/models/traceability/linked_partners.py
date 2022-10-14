from api_partner.helpers import DB_USER_PARTNER

from django.db import models
from django.db.models import Q


class LinkedPartner(models.Model):
    """
    """
    admin = models.ForeignKey(to="api_admin.Admin", on_delete=models.CASCADE)
    partner_id = models.BigIntegerField()

    @property
    def partner(self):
        """
        Get user core model on Admin DB, this is a nullable field
        """
        # Only if adviser_id is not null get instance
        from api_partner.models import Partner
        filters = [Q(id=self.partner_id)]
        return Partner.objects.using(DB_USER_PARTNER).filter(*filters).first()

    class Meta:
        verbose_name = "Linked Partner"
        verbose_name_plural = "Linked Partners"
        unique_together = ("admin", "partner_id")

    def __str__(self):
        return f"{self.admin} - {self.partner}"
