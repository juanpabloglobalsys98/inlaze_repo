from api_admin.helpers import DB_ADMIN
from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import Q

User = get_user_model()


class InactiveHistory(models.Model):
    partner = models.ForeignKey(to="api_partner.Partner", on_delete=models.CASCADE)
    active_inactive_code_reason = models.ForeignKey(
        to="api_partner.InactiveActiveCodeReason", on_delete=models.CASCADE)

    adviser_id = models.BigIntegerField(default=None, null=True)

    @property
    def adviser(self):
        """
        Get user core model on Admin DB, this is a nullable field
        """
        # Only if adviser_id is not null get instance
        if self.adviser_id:
            filters = [Q(id=self.adviser_id)]
            return User.objects.using(DB_ADMIN).filter(*filters).first()

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Inactive History"
        verbose_name_plural = "Inactive Histories"

    def __str__(self):
        return f"{self.partner} - {self.active_inactive_code_reason}"
