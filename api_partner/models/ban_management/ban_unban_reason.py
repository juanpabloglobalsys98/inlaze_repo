from api_admin.helpers import DB_ADMIN
from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import Q
from django.utils.translation import gettext as _

User = get_user_model()


class BanUnbanReason(models.Model):
    """
    """
    partner = models.ForeignKey(
        to="api_partner.Partner",
        on_delete=models.CASCADE,
    )
    ban_unban_code_reason = models.ForeignKey(
        to="api_partner.BanUnbanCodeReason",
        on_delete=models.CASCADE,
        null=True,
        default=None,
    )
    code_reason_id = models.BigIntegerField(null=True)
    adviser_id = models.BigIntegerField()

    @property
    def adviser(self):
        """
        Get user core model on Admin DB, this is a nullable field
        """
        # Only if adviser_id is not null get instance
        if self.adviser_id:
            filters = [Q(id=self.adviser_id)]
            return User.objects.using(DB_ADMIN).filter(*filters).first()

    created_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Ban unban reason"
        verbose_name_plural = "Ban unban reasons"

    def __str__(self):
        return f"banned partner: {self.partner.user.get_full_name()} \n ban reason: {self.ban_unban_code_reason.reason}"
