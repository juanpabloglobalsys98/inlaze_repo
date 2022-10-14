from django.db import models
from django.utils.translation import gettext as _


class BanUnbanCodeReason(models.Model):
    """
    """
    title = models.CharField(max_length=255)
    reason = models.TextField()

    is_ban_reason = models.BooleanField(default=True)
    """
    Determines if the current reason is ban reason (True) or unban reason 
    (False)
    """

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Ban unban code reason"
        verbose_name_plural = "Ban unban code reasons"

    def __str__(self):
        return f"ban title: {self.title} - ban reason: {self.reason}"
