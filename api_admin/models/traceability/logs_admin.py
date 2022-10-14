from django.db import models
from django.db.models.fields import DateTimeField
from django.utils.translation import gettext as _


class LogAdmin(models.Model):
    """
    """

    admin = models.ForeignKey(
        to="api_admin.Admin",
        on_delete=models.CASCADE,
    )

    action = models.CharField(max_length=2048)
    method = models.CharField(max_length=10)
    params = models.CharField(max_length=1024)
    created_at = DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Log Admin"
        verbose_name_plural = "Logs Admin"

    def __str__(self):
        return f"{self.action} - {self.admin}"
