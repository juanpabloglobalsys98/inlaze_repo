from django.conf import settings
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext as _


class ValidationCode(models.Model):
    """
    This is a Betting Partner user account

    Fields
    ---
    """
    def get_current_expiration():
        """
        Get current time from
        """
        default_minutes = int(settings.EXPIRATION_ADDER_MINUTES)
        return timezone.now() + timezone.timedelta(minutes=default_minutes)

    user = models.OneToOneField(
        to="core.user",
        related_name="validation_code",
        on_delete=models.CASCADE,
    )
    email = models.EmailField(unique=True, null=True, default=None)
    phone = models.CharField(unique=True, max_length=50, null=True, default=None)
    code = models.CharField(max_length=32)

    expiration = models.DateTimeField(default=get_current_expiration)
    attempts = models.SmallIntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Validation code"
        verbose_name_plural = "Validation codes"

    def __str__(self):
        return f"code: {self.code} - expiration: {self.expiration}"
