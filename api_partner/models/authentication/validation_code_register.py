from django.conf import settings
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext as _


class ValidationCodeRegister(models.Model):
    """
    """
    def get_current_expiration():
        """
        Get expiration time from settings
        """
        default_minutes = int(settings.EXPIRATION_ADDER_MINUTES)
        return timezone.now() + timezone.timedelta(minutes=default_minutes)

    code = models.CharField(max_length=32)
    first_name = models.CharField(max_length=150, blank=True)
    second_name = models.CharField(max_length=150, blank=True, default="")
    last_name = models.CharField(max_length=150, blank=True)
    second_last_name = models.CharField(max_length=150, blank=True, default="")
    email = models.EmailField(max_length=250)
    password = models.CharField(max_length=128)
    phone = models.CharField(max_length=50, null=True, default=None)
    valid_phone_by = models.SmallIntegerField(null=True)
    adviser_id = models.BigIntegerField(null=True)
    expiration = models.DateTimeField(default=get_current_expiration)
    attempts = models.SmallIntegerField(default=0)
    created_at = models.DateTimeField(default=timezone.now)
    is_used = models.BooleanField(default=False)
    reg_source = models.SmallIntegerField(default=0)
    """
    From Partner RegSource enumerator
    """

    class Meta:
        verbose_name = "Validation code register"
        verbose_name_plural = "Validation code registers"

    def __str__(self):
        return f"code: {self.code} - expiration: {self.expiration}"
