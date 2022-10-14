from django.conf import settings
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext as _


class ValidationCode(models.Model):
    """
    Validation code for operations that requries external security like 
    password recovery
    ---
    """
    def _get_current_expiration():
        """
        Get from current time plus expiration adder minutes
        """
        default_minutes = settings.EXPIRATION_ADDER_MINUTES
        return timezone.now() + timezone.timedelta(minutes=default_minutes)

    email = models.EmailField(unique=True)
    code = models.CharField(max_length=32)

    expiration = models.DateTimeField(default=_get_current_expiration)
    attempts = models.SmallIntegerField(default=0)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("email", "code")

    def is_valid(self):
        return (
            # Expiration time has not been exceeded
            self.expiration > timezone.now() and
            # Max attemps is not reached
            settings.MAX_VALIDATION_CODE_ATTEMPTS > self.attempts
        )

    def __str__(self):
        return f"code: {self.code} - expiration: {self.expiration} - email: {self.email}"
