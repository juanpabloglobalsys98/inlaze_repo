from django.db import models
from django.utils.translation import gettext as _
from django.contrib.auth import get_user_model

User = get_user_model()


class NameChangeRequest(models.Model):
    """
    """
    user = models.ForeignKey(User, on_delete=models.DO_NOTHING)

    first_name = models.CharField(_('first name'), max_length=150, blank=True)
    second_name = models.CharField(_('second name'), max_length=150, blank=True)
    last_name = models.CharField(_('last name'), max_length=150, blank=True)
    second_last_name = models.CharField(_('second last name'), max_length=150, blank=True)

    is_accepted = models.BooleanField(default=None, null=True)
    created_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Name change request"
        verbose_name_plural = "Name change requests"

    def __str__(self):
        return f"New name: {self.first_name} \n New last name: {self.last_name}"
