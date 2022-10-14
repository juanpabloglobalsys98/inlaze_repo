from django.utils import timezone
from django.db import models


class LevelPercentageBase(models.Model):
    percentages = models.JSONField()
    created_by = models.ForeignKey(
        to="api_admin.Admin",
        on_delete=models.SET_NULL,
        null=True,
    )
    created_at = models.DateTimeField(default=timezone.now)
