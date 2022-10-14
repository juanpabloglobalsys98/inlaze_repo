from django.db import models


class InactiveActiveCodeReason(models.Model):
    title = models.CharField(max_length=255, unique=True)
    reason = models.TextField(unique=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Inactive Active Code Reason"
        verbose_name_plural = "Inactive Active Codes Reasons"

    def __str__(self):
        return f"{self.title}"
