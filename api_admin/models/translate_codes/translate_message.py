from django.db import models


class TranslateMessage(models.Model):
    language = models.CharField(max_length=7)
    message = models.TextField()
    code = models.ForeignKey(
        to="api_admin.CodeReason",
        related_name="translate_messages",
        on_delete=models.CASCADE,
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("language", "code")

    def __str__(self):
        return f"language: {self.language} - message: {self.message}"
