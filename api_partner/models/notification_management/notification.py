from django.db import models


class Notification(models.Model):
    partner = models.ForeignKey(to="api_partner.Partner", on_delete=models.CASCADE)

    title = models.CharField(max_length=50)
    message = models.TextField()

    url = models.CharField(max_length=255)

    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Notification"
        verbose_name_plural = "Notifications"

    def __str__(self):
        return f"{self.title}"
