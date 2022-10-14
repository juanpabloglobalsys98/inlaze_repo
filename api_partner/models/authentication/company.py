from django.db import models


class Company(models.Model):
    """
    """
    partner = models.OneToOneField("api_partner.Partner", on_delete=models.CASCADE, primary_key=True)

    company_id = models.CharField(max_length=100)
    social_reason = models.CharField(max_length=255)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Company"
        verbose_name_plural = "Companies"
        unique_together = ("company_id", "social_reason")

    def __str__(self):
        return f"{self.company_id} - {self.social_reason}"
