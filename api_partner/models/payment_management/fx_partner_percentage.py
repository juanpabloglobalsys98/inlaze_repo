from django.db import models


class FxPartnerPercentage(models.Model):
    """
    """
    percentage_fx = models.FloatField(default=0.95)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.percentage_fx}"
