import os

from core.helpers import (
    CountryPartner,
    IdentificationType,
    S3StandardIA,
)
from django.db import models


class AdditionalInfo(models.Model):
    """
    This is a Betting Partner user account

    ### Fields

    """
    partner = models.OneToOneField("api_partner.Partner", on_delete=models.CASCADE, primary_key=True)

    class PersonType(models.IntegerChoices):
        """
        """
        PERSON = 0
        COMPANY = 1
    person_type = models.SmallIntegerField(null=True, default=None)

    identification = models.CharField(max_length=60, null=True, default=None)
    identification_type = models.SmallIntegerField(null=True, default=None)

    country = models.CharField(max_length=3)
    city = models.CharField(max_length=100, null=True, default=None)
    fiscal_address = models.CharField(max_length=255, null=True, default=None)

    channel_name = models.CharField(max_length=200, null=True, default=None)
    channel_url = models.URLField(unique=True, null=True, default=None)

    class ChannelType(models.IntegerChoices):
        """
        """
        TELEGRAM = 0
        YOUTUBE = 1
        # TWITTER = 2
        INSTAGRAM = 3
        WHATSAPP = 4
    channel_type = models.IntegerField(null=True, default=None)

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Additional information"
        verbose_name_plural = "Additional informations"
        unique_together = ("identification", "identification_type")

    def __str__(self):
        return f"channel name: {self.channel_name} - channel url: {self.channel_url}"
