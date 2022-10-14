from django.db import models
from django.utils.translation import gettext as _


class ChannelChangeRequest(models.Model):
    """
    """
    additional_info = models.OneToOneField("api_partner.AdditionalInfo", on_delete=models.CASCADE)

    channel_url = models.CharField(max_length=200)
    channel_name = models.CharField(max_length=200)

    class ChannelType(models.IntegerChoices):
        """
        """
        TELEGRAM = 0
        FACEBOOK = 1
        TWITTER = 2
        INSTAGRAM = 3
    channel_type = models.IntegerField()

    is_accepted = models.BooleanField(default=None, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Channel change request"
        verbose_name_plural = "Channel change requests"

    def __str__(self):
        return f"New channel: {self.channel_name} \n url: {self.channel_url}"
