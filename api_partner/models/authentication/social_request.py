from django.db import models


class SocialChannelRequest(models.Model):
    partner_level_request = models.ForeignKey(
        to="api_partner.PartnerLevelRequest",
        on_delete=models.CASCADE,
        related_name="channels",
    )
    name = models.CharField(max_length=200)
    url = models.URLField()

    class ChannelType(models.IntegerChoices):
        """
        """
        TELEGRAM = 0
        YOUTUBE = 1
        TWITTER = 2
        INSTAGRAM = 3
        WHATSAPP = 4
        TWITCH = 5
    type = models.SmallIntegerField()

    class Meta:
        unique_together = (
            "partner_level_request",
            "url",
        )

    def __str__(self) -> str:
        return f"Channel {self.name} at {self.url}"
