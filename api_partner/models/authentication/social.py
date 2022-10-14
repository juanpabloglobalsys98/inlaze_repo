from django.db import models


class SocialChannel(models.Model):
    partner = models.ForeignKey(
        to="api_partner.Partner",
        related_name="channels",
        on_delete=models.CASCADE,
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
    type_channel = models.SmallIntegerField(default=0)
    is_active = models.BooleanField(default=True)
    deleted_at = models.DateTimeField(null=True, default=None)

    class Meta:
        unique_together = (
            "partner",
            "url",
        )

    def __str__(self) -> str:
        return f"Channel {self.name} at {self.url}"
