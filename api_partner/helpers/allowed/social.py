from django.db import models
from django.utils.translation import gettext as _


class AllowedChannels(models.IntegerChoices):
    TELEGRAM = 0, "Telegram"
    YOUTUBE = 1, "YouTube"
    TWITTER = 2, "Twitter"
    INSTAGRAM = 3, "Instagram"
    WHATSAPP = 4, "WhatsApp"
    TWITCH = 5, "Twitch"
