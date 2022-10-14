from django.db import models
from django.utils.translation import gettext as _


class CountryCode(models.TextChoices):
    """
    Defines the Country
    """
    BRAZIL = "BRA", _("Brazil")
    COLOMBIA = "COL", _("Colombia")
    MEXICO = "MEX", _("Mexico")
    PERU = "PER", _("Peru")
    SPAIN = "ESP", _("Spain")
    