
from django.db import models
from django.utils.translation import gettext as _


class PartnerLevelCHO(models.IntegerChoices):
    BASIC = 0, _("Basic")
    PRIME = 1, _("Prime")
