
from django.db import models
from django.utils.translation import gettext as _


class PartnerAccumStatusCHO(models.IntegerChoices):
    ACTIVE = 0
    INACTIVE = 1
    BY_CAMPAIGN = 2


class PartnerAccumUpdateReasonCHO(models.IntegerChoices):
    PARTNER_REQUEST = 0
    ADVISER_ASSIGN = 1
    ADVISER_UNASSIGN = 2
    ADVISER_CHANGE_PARTNER_LEVEL = 3
    CAMPAIGN = 4
    CAMPAIGN_SPECIFIC = 5
    CHANGE_LEVEL_PERCENTAGE = 6
