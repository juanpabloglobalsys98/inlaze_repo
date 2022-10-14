from api_partner.helpers import PartnerStatusCHO
from django.db import models
from django.utils import timezone
from django.utils.translation import gettext as _


class PartnerLevelRequest(models.Model):
    partner = models.ForeignKey(
        to="api_partner.Partner",
        on_delete=models.CASCADE,
    )
    level = models.SmallIntegerField(default=0)
    status = models.SmallIntegerField(default=PartnerStatusCHO.REQUESTED)
    answered_at = models.DateTimeField(default=timezone.now)
    created_at = models.DateTimeField(default=timezone.now)
