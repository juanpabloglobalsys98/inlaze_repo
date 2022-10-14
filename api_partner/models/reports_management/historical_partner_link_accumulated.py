from api_partner.helpers import (
    PartnerAccumStatusCHO,
    PartnerLevelCHO,
)
from django.db import models


class HistoricalPartnerLinkAccum(models.Model):

    partner_link_accum = models.ForeignKey(to="api_partner.PartnerLinkAccumulated", on_delete=models.CASCADE,)

    prom_code = models.CharField(max_length=50, null=True, default=None)
    link = models.ForeignKey(to="api_partner.Link", on_delete=models.CASCADE, null=True)

    is_assigned = models.BooleanField()
    percentage_cpa = models.FloatField()
    is_percentage_custom = models.BooleanField()

    tracker = models.FloatField()
    tracker_deposit = models.FloatField()
    tracker_registered_count = models.FloatField()
    tracker_first_deposit_count = models.FloatField()
    tracker_wagering_count = models.FloatField()

    status = models.IntegerField(default=PartnerAccumStatusCHO.BY_CAMPAIGN)
    partner_level = models.SmallIntegerField(default=PartnerLevelCHO.BASIC)
    assigned_at = models.DateTimeField(default=None)

    adviser_id = models.SmallIntegerField(null=True)

    update_reason = models.SmallIntegerField()

    def __str__(self):
        return f"{self.id} - current month {self.is_percentage_custom}"
