from api_partner.helpers import PartnerLevelCHO, PartnerAccumStatusCHO
from django.db import models


class PartnerLinkAccumulated(models.Model):
    """
    PartnerLinkAccumulated acummulate the CPA data getted for links
    (every entry have data of certain link that have the partner) on
    current month. At start of every month
    the the data on that entries will restart to default values
    """
    partner = models.ForeignKey(to="api_partner.Partner", on_delete=models.CASCADE,
                                related_name='partnerlinkaccumulated_to_partner')

    campaign = models.ForeignKey(to="api_partner.Campaign", on_delete=models.CASCADE,
                                 related_name='partnerlinkaccumulated_to_campaign')

    prom_code = models.CharField(max_length=50, null=True, default=None)
    is_assigned = models.BooleanField(default=True)
    cpa_count = models.IntegerField(default=0)

    fixed_income = models.FloatField(default=0)
    currency_fixed_income = models.CharField(max_length=3)
    """
    Currency of fixed income of Bookmaker
    """

    fixed_income_local = models.FloatField(default=0)
    currency_local = models.CharField(max_length=3)
    """
    Currency local of partner that have the relation with Link
    """
    percentage_cpa = models.FloatField(default=0.75)
    is_percentage_custom = models.BooleanField(default=False)

    tracker = models.FloatField(default=1.0)
    tracker_deposit = models.FloatField(default=1.0)
    tracker_registered_count = models.FloatField(default=1.0)
    tracker_first_deposit_count = models.FloatField(default=1.0)
    tracker_wagering_count = models.FloatField(default=1.0)

    status = models.IntegerField(default=PartnerAccumStatusCHO.BY_CAMPAIGN)

    partner_level = models.SmallIntegerField(default=PartnerLevelCHO.BASIC)

    assigned_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Partner link accumulated"
        verbose_name_plural = "Partner links accumulated"
        unique_together = ("partner", "campaign")

    def __str__(self):
        return f"{self.id} - current month {self.cpa_count}"
