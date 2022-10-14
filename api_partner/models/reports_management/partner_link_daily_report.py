from django.db import models
from django.utils import timezone


class PartnerLinkDailyReport(models.Model):
    partner_link_accumulated = models.ForeignKey(
        to='api_partner.PartnerLinkAccumulated', on_delete=models.CASCADE,
        related_name='Partnerlinkdailyreport_to_partnerlinkaccumulated')
    betenlace_daily_report = models.OneToOneField("api_partner.BetenlaceDailyReport", on_delete=models.CASCADE)

    fixed_income = models.FloatField(
        null=True,
        default=None,
    )
    fixed_income_unitary = models.FloatField(
        null=True,
        default=None,
    )

    currency_fixed_income = models.CharField(
        max_length=3,
    )
    currency_local = models.CharField(
        max_length=3,
    )

    fx_book_local = models.FloatField(
        null=True,
        default=None,
    )
    """
    fx conversion for fixed_income bookmaker to local included the respective 
    fx_percentage (This is used ONLY for payment/withdrawal process), this make 
    more easy the analytic and data extraction also include chain of 
    conversions for example if local is MXN and money from is EUR this must be 
    changed to USD and finally to MXN
    """
    fx_book_net_revenue_local = models.FloatField(
        null=True,
        default=None,
    )
    """
    fx conversion for net_revenue bookmaker to local included the respective 
    fx_percentage (This is used ONLY for payment/withdrawal process), this make 
    more easy the analytic and data extraction also include chain of 
    conversions for example if local is MXN and money from is EUR this must be 
    changed to USD and finally to MXN
    """
    fx_percentage = models.FloatField(
        null=True,
        default=None,
    )

    fixed_income_local = models.FloatField(
        null=True,
        default=None,
    )
    fixed_income_unitary_local = models.FloatField(
        null=True,
        default=None,
    )

    cpa_count = models.IntegerField(
        null=True,
        default=None,
    )
    percentage_cpa = models.FloatField(
        null=True,
        default=None,
    )

    """
    Tracked values
    """
    deposit = models.FloatField(
        null=True,
        default=None,
    )
    registered_count = models.IntegerField(
        null=True,
        default=None,
    )
    first_deposit_count = models.IntegerField(
        null=True,
        default=None,
    )
    wagering_count = models.IntegerField(
        null=True,
        default=None,
    )

    tracker = models.FloatField(
        null=True,
        default=None,
    )
    """
    Tracker percentage for cpa count
    """
    tracker_deposit = models.FloatField(
        null=True,
        default=None,
    )
    tracker_registered_count = models.FloatField(
        null=True,
        default=None,
    )
    tracker_first_deposit_count = models.FloatField(
        null=True,
        default=None,
    )
    tracker_wagering_count = models.FloatField(
        null=True,
        default=None,
    )

    # Adviser data
    adviser_id = models.BigIntegerField(
        null=True,
        default=None,
    )
    """
    Key of adviser that have relation at moment of partner link accumulated
    """

    fixed_income_adviser = models.FloatField(
        null=True,
        default=None,
    )
    """
    Fixed income adviser in currency of bookmaker according to percentage
    """
    fixed_income_adviser_local = models.FloatField(
        null=True,
        default=None,
    )
    """
    Fixed income adviser in currency of partner according to percentage
    """

    net_revenue_adviser = models.FloatField(
        null=True,
        default=None,
    )
    """
    Revenue share adviser in currency of bookmaker according to percentage
    """
    net_revenue_adviser_local = models.FloatField(
        null=True,
        default=None,
    )
    """
    Revenue share adviser in currency of partner according to percentage
    """

    fixed_income_adviser_percentage = models.FloatField(
        null=True,
        default=None,
    )
    net_revenue_adviser_percentage = models.FloatField(
        null=True,
        default=None,
    )

    referred_by = models.ForeignKey(
        "api_partner.Partner",
        null=True,
        default=None,
        on_delete=models.SET_NULL
    )

    fixed_income_referred = models.FloatField(
        null=True,
        default=None,
    )
    """
    Fixed income referred in currency of bookmaker according to percentage
    """
    net_revenue_referred = models.FloatField(
        null=True,
        default=None,
    )
    """
    Revenue share referred in currency of bookmaker according to percentage
    """

    fixed_income_referred_local = models.FloatField(
        null=True,
        default=None,
    )
    """
    Fixed income referred in currency of partner according to value
    """
    net_revenue_referred_local = models.FloatField(
        null=True,
        default=None,
    )
    """
    net referred in currency of partner according to value
    """
    fixed_income_referred_percentage = models.FloatField(
        null=True,
        default=None,
    )
    """
    Fixed income referred in currency of partner according to percentage
    """

    net_revenue_referred_percentage = models.FloatField(
        null=True,
        default=None,
    )

    """
    net revenue income referred in currency of partner according to percentage
    """

    created_at = models.DateField(default=timezone.now)

    class Meta:
        verbose_name = "Partner link daily report"
        verbose_name_plural = "Partner link daily reports"
        unique_together = ("partner_link_accumulated", "created_at",)

    def __str__(self):
        return f"{self.partner_link_accumulated.id} - daily {self.cpa_count}"
