from django.db import models
from django.utils import timezone


class BetenlaceDailyReport(models.Model):
    """
    """
    def last_fx_partner():
        """
        Get the current last FxPartner from Database
        """
        from api_partner.models import FxPartner
        return FxPartner.objects.all().order_by("-created_at").first().pk
    betenlace_cpa = models.ForeignKey(
        to='api_partner.BetenlaceCPA',
        on_delete=models.CASCADE,
        related_name='Betenlacedailyreport_to_BetenlaceCPA',
    )

    currency_condition = models.CharField(max_length=3)

    deposit = models.FloatField(
        null=True,
        default=None,
    )
    stake = models.FloatField(
        null=True,
        default=None,
    )

    net_revenue = models.FloatField(
        null=True,
        default=None,
    )
    revenue_share = models.FloatField(
        null=True,
        default=None,
    )

    currency_fixed_income = models.CharField(max_length=3)

    fixed_income = models.FloatField(
        null=True,
        default=None,
    )
    fixed_income_unitary = models.FloatField(
        null=True,
        default=None,
    )

    fx_partner = models.ForeignKey(
        to="api_partner.FxPartner",
        on_delete=models.SET_NULL,
        null=True,
        default=None,
    )
    """
    fx_partner of same day that getted data
    """

    click_count = models.IntegerField(
        null=True,
        default=None,
    )
    """
    Click count from click tracking, this makes more easy the process to get 
    count of clicks. Take null value while every day the clicks that are not 
    calculated will recalculated with Celery task 
    """

    registered_count = models.IntegerField(
        null=True,
        default=None,
    )
    cpa_count = models.IntegerField(
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

    created_at = models.DateField(
        default=timezone.now,
    )

    class Meta:
        verbose_name = "Betenlace daily report"
        verbose_name_plural = "Betenlace daily reports"
        unique_together = ("betenlace_cpa", "created_at", )

    def __str__(self):
        return f" Betenlace daily report {self.betenlace_cpa}"
