from django.db import models
from django.utils import timezone
from core.models import User


class Campaign(models.Model):
    """
        BookMakers Campaign
    """
    bookmaker = models.ForeignKey(
        to="api_partner.Bookmaker",
        on_delete=models.CASCADE,
        related_name='campaign_to_bookmaker',
    )

    title = models.CharField(max_length=70)

    deposit_condition = models.FloatField(default=0)
    stake_condition = models.FloatField(default=0)
    lose_condition = models.FloatField(default=0)

    deposit_condition_campaign_only = models.FloatField(default=0)
    stake_condition_campaign_only = models.FloatField(default=0)
    lose_condition_campaign_only = models.FloatField(default=0)

    currency_condition_campaign_only = models.CharField(max_length=3)

    currency_condition = models.CharField(max_length=3)

    countries = models.CharField(max_length=200)
    fixed_income_unitary = models.FloatField(default=0)
    currency_fixed_income = models.CharField(max_length=3)

    class Status(models.IntegerChoices):
        NOT_AVALAIBLE = 0
        COMING_SOON = 1
        AVAILABLE = 2
        OUT_STOCK = 3
        INACTIVE = 4

        @classmethod
        def allowed_save(self):
            """
            Get enum values only for create validation
            """
            values = []
            for i in self.values:
                if (i != self.OUT_STOCK):
                    values.append(i)
            return values

    status = models.IntegerField(default=Status.COMING_SOON)

    default_percentage = models.FloatField(default=0.75)
    temperature = models.FloatField(default=1.0)

    # Tracker default percentages
    tracker = models.FloatField(default=1.0)
    tracker_deposit = models.FloatField(default=1.0)
    tracker_registered_count = models.FloatField(default=1.0)
    tracker_first_deposit_count = models.FloatField(default=1.0)
    tracker_wagering_count = models.FloatField(default=1.0)

    last_inactive_at = models.DateTimeField(default=timezone.now)
    fixed_income_updated_at = models.DateTimeField(default=timezone.now)
    cpa_limit = models.IntegerField(null=True, default=None)

    has_links = models.BooleanField(default=False)

    api_key = models.CharField(max_length=128, null=True, default=None, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Campaign"
        verbose_name_plural = "Campaigns"

    def __str__(self):
        return f"Campaign title: {self.bookmaker.name} {self.title} - status: {self.status}"
