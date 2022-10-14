from dateutil.relativedelta import relativedelta
from django.db import models
from django.utils import timezone
from django.utils.timezone import datetime


class PartnerLinkMonth(models.Model):
    """
    """
    def get_count_limit_at():
        """
        Get the first day of the next month
        """
        date_now = timezone.now()
        return datetime(year=date_now.year, month=date_now.month, day=1).astimezone() + relativedelta(months=+1)

    fixed_income_month = models.ForeignKey(to="api_admin.FixedIncomeAdviserMonth", on_delete=models.CASCADE)

    cpa_count = models.IntegerField()
    was_counted = models.BooleanField(default=True)

    count_limit_at = models.DateTimeField(default=get_count_limit_at)

    class Meta:
        verbose_name = "Partner link month"
        verbose_name_plural = "Partner links month"

    def __str__(self):
        return f"CPA count: {self.cpa_count} - was counted {self.was_counted} - {self.count_limit_at}"
