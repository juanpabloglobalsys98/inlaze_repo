from django.db import models


class FixedIncomeCondition(models.Model):
    """
    """

    partner_count = models.IntegerField()
    cpa_amount = models.IntegerField()
    limit_extra_fixed_income = models.IntegerField()

    value_fixed_income = models.FloatField()

    percentage_extra_fixed_income = models.FloatField()
    percentage_half_condition = models.FloatField()

    condition_at = models.DateField(auto_now_add=True)

    class Meta:
        verbose_name = "Fixed income condition"
        verbose_name_plural = "Fixed incomes condition"

    def __str__(self):
        return f"CPA: {self.cpa_amount} - value fixed income: {self.value_fixed_income}"
