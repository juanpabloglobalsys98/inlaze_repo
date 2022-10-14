from django.db import models


class FixedIncomeAdviserMonth(models.Model):
    """
    """
    admin = models.ForeignKey(to="api_admin.Admin", on_delete=models.CASCADE)
    condition = models.ForeignKey(to="api_admin.FixedIncomeCondition", on_delete=models.CASCADE)

    class Meta:
        verbose_name = "Fixed Income Adviser Month"
        verbose_name_plural = "Fixed Incomes Adviser Month"

    def __str__(self):
        return f"{self.admin} - {self.condition}"
