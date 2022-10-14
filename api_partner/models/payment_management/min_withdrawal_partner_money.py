from django.db import models


class MinWithdrawalPartnerMoney(models.Model):
    """
    """
    min_usd_by_level = models.JSONField()
    created_by = models.SmallIntegerField(null=True)
    created_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Min Withdrawal Partner Money"
        verbose_name_plural = "Min Withdrawals partner Money"

    def __str__(self):
        return f"id {self.id} - min_level {self.min_usd_by_level}"
