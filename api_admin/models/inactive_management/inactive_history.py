from django.db import models


class InactiveHistory(models.Model):
    adviser_from = models.ForeignKey(to="api_admin.Admin", on_delete=models.SET_NULL,
                                     related_name="inactivehistory_from", null=True)
    """
    Adviser who apply the inactive or active to another adviser
    """
    adviser_to = models.ForeignKey(to="api_admin.Admin", on_delete=models.SET_NULL,
                                   related_name="inactivehistory_to", null=True)
    """
    Adviser that recieve the inactive or active action
    """

    active_inactive_code_reason = models.ForeignKey(
        to="api_admin.InactiveActiveCodeReason", on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Inactive History"
        verbose_name_plural = "Inactive Histories"

    def __str__(self):
        return f"{self.adviser_to} - {self.active_inactive_code_reason}"
