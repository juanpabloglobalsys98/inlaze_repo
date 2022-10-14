from django.db import models


class PartnerLevelHistory(models.Model):
    """
    Keep partner level history change.
    """
    partner_id = models.BigIntegerField()
    admin = models.ForeignKey(
        to="api_admin.Admin",
        related_name="level_history",
        on_delete=models.CASCADE,
        null=True,
        default=None,
    )
    previous_level = models.SmallIntegerField(null=True, default=None)
    new_level = models.SmallIntegerField(null=True, default=None)

    class ChangedBy(models.IntegerChoices):
        REQUEST = 0
        ADMIN = 1

    changed_by = models.SmallIntegerField(default=ChangedBy.REQUEST)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.partner_id} old: {self.previous_level} new:{self.new_level}"
