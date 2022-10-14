from django.db import models
from django.utils.translation import gettext as _


class Rank(models.Model):
    """
    """

    name = models.CharField(max_length=100, unique=True)
    percentage_cpa = models.FloatField()
    down_cond_cpa = models.IntegerField()
    up_cond_cpa = models.IntegerField()

    class Meta:
        verbose_name = "Rank"
        verbose_name_plural = "Ranks"

    def __str__(self):
        return f"Rank name: {self.name} - down_cond_cpa: {self.down_cond_cpa} - up_cond_cpa: {self.up_cond_cpa}"
