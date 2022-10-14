from django.db import models


class Link(models.Model):
    """  
        Links
    """
    partner_link_accumulated = models.OneToOneField(
        to='api_partner.PartnerLinkAccumulated', on_delete=models.SET_NULL,
        related_name='link_to_partner_link_accumulated', null=True)
    campaign = models.ForeignKey(to='api_partner.Campaign', on_delete=models.CASCADE, related_name='link_to_campaign')

    prom_code = models.CharField(max_length=50)
    url = models.URLField(unique=True)

    class Status(models.IntegerChoices):
        AVAILABLE = 1
        ASSIGNED = 2
        UNAVAILABLE = 3
        GROWTH = 4
    status = models.IntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Link"
        verbose_name_plural = "Links"
        unique_together = ("campaign", "prom_code")

    def __str__(self):
        return f"{self.prom_code} - {self.campaign.title}"
