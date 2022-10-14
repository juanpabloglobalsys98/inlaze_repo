from api_partner.models.reports_management import (
    Campaign,
    Link,
)
from django.db.models import Q


def recalculate_temperature(campaign):
    links = Link.objects.filter(
        Q(campaign=campaign),
        ~Q(status=Link.Status.UNAVAILABLE)
    )
    total = links.count()
    assigned = links.filter(
        Q(status=Link.Status.ASSIGNED)
    ).count()

    if (total == 0 or total == assigned):
        campaign.has_links = False
        if(campaign.status != Campaign.Status.NOT_AVALAIBLE and campaign.status != Campaign.Status.INACTIVE):
            campaign.status = Campaign.Status.OUT_STOCK

        campaign.temperature = 1
        campaign.save()
    else:
        campaign.has_links = True
        if(campaign.status != Campaign.Status.NOT_AVALAIBLE and campaign.status != Campaign.Status.INACTIVE):
            campaign.status = Campaign.Status.AVAILABLE

        campaign.temperature = assigned/total
        campaign.save()


def calculate_temperature(campaign, data_status):
    filters = (
        Q(campaign=campaign),
        ~Q(status=Link.Status.UNAVAILABLE),
    )
    links = Link.objects.filter(*filters)

    total = links.count()

    filters = (Q(status=Link.Status.ASSIGNED),)
    assigned = links.filter(*filters).count()

    temperature = 1 if total == 0 else assigned/total

    if (temperature == 1 and data_status == Campaign.Status.AVAILABLE):
        return Campaign.Status.OUT_STOCK, temperature

    return data_status, temperature
