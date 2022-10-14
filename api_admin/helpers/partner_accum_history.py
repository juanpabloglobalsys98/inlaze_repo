

def create_history(instance, update_reason, adviser):
    from api_partner.models import (
        HistoricalPartnerLinkAccum,
        Link,
    )
    link = Link.objects.filter(partner_link_accumulated_id=instance.id).first()
    HistoricalPartnerLinkAccum.objects.create(
        partner_link_accum=instance,
        prom_code=instance.prom_code,
        link=link,
        is_assigned=instance.is_assigned,
        percentage_cpa=instance.percentage_cpa,
        is_percentage_custom=instance.is_percentage_custom,
        tracker=instance.tracker,
        tracker_deposit=instance.tracker_deposit,
        tracker_registered_count=instance.tracker_registered_count,
        tracker_first_deposit_count=instance.tracker_first_deposit_count,
        tracker_wagering_count=instance.tracker_wagering_count,
        status=instance.status,
        partner_level=instance.partner_level,
        assigned_at=instance.assigned_at,
        adviser_id=adviser,
        update_reason=update_reason
    )
