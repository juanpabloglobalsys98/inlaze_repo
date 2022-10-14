from django.conf import settings
from django.db.models import Q


def get_adviser_id_for_partner(
    adviser_id=None,
):
    """
    Returns an adviser id if it exists. Will return a default adviser
    id if no adviser is found.
    """
    from api_admin.helpers import DB_ADMIN
    from api_partner.helpers import DB_USER_PARTNER
    from api_partner.models import Partner
    from core.models import User

    if not adviser_id:
        query = Q(was_linked=False)
        partners_non_linked = Partner.objects.db_manager(DB_USER_PARTNER).filter(query).count()
        query = Q(
            is_active=True,
            is_staff=True,
        )
        available_advisers = User.objects.db_manager(DB_ADMIN).filter(query)
        if available_advisers.count() > 0:
            index = (partners_non_linked % available_advisers.count())
            adviser_id = available_advisers[index].id
    else:
        adviser = User.objects.using(DB_ADMIN).filter(id=adviser_id).first()
        if adviser is not None:
            adviser_id = adviser.id

    return adviser_id or int(settings.ADVISER_ID_LINKED_DEFAULT)
