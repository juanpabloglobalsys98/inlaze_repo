import logging
import sys
import traceback

from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import (
    Partner,
    Withdrawal,
    RevenueAccomulated
)
from django.conf import settings
from django.core.management.base import (
    BaseCommand
)
from django.db import transaction
from django.db.models import (
    Q,
    Sum,
)
from django.utils.timezone import (
    datetime
)

from api_partner.models import BankAccount

logger = logging.getLogger(__name__)

TO_UPDATE = {
    'commision_accumulated': 0,
    'cpas': 0,
    'registered': 0,
    'deposits': 0,
    'netrevenue': 0,
    'netrevenue': 0,
    'netplayer': 0,
    'stake': 0,
    'clicks_excluded': 0
}


class Command(BaseCommand):
    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def handle(self, *args, **options):
        logger.info("Withdrawal monthly")
        forecasters = Partner.objects.using(DB_USER_PARTNER).filter(
            ~Q(forecastercampaign_to_forecaster=None)
        )
        sid = transaction.savepoint(using=DB_USER_PARTNER)
        try:
            for forecaster in forecasters.iterator():
                logger.info(
                    f"Iter to {forecaster}"
                )
                forecasterC = forecaster.forecastercampaign_to_forecaster.filter(
                    ~Q(link=None))

                suma = forecasterC.aggregate(Sum("commision_accumulated"))\
                    .get("commision_accumulated__sum")

                accomulated = forecasterC.\
                    aggregate(Sum("revenue_to_forecaster__value"))\
                    .get("revenue_to_forecaster__value__sum")

                if accomulated:
                    suma += accomulated

                if (suma and suma >= int(settings.WITHDRAWAL_AMOUNT)):
                    bankaccount = BankAccount.objects.filter(
                        user=forecaster.user.forecaster
                    ).first()
                    Withdrawal.objects.create(
                        amount=suma,
                        updated_at=datetime.now(),
                        bankaccount=bankaccount,
                        forecaster=forecaster
                    )
                    forecasterC.update(**TO_UPDATE)
                    RevenueAccomulated.objects.filter(
                        forecaster_campaign__in=forecasterC
                    ).delete()
                    logger.info(f"{forecaster} updated")
                else:
                    forecaster_to_revenue = forecasterC.filter(
                        ~Q(commision_accumulated=0)
                    )
                    revenues_accummulated = [
                        RevenueAccomulated(
                            forecaster_campaign=forecaster_i,
                            value=forecaster_i.commision_accumulated)
                        for forecaster_i in forecaster_to_revenue]
                    RevenueAccomulated.objects.bulk_create(
                        revenues_accummulated)
                    forecasterC.update(**TO_UPDATE)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error((
                f"Something is wrong when try to generate withdrawal\n\n{e}"
            ))
            transaction.savepoint_rollback(
                sid, using=DB_USER_PARTNER)
