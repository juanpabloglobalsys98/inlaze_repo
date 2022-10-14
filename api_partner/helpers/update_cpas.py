import math
from datetime import datetime


class UpdateCpasHandler():

    def update_cpas(
            self, forecasterC, effectives,
            clicks, deposits, HistoriesForecaster, updated_at=datetime.now()):

        commission = forecasterC.forecaster_campaign_commission.first()

        if commission:
            TO_CUSTOM = commission.commission_in_pesos
        else:
            TO_CUSTOM = \
                forecasterC.link.campaign.commission_in_pesos

        last_registered = forecasterC.registered
        last_cpas = forecasterC.cpas

        efec = effectives + forecasterC.clicks_excluded + forecasterC.cpas
        forecasterC.registered += clicks
        cpa_total = math.floor(efec * forecasterC.tracked)
        forecasterC.cpas = cpa_total
        forecasterC.clicks_excluded = math.floor(efec - forecasterC.cpas)
        forecasterC.deposits = float(deposits)
        forecasterC.commision_accumulated = (cpa_total*TO_CUSTOM)
        forecasterC.comission_updated_at = updated_at
        forecasterC.save()

        last_registered = forecasterC.registered - last_registered
        last_cpas = forecasterC.cpas - last_cpas

        if(last_registered < 0):
            last_registered *= -1

        if(last_cpas < 0):
            last_cpas *= -1

        commission = last_cpas * TO_CUSTOM

        # Create historial forecaster
        HistoriesForecaster.objects.create(
            forecaster_campaign=forecasterC,
            commission=commission,
            registered=last_registered,
            cpas=last_cpas,
            deposit=float(deposits),
            created_at=updated_at
        )
