import csv
from datetime import (
    datetime,
    timedelta,
)
from io import StringIO

import requests
from api_forecaster.helpers import (
    DB_USER_FORECASTER,
    UpdateCpasHandler,
)
from api_forecaster.models import (
    ForecasterCampaign,
    HistoriesForecaster,
)
from core.management.commands.yajuego import CAMPAIGN
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
import pytz

CAMPAIGN_ESP = 35
CAMPAIGN_LATAM = 36


class Command(BaseCommand, UpdateCpasHandler):

    @transaction.atomic(using=DB_USER_FORECASTER, savepoint=True)
    def handle(self, *args, **options):
        print("Making call to API Willian Hill")
        now = datetime.now()
        less = datetime.now() - timedelta(days=1)
        less = less.strftime("%Y/%m/%d")
        now = now.strftime("%Y/%m/%d")
        less = datetime(2021, 10, 1).strftime("%Y-%m-%d")
        now = datetime(2021, 10, 31).strftime("%Y-%m-%d")
        url = 'https://partners.williamhill.com/api/affreporting.asp?key=b1995bdd75c746d195623c2d36b40c1e&reportname=Member%20Report%20-%20Detailed&reportformat=csv&reportmerchantid=0&reportstartdate=' + less + '&reportenddate=' + now
        willian_hill = requests.get(url).text
        f = StringIO(willian_hill)
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if(len(row) == 77) and \
                    row[3] != 'period':
                prom = row[10]
                registered = row[16]
                cpas = row[74]
                netplayer = row[26]
                deposits = row[27]
                stake = row[29]
                period = row[3]
                period_format = datetime.now()
                if period != "":
                    period_format = period.split("/")
                    period_format = datetime(
                        int(period_format[2]),
                        int(period_format[0]),
                        int(period_format[1]),
                        tzinfo=pytz.UTC)

                forecasterC = ForecasterCampaign.objects.filter(
                    Q(link_to_forecastercampaign__prom_code=prom),
                    (
                        Q(link_to_forecastercampaign__campaign__id=CAMPAIGN_ESP
                          ) |
                        Q(link_to_forecastercampaign__campaign__id=CAMPAIGN_LATAM
                          )
                    )
                ).first()

                if forecasterC:
                    print(f'{prom} - {registered} - {cpas} - {deposits}')
                    try:
                        sid = transaction.savepoint(using=DB_USER_FORECASTER)
                        self.update_cpas(
                            forecasterC,
                            int(cpas), int(registered),
                            float(deposits),
                            HistoriesForecaster,
                            updated_at=period_format
                        )
                    except Exception as e:
                        print(e)
                        transaction.savepoint_rollback(
                            sid, using=DB_USER_FORECASTER)
