import csv
from datetime import (
    datetime,
    timedelta,
)
from io import StringIO

import pytz
import requests
from api_partner.helpers import (
    DB_USER_PARTNER,
    UpdateCpasHandler,
)
from api_partner.models import (
    ForecasterCampaign,
    HistoriesForecaster,
)
from django.core.management.base import BaseCommand
from django.db import transaction


class Command(BaseCommand, UpdateCpasHandler):

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def handle(self, *args, **options):
        print("Making call to API Codere")
        now = datetime.now()
        less = datetime.now() - timedelta(days=1)
        less = less.strftime("%Y/%m/%d")
        now = now.strftime("%Y/%m/%d")
        less = datetime(2021, 10, 1).strftime("%Y-%m-%d")
        now = datetime(2021, 10, 31).strftime("%Y-%m-%d")
        url = "https://coderebet:sportbetcodere0199@portal.codere-partners.com/outer/csv.jhtm?reportId=1267&campaignId=63842&statDate_from=" + \
            less+"&statDate_to="+now+"&reportBy1=profile&reportBy2=merchant"
        codere = requests.get(url).text
        f = StringIO(codere)
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if(len(row) == 33) \
                    and (row[0] != 'Merchant' and row[0] != ''):
                merchant = row[0]
                prom = row[1]
                registered = int(row[7])
                cpas = int(row[20])
                deposits = row[15]

                if merchant == "Codere Colombia":
                    campaign = 1
                if merchant == "Codere Mexico":
                    campaign = 200
                if merchant == "Codere Spain":
                    campaign = 300

                forecasterC = ForecasterCampaign.objects.filter(
                    link_to_forecastercampaign__prom_code=prom,
                    link_to_forecastercampaign__campaign__id=campaign
                ).first()

                if forecasterC:
                    print(
                        f'{merchant} - {prom} - {registered} - {cpas} - {deposits}')
                    try:
                        sid = transaction.savepoint(using=DB_USER_PARTNER)
                        self.update_cpas(
                            forecasterC, cpas,
                            registered, deposits,
                            HistoriesForecaster,
                            tzinfo=pytz.UTC)
                    except Exception as e:
                        print(e)
                        transaction.savepoint_rollback(
                            sid, using=DB_USER_PARTNER)

                # print(f'{merchant} - {prom} - {clicks} - {effectives} - {deposits}')
