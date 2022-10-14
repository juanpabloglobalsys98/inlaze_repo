from django.core.management.base import BaseCommand, CommandError

# Locals libs
from datetime import datetime,timedelta
import requests
from io import StringIO
import csv
import json
import ast
import time


class Command(BaseCommand):
    
    def handle(self,*args, **options):
        print("Making call to API Dafet")
        now = datetime.now()
        less = datetime.now() - timedelta(days=1)
        less = less.strftime("%Y/%m/%d")
        now = now.strftime("%Y/%m/%d")
        less = datetime(2021, 10, 1).strftime("%Y-%m-%d")
        now = datetime(2021, 10, 21).strftime("%Y-%m-%d")
        url = 'https://dafabet.dataexport.netrefer.com/v2/export/reports/affiliate/XML_CustomerReporting_InclSubAff?authorization=MjQ4NDMzLkdiWDViY0Q5cXB6azJqR2Nxa1BGcnc9PQ==&playerID=all&username=all&websiteID=all&productID=all&brandID=all&customersource=all&customerTypeID=all&rewardplanID=all&countryID=all&FilterBySignUpDate=0&FilterBySignUpDateFrom=2016-10-01&FilterBySignUpDateTo=2016-10-31&FilterByExpirationDate=0&FilterByExpirationDateFrom=2016-10-01&FilterByExpirationDateTo=2016-10-31&FilterByActivityDate=1&FilterByActivityDateFrom=' + less + '&FilterByActivityDateTo=' + now
        dafabet = requests.get(url).text
        dafabet = json.loads(dafabet)
        records = {}
        for row in dafabet:
            if 'Customer Reference ID' in row:
                prom_code = row.get('Marketing Source name')
                suma = 0
                if row.get('CPA Processed Date') != '':
                    suma = 1
                if not prom_code in records:
                    records[prom_code] = {
                        'prom_code':prom_code,
                        'clicks': 1,
                        'efective_clicks':suma,
                        'deposits':float(row.get('Deposits'))
                    }
                else:
                    records[prom_code]['deposits'] += float(row.get('Deposits'))
                    records[prom_code]['efective_clicks'] += suma
                    records[prom_code]['clicks'] += 1
        for record in records:
            print(records[record])
            print("\n\n")