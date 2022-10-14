from django.core.management.base import BaseCommand, CommandError

# Locals libs
from datetime import datetime, timedelta
from io import StringIO
import os
import sys
import time
import ast
import json
import csv
import requests
import xml
import xml.etree.ElementTree as ET


class Command(BaseCommand):

    def handle(self, *args, **options):
        print("Making call to API Betway")
        now = datetime.now()
        less = datetime.now() - timedelta(days=1)
        less = less.strftime("%Y/%m/%d")
        now = now.strftime("%Y/%m/%d")
        less = datetime(2021, 10, 1).strftime("%Y-%m-%d")
        now = datetime(2021, 10, 31).strftime("%Y-%m-%d")
        # url = 'https://www.betwaypartners.com/api/cpa/traffic?username=bet21&apikey=f5bc52c8-d510-45bf-8c0a-d39537d1ccb4&start='+ less + '&end=' + now
        url = 'https://affiliates-exportapi.betconstruct.com/global/api/ExternalApi/postback'
        # url = 'https://www.betwaypartners.com/api/payments?username=bet21&apikey=f5bc52c8-d510-45bf-8c0a-d39537d1ccb4&start='+ less + '&end=' + now
        vbet = requests.get(url, data={
            "command": "getAffiliateReport",
            "params": {
                "fromDate": "2021-10-1",
                "toDate": "2021-11-4",
                "affiliateId": 543876,
                "requestHash": "91644af28ba47db1238bf8842f52bb3e7db4b06ffe4385ae6fae01db50352edd"
            }
        }).text
        print(vbet)
