import logging
import math
import sys
import traceback
from io import StringIO

import numpy as np
import pandas as pd
import requests
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerAccumStatusCHO,
)
from api_partner.models import (
    AccountReport,
    Campaign,
    Link,
    PartnerLinkAccumulated,
)
from core.helpers import CurrencyAll
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils import timezone
from django.utils.timezone import (
    datetime,
    make_aware,
    timedelta,
)
from tqdm import tqdm

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser):
        """
        Arguments that have the custom command runserver
        """
        parser.add_argument(
            "-fd", "--fromdate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d"),
            help='Determine date from for get data of commisions')
        parser.add_argument(
            "-td", "--todate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d"),
            help='Determine date to for get data of commisions'
        )
        parser.add_argument("-c", "--campaign", default="yajuego 80",
                            choices=['yajuego 80', 'yajuego 50'], help='Title of campaign')
        parser.add_argument("-f", "--file", nargs='?', help=('name of file where storage the csv, if supplied the DB '
                                                             'statement will not executed'))

    def handle(self, *args, **options):
        """
        Get data from API of bookmaker Yajuego with CSV files using 
        the pandas module with high performance, on command use tqdm for 
        progress bar

        CSV columns
        ---
        - rowid : `np.unint8`
            Indicates if value is summary or single entry (1 for single entry, 
            2 for summarized data)
        - currencysymbol : `string`
            Indicates the used currency on money operations like deposits, 
            stake operations like `COP`
        - siteid : `string`
            Equivalent to prom_code from Model `Link` and `AccountReport`
        - playerid : `string`
            Equivalent to punter_id from Model `AccountReport`, unique 
            identification (username, database id) for identificate a punter on 
            Bookmaker
        - Deposits : `np.float32`
            Equivalent to deposit from Model `AccountReport`, quantity of money 
            that user has Deposited to their account for bet's on boomaker web
        - stake:`np.uint32`
            Total of wagered money by player/punter_id on supplied date,
            the Netrevunue can determinate if player won or loss the
            bets
        - CPACommission : `np.float32`
            Equivalent to fixed_income from Model `AccountReport`, fixed_income payed
            fixed_income for cpa's completed
        - Netrevenue : `np.float32`
            Equivalent to net_revenue from Model `AccountReport`, Profit earned 
            so far from that player/punter_id, usually this is the stake - 20% 
            of stake (only if player/punter_id loss all bet), this take a 
            positive value when player/punter_id loss money and take negative 
            when player/punter_id won the bets. This is a sum of outcomes 
            (results) of the bets has the player/punter_id placed and the 
            bookmaker received a result. This not have the count of bets
        - %Commission : `np.float32`
            Equivalent to revenue_share from Model `AccountReport`, Revenue 
            Share from users, this is the 20% of stake (only if 
            player/punter_id loss all bet). this take a positive value when 
            player/punter_id loss money and take negative when player/punter_id 
            won the bets. This is a sum of outcomes (results) of the bets has 
            the player/punter_id placed and the bookmaker received a result. 
            This not have the count of bets
        - cpacommissioncount : `np.uint32`
            Equivalent to cpa_betenlace and cpa_partner from Model 
            `AccountReport`, managed with value cpa_count, this is the quantity 
            of cpa's, for every punter can have value 0 or 1
        - registrationdate : `string`
            Equivalent to registration_at from Model `AccountReport`
            with format mm/dd/aaaa
        - firstdeposit : `string`
            Equivalent to registration_at from Model `AccountReport`
            with format mm/dd/aaaa
        ### Index columns
        The columns of pandas dataframe are indexed for this way
        'row_id': 0, 
        'currency_symbol': 1, 
        'prom_code': 2, 
        'punter_id': 3, 
        'deposit': 4, 
        'stake': 5, 
        'fixed_income': 6, 
        'net_revenue': 7, 
        'revenue_share': 8, 
        'cpa_count': 9, 
        'registered_at': 10, 
        'first_deposit_at': 11,
        """
        logger.info("Making call to API Account Ya juego")
        logger.info(f"Campaign Title -> {options.get('campaign')}")
        logger.info(f"From date -> {options.get('fromdate')}")
        logger.info(f"To date -> {options.get('todate')}")
        logger.info(f"File to save -> {options.get('file')}")
        from_date_str = options.get("fromdate")
        to_date_str = options.get("todate")
        campaign_title = options.get("campaign")
        try:
            from_date = datetime.strptime(from_date_str, "%Y/%m/%d")
            to_date = datetime.strptime(to_date_str, "%Y/%m/%d")
            if from_date > to_date:
                logger.error("\"From date\" is greather than \"to date\"")
                return
        except:
            logger.error("'From date' or 'to date' have bad format. Expected format\"aaaa/mm/dd\"")
            return

        # Get id of Campaign Title
        filters = [Q(campaign_title__iexact=campaign_title)]
        campaign = Campaign.objects.using(DB_USER_PARTNER).annotate(
            campaign_title=Concat('bookmaker__name', Value(' '), 'title')).filter(*filters).first()
        if not campaign:
            logger.error(f"Campaign with title \"{campaign_title}\" not found in DB")
            return

        if (campaign_title == "yajuego 80"):
            yajuego_key = settings.API_ACCOUNT_REPORT_YAJUEGO80_KEY
            yajuego_account_id = settings.API_ACCOUNT_REPORT_YAJUEGO80_ACCOUNT_ID

        if (campaign_title == "yajuego 50"):
            yajuego_key = settings.API_ACCOUNT_REPORT_YAJUEGO50_KEY
            yajuego_account_id = settings.API_ACCOUNT_REPORT_YAJUEGO50_ACCOUNT_ID

        try:
            url = (
                "https://webaffiliates.yajuego.co/api/affreporting.asp?"
                f"key={yajuego_key}&reportname=AccountReport&"
                f"reportformat=csv&reportmerchantid={yajuego_account_id}"
                "&reportstartdate=" + from_date_str + "&reportenddate=" + to_date_str)
            response = requests.get(url)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error((
                "Something is wrong at get data from API, check if current "
                "connection IP/VPN is on Whitelist of API server, if problem "
                f"still check traceback:\n\n{''.join(e)}"
            ))
            return

        try:
            # set the characters and line based interface to stream I/O
            data_io = StringIO(
                response.text[response.text.index("\"rowid\""):])
        except:
            if "No Records" in response.text:
                logger.error("Data not found at requested url")
                logger.error(f"Request url: {url}")
                logger.error("Data obtained")
                logger.error(response.text)
                return

            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error((
                "Something is wrong at get data from API, check the credentials"
                " (key and reportmerchantid) if problem persist check traceback:"
                f"\n\n{''.join(e)}"
            ))
            logger.error(f"Request url: {url}")
            logger.error("Data obtained")
            logger.error(response.text)
            return
        # Create the DataFrame
        cols_to_use = [
            "rowid",
            "currencysymbol",
            "siteid",
            "playerid",
            "Deposits",
            "stake",
            "CPACommission",
            "Netrevenue",
            "%Commission",
            "cpacommissioncount",
            "registrationdate",
            "firstdeposit",
        ]
        df = pd.read_csv(data_io, sep=",",
                         usecols=cols_to_use, dtype={
                             "rowid": np.uint8,
                             "currencysymbol": "string",
                             "siteid": "string",
                             "playerid": "string",
                             "Deposits": np.float32,
                             "stake": np.float32,
                             "CPACommission": np.float32,
                             "Netrevenue": np.float32,
                             "%Commission": np.float32,
                             "cpacommissioncount": np.uint32,
                             "registrationdate": "string",
                             "firstdeposit": "string",
                         }
                         )[cols_to_use]

        df.rename(inplace=True,
                  columns={
                      "rowid": "row_id",
                      "currencysymbol": "currency_symbol",
                      "siteid": "prom_code",
                      "playerid": "punter_id",
                      "Deposits": "deposit",
                      "stake": "stake",
                      "CPACommission": "fixed_income",
                      "Netrevenue": "net_revenue",
                      "%Commission": "revenue_share",
                      "cpacommissioncount": "cpa_count",
                      "registrationdate": "registered_at",
                      "firstdeposit": "first_deposit_at",
                  }
                  )
        # "rowid","currencysymbol","totalrecords","merchantname",
        # "memberid","username","siteid","bannerid","creativename",
        # "bannertype","playerid","registrationdate","merchplayername",
        # "firstdeposit","Deposits","Netrevenue","stake","%Commission",
        # "CPACommission","cpacommissioncount","totalcommission","new"

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df.to_csv(options.get("file"), index=False, encoding="utf-8")
            return

        # Filter data - Override in same place of memory group/sum data
        # rowid == 2
        df.drop(df[df.eval("(row_id == 2)", engine='numexpr')].index, inplace=True)

        if(from_date != to_date):
            logger.error("Date from and to are equal this data cannot be used for update on DB")
            return

        # Get related link from prom_codes and campaign, QUERY
        filters = [Q(prom_code__in=df.prom_code.unique()), Q(campaign_id=campaign.id)]
        links = Link.objects.filter(*filters).select_related("partner_link_accumulated")

        # Get account reports from previous links and punter_id, QUERY
        filters = [Q(link__in=links.values_list("pk", flat=True)), Q(punter_id__in=df.punter_id.unique())]
        account_reports = AccountReport.objects.filter(*filters)

        # Acumulators bulk create and update
        account_reports_update = []
        account_reports_create = []

        # Set keys by index based on colum names of Dataframe
        keys = {key: index for index, key in enumerate(df.columns.values)}

        # Dictionary with sum of cpa's by prom_code
        cpa_by_prom_code_sum = {}

        # Dictionary with current applied sum of cpa's by prom_code
        cpa_by_prom_code_iter = {}
        for prom_code in df.prom_code.unique():
            cpa_by_prom_code_sum[prom_code] = df.loc[df.prom_code.values == prom_code, "cpa_count"].sum()
            cpa_by_prom_code_iter[prom_code] = 0

        for row in tqdm(zip(*df.to_dict('list').values())):
            """
            'row_id': 0, 
            'currency_symbol': 1, 
            'prom_code': 2, 
            'punter_id': 3, 
            'deposit': 4, 
            'stake': 5, 
            'fixed_income': 6, 
            'net_revenue': 7, 
            'revenue_share': 8, 
            'cpa_count': 9, 
            'registered_at': 10, 
            'first_deposit_at': 11,
            """
            if(row[keys.get("cpa_count")] > 1):
                # Prevent a cpacommissioncount bad value
                logger.error(
                    f"cpa_count is greather than one! punter {row[keys.get('punter_id')]}, campaign {campaign_title}")
                return

            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)

            if not link:
                logger.warning(f"Link with prom_code={row[keys.get('prom_code')]} and campaign={campaign_title}")
                continue

            # Get currency
            try:
                currency = eval(f"CurrencyAll.{row[keys.get('currency_symbol')].upper()}")
            except:
                logger.error(f"Currency value {row[keys.get('currency_symbol')].upper()} undefined or bad value with "
                             f"prom_code={row[keys.get('prom_code')]} and campaign={campaign_title}")
                continue

            # Check registrationdate null
            if (not pd.isna(row[keys.get("registered_at")])):
                registered_at = make_aware(datetime.strptime(row[keys.get("registered_at")], "%m/%d/%Y"))
            else:
                registered_at = None

            # Check registrationdate null
            if (not pd.isna(row[keys.get("first_deposit_at")])):
                first_deposit_at = make_aware(datetime.strptime(row[keys.get("first_deposit_at")], "%m/%d/%Y"))
            else:
                first_deposit_at = None

            # Get current entry of account report based on link and punter_id
            account_report = next(
                filter(
                    lambda account_report: account_report.link_id == link.pk and
                    account_report.punter_id == row[keys.get("punter_id")], account_reports
                ),
                None
            )

            # Get current partner that have the current link
            partner_link_accumulated = link.partner_link_accumulated
            if partner_link_accumulated:
                # Validate if link has relationship with partner and if has verify if status is equal to status campaign
                if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                    # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                    if(campaign.status == Campaign.Status.INACTIVE) and (to_date.date() >= campaign.last_inactive_at.date()):
                        msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                        logger.warning(msg)
                        partner_link_accumulated = None
                elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                    msg = f"link with prom_code {partner_link_accumulated.prom_code} has custom status inactive"
                    logger.warning(msg)
                    partner_link_accumulated = None

            if account_report:
                # Case and exist entry
                # Fixed income is according to cpa
                if(row[keys.get("cpa_count")] == 1 and account_report.cpa_betenlace):
                    logger.warning((
                        f"cpa_commissioncount for punter {row[keys.get('punter_id')]} on campaign {campaign_title} "
                        "is already with value 1, something is wrong with data"))
                    continue
                account_report_update = self.account_report_update(
                    keys, row, from_date, first_deposit_at, account_report, partner_link_accumulated,
                    cpa_by_prom_code_sum, cpa_by_prom_code_iter)
                account_reports_update.append(account_report_update)
            else:
                # Case new entry
                account_report_new = self.account_report_create(
                    row, keys, link, currency, registered_at, first_deposit_at, partner_link_accumulated, from_date,
                    cpa_by_prom_code_sum, cpa_by_prom_code_iter)

                account_reports_create.append(account_report_new)

        with transaction.atomic(using=DB_USER_PARTNER):
            if(account_reports_create):
                AccountReport.objects.bulk_create(account_reports_create)
            if(account_reports_update):
                AccountReport.objects.bulk_update(account_reports_update, (
                    "deposit",
                    "net_revenue",
                    "stake",
                    "fixed_income",
                    "partner_link_accumulated",
                    "net_revenue",
                    "revenue_share",
                    "cpa_betenlace",
                    "cpa_partner",
                    "registered_at",
                    "first_deposit_at",
                    "cpa_at",
                ))

    def calc_tracker(self, keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum,
                     cpa_by_prom_code_iter):
        """
        Calc cpa's according to Tracker value, tracker have values beetwhen 0 
        to 1.0 if partner have lesser to 1 apply tracker only if total of 
        cpa_count is higher than MIN_CPA_TRACKER_DAY
        """
        if(partner_link_accumulated.tracker < 1 and
           cpa_by_prom_code_sum.get(row[keys.get("prom_code")]) > settings.MIN_CPA_TRACKER_DAY
           ):
            tracker_cpa = math.floor(
                partner_link_accumulated.tracker*cpa_by_prom_code_sum.get(row[keys.get("prom_code")]))

            # if current_counted_cpa for partner is lesser than
            # tracker_cpa count for partner
            if(cpa_by_prom_code_iter.get(row[keys.get("prom_code")]) < tracker_cpa):
                account_report.cpa_partner = row[keys.get("cpa_count")]
                cpa_by_prom_code_iter[row[keys.get("prom_code")]] += 1
        else:
            account_report.cpa_partner = row[keys.get("cpa_count")]

        return account_report

    def account_report_update(
            self, keys, row, from_date, first_deposit_at, account_report, partner_link_accumulated,
            cpa_by_prom_code_sum, cpa_by_prom_code_iter):
        account_report.deposit += row[keys.get("deposit")]
        account_report.stake += row[keys.get("stake")]

        account_report.revenue_share += row[keys.get("revenue_share")]
        account_report.net_revenue += row[keys.get("net_revenue")]

        account_report.first_deposit_at = first_deposit_at

        if account_report.cpa_betenlace != 1:
            account_report.cpa_betenlace = row[keys.get("cpa_count")]
            if(row[keys.get("cpa_count")]):
                # Case when cpa is True or 1
                account_report.cpa_at = from_date
                account_report.fixed_income = row[keys.get("fixed_income")]
                account_report.partner_link_accumulated = partner_link_accumulated
                if(partner_link_accumulated):
                    account_report = self.calc_tracker(
                        keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum,
                        cpa_by_prom_code_iter)
        return account_report

    def account_report_create(
            self, row, keys, link, currency, registered_at, first_deposit_at, partner_link_accumulated, from_date,
            cpa_by_prom_code_sum, cpa_by_prom_code_iter):
        account_report = AccountReport(
            partner_link_accumulated=partner_link_accumulated,
            punter_id=row[keys.get("punter_id")],
            deposit=row[keys.get("deposit")],
            stake=row[keys.get("stake")],
            fixed_income=row[keys.get("fixed_income")],
            net_revenue=row[keys.get("net_revenue")],
            revenue_share=row[keys.get("revenue_share")],
            currency_condition=currency,
            currency_fixed_income=currency,
            cpa_betenlace=row[keys.get("cpa_count")],
            first_deposit_at=first_deposit_at,
            link=link,
            registered_at=registered_at,
            created_at=from_date,
        )

        if(row[keys.get("cpa_count")]):
            # Case when cpa is True or 1
            account_report.cpa_at = from_date
            if(partner_link_accumulated):
                account_report = self.calc_tracker(
                    keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum, cpa_by_prom_code_iter)
        return account_report
