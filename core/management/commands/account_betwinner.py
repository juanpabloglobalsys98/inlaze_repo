import json
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
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils.timezone import (
    datetime,
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
            "-fd",
            "--fromdate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            help="Determine date from for get data of commisions",
        )
        parser.add_argument(
            "-td",
            "--todate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            help="Determine date to for get data of commisions",
        )
        parser.add_argument(
            "-c",
            "--campaign",
            default="betwinner col",
            choices=(
                "betwinner br",
                "betwinner col",
                "betwinner latam",
            ),
            help="Title of campaign",
        )
        parser.add_argument(
            "-f",
            "--file",
            nargs='?',
            help="name of file where storage the csv, if supplied the DB statement will not executed",
        )

    def handle(self, *args, **options):
        """
        Get data from API of bookmaker BetWinner with CSV files using 
        the pandas module with high performance, on command use tqdm for 
        progress bar.

        Account report is the actions of every punter on range of date.

        Betwinner retrieve data via post with X-Access-Key and X-Secret-Key on 
        headers, this is a property affiliates

        ### CSV columns
        - prom_code : `string`
            Equivalent to raw var "SubID 1", used on Model `Link` and
            `MemberReport (Month, daily) for betenalce and partners`, this is
            the key that identifies a certain promotional link
        - deposit : `np.float32`
            Equivalent raw var "Deposits, $", used on Models `AccountReport`, 
            quantity of deposited money by punter
        - stake : `np.float32`
            Equivalent to raw var "Bet, $", quantity of wagered money by 
            punter
        - registered_at : `np.uint32`
            Equivalent to raw var "Registrations", used on Models 
            `AccountReport`, if value is 1 is that day of report when this user 
            was registered in case 0 not registered the determined day, other 
            is error
        - first_deposit_at : `np.uint32`
            Equivalent to raw var "First Deposits" used on Models `AccountReport`, 
            if value is 1 is that day of report when this user was made a 
            first deposit in case 0 not first deposit the determined day, other
            case is error
        - revenue_share : `np.float32`
            Equivalent to raw var "Revenue, $", used on Models `AccountReport`, 
            shared money by bookmaker to betenlace, 
            actually that value is fixed_income and not revenue_share

        ### Known vars for Betenlace
        - prom_code
        - punter_id
        - deposit
        - stake
        - revenue_share
        - registered_count
        - first_deposit_count
        """
        logger.info(
            "Making call to API Account BetWinner\n"
            f"Campaign Title -> \"{options.get('campaign')}\"\n"
            f"From date -> \"{options.get('fromdate')}\"\n"
            f"To date -> \"{options.get('todate')}\"\n"
            f"File to save -> \"{options.get('file')}\""
        )
        from_date_str = options.get("fromdate")
        to_date_str = options.get("todate")
        campaign_title = options.get("campaign")
        try:
            from_date = datetime.strptime(from_date_str, "%Y-%m-%d")
            to_date = datetime.strptime(to_date_str, "%Y-%m-%d")
            if from_date > to_date:
                logger.error(f"From date: \"{from_date}\" is greather than to date: \"{to_date}\"")
                return
        except:
            logger.error(
                f"From date: \"{from_date}\" or to date: \"{to_date}\" have bad format. Expected format\"YYYY-mm-dd\""
            )
            return

        # Get id of Campaign Title
        filters = [Q(campaign_title__iexact=campaign_title)]
        campaign = Campaign.objects.using(DB_USER_PARTNER).annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        ).filter(
            *filters,
        ).first()
        if not campaign:
            logger.error(f"Campaign with title \"{campaign_title}\" not found in DB")
            return

        # Quantity in USD for count every CPA, This is TEMPORARLY
        revenue_for_cpa = 35

        if (campaign_title == "betwinner col"):
            betwinner_access_key = settings.API_MEMBER_REPORT_BETWINNERCOL_ACCESS_KEY
            betwinner_secret_key = settings.API_MEMBER_REPORT_BETWINNERCOL_SECRET_KEY

        if (campaign_title == "betwinner br"):
            betwinner_access_key = settings.API_MEMBER_REPORT_BETWINNERBR_ACCESS_KEY
            betwinner_secret_key = settings.API_MEMBER_REPORT_BETWINNERBR_SECRET_KEY

        if (campaign_title == "betwinner latam"):
            betwinner_access_key = settings.API_MEMBER_REPORT_BETWINNERLATAM_ACCESS_KEY
            betwinner_secret_key = settings.API_MEMBER_REPORT_BETWINNERLATAM_SECRET_KEY

        url = "https://api.betwinneraffiliates.com/affiliates/reports"
        headers = {
            "X-Access-Key": betwinner_access_key,
            "X-Secret-Key": betwinner_secret_key,
            "HTTP_ACCEPT": "application/json",
            "HTTP_CONTENT_TYPE": "application/json",
        }
        body = {
            "from": f"{from_date_str} 00:00:00",
            "to": f"{to_date_str} 23:59:59",
            "limit": 1,
            "offset": 0,
            # Vars that will group by the incoming data from Betwinner
            "dimensions": [
                "subid1",
                "site_player_id",
            ],
            # Others var that will shown on incoming data (At group by will sum)
            "metrics": [
                "deposits_all_sum",
                "bet_new_sum",
                "registrations_count",
                "deposits_first_count",
                "revenue_sum",
            ],
            "sorting": [
                {
                    "sort_by": "subid1",
                    "sort_dir": "desc",
                },
            ],
            "filters": {},
            "having": {},
            "search": {},
            "players_filter": "all",
            "metrics_format": "raw",
        }

        response_obj = requests.post(url=url, json=body, headers=headers)

        try:
            response = json.loads(response_obj.text)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            logger.error(
                "Something is wrong at get data from API, check if current "
                "connection IP/VPN is on Whitelist of API server,\n\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"request headers: {headers}\n"
                f"response status:\n{response_obj.status_code}\n"
                f"response text:\n{response_obj.text}\n\n"
                f"if problem still check traceback:\n{''.join((e))}"
            )
            return

        if not response.get("success"):
            logger.error(
                f"report not success with code: {response.get('code')}, message: {response.get('message')}"
            )
            return

        try:
            response_obj = requests.get(url=response.get("misc").get("export_urls").get("csv"))
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            logger.error(
                f"Something is wrong at get data from API csv exported\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"request headers: {headers}\n\n"
                f"response dict: {response}\n\n"
                f"If problem persist check traceback:\n\n{''.join(e)}"
            )
            return

        # Case csv empty, no records
        if(not response_obj.text):
            logger.warning(
                "Data not found at requested url\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"request headers: {headers}\n\n"
                f"response text: {response_obj.text}\n\n"
            )
            return

        try:
            # set the characters and line based interface to stream I/O
            data_io = StringIO(response_obj.text)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            logger.error(
                "Something is wrong at get data from API, check the credentials\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"request headers: {headers}\n\n"
                f"response text: {response_obj.text}\n\n"
                "If problem persist check traceback:"
                f"\n\n{''.join(e)}"
            )
            return

        # Create the DataFrame
        cols_to_use = [
            "SubID 1",
            "Player ID",
            "Deposits, $",
            "Bets, $",
            "Revenue, $",
            "Registrations",
            "First Deposits",
        ]
        df = pd.read_csv(
            filepath_or_buffer=data_io,
            sep=",",
            usecols=cols_to_use,
            dtype={
                "SubID 1": "string",
                "Player ID": "string",
                "Deposits, $": np.float32,
                "Bets, $": np.float32,
                "Revenue, $": np.float32,
                "Registrations": np.uint32,
                "First Deposits": np.uint32,
            },
        )[cols_to_use]

        df.rename(
            inplace=True,
            columns={
                "SubID 1": "prom_code",
                "Player ID": "punter_id",
                "Deposits, $": "deposit",
                "Bets, $": "stake",
                "Revenue, $": "revenue_share",
                "Registrations": "registered_count",
                "First Deposits": "first_deposit_count",
            },
        )

        # "SubID 1","Player ID","Impressions","Visits","Registrations",
        # "First Deposits","Deposits","First Deposits, $","Deposits, $",
        # "Withdrawals,$","Players w/ Bet","Profit, $","Chargebacks, $",
        # "Commissions, $","Revenue, $"

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df.to_csv(
                path_or_buf=options.get("file"),
                index=False,
                encoding="utf-8",
            )
            return

        # Filter data - Override in same place of memory data about non
        # authenticated punters
        df.drop(
            labels=df[df.eval("(punter_id == 'Not Registered')", engine='numexpr')].index,
            inplace=True,
        )

        if(from_date != to_date):
            logger.error("Date from and to are equal this data cannot be used for update on DB")
            return

        # Get related link from prom_codes and campaign, QUERY
        filters = (
            Q(prom_code__in=df.prom_code.unique()),
            Q(campaign_id=campaign.id),
        )
        links = Link.objects.filter(*filters).select_related("partner_link_accumulated")

        # Get account reports from previous links and punter_id, QUERY
        filters = (
            Q(link__in=links.values_list("pk", flat=True)),
            Q(punter_id__in=df.punter_id.unique()),
        )
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
            cpa_by_prom_code_sum[prom_code] = int(
                df.loc[df.prom_code.values == prom_code, "revenue_share"].sum()/revenue_for_cpa
            )
            cpa_by_prom_code_iter[prom_code] = 0

        for row in tqdm(zip(*df.to_dict('list').values())):
            """
            prom_code
            punter_id
            deposit
            stake
            revenue_share
            registered_count
            first_deposit_count
            """
            cpa_count = int(row[keys.get("revenue_share")]/revenue_for_cpa)

            if(cpa_count > 1):
                # Prevent a cpacommissioncount bad value
                logger.error(
                    f"cpa_count is greather than one! punter_id=\"{row[keys.get('punter_id')]}\", campaign=\"{campaign_title}\", "
                    f"revenue share based=\"{row[keys.get('revenue_share')]}\"")
                return

            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)

            if not link:
                logger.warning(f"Link with prom_code=\"{row[keys.get('prom_code')]} and campaign=\"{campaign_title}\"")
                continue

            # Check registrationdate null registered_count
            if (row[keys.get("registered_count")] == 1):
                registered_at = from_date.date()
            elif(row[keys.get("registered_count")] > 1):
                logger.warning(
                    f"Registered_count have value greather than 1 for account report with\n"
                    f"campaign_title=\"{campaign_title}\"\n"
                    f"prom_code=\"{row[keys.get('prom_code')]}\"\n"
                    f"punter_id=\"{row[keys.get('punter_id')]}\"\n"
                    f"registered_count=\"{row[keys.get('registered_count')]}\""
                )
                registered_at = None
            else:
                registered_at = None

            # Check registrationdate null
            if (row[keys.get("first_deposit_count")] == 1):
                first_deposit_at = from_date.date()
            elif (row[keys.get("first_deposit_count")] > 1):
                logger.warning(
                    f"first_deposit_count have value greather than 1 for account report with:\n"
                    f"campaign_title=\"{campaign_title}\n"
                    f"prom_code=\"{row[keys.get('prom_code')]}\"\n"
                    f"punter_id=\"{row[keys.get('punter_id')]}\"\n"
                    f"first deposit count {row[keys.get('first_deposit_count')]}"
                )
                first_deposit_at = None
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
                if(cpa_count == 1 and account_report.cpa_betenlace):
                    logger.warning(
                        f"cpa_commissioncount for punter_id=\"{row[keys.get('punter_id')]}\" on "
                        f"campaign_title=\"{campaign_title}\" is already with value 1, something is wrong with data"
                    )
                    continue
                account_report_update = self.account_report_update(
                    keys, row, from_date, first_deposit_at, registered_at, account_report, partner_link_accumulated,
                    cpa_by_prom_code_sum, cpa_by_prom_code_iter, cpa_count)
                account_reports_update.append(account_report_update)
            else:
                # Case new entry
                account_report_new = self.account_report_create(
                    row, keys, link, campaign, registered_at, first_deposit_at, partner_link_accumulated, from_date,
                    cpa_by_prom_code_sum, cpa_by_prom_code_iter, cpa_count)

                account_reports_create.append(account_report_new)

        with transaction.atomic(using=DB_USER_PARTNER):
            if(account_reports_create):
                AccountReport.objects.bulk_create(account_reports_create)
            if(account_reports_update):
                AccountReport.objects.bulk_update(account_reports_update, (
                    "partner_link_accumulated",
                    "deposit",
                    "stake",
                    "fixed_income",
                    "cpa_betenlace",
                    "cpa_partner",
                    "registered_at",
                    "first_deposit_at",
                    "cpa_at",
                ))

    def calc_tracker(self, keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum,
                     cpa_by_prom_code_iter, cpa_count):
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
                account_report.cpa_partner = cpa_count
                cpa_by_prom_code_iter[row[keys.get("prom_code")]] += 1
        else:
            account_report.cpa_partner = cpa_count

        return account_report

    def account_report_update(
            self, keys, row, from_date, first_deposit_at, registered_at, account_report, partner_link_accumulated,
            cpa_by_prom_code_sum, cpa_by_prom_code_iter, cpa_count):
        account_report.deposit += row[keys.get("deposit")]
        account_report.stake += row[keys.get("stake")]

        if registered_at:
            account_report.registered_at = registered_at

        if first_deposit_at:
            account_report.first_deposit_at = first_deposit_at

        if account_report.cpa_betenlace != 1:
            account_report.cpa_betenlace = cpa_count
            if cpa_count:
                # Case when cpa is True or 1
                account_report.cpa_at = from_date.date()
                account_report.fixed_income = row[keys.get("revenue_share")]
                account_report.partner_link_accumulated = partner_link_accumulated

                if(partner_link_accumulated):
                    account_report = self.calc_tracker(
                        keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum,
                        cpa_by_prom_code_iter, cpa_count)
        return account_report

    def account_report_create(
            self, row, keys, link, campaign, registered_at, first_deposit_at, partner_link_accumulated, from_date,
            cpa_by_prom_code_sum, cpa_by_prom_code_iter, cpa_count):
        account_report = AccountReport(
            partner_link_accumulated=partner_link_accumulated,
            punter_id=row[keys.get("punter_id")],
            deposit=row[keys.get("deposit")],
            stake=row[keys.get("stake")],

            fixed_income=row[keys.get("revenue_share")],

            currency_condition=campaign.currency_condition,
            currency_fixed_income=campaign.currency_fixed_income,

            cpa_betenlace=cpa_count,
            first_deposit_at=first_deposit_at,
            link=link,
            registered_at=registered_at,
            created_at=from_date,
        )

        if cpa_count:
            # Case when cpa is True or 1
            account_report.cpa_at = from_date
            if partner_link_accumulated:
                account_report = self.calc_tracker(
                    keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum, cpa_by_prom_code_iter,
                    cpa_count)
        return account_report
