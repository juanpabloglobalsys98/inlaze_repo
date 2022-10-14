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
    BetenlaceCPA,
    BetenlaceDailyReport,
    Campaign,
    FxPartner,
    FxPartnerPercentage,
    Link,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
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
        parser.add_argument("-c", "--campaign", default="rushbet col",
                            choices=('rushbet col',), help='Title of campaign')
        parser.add_argument(
            "-fr", "--file_raw", nargs='?',
            help=(
                'name of file where storage the raw input file, if supplied the DB '
                'statement will not executed, only can create file raw or file normal one at time'
            )
        )
        parser.add_argument("-f", "--file", nargs='?', help=('name of file where storage the csv, if supplied the DB '
                                                             'statement will not executed'))
        parser.add_argument("-upm", "--update_month", choices=["False", "True"], default="True",
                            help='Full update or not the month accumulated data')

    def handle(self, *args, **options):
        """
        Get data from API of bookmaker Rushbet with CSV files using 
        the pandas module with high performance, on command use tqdm for 
        progress bar.

        Member report is the summarized data from all punters of range of date

        CSV columns
        ---
        - rowid : `np.uint8`
            row that indicastes 1 for normal data, 2 for summarized data, 
            equivalent to `row_id`
        - currencysymbol : `string`
            Indicates the used currency on money operations like
            deposits, stake operations like COP
        - siteid : `string`
            Equivalent to prom_code from Model `Link` and
            `MemberReport (Month, daily) for betenalce and partners`
        - newpurchases : `np.float32`
            Equivalent to deposit from Model MemberReport (Month, daily) for 
            betenlace and partners`, this is a positive integer. WARNING
            Newpurchases may refers to only first deposit. normal purchases
            are not present on Rushbet
        - netwagers:`np.uint32`
            Equivalent to stake, total of wagered money by player/punter_id on 
            supplied date, the Netrevunue can determinate if player won or loss 
            the bets. THIS VALUE ARE NOT SUPPLIED FOR Rushbet
        - netrevenue : `np.float32`
            Profit earned so far from that player/punter_id, usually this is
            the stake - 20% of stake (only if player/punter_id loss all bet), this
            take a positive value when player/punter_id loss money and take
            negative when player/punter_id won the bets. This is a sum of
            outcomes (results) of the bets has the player/punter_id placed
            and the bookmaker received a result. This not have the count 
            of bets. 

            THIS VALUE ARE NOT SUPPLIED FOR Rushbet but is calculated 
            based on revenue share could generate an incorrect values
        - revsharecommission : `np.float32`
            Comission earned from users, this is the 20% of stake (only 
            if player/punter_id loss all bet). this take a positive value 
            when player/punter_id loss money and take negative when 
            player/punter_id won the bets. This is a sum of
            outcomes (results) of the bets has the player/punter_id placed
            and the bookmaker received a result. This not have the count 
            of bets

        ### Index columns
        The columns of pandas dataframe are indexed for this way
        "row_id":0
        "currency_symbol":1
        "prom_code":2
        "deposit":3
        "stake":4
        "fixed_income":5
        "net_revenue":6
        "revenue_share":7
        "registered_count":8
        "cpa_count":9
        "first_deposit_count":10
        "wagering_count":11
        """
        logger.info("Making call to API Account Rushbet")
        logger.info(f"Campaign Title -> {options.get('campaign')}")
        logger.info(f"From date -> {options.get('fromdate')}")
        logger.info(f"To date -> {options.get('todate')}")
        logger.info(f"File to save raw -> {options.get('file_raw')}")
        logger.info(f"File to save -> {options.get('file')}")
        logger.info(f"update month -> {options.get('update_month')}")
        from_date_str = options.get("fromdate")
        to_date_str = options.get("todate")
        campaign_title = options.get("campaign")
        update_month = eval(options.get("update_month"))

        try:
            from_date = make_aware(datetime.strptime(from_date_str, "%Y/%m/%d"))
            to_date = make_aware(datetime.strptime(to_date_str, "%Y/%m/%d"))
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

        if (campaign_title == "rushbet col"):
            rushbet_key = settings.API_ACCOUNT_REPORT_RUSHBETCOL_KEY
            rushbet_account_id = settings.API_ACCOUNT_REPORT_RUSHBETCOL_ACCOUNT_ID
            revenue_share_percentage = 0.30

        try:
            url = (
                "https://partners.rush-affiliates.com/api/affreporting.asp?"
                f"key={rushbet_key}&reportname=Member%20Report%20-%20Detailed&reportformat=csv&"
                f"reportmerchantid={rushbet_account_id}&"
                f"reportstartdate={from_date_str}&reportenddate={to_date_str}"
            )
            response = requests.get(url)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error(
                "Something is wrong at get data from API, check if current "
                "connection IP/VPN is on Whitelist of API server, if problem "
                f"still check traceback:\n\n{''.join(e)}"
            )
            return

        try:
            # set the characters and line based interface to stream I/O
            data_io = StringIO(response.text[response.text.index("\"rowid\""):])
        except:
            if "No Records" in response.text:
                logger.error("Data not found at requested url")
                logger.error(f"Request url: {url}")
                logger.error("Data obtained")
                logger.error(response.text)
                return

            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error(
                "Something is wrong at get data from API, check the credentials"
                " (key and reportmerchantid) if problem persist check traceback:"
                f"\n\n{''.join(e)}"
            )
            logger.error(f"Request url: {url}")
            logger.error("Data obtained")
            logger.error(response.text)
            return

        if(options.get("file_raw")):
            with open(f"{options.get('file_raw')}.csv", "w") as out:
                # File case save to disk and prevent execute on DB
                out.write(response.text[response.text.index("\"rowid\""):])
                return

        # Create the DataFrame
        cols_to_use = [
            "rowid",
            "currencysymbol",
            "siteid",
            "revsharecommission",
            "downloads",
            "firstdepositcount",
            "wageraccountcount",
            "newpurchases",
        ]
        df = pd.read_csv(data_io, sep=",",
                         usecols=cols_to_use, dtype={
                             "rowid": np.uint8,
                             "currencysymbol": "string",
                             "siteid": "string",
                             "revsharecommission": np.float32,
                             "downloads": np.uint32,
                             "firstdepositcount": np.uint32,
                             "wageraccountcount": np.uint32,
                         }
                         )[cols_to_use]

        df.rename(inplace=True,
                  columns={
                      "rowid": "row_id",
                      "currencysymbol": "currency_symbol",
                      "siteid": "prom_code",
                      # Warning newpurchases may only apply for only
                      # first deposit
                      "newpurchases": "deposit",
                      "revsharecommission": "revenue_share",
                      "downloads": "registered_count",
                      "firstdepositcount": "first_deposit_count",
                      "wageraccountcount": "wagering_count",
                  }
                  )

        # "rowid","currencysymbol","totalrecords","period","merchantname",
        # "memberid","username","country","memberid","siteid","sitename",
        # "impressions","clicks","clickthroughratio","downloads",
        # "downloadratio","newaccountratio","newdepositingacc","newaccounts",
        # "firstdepositcount","activeaccounts","activedays","newpurchases",
        # "purchaccountcount","wageraccountcount","avgactivedays",
        # "netrevenueplayer","purchases","netrevenue","netwagers",
        # "prod1wagers","prod1netrevenue","prod1commission",
        # "revsharecommission","totalcpacommission","cpacommissioncount",
        # "referralcommissiontotal","totalcommission"

        # "rowid","currencysymbol","totalrecords","period","merchantname",
        # "memberid","username","country","memberid","siteid","sitename",
        # "impressions","clicks","clickthroughratio","downloads",
        # "downloadratio","newaccountratio","newdepositingacc","newaccounts",
        # "firstdepositcount","activeaccounts","activedays","newpurchases",
        # "purchaccountcount","wageraccountcount","avgactivedays",
        # "revsharecommission","totalcommission"
        # "installs"

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df.to_csv(options.get("file"), index=False, encoding="utf-8")
            return

        # Filter data - Override in same place of memory group/sum data
        # rowid == 2
        df.drop(df[df.eval("(row_id == 2)", engine='numexpr')].index, inplace=True)
        # Setup dataframe to a list of dictionaries for best performance
        # df_dict = df.to_dict('records')

        if(from_date != to_date):
            logger.error("Date from and to are equal this data cannot be used for update on DB")
            return

        # Get related link from prom_codes and campaign, QUERY
        filters = [Q(prom_code__in=df.prom_code.unique()), Q(campaign_id=campaign.id)]
        links = Link.objects.filter(*filters).select_related("partner_link_accumulated").select_related("betenlacecpa")

        betnelacecpas = links.values_list("betenlacecpa", flat=True)

        betenlace_daily_reports = BetenlaceDailyReport.objects.filter(
            betenlace_cpa__in=betnelacecpas, created_at=from_date.date())
        partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(
            betenlace_daily_report__in=betenlace_daily_reports)

        # Get the last Fx value
        filters = [Q(created_at__lte=from_date+timedelta(days=1))]
        fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        if(fx_partner is None):
            # Get just next from supplied date
            filters = [Q(created_at__gte=from_date+timedelta(days=1))]
            fx_partner = FxPartner.objects.filter(*filters).order_by("created_at").first()

        # If still none prevent execution
        if(fx_partner is None):
            logger.error("Undefined fx_partner on DB")
            return

        fx_partner_percentage = fx_partner.fx_percentage

        campaing_currency_fixed_income_str = campaign.currency_fixed_income.lower()

        # Acumulators bulk create and update
        member_reports_betenlace_month_update = []
        member_reports_daily_betenlace_update = []
        member_reports_daily_betenlace_create = []

        member_reports_partner_month_update = []
        member_reports_daily_partner_update = []
        member_reports_daily_partner_create = []

        # Set keys by index based on colum names of Dataframe
        keys = {key: index for index, key in enumerate(df.columns.values)}

        # df.loc[np.isnan(df.fixed_income.values), "fixed_income"] = 0

        for row in tqdm(zip(*df.to_dict('list').values())):
            """
            "row_id":0
            "currency_symbol":1
            "prom_code":2
            "deposit":3
            "stake":4
            "fixed_income":5
            "net_revenue":6
            "revenue_share":7
            "registered_count":8
            "cpa_count":9
            "first_deposit_count":10
            "wagering_count":11
            """
            # Get link according to prom_code of current loop
            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)
            if not link:
                logger.warning(
                    f"Link with prom_code=\"{row[keys.get('prom_code')]}\" and campaign=\"{campaign_title}\" "
                    "not found on database")
                continue

            try:
                # Get current entry of member report based on link (prom_code)
                betenlace_cpa = link.betenlacecpa
            except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
                logger.error(f"Betenlace CPA entry not found for link with prom_code={row[keys.get('prom_code')]}")
                continue

            # Betenlace Month
            if(update_month):
                betenlace_cpa = self.betenlace_month_update(keys, row, betenlace_cpa, revenue_share_percentage)
                member_reports_betenlace_month_update.append(betenlace_cpa)

            # Betenlace Daily
            betenlace_daily = next(filter(lambda betenlace_daily: betenlace_daily.betenlace_cpa_id ==
                                   betenlace_cpa.pk and betenlace_daily.created_at == from_date.date(), betenlace_daily_reports), None)

            if(betenlace_daily):
                betenlace_daily = self.betenlace_daily_update(
                    keys, row, betenlace_daily, campaign, fx_partner, revenue_share_percentage)
                member_reports_daily_betenlace_update.append(betenlace_daily)
            else:
                betenlace_daily = self.betenlace_daily_create(
                    from_date.date(), keys, row, betenlace_cpa, campaign, fx_partner, revenue_share_percentage)
                member_reports_daily_betenlace_create.append(betenlace_daily)

            # Partner Month
            partner_link_accumulated = link.partner_link_accumulated

            # When partner have not assigned the link must be continue to next loop
            if(partner_link_accumulated is None):
                continue

            # Validate if link has relationship with partner and if has verify if status is equal to status campaign
            if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                if(campaign.status == Campaign.Status.INACTIVE) and (to_date.date() >= campaign.last_inactive_at.date()):
                    msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                    logger.warning(msg)
                    continue
            elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                msg = f"link with prom_code {partner_link_accumulated.prom_code} has custom status inactive"
                logger.warning(msg)
                continue

            # Tracker
            # if(row[keys.get('cpa_count')] > settings.MIN_CPA_TRACKER_DAY):
            #     cpa_count = math.floor(row[keys.get('cpa_count')]*partner_link_accumulated.tracker)
            # else:
            #     cpa_count = row[keys.get('cpa_count')]

            # Fx
            partner_currency_str = partner_link_accumulated.currency_local.lower()
            fx_book_partner = self.calc_fx(fx_partner, fx_partner_percentage,
                                           campaing_currency_fixed_income_str, partner_currency_str)

            fixed_income_partner_unitary = campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa
            # fixed_income_partner = cpa_count * fixed_income_partner_unitary
            fixed_income_partner_unitary_local = (
                campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa * fx_book_partner)
            # fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

            if(update_month):
                partner_link_accumulated = self.partner_link_month_update(partner_link_accumulated)
                member_reports_partner_month_update.append(partner_link_accumulated)

            # Partner Daily
            partner_link_daily = next(
                filter(
                    lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id ==
                    betenlace_daily.id, partner_link_dailies_reports),
                None)
            # partner_link_daily = PartnerLinkDailyReport.objects.filter(betenlace_daily_report=betenlace_daily).first()

            if(partner_link_daily):
                # Recalculate fixed_incomes for update
                fixed_income_partner_unitary = betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa
                # fixed_income_partner = cpa_count * fixed_income_partner_unitary
                fixed_income_partner_unitary_local = (
                    betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa * fx_book_partner)
                # fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

                partner_link_daily = self.partner_link_daily_update(
                    fx_book_partner, fx_partner_percentage, fixed_income_partner_unitary,
                    fixed_income_partner_unitary_local, partner_link_daily)
                member_reports_daily_partner_update.append(partner_link_daily)
            else:
                partner_link_daily = self.partner_link_daily_create(
                    from_date.date(), campaign, betenlace_daily,
                    partner_link_accumulated, fx_book_partner, fx_partner_percentage,
                    fixed_income_partner_unitary, fixed_income_partner_unitary_local)
                member_reports_daily_partner_create.append(partner_link_daily)

        with transaction.atomic(using=DB_USER_PARTNER):
            if(member_reports_betenlace_month_update):
                BetenlaceCPA.objects.bulk_update(member_reports_betenlace_month_update, (
                    "deposit",
                    "net_revenue",
                    "revenue_share",
                    "registered_count",
                    "first_deposit_count",
                    "wagering_count",
                ))

            if(member_reports_daily_betenlace_update):
                BetenlaceDailyReport.objects.bulk_update(member_reports_daily_betenlace_update, (
                    "deposit",
                    "net_revenue",
                    "revenue_share",
                    "fixed_income_unitary",
                    "fx_partner",
                    "registered_count",
                    "cpa_count",
                    "first_deposit_count",
                    "wagering_count",
                ))

            if(member_reports_daily_betenlace_create):
                BetenlaceDailyReport.objects.bulk_create(member_reports_daily_betenlace_create)

            if(member_reports_partner_month_update):
                PartnerLinkAccumulated.objects.bulk_update(member_reports_partner_month_update, (
                    "cpa_count",
                    "fixed_income",
                    "fixed_income_local",
                ))

            if(member_reports_daily_partner_update):
                PartnerLinkDailyReport.objects.bulk_update(member_reports_daily_partner_update, (
                    "fixed_income_unitary",
                    "fx_book_local",
                    "fx_percentage",
                    "fixed_income_unitary_local",
                ))

            if(member_reports_daily_partner_create):
                PartnerLinkDailyReport.objects.bulk_create(member_reports_daily_partner_create)

    def calc_fx(self, fx_partner, fx_partner_percentage, campaing_currency_fixed_income_str,
                partner_currency_str):
        if(campaing_currency_fixed_income_str != partner_currency_str):
            try:
                fx_book_partner = eval(
                    f"fx_partner.fx_{campaing_currency_fixed_income_str}_{partner_currency_str}") * fx_partner_percentage
            except:
                logger.error(
                    f"Fx conversion from {campaing_currency_fixed_income_str} to {partner_currency_str} undefined on DB")
        else:
            fx_book_partner = 1
        return fx_book_partner

    def partner_link_daily_create(
        self, from_date, campaign, betenlace_daily,
            partner_link_accumulated, fx_book_partner, fx_partner_percentage,
            fixed_income_partner_unitary, fixed_income_partner_unitary_local):
        partner_link_daily = PartnerLinkDailyReport(
            betenlace_daily_report=betenlace_daily,
            partner_link_accumulated=partner_link_accumulated,
            # fixed_income=fixed_income_partner,
            fixed_income_unitary=fixed_income_partner_unitary,
            currency_fixed_income=campaign.currency_fixed_income,
            fx_book_local=fx_book_partner,
            # fixed_income_local=fixed_income_partner_local,
            fixed_income_unitary_local=fixed_income_partner_unitary_local,
            currency_local=partner_link_accumulated.currency_local,
            # cpa_count=cpa_count,
            percentage_cpa=partner_link_accumulated.percentage_cpa,
            fx_percentage=fx_partner_percentage,
            tracker=partner_link_accumulated.tracker,
            created_at=from_date,
        )
        return partner_link_daily

    def partner_link_daily_update(
        self, fx_book_partner, fx_partner_percentage, fixed_income_partner_unitary,
            fixed_income_partner_unitary_local, partner_link_daily):

        partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary

        partner_link_daily.fx_book_local = fx_book_partner
        partner_link_daily.fx_percentage = fx_partner_percentage

        partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

        # partner_link_daily.cpa_count = cpa_count
        return partner_link_daily

    def partner_link_month_update(
            self, partner_link_accumulated):
        # partner_link_accumulated.cpa_count += cpa_count
        # partner_link_accumulated.fixed_income += fixed_income_partner
        # partner_link_accumulated.fixed_income_local += fixed_income_partner_local
        return partner_link_accumulated

    def betenlace_daily_create(
            self, from_date, keys, row, betenlace_cpa, campaign, fx_partner, revenue_share_percentage):
        betenlace_daily = BetenlaceDailyReport(
            betenlace_cpa=betenlace_cpa,

            deposit=row[keys.get('deposit')],
            # fixed_income=fixed_income,

            net_revenue=row[keys.get("revenue_share")] / revenue_share_percentage,
            revenue_share=row[keys.get("revenue_share")],
            fixed_income_unitary=campaign.fixed_income_unitary,

            fx_partner=fx_partner,

            currency_condition=campaign.currency_condition,
            currency_fixed_income=campaign.currency_fixed_income,

            registered_count=row[keys.get('registered_count')],
            # cpa_count=cpa_count,
            first_deposit_count=row[keys.get('first_deposit_count')],
            wagering_count=row[keys.get('wagering_count')],
            created_at=from_date)

        return betenlace_daily

    def betenlace_daily_update(self, keys, row, betenlace_daily, campaign, fx_partner, revenue_share_percentage):
        betenlace_daily.deposit = row[keys.get("deposit")]

        # betenlace_daily.fixed_income = cpa_count * fixed_income_campaign

        betenlace_daily.net_revenue = row[keys.get("revenue_share")] / revenue_share_percentage
        betenlace_daily.revenue_share = row[keys.get("revenue_share")]

        # betenlace_daily.cpa_count = cpa_count

        betenlace_daily.fixed_income_unitary = campaign.fixed_income_unitary

        betenlace_daily.fx_partner = fx_partner

        betenlace_daily.registered_count = row[keys.get('registered_count')]
        betenlace_daily.first_deposit_count = row[keys.get('first_deposit_count')]
        betenlace_daily.wagering_count = row[keys.get('wagering_count')]

        return betenlace_daily

    def betenlace_month_update(self, keys, row, betenlace_cpa, revenue_share_percentage):
        # betenlace_cpa.fixed_income += fixed_income_campaign * cpa_count

        betenlace_cpa.deposit += row[keys.get("deposit")]

        betenlace_cpa.net_revenue += row[keys.get("revenue_share")] / revenue_share_percentage
        betenlace_cpa.revenue_share += row[keys.get("revenue_share")]

        betenlace_cpa.registered_count += row[keys.get('registered_count')]
        # betenlace_cpa.cpa_count += cpa_count
        betenlace_cpa.first_deposit_count += row[keys.get('first_deposit_count')]
        betenlace_cpa.wagering_count += row[keys.get('wagering_count')]

        return betenlace_cpa
