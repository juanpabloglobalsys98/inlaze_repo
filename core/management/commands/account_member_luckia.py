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
            default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            help='Determine date from for get data of commisions')
        parser.add_argument(
            "-td", "--todate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            help='Determine date to for get data of commisions'
        )
        parser.add_argument("-c", "--campaign", default="luckia col",
                            choices=['luckia esp', 'luckia col'], help='Title of campaign')
        parser.add_argument("-f", "--file", nargs='?', help=('name of file where storage the csv, if supplied the DB '
                                                             'statement will not executed'))
        parser.add_argument("-upm", "--update_month", choices=["False", "True"], default="True",
                            help='Full update or not the month accumulated data')

    def handle(self, *args, **options):
        logger.info("Making call to API Luckia")
        logger.info("Campaign Title -> {}".format(options.get("campaign")))
        logger.info("From date -> {}".format(options.get("fromdate")))
        logger.info("To date -> {}".format(options.get("todate")))
        logger.info("File to save -> {}".format(options.get("file")))
        logger.info("update month -> {}".format(options.get("update_month")))
        from_date_str = options.get("fromdate")
        to_date_str = options.get("todate")
        campaign_title = options.get("campaign")
        update_month = eval(options.get("update_month"))

        try:
            from_date = datetime.strptime(from_date_str, "%Y-%m-%d")
            to_date = datetime.strptime(to_date_str, "%Y-%m-%d")
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
            logger.error("Campaign with title \"{}\" not found in DB".format(campaign_title))
            return

        if (campaign_title == "luckia col"):
            luckia_key = settings.API_ACCOUNT_MEMBER_REPORT_LUCKIA_KEY_COL
            revenue_share_percentage = settings.API_LUCKIA_RS_PERCENTAGE_COL

        if (campaign_title == "luckia esp"):
            luckia_key = settings.API_ACCOUNT_MEMBER_REPORT_LUCKIA_KEY_ESP
            revenue_share_percentage = settings.API_LUCKIA_RS_PERCENTAGE_ESP

        filter_not_data = "[{\"column\":\"No Data was Found\"}]"

        # Dataframe values
        cols_to_use = [
            "Customer Reference ID",
            "Marketing Source name",
            "Signup Date",
            "Deposits",
            "Turnover",
            "Total Net Revenue",
            "CPA Processed Date",
        ]

        # Excluded vars on Dataframe
        dtype_exclude = {
            "Alias": bool,
            "AffiliateID": bool,
            "Country": bool,
            "Reward Plan": bool,
            "Marketing Source ID": bool,
            "URL": bool,
            "Expiry Date": bool,
            "Customer Type": bool,
            "Customer Level": bool,
            "Brand": bool,
            "Customer Signup Source": bool,
            "Gross Revenue": bool,
            "Contributions": bool,
            "Transactions": bool,
            "Points": bool,
            "Adj (Chargebacks)": bool,
            "Total Net Revenue MTD": bool,
        }

        # Punters by activity
        url = (
            "https://redluckia.dataexport.netrefer.com/v2/export/reports/affiliate/XML_CustomerReporting_InclSubAff?"
            f"authorization={luckia_key}&playerID=all&username=all&websiteID=all&productID=all&brandID=all&"
            "customersource=all&customerTypeID=all&rewardplanID=all&countryID=all&FilterBySignUpDate=0&"
            f"FilterBySignUpDateFrom={from_date_str}&FilterBySignUpDateTo={to_date_str}&FilterByExpirationDate=0&"
            f"FilterByExpirationDateFrom=2016-10-01&FilterByExpirationDateTo=2016-10-31&FilterByActivityDate=1&"
            f"FilterByActivityDateFrom={from_date_str}&FilterByActivityDateTo={to_date_str}"
        )

        try:
            response_obj = requests.get(url)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error(
                "Something is wrong at get data from API for punters actvity filter, check if current "
                "connection IP/VPN is on Whitelist of API server."
                f"url: {url}\n\n"
                f"if problem still check traceback:\n\n{''.join(e)}"
            )
            return

        if (response_obj.status_code != 200):
            logger.error(
                f"Response for Punters by activity reach status {response_obj.status_code}, check "
                "credentials, connection IP/VPN is on Whitelist of API server or server status.\n"
                f"url: {url}"
            )
            return

        found_data = True
        if (response_obj.text == filter_not_data):
            logger.warning(
                f"Not found data by punters activity\n"
                f"url: {url}\n"
                f"Response text:\n{response_obj.text}\n"
                f"Response status:\n{response_obj.status_code}"
            )
            found_data = False

        # Initialize dataframe for punters filtered by activity date
        df_act = None
        if (found_data):
            # Stream I/O
            data_io = StringIO(response_obj.text)
            try:
                # Create the DataFrame
                df_act = pd.read_json(
                    data_io,
                    orient="records",
                    dtype={
                        "Customer Reference ID": "string",
                        "Marketing Source name": "string",
                        "Signup Date": "string",
                        "Deposits": np.float32,
                        "Turnover": np.float32,
                        "Total Net Revenue": np.float32,
                        "CPA Processed Date": "string",
                        # Others values that will ignored, set value with
                        # data type of smallest size
                        **dtype_exclude,
                    }
                )
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.error(
                    "Something is wrong at get data from API by punters activity, check the Authorization KEY "
                    "and whitelist on server API .\n"
                    f"url: {url}\n"
                    f"Response text:\n{response_obj.text}\n"
                    f"Response statis:\n{response_obj.status_code}\n\n"
                    f"if problem persist check traceback:\n\n{''.join(e)}"
                )
                return

            df_act = df_act[cols_to_use]
            df_act.rename(inplace=True,
                          columns={
                              "Customer Reference ID": "punter_id",
                              "Marketing Source name": "prom_code",
                              "Signup Date": "registered_at",
                              "Deposits": "deposit",
                              "Turnover": "stake",
                              "Total Net Revenue": "net_revenue",
                              "CPA Processed Date": "cpa_at",
                          }
                          )

            if (options.get("file")):
                df_act.to_csv(f"{options.get('file')}_activity.csv", index=False, encoding="utf-8")

        # Punters by Signup Date
        url = (
            "https://redluckia.dataexport.netrefer.com/v2/export/reports/affiliate/XML_CustomerReporting_InclSubAff?"
            f"authorization={luckia_key}&playerID=all&username=all&websiteID=all&productID=all&brandID=all&"
            "customersource=all&customerTypeID=all&rewardplanID=all&countryID=all&FilterBySignUpDate=1&"
            f"FilterBySignUpDateFrom={from_date_str}&FilterBySignUpDateTo={to_date_str}&FilterByExpirationDate=0&"
            f"FilterByExpirationDateFrom=2016-10-01&FilterByExpirationDateTo=2016-10-31&FilterByActivityDate=0&"
            f"FilterByActivityDateFrom={from_date_str}&FilterByActivityDateTo={to_date_str}"
        )

        try:
            response_obj = requests.get(url)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error(
                "Something is wrong at get data from API for Signup Date, check the Authorization KEY "
                "and whitelist on server API .\n"
                f"url: {url}\n"
                f"Response text:\n{response_obj.text}\n"
                f"Response statis:\n{response_obj.status_code}\n\n"
                f"if problem persist check traceback:\n\n{''.join(e)}"
            )
            return

        if (response_obj.status_code != 200):
            logger.error(
                f"Response for Signup Date reach status {response_obj.status_code}, check "
                "credentials, connection IP/VPN is on Whitelist of API server or server status.\n"
                f"url: {url}"
            )
            return

        found_data = True
        if (response_obj.text == filter_not_data):
            logger.warning(
                f"Not found data by punters Signup\n"
                f"url: {url}\n"
                f"Response text:\n{response_obj.text}\n"
                f"Response status:\n{response_obj.status_code}"
            )
            found_data = False

        # Initialize dataframe by Signup date
        df_sig = None
        if(found_data):
            # Stream I/O
            data_io = StringIO(response_obj.text)
            try:
                # Create the DataFrame
                df_sig = pd.read_json(
                    data_io,
                    orient="records",
                    dtype={
                        "Customer Reference ID": "string",
                        "Marketing Source name": "string",
                        "Signup Date": "string",
                        "Deposits": np.float32,
                        "Turnover": np.float32,
                        "Total Net Revenue": np.float32,
                        "CPA Processed Date": "string",
                        # Others values that will ignored, set value with
                        # smallest size
                        **dtype_exclude,
                    }
                )
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.error(
                    "Something is wrong at get data from API by punters Signup, check the Authorization KEY "
                    "and whitelist on server API .\n"
                    f"url: {url}\n"
                    f"Response text:\n{response_obj.text}\n"
                    f"Response statis:\n{response_obj.status_code}\n\n"
                    f"if problem persist check traceback:\n\n{''.join(e)}"
                )
                return

            df_sig = df_sig[cols_to_use]
            df_sig.rename(inplace=True,
                          columns={
                              "Customer Reference ID": "punter_id",
                              "Marketing Source name": "prom_code",
                              "Signup Date": "registered_at",
                              "Deposits": "deposit",
                              "Turnover": "stake",
                              "Total Net Revenue": "net_revenue",
                              "CPA Processed Date": "cpa_at",
                          }
                          )
            if(options.get("file")):
                # File case save to disk and prevent execute on DB
                df_sig.to_csv(f"{options.get('file')}_sigup_date.csv", index=False, encoding="utf-8")

        if(df_act is None and df_sig is None):
            logger.warning(f"No data for campaign {campaign_title} date_from {from_date_str} date_to {to_date_str}")
            return

        # Combine Dataframe by Activity date and Dataframe by Signup date
        df = pd.concat([df_act, df_sig], copy=False)

        # Remove Repeated punters, keep the first ocurrency and remove others
        df.drop_duplicates(subset=["punter_id"], inplace=True)

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df.to_csv(f"{options.get('file')}.csv", index=False, encoding="utf-8")
            return

        if(from_date != to_date):
            logger.error("Date from and to are equal this data cannot be used for update on DB")
            return

        # Get related link from prom_codes and campaign, QUERY
        filters = [Q(prom_code__in=df.prom_code.unique()), Q(campaign_id=campaign.id)]
        links = Link.objects.filter(*filters).select_related("partner_link_accumulated").select_related("betenlacecpa")

        # Get data for Account report
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
        date_filter_str = to_date.strftime("%d-%m-%Y")
        net_revenue_sum = {}
        deposit_revenue_sum = {}
        stake_revenue_sum = {}
        registered_count_sum = {}
        for prom_code in df.prom_code.unique():
            cpa_by_prom_code_sum[prom_code] = df.loc[
                (df.prom_code.values == prom_code) &
                (df.cpa_at.values == date_filter_str),
                "cpa_at"
            ].count()
            cpa_by_prom_code_iter[prom_code] = 0

            registered_count_sum[prom_code] = df.loc[
                (df.prom_code.values == prom_code) &
                (df.registered_at.values == date_filter_str),
                "registered_at"
            ].count()

            deposit_revenue_sum[prom_code] = 0
            stake_revenue_sum[prom_code] = 0
            net_revenue_sum[prom_code] = 0

        # Account Report Case
        for row in tqdm(zip(*df.to_dict('list').values())):
            """
            punter_id
            prom_code
            registered_at
            deposit
            stake
            net_revenue
            cpa_at
            """
            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)

            deposit_revenue_sum[row[keys.get("prom_code")]] += row[keys.get("deposit")]
            stake_revenue_sum[row[keys.get("prom_code")]] += row[keys.get("stake")]
            net_revenue_sum[row[keys.get("prom_code")]] += row[keys.get("net_revenue")]

            if not link:
                logger.warning(
                    f"Link with prom_code \"{row[keys.get('prom_code')]}\" and campaign \"{campaign_title}\" not found on database at Account report Case")
                continue

            # Check registration_date null
            if (row[keys.get("registered_at")]):
                registered_at = make_aware(datetime.strptime(row[keys.get("registered_at")], "%d-%m-%Y"))
            else:
                logger.warning(
                    f"registered_at is null on campaign title \"{campaign_title}\" with prom_code \"{row[keys.get('prom_code')]}\"")
                registered_at = None

            if (row[keys.get("cpa_at")]):
                cpa_at = make_aware(datetime.strptime(row[keys.get("cpa_at")], "%d-%m-%Y"))
                cpa_count = int(cpa_at.date() == from_date.date())
            else:
                cpa_at = None
                cpa_count = 0

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
                account_report_update = self.account_report_update(
                    keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum, cpa_by_prom_code_iter,
                    campaign, cpa_at, cpa_count, revenue_share_percentage)
                account_reports_update.append(account_report_update)
            else:
                # Case new entry
                account_report_new = self.account_report_create(
                    row, keys, link, registered_at, partner_link_accumulated, from_date, cpa_by_prom_code_sum,
                    cpa_by_prom_code_iter, campaign, cpa_at, cpa_count, revenue_share_percentage)
                account_reports_create.append(account_report_new)

        # Get data for Member report
        betnelacecpas = links.values_list("betenlacecpa", flat=True)

        betenlace_daily_reports = BetenlaceDailyReport.objects.filter(
            betenlace_cpa__in=betnelacecpas, created_at=from_date.date())
        partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(
            betenlace_daily_report__in=betenlace_daily_reports)

        # Get the last Fx value
        filters = [Q(created_at__lte=make_aware(from_date+timedelta(days=1)))]
        fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        if(fx_partner is None):
            # Get just next from supplied date
            filters = [Q(created_at__gte=make_aware(from_date+timedelta(days=1)))]
            fx_partner = FxPartner.objects.filter(*filters).order_by("created_at").first()

        # If still none prevent execution
        if(fx_partner is None):
            logger.error("Undefined fx_partner on DB")
            return

        filters = [Q(updated_at__lte=make_aware(from_date+timedelta(days=1)))]
        fx_partner_percentage = FxPartnerPercentage.objects.filter(*filters).order_by("-updated_at").first()

        if(fx_partner_percentage is None):
            # Get just next from supplied date
            filters = [Q(updated_at__gte=make_aware(from_date+timedelta(days=1)))]
            fx_partner_percentage = FxPartnerPercentage.objects.filter(*filters).order_by("updated_at").first()

        if(fx_partner_percentage is None):
            logger.warn("Undefined fx_partner on DB, using default 95%")
            fx_partner_percentage = 0.95
        else:
            fx_partner_percentage = fx_partner_percentage.percentage_fx

        campaing_currency_fixed_income_str = campaign.currency_fixed_income.lower()

        # Acumulators bulk create and update
        member_reports_betenlace_month_update = []
        member_reports_daily_betenlace_update = []
        member_reports_daily_betenlace_create = []

        member_reports_partner_month_update = []
        member_reports_daily_partner_update = []
        member_reports_daily_partner_create = []

        for prom_code in tqdm(df.prom_code.unique()):
            """
            "prom_code"
            "deposit"
            "stake"
            "net_revenue"
            "registered_count"
            "cpa_count"
            """
            # Get link according to prom_code of current loop
            link = next(filter(lambda link: link.prom_code == prom_code, links), None)
            if not link:
                logger.warn(
                    f"Link with prom_code \"{prom_code}\" and campaign \"{campaign_title}\" not found on database at Member report Case")
                continue

            try:
                # Get current entry of member report based on link (prom_code)
                betenlace_cpa = link.betenlacecpa
            except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
                logger.error(f"Betenlace CPA entry not found for link with prom_code \"{prom_code}\"")
                continue

            # Get sum vars of current prom_code
            cpa_count = cpa_by_prom_code_sum[prom_code]
            deposit = deposit_revenue_sum[prom_code]
            stake = stake_revenue_sum[prom_code]
            net_revenue = net_revenue_sum[prom_code]
            registered_count = registered_count_sum[prom_code]

            # Betenlace Month
            if(update_month):
                betenlace_cpa = self.betenlace_month_update(
                    betenlace_cpa, deposit, stake, campaign, net_revenue, cpa_count, registered_count,
                    revenue_share_percentage)
                member_reports_betenlace_month_update.append(betenlace_cpa)

            # Betenlace Daily
            betenlace_daily = next(filter(lambda betenlace_daily: betenlace_daily.betenlace_cpa_id ==
                                          betenlace_cpa.pk and betenlace_daily.created_at == from_date.date(), betenlace_daily_reports), None)

            if(betenlace_daily):
                betenlace_daily = self.betenlace_daily_update(
                    betenlace_daily, deposit, stake, campaign, net_revenue, cpa_count, registered_count,
                    revenue_share_percentage)
                member_reports_daily_betenlace_update.append(betenlace_daily)
            else:
                betenlace_daily = self.betenlace_daily_create(
                    from_date.date(),
                    betenlace_cpa, deposit, stake, campaign, net_revenue, cpa_count, registered_count,
                    revenue_share_percentage)
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
            if(cpa_count > settings.MIN_CPA_TRACKER_DAY):
                cpa_count = math.floor(cpa_count*partner_link_accumulated.tracker)
            else:
                cpa_count = cpa_count

            # Fx
            partner_currency_str = partner_link_accumulated.currency_local.lower()
            fx_book_partner = self.calc_fx(fx_partner, fx_partner_percentage,
                                           campaing_currency_fixed_income_str, partner_currency_str)

            fixed_income_partner_unitary = campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa
            fixed_income_partner = cpa_count * fixed_income_partner_unitary
            fixed_income_partner_unitary_local = (
                campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa * fx_book_partner)
            fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

            if(update_month):
                partner_link_accumulated = self.partner_link_month_update(
                    partner_link_accumulated, cpa_count, fixed_income_partner,
                    fixed_income_partner_local)
                member_reports_partner_month_update.append(partner_link_accumulated)

            # Partner Daily
            partner_link_daily = next(
                filter(
                    lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id ==
                    betenlace_daily.id, partner_link_dailies_reports),
                None)

            if(partner_link_daily):
                # Recalculate fixed_incomes for update
                fixed_income_partner_unitary = betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa
                fixed_income_partner = cpa_count * fixed_income_partner_unitary
                fixed_income_partner_unitary_local = (
                    betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa * fx_book_partner)
                fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

                partner_link_daily = self.partner_link_daily_update(
                    cpa_count, fx_book_partner, fx_partner_percentage, fixed_income_partner_unitary,
                    fixed_income_partner, fixed_income_partner_unitary_local, fixed_income_partner_local,
                    partner_link_daily)
                member_reports_daily_partner_update.append(partner_link_daily)
            else:
                partner_link_daily = self.partner_link_daily_create(
                    from_date, campaign, betenlace_daily,
                    partner_link_accumulated, cpa_count, fx_book_partner, fx_partner_percentage,
                    fixed_income_partner_unitary, fixed_income_partner, fixed_income_partner_unitary_local,
                    fixed_income_partner_local)
                member_reports_daily_partner_create.append(partner_link_daily)

        with transaction.atomic(using=DB_USER_PARTNER):
            if(account_reports_create):
                AccountReport.objects.bulk_create(account_reports_create)
            if(account_reports_update):
                AccountReport.objects.bulk_update(account_reports_update, (
                    "deposit",
                    "net_revenue",
                    "stake",
                    "fixed_income",
                    "net_revenue",
                    "revenue_share",
                    "cpa_betenlace",
                    "cpa_partner",
                    "registered_at",
                    "cpa_at",
                ))
            if(member_reports_betenlace_month_update):
                BetenlaceCPA.objects.bulk_update(member_reports_betenlace_month_update, (
                    "deposit",
                    "stake",
                    "fixed_income",
                    "net_revenue",
                    "revenue_share",
                    "registered_count",
                    "cpa_count",
                ))

            if(member_reports_daily_betenlace_update):
                BetenlaceDailyReport.objects.bulk_update(member_reports_daily_betenlace_update, (
                    "deposit",
                    "stake",
                    "fixed_income",
                    "net_revenue",
                    "revenue_share",
                    "fixed_income_unitary",
                    "registered_count",
                    "cpa_count",
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
                    "fixed_income",
                    "fixed_income_unitary",
                    "fx_book_local",
                    "fx_percentage",
                    "fixed_income_local",
                    "fixed_income_unitary_local",
                    "cpa_count",
                ))

            if(member_reports_daily_partner_create):
                PartnerLinkDailyReport.objects.bulk_create(member_reports_daily_partner_create)

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
            self, keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum, cpa_by_prom_code_iter,
            campaign, cpa_at, cpa_count, revenue_share_percentage):
        account_report.deposit += row[keys.get("deposit")]
        account_report.stake += row[keys.get("stake")]

        account_report.net_revenue += row[keys.get("net_revenue")]
        account_report.revenue_share += revenue_share_percentage * row[keys.get("net_revenue")]

        account_report.cpa_at = cpa_at

        if account_report.cpa_betenlace != 1:

            account_report.cpa_betenlace = cpa_count
            if (cpa_count):
                # Case when cpa is True or 1
                account_report.fixed_income = campaign.fixed_income_unitary
                account_report.partner_link_accumulated = partner_link_accumulated
                if(partner_link_accumulated):
                    account_report = self.calc_tracker(
                        keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum,
                        cpa_by_prom_code_iter, cpa_count)
        return account_report

    def account_report_create(
            self, row, keys, link, registered_at, partner_link_accumulated, from_date, cpa_by_prom_code_sum,
            cpa_by_prom_code_iter, campaign, cpa_at, cpa_count, revenue_share_percentage):
        account_report = AccountReport(
            partner_link_accumulated=partner_link_accumulated,
            punter_id=row[keys.get("punter_id")],
            deposit=row[keys.get("deposit")],
            stake=row[keys.get("stake")],
            fixed_income=campaign.fixed_income_unitary * cpa_count,
            net_revenue=row[keys.get("net_revenue")],
            revenue_share=row[keys.get("net_revenue")]*revenue_share_percentage,
            currency_condition=campaign.currency_condition,
            currency_fixed_income=campaign.currency_fixed_income,
            cpa_betenlace=cpa_count,
            link=link,
            registered_at=registered_at,
            cpa_at=cpa_at,
            created_at=from_date,
        )

        if(cpa_count):
            # Case when cpa is True or 1
            if(partner_link_accumulated):
                account_report = self.calc_tracker(
                    keys, row, account_report, partner_link_accumulated, cpa_by_prom_code_sum, cpa_by_prom_code_iter,
                    cpa_count)
        return account_report

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
            self, from_date, campaign, betenlace_daily, partner_link_accumulated,
            cpa_count, fx_book_partner, fx_partner_percentage, fixed_income_partner_unitary, fixed_income_partner,
            fixed_income_partner_unitary_local, fixed_income_partner_local):
        partner_link_daily = PartnerLinkDailyReport(
            betenlace_daily_report=betenlace_daily,
            partner_link_accumulated=partner_link_accumulated,
            fixed_income=fixed_income_partner,
            fixed_income_unitary=fixed_income_partner_unitary,
            currency_fixed_income=campaign.currency_fixed_income,
            fx_book_local=fx_book_partner,
            fixed_income_local=fixed_income_partner_local,
            fixed_income_unitary_local=fixed_income_partner_unitary_local,
            currency_local=partner_link_accumulated.currency_local,
            cpa_count=cpa_count,
            percentage_cpa=partner_link_accumulated.percentage_cpa,
            fx_percentage=fx_partner_percentage,
            tracker=partner_link_accumulated.tracker,
            created_at=from_date,
        )

        return partner_link_daily

    def partner_link_daily_update(
            self,
            cpa_count, fx_book_partner, fx_partner_percentage, fixed_income_partner_unitary, fixed_income_partner,
            fixed_income_partner_unitary_local, fixed_income_partner_local, partner_link_daily):

        partner_link_daily.fixed_income = fixed_income_partner
        partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary

        partner_link_daily.fx_book_local = fx_book_partner
        partner_link_daily.fx_percentage = fx_partner_percentage

        partner_link_daily.fixed_income_local = fixed_income_partner_local
        partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

        partner_link_daily.cpa_count = cpa_count

        return partner_link_daily

    def partner_link_month_update(
            self, partner_link_accumulated, cpa_count, fixed_income_partner,
            fixed_income_partner_local):
        partner_link_accumulated.cpa_count += cpa_count
        partner_link_accumulated.fixed_income += fixed_income_partner
        partner_link_accumulated.fixed_income_local += fixed_income_partner_local

        return partner_link_accumulated

    def betenlace_daily_create(
            self, from_date, betenlace_cpa, deposit, stake, campaign, net_revenue, cpa_count, registered_count,
            revenue_share_percentage):
        betenlace_daily = BetenlaceDailyReport(
            betenlace_cpa=betenlace_cpa,
            deposit=deposit,
            stake=stake,
            fixed_income=campaign.fixed_income_unitary * cpa_count,
            net_revenue=net_revenue,
            revenue_share=net_revenue * revenue_share_percentage,
            fixed_income_unitary=campaign.fixed_income_unitary,
            currency_condition=campaign.currency_condition,
            currency_fixed_income=campaign.currency_fixed_income,
            registered_count=registered_count,
            cpa_count=cpa_count,
            created_at=from_date)

        return betenlace_daily

    def betenlace_daily_update(
            self, betenlace_daily, deposit, stake, campaign, net_revenue, cpa_count, registered_count,
            revenue_share_percentage):
        betenlace_daily.deposit = deposit
        betenlace_daily.stake = stake
        betenlace_daily.fixed_income = campaign.fixed_income_unitary * cpa_count
        betenlace_daily.net_revenue = net_revenue
        betenlace_daily.revenue_share = net_revenue * revenue_share_percentage
        betenlace_daily.fixed_income_unitary = campaign.fixed_income_unitary
        betenlace_daily.registered_count = registered_count
        betenlace_daily.cpa_count = cpa_count

        return betenlace_daily

    def betenlace_month_update(
            self, betenlace_cpa, deposit, stake, campaign, net_revenue, cpa_count, registered_count,
            revenue_share_percentage):
        betenlace_cpa.deposit += deposit
        betenlace_cpa.stake += stake
        betenlace_cpa.fixed_income += campaign.fixed_income_unitary * cpa_count
        betenlace_cpa.net_revenue += net_revenue
        betenlace_cpa.revenue_share += net_revenue * revenue_share_percentage
        betenlace_cpa.registered_count += registered_count
        betenlace_cpa.cpa_count += cpa_count
        return betenlace_cpa
