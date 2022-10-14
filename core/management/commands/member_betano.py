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
    Link,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
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
            "-fd",
            "--fromdate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d"),
            help="Determine date from for get data of commisions",
        )
        parser.add_argument(
            "-td",
            "--todate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d"),
            help="Determine date to for get data of commisions",
        )
        parser.add_argument(
            "-c",
            "--campaign",
            default="betano pe",
            choices=("betano pe", "betano cl",),
            help="Title of campaign",
        )
        parser.add_argument(
            "-fr",
            "--file_raw",
            nargs="?",
            help=(
                "name of file where storage the raw input file, if supplied the DB "
                "statement will not executed, only can create file raw or file normal one at time"
            ),
        )
        parser.add_argument(
            "-f",
            "--file",
            nargs="?",
            help=(
                  "name of file where storage the csv, if supplied the DB "
                  "statement will not executed, only can create file raw or file normal one at time"
            ),
        )
        parser.add_argument(
            "-upm",
            "--update_month",
            choices=("False", "True",),
            default="True",
            help="Full update or not the month accumulated data",
        )

    def handle(self, *args, **options):
        """
        Get data from API of bookmaker Yajuego with CSV files using 
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
        - purchases : `np.float32`
            Equivalent to deposit from Model MemberReport (Month, daily) for 
            betenalce and partners`, this is apositive integer
        - netwagers:`np.uint32`
            Equivalent to stake, total of wagered money by player/punter_id on 
            supplied date, the Netrevunue can determinate if player won or loss 
            the bets
        - netrevenue : `np.float32`
            Profit earned so far from that player/punter_id, usually this is
            the stake - 20% of stake (only if player/punter_id loss all bet), this
            take a positive value when player/punter_id loss money and take
            negative when player/punter_id won the bets. This is a sum of
            outcomes (results) of the bets has the player/punter_id placed
            and the bookmaker received a result. This not have the count 
            of bets
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
        - row_id
        - currency_symbol
        - prom_code
        - deposit
        - stake
        - fixed_income
        - net_revenue
        - revenue_share
        - registered_count
        - cpa_count
        - first_deposit_count
        - wagering_count
        """
        logger.info(
            "Making call to API Member Betano\n"
            f"Campaign Title -> {options.get('campaign')}\n"
            f"From date -> {options.get('fromdate')}\n"
            f"To date -> {options.get('todate')}\n"
            f"File to save raw -> {options.get('file_raw')}\n"
            f"File to save -> {options.get('file')}\n"
            f"update month -> {options.get('update_month')}"
        )
        from_date_str = options.get("fromdate")
        to_date_str = options.get("todate")
        campaign_title = options.get("campaign")
        update_month = eval(options.get("update_month"))

        try:
            from_date = make_aware(datetime.strptime(from_date_str, "%Y/%m/%d"))
            to_date = make_aware(datetime.strptime(to_date_str, "%Y/%m/%d"))
            if from_date > to_date:
                logger.error(f"from_date=\"{from_date}\" is greather than to_date=\"{to_date}\"")
                return
        except:
            logger.error("from_date or to_date have bad format. Expected format\"AAAA/mm/dd\"")
            return

        # Get id of Campaign Title
        filters = (
            Q(campaign_title__iexact=campaign_title),
        )
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

        if (campaign_title == "betano pe"):
            betano_key = settings.API_MEMBER_REPORT_BETANOPE_KEY
            betano_account_id = settings.API_MEMBER_REPORT_BETANOPE_ACCOUNT_ID

        if (campaign_title == "betano cl"):
            betano_key = settings.API_MEMBER_REPORT_BETANOCL_KEY
            betano_account_id = settings.API_MEMBER_REPORT_BETANOCL_ACCOUNT_ID

        try:
            url = (
                f"https://affiliates.betano.com/api/affreporting.asp?key={betano_key}"
                f"&reportname=Member%20Report%20-%20Detailed&reportformat=csv"
                f"&reportmerchantid={betano_account_id}&reportstartdate={from_date_str}"
                f"&reportenddate={to_date_str}"
            )
            response = requests.get(url=url)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            logger.error(
                "Something is wrong at get data from API, check if current "
                "connection IP/VPN is on Whitelist of API server, if problem "
                f"still check traceback:\n\n{''.join(e)}"
            )
            return

        if(options.get("file_raw")):
            with open(f"{options.get('file_raw')}.csv", "w") as out:
                # File case save to disk and prevent execute on DB
                out.write(response.text)
            return

        try:
            # set the characters and line based interface to stream I/O
            data_io = StringIO(response.text[response.text.index("\"rowid\""):])
        except:
            if "No Records" in response.text:
                logger.error(
                    "Data not found at requested url\n"
                    f"Request url: {url}\n"
                    "Data obtained\n"
                    f"{response.text}"
                )
                return

            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            logger.error(
                "Something is wrong at get data from API, check the credentials"
                " (key and reportmerchantid) if problem persist check traceback:"
                f"\n\n{''.join(e)}\n\n"
                f"Request url:\n {url}\n"
                "Data obtained\n"
                f"{response.text}"
            )
            return

        # Create the DataFrame
        cols_to_use = [
            "rowid",
            "currencysymbol",
            "siteid",
            "purchases",
            # "netwagers",
            # "totalcpacommission",
            "grossrevenue",
            "revsharecommission",
            "downloads",
            "cpacommissioncount",
            "firstdepositcount",
            "wageraccountcount",
        ]
        df = pd.read_csv(
            filepath_or_buffer=data_io,
            sep=",",
            usecols=cols_to_use,
            dtype={
                "rowid": np.uint8,
                # "currencysymbol": "string",
                "siteid": "string",
                "purchases": np.float32,
                # "netwagers": np.float32,
                # "totalcpacommission": np.float32,
                "grossrevenue": np.float32,
                "revsharecommission": np.float32,
                "downloads": np.uint32,
                "cpacommissioncount": np.uint32,
                "firstdepositcount": np.uint32,
                "wageraccountcount": np.uint32,
            },
        )[cols_to_use]

        df.rename(
            inplace=True,
            columns={
                "rowid": "row_id",
                # "currencysymbol": "currency_symbol",
                "siteid": "prom_code",
                "purchases": "deposit",
                # "netwagers": "stake",
                # "totalcpacommission": "fixed_income",
                "grossrevenue": "net_revenue",
                "revsharecommission": "revenue_share",
                "downloads": "registered_count",
                "cpacommissioncount": "cpa_count",
                "firstdepositcount": "first_deposit_count",
                "wageraccountcount": "wagering_count",
            },
        )

        # "rowid","currencysymbol","totalrecords","period","merchantname",
        # "memberid","username","country","memberid","siteid","sitename",
        # "impressions","clicks","installs","clickthroughratio","downloads",
        # "downloadratio","newaccountratio","newdepositingacc","newaccounts",
        # "firstdepositcount","activeaccounts","activedays","newpurchases",
        # "purchaccountcount","wageraccountcount","avgactivedays",
        # "grossrevenueplayer","netrevenueplayer","purchases","chargebacks",
        # "grossrevenue","prod2bets","prod2tournament","prod2revenue",
        # "prod2netrevenue","prod2commission","prod4bets","prod4tournament",
        # "prod4revenue","prod4netrevenue","prod4commission",
        # "revsharecommission","totalcpacommission","cpacommissioncount",
        # "totalcommission"

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df.to_csv(
                path_or_buf=f"{options.get('file')}.csv",
                index=False,
                encoding="utf-8",
            )
            return

        # Filter data - Override in same place of memory group/sum data
        # rowid == 2
        df.drop(
            labels=df[df.eval("(row_id == 2)", engine='numexpr')].index,
            inplace=True,
        )
        # Setup dataframe to a list of dictionaries for best performance
        # df_dict = df.to_dict('records')

        if(from_date != to_date):
            logger.error("Date from and to are equal this data cannot be used for update on DB")
            return

        # Get related link from prom_codes and campaign, QUERY
        filters = (
            Q(prom_code__in=df.prom_code.unique()),
            Q(campaign_id=campaign.id),
        )
        links = Link.objects.filter(
            *filters,
        ).select_related(
            "partner_link_accumulated",
            "partner_link_accumulated__partner",
            "betenlacecpa",
        )

        betenlacecpas_pk = links.values_list("betenlacecpa__pk", flat=True)

        filters = (
            Q(betenlace_cpa__pk__in=betenlacecpas_pk),
            Q(created_at=from_date.date()),
        )
        betenlace_daily_reports = BetenlaceDailyReport.objects.filter(*filters)

        filters = (
            Q(betenlace_daily_report__in=betenlace_daily_reports),
        )
        partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(*filters)

        # Get the last Fx value
        filters = (
            Q(created_at__gte=from_date),
        )
        fx_partner = FxPartner.objects.filter(*filters).order_by("created_at").first()

        if(fx_partner is None):
            # Get just next from supplied date
            filters = (
                Q(created_at__lte=from_date),
            )
            fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        # If still none prevent execution
        if(fx_partner is None):
            logger.error("Undefined fx_partner on DB")
            return

        fx_partner_percentage = fx_partner.fx_percentage

        currency_condition_str = campaign.currency_condition.lower()
        currency_fixed_income_str = campaign.currency_fixed_income.lower()

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
            - row_id
            - prom_code
            - deposit
            - net_revenue
            - revenue_share
            - registered_count
            - cpa_count
            - first_deposit_count
            - wagering_count
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
                betenlace_cpa = self.betenlace_month_update(
                    keys=keys,
                    row=row,
                    betenlace_cpa=betenlace_cpa,
                    campaign=campaign,
                )
                member_reports_betenlace_month_update.append(betenlace_cpa)

            # Betenlace Daily
            betenlace_daily = next(
                filter(
                    lambda betenlace_daily: (
                        betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                        betenlace_daily.created_at == from_date.date()
                    ),
                    betenlace_daily_reports,
                ),
                None,
            )

            if(betenlace_daily):
                betenlace_daily = self.betenlace_daily_update(
                    keys=keys,
                    row=row,
                    betenlace_daily=betenlace_daily,
                    campaign=campaign,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_update.append(betenlace_daily)
            else:
                betenlace_daily = self.betenlace_daily_create(
                    from_date=from_date.date(),
                    keys=keys,
                    row=row,
                    betenlace_cpa=betenlace_cpa,
                    campaign=campaign,
                    fx_partner=fx_partner,
                )
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
            if(row[keys.get('cpa_count')] > settings.MIN_CPA_TRACKER_DAY):
                cpa_count = math.floor(row[keys.get('cpa_count')]*partner_link_accumulated.tracker)
            else:
                cpa_count = row[keys.get('cpa_count')]

            tracked_data = self.get_tracker_values(
                keys=keys,
                row=row,
                partner_link_accumulated=partner_link_accumulated,
            )

            # Fx Currency Fixed income
            partner_currency_str = partner_link_accumulated.currency_local.lower()
            fx_fixed_income_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_fixed_income_str,
                partner_currency_str=partner_currency_str,
            )

            fixed_income_partner_unitary = campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa
            fixed_income_partner = cpa_count * fixed_income_partner_unitary
            fixed_income_partner_unitary_local = (
                campaign.fixed_income_unitary *
                partner_link_accumulated.percentage_cpa *
                fx_fixed_income_partner
            )
            fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

            # Fx Currency Condition
            fx_condition_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_condition_str,
                partner_currency_str=partner_currency_str,
            )

            if(update_month):
                partner_link_accumulated = self.partner_link_month_update(
                    partner_link_accumulated=partner_link_accumulated,
                    cpa_count=cpa_count,
                    fixed_income_partner=fixed_income_partner,
                    fixed_income_partner_local=fixed_income_partner_local,
                )
                member_reports_partner_month_update.append(partner_link_accumulated)

            # Partner Daily
            partner_link_daily = next(
                filter(
                    lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id == betenlace_daily.id,
                    partner_link_dailies_reports,
                ),
                None,
            )

            if(partner_link_daily):
                # Recalculate fixed_incomes for update
                # fixed_income_partner_unitary = betenlace_daily.fixed_income_unitary * partner_link_daily.percentage_cpa
                # fixed_income_partner = cpa_count * fixed_income_partner_unitary
                # fixed_income_partner_unitary_local = (
                #     betenlace_daily.fixed_income_unitary *
                #     partner_link_daily.percentage_cpa *
                #     fx_fixed_income_partner
                # )
                # fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

                partner_link_daily = self.partner_link_daily_update(
                    cpa_count=cpa_count,
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner=fixed_income_partner,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    fixed_income_partner_local=fixed_income_partner_local,
                    partner_link_daily=partner_link_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    partner=partner_link_accumulated.partner,
                    betenlace_daily=betenlace_daily,
                )
                member_reports_daily_partner_update.append(partner_link_daily)
            else:
                partner_link_daily = self.partner_link_daily_create(
                    from_date=from_date.date(),
                    campaign=campaign,
                    betenlace_daily=betenlace_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    cpa_count=cpa_count,
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner=fixed_income_partner,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    fixed_income_partner_local=fixed_income_partner_local,
                    partner=partner_link_accumulated.partner,
                )
                member_reports_daily_partner_create.append(partner_link_daily)

        with transaction.atomic(using=DB_USER_PARTNER):
            if(member_reports_betenlace_month_update):
                BetenlaceCPA.objects.bulk_update(
                    objs=member_reports_betenlace_month_update,
                    fields=(
                        "deposit",
                        # "stake",
                        "fixed_income",
                        "net_revenue",
                        "revenue_share",
                        "registered_count",
                        "cpa_count",
                        "first_deposit_count",
                        "wagering_count",
                    ),
                )

            if(member_reports_daily_betenlace_update):
                BetenlaceDailyReport.objects.bulk_update(
                    objs=member_reports_daily_betenlace_update,
                    fields=(
                        "deposit",
                        # "stake",
                        "net_revenue",
                        "revenue_share",
                        "fixed_income",
                        "fixed_income_unitary",
                        "fx_partner",
                        "registered_count",
                        "cpa_count",
                        "first_deposit_count",
                        "wagering_count",
                    ),
                )

            if(member_reports_daily_betenlace_create):
                BetenlaceDailyReport.objects.bulk_create(
                    objs=member_reports_daily_betenlace_create,
                )

            if(member_reports_partner_month_update):
                PartnerLinkAccumulated.objects.bulk_update(
                    objs=member_reports_partner_month_update,
                    fields=(
                        "cpa_count",
                        "fixed_income",
                        "fixed_income_local",
                    ),
                )

            if(member_reports_daily_partner_update):
                PartnerLinkDailyReport.objects.bulk_update(
                    objs=member_reports_daily_partner_update,
                    fields=(
                        "fixed_income",
                        "fixed_income_unitary",
                        "fx_book_local",
                        "fx_book_net_revenue_local",
                        "fx_percentage",
                        "fixed_income_local",
                        "fixed_income_unitary_local",
                        "cpa_count",
                        "percentage_cpa",
                        "tracker",
                        "tracker_deposit",
                        "tracker_registered_count",
                        "tracker_first_deposit_count",
                        "tracker_wagering_count",
                        "deposit",
                        "registered_count",
                        "first_deposit_count",
                        "wagering_count",
                        "adviser_id",
                        "fixed_income_adviser",
                        "fixed_income_adviser_local",
                        "net_revenue_adviser",
                        "net_revenue_adviser_local",
                        "fixed_income_adviser_percentage",
                        "net_revenue_adviser_percentage",
                        "referred_by",
                        "fixed_income_referred",
                        "fixed_income_referred_local",
                        "net_revenue_referred",
                        "net_revenue_referred_local",
                        "fixed_income_referred_percentage",
                        "net_revenue_referred_percentage",
                    ),
                )

            if(member_reports_daily_partner_create):
                PartnerLinkDailyReport.objects.bulk_create(
                    objs=member_reports_daily_partner_create,
                )

    def get_tracker_values(
        self,
        keys,
        row,
        partner_link_accumulated,
    ):
        tracked_data = {}
        if (keys.get("deposit") is not None):
            tracked_data["deposit"] = row[keys.get("deposit")]*partner_link_accumulated.tracker_deposit

        if (keys.get("registered_count") is not None):
            if(row[keys.get("registered_count")] > 1):
                tracked_data["registered_count"] = math.floor(
                    row[keys.get("registered_count")]*partner_link_accumulated.tracker_registered_count
                )
            else:
                tracked_data["registered_count"] = row[keys.get("registered_count")]

        if (keys.get("first_deposit_count") is not None):
            if(row[keys.get("first_deposit_count")] > 1):
                tracked_data["first_deposit_count"] = math.floor(
                    row[keys.get("first_deposit_count")]*partner_link_accumulated.tracker_first_deposit_count
                )
            else:
                tracked_data["first_deposit_count"] = row[keys.get("first_deposit_count")]

        if (keys.get("wagering_count") is not None):
            if(row[keys.get("wagering_count")] > 1):
                tracked_data["wagering_count"] = math.floor(
                    row[keys.get("wagering_count")]*partner_link_accumulated.tracker_wagering_count
                )
            else:
                tracked_data["wagering_count"] = row[keys.get("wagering_count")]

        return tracked_data

    def calc_fx(
        self,
        fx_partner,
        fx_partner_percentage,
        currency_from_str,
        partner_currency_str,
    ):
        if(currency_from_str != partner_currency_str):
            try:
                fx_book_partner = eval(
                    f"fx_partner.fx_{currency_from_str}_{partner_currency_str}") * fx_partner_percentage
            except:
                logger.error(
                    f"Fx conversion from {currency_from_str} to {partner_currency_str} undefined on DB")
        else:
            fx_book_partner = 1
        return fx_book_partner

    def partner_link_daily_create(
        self,
        from_date,
        campaign,
        betenlace_daily,
        partner_link_accumulated,
        cpa_count,
        tracked_data,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner,
        fixed_income_partner_unitary_local,
        fixed_income_partner_local,
        partner,
    ):
        # Calculate Adviser payment
        if (partner.fixed_income_adviser_percentage is None):
            fixed_income_adviser = None
            fixed_income_adviser_local = None
        else:
            fixed_income_adviser = (
                fixed_income_partner *
                partner.fixed_income_adviser_percentage
            )
            fixed_income_adviser_local = (
                fixed_income_adviser *
                fx_fixed_income_partner
            )

        if (partner.net_revenue_adviser_percentage is None):
            net_revenue_adviser = None
            net_revenue_adviser_local = None
        else:
            net_revenue_adviser = (
                betenlace_daily.net_revenue * partner.net_revenue_adviser_percentage
                if betenlace_daily.net_revenue is not None
                else
                0
            )
            net_revenue_adviser_local = (
                net_revenue_adviser * fx_condition_partner
            )

    # Calculate referred payment
        if (partner.fixed_income_referred_percentage is None):
            fixed_income_referred = None
            fixed_income_referred_local = None
        else:
            fixed_income_referred = (
                fixed_income_partner *
                partner.fixed_income_referred_percentage
            )
            fixed_income_referred_local = (
                fixed_income_referred *
                fx_fixed_income_partner
            )

        if (partner.net_revenue_referred_percentage is None):
            net_revenue_referred = None
            net_revenue_referred_local = None
        else:
            net_revenue_referred = (
                betenlace_daily.net_revenue * partner.net_revenue_referred_percentage
                if betenlace_daily.net_revenue is not None
                else
                0
            )
            net_revenue_referred_local = (
                net_revenue_referred * fx_condition_partner
            )

        partner_link_daily = PartnerLinkDailyReport(
            betenlace_daily_report=betenlace_daily,
            partner_link_accumulated=partner_link_accumulated,

            currency_fixed_income=campaign.currency_fixed_income,
            fixed_income=fixed_income_partner,
            fixed_income_unitary=fixed_income_partner_unitary,

            currency_local=partner_link_accumulated.currency_local,
            fx_book_local=fx_fixed_income_partner,
            fx_book_net_revenue_local=fx_condition_partner,
            fx_percentage=fx_partner_percentage,

            fixed_income_local=fixed_income_partner_local,
            fixed_income_unitary_local=fixed_income_partner_unitary_local,

            cpa_count=cpa_count,
            percentage_cpa=partner_link_accumulated.percentage_cpa,

            deposit=tracked_data.get("deposit"),
            registered_count=tracked_data.get("registered_count"),
            first_deposit_count=tracked_data.get("first_deposit_count"),
            wagering_count=tracked_data.get("wagering_count"),

            tracker=partner_link_accumulated.tracker,
            tracker_deposit=partner_link_accumulated.tracker_deposit,
            tracker_registered_count=partner_link_accumulated.tracker_registered_count,
            tracker_first_deposit_count=partner_link_accumulated.tracker_first_deposit_count,
            tracker_wagering_count=partner_link_accumulated.tracker_wagering_count,

            # Adviser base data
            adviser_id=partner.adviser_id,
            fixed_income_adviser_percentage=partner.fixed_income_adviser_percentage,
            net_revenue_adviser_percentage=partner.net_revenue_adviser_percentage,

            fixed_income_adviser=fixed_income_adviser,
            fixed_income_adviser_local=fixed_income_adviser_local,
            net_revenue_adviser=net_revenue_adviser,
            net_revenue_adviser_local=net_revenue_adviser_local,

            # referred base data
            referred_by=partner.referred_by,
            fixed_income_referred_percentage=partner.fixed_income_referred_percentage,
            net_revenue_referred_percentage=partner.net_revenue_referred_percentage,

            fixed_income_referred=fixed_income_referred,
            fixed_income_referred_local=fixed_income_referred_local,
            net_revenue_referred=net_revenue_referred,
            net_revenue_referred_local=net_revenue_referred_local,

            created_at=from_date,
        )
        return partner_link_daily

    def partner_link_daily_update(
        self,
        cpa_count,
        tracked_data,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner,
        fixed_income_partner_unitary_local,
        fixed_income_partner_local,
        partner_link_daily,
        partner_link_accumulated,
        partner,
        betenlace_daily,
    ):
        partner_link_daily.fixed_income = fixed_income_partner
        partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary

        partner_link_daily.fx_book_local = fx_fixed_income_partner
        partner_link_daily.fx_book_net_revenue_local = fx_condition_partner
        partner_link_daily.fx_percentage = fx_partner_percentage

        partner_link_daily.fixed_income_local = fixed_income_partner_local
        partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

        partner_link_daily.cpa_count = cpa_count
        partner_link_daily.percentage_cpa = partner_link_accumulated.percentage_cpa

        partner_link_daily.tracker = partner_link_accumulated.tracker
        partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
        partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
        partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
        partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

        partner_link_daily.deposit = tracked_data.get("deposit")
        partner_link_daily.registered_count = tracked_data.get("registered_count")
        partner_link_daily.first_deposit_count = tracked_data.get("first_deposit_count")
        partner_link_daily.wagering_count = tracked_data.get("wagering_count")

        # Calculate Adviser payment
        partner_link_daily.adviser_id = partner.adviser_id
        partner_link_daily.fixed_income_adviser_percentage = partner.fixed_income_adviser_percentage
        partner_link_daily.net_revenue_adviser_percentage = partner.net_revenue_adviser_percentage

        if (partner.fixed_income_adviser_percentage is None):
            partner_link_daily.fixed_income_adviser = None
            partner_link_daily.fixed_income_adviser_local = None
        else:
            partner_link_daily.fixed_income_adviser = (
                partner_link_daily.fixed_income *
                partner.fixed_income_adviser_percentage
            )
            partner_link_daily.fixed_income_adviser_local = (
                partner_link_daily.fixed_income_adviser *
                fx_fixed_income_partner
            )

        if (partner.net_revenue_adviser_percentage is None):
            partner_link_daily.net_revenue_adviser = None
            partner_link_daily.net_revenue_adviser_local = None
        else:
            partner_link_daily.net_revenue_adviser = (
                betenlace_daily.net_revenue * partner.net_revenue_adviser_percentage
                if betenlace_daily.net_revenue is not None
                else
                0
            )
            partner_link_daily.net_revenue_adviser_local = (
                partner_link_daily.net_revenue_adviser * fx_condition_partner
            )

        # Calculate referred payment
        partner_link_daily.referred_by = partner.referred_by
        partner_link_daily.fixed_income_referred_percentage = partner.fixed_income_referred_percentage
        partner_link_daily.net_revenue_referred_percentage = partner.net_revenue_referred_percentage

        if (partner.fixed_income_referred_percentage is None):
            partner_link_daily.fixed_income_referred = None
            partner_link_daily.fixed_income_referred_local = None
        else:
            partner_link_daily.fixed_income_referred = (
                partner_link_daily.fixed_income *
                partner.fixed_income_referred_percentage
            )
            partner_link_daily.fixed_income_referred_local = (
                partner_link_daily.fixed_income_referred *
                fx_fixed_income_partner
            )

        if (partner.net_revenue_referred_percentage is None):
            partner_link_daily.net_revenue_referred = None
            partner_link_daily.net_revenue_referred_local = None
        else:
            partner_link_daily.net_revenue_referred = (
                betenlace_daily.net_revenue * partner.net_revenue_referred_percentage
                if betenlace_daily.net_revenue is not None
                else
                0
            )
            partner_link_daily.net_revenue_referred_local = (
                partner_link_daily.net_revenue_referred * fx_condition_partner
            )
        return partner_link_daily

    def partner_link_month_update(
        self,
        partner_link_accumulated,
        cpa_count,
        fixed_income_partner,
        fixed_income_partner_local,
    ):
        partner_link_accumulated.cpa_count += cpa_count
        partner_link_accumulated.fixed_income += fixed_income_partner
        partner_link_accumulated.fixed_income_local += fixed_income_partner_local
        return partner_link_accumulated

    def betenlace_daily_create(
        self,
        from_date,
        keys,
        row,
        betenlace_cpa,
        campaign,
        fx_partner,
    ):
        betenlace_daily = BetenlaceDailyReport(
            betenlace_cpa=betenlace_cpa,

            currency_condition=campaign.currency_condition,

            deposit=row[keys.get("deposit")],
            # stake=row[keys.get("stake")],

            net_revenue=row[keys.get("net_revenue")],
            revenue_share=row[keys.get("revenue_share")],

            currency_fixed_income=campaign.currency_fixed_income,

            fixed_income_unitary=campaign.fixed_income_unitary,
            fixed_income=campaign.fixed_income_unitary*row[keys.get("cpa_count")],

            fx_partner=fx_partner,

            registered_count=row[keys.get('registered_count')],
            cpa_count=row[keys.get('cpa_count')],
            first_deposit_count=row[keys.get('first_deposit_count')],
            wagering_count=row[keys.get('wagering_count')],
            created_at=from_date,
        )

        return betenlace_daily

    def betenlace_daily_update(
        self,
        keys,
        row,
        betenlace_daily,
        campaign,
        fx_partner,
    ):
        betenlace_daily.deposit = row[keys.get("deposit")]
        # betenlace_daily.stake = row[keys.get("stake")]
        betenlace_daily.net_revenue = row[keys.get("net_revenue")]
        betenlace_daily.revenue_share = row[keys.get("revenue_share")]

        betenlace_daily.fixed_income_unitary = campaign.fixed_income_unitary
        betenlace_daily.fixed_income = campaign.fixed_income_unitary * row[keys.get("cpa_count")]

        betenlace_daily.fx_partner = fx_partner

        betenlace_daily.registered_count = row[keys.get("registered_count")]
        betenlace_daily.cpa_count = row[keys.get("cpa_count")]
        betenlace_daily.first_deposit_count = row[keys.get("first_deposit_count")]
        betenlace_daily.wagering_count = row[keys.get("wagering_count")]
        return betenlace_daily

    def betenlace_month_update(
        self,
        keys,
        row,
        betenlace_cpa,
        campaign,
    ):
        betenlace_cpa.deposit += row[keys.get('deposit')]
        # betenlace_cpa.stake += row[keys.get('stake')]
        betenlace_cpa.fixed_income += campaign.fixed_income_unitary*row[keys.get('cpa_count')]
        betenlace_cpa.net_revenue += row[keys.get('net_revenue')]
        betenlace_cpa.revenue_share += row[keys.get('revenue_share')]
        betenlace_cpa.registered_count += row[keys.get('registered_count')]
        betenlace_cpa.cpa_count += row[keys.get('cpa_count')]
        betenlace_cpa.first_deposit_count += row[keys.get('first_deposit_count')]
        betenlace_cpa.wagering_count += row[keys.get('wagering_count')]
        return betenlace_cpa
