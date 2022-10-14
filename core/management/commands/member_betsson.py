import json
import logging
import math
import re
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
            default="betsson col",
            choices=(
                "betsson col",
            ),
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
            choices=(
                "False",
                "True",
            ),
            default="True",
            help="Full update or not the month accumulated data",
        )

    def handle(self, *args, **options):
        """
        Get data from API of bookmaker Betsson with CSV files using 
        the pandas module with high performance, on command use tqdm for 
        progress bar.

        Member report is the summarized data from all punters of range of date
        """
        logger.info(
            "Making call to API Member Betsson\n"
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
            from_datetime = make_aware(datetime.strptime(from_date_str, "%Y-%m-%d"))
            to_datetime = make_aware(datetime.strptime(to_date_str, "%Y-%m-%d"))
            if from_datetime > to_datetime:
                logger.error(f"from_date=\"{from_datetime}\" is greather than to_date=\"{to_datetime}\"")
                return
        except:
            logger.error("from_date or to_date have bad format. Expected format\"AAAA-mm-dd\"")
            return

        # Force zero leading
        from_date_str = from_datetime.strftime("%Y-%m-%d")
        to_date_str = to_datetime.strftime("%Y-%m-%d")
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

        if (campaign_title == "betsson col"):
            bookmaker_channel_id = settings.API_ACCOUNT_MEMBER_REPORT_BETSSON_COL_CHANNEL_ID
            bookmaker_client_id = settings.API_ACCOUNT_MEMBER_REPORT_BETSSON_COL_CLIENT_ID
            bookmaker_client_secret = settings.API_ACCOUNT_MEMBER_REPORT_BETSSON_COL_CLIENT_SECRET

            # Get OAuth2
        url = "https://affiliates.betssongroupaffiliates.com/oauth/access_token"
        body = {
            "client_id": bookmaker_client_id,
            "client_secret": bookmaker_client_secret,
            "grant_type": "client_credentials",
            "scope": "r_user_stats",
        }

        response_obj = requests.post(url=url, data=body)

        if (response_obj.status_code != 200):
            logger.error(
                "Status code is not 200 at try to get Authorization from API, "
                "check credendials and connection status\n\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"response status:\n{response_obj.status_code}\n"
                f"response text:\n{response_obj.text}"
            )
            return

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
                "Something is wrong at get Authorization from API, check credendials and connection status\n\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"response status:\n{response_obj.status_code}\n"
                f"response text:\n{response_obj.text}\n\n"
                f"if problem still check traceback:\n{''.join(e)}"
            )
            return

        # Make sure access_token is ok
        if (not "token_type" in response or not "access_token" in response):
            logger.error(
                "Something is wrong at get Authorization from API, check credendials and connection status\n\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"response status:\n{response_obj.status_code}\n"
                f"response text:\n{response_obj.text}\n\n"
            )
            return

        # Retrieve Oauth2 and create requried Auth headers
        token_type = response.get("token_type")
        access_token = response.get("access_token")

        authorization = f"{token_type} {access_token}"
        headers = {
            "Authorization": authorization,
        }
        url = (
            f"https://affiliates.betssongroupaffiliates.com/statistics.php?p={bookmaker_channel_id}&"
            f"d1={from_date_str}&d2={to_date_str}&cg=&c=&m=&o=&s=&sd=1&sc=1&mode=csv&sbm=1&dnl=1"
        )

        response_obj = requests.get(url=url, headers=headers)

        if (
            (
                response_obj.text == "" or "No data" in response_obj.text) or
            not ("Campaign" in response_obj.text)
        ):
            logger.warning(
                f"Data not found for campaign_title {campaign_title} from_date {from_date_str} to_date {to_date_str}"
                f" at requested url"
                f"Request url: {url}\n"
                "Data obtained\n"
                f"{response_obj.text}"
            )
            return

        if(options.get("file_raw")):
            with open(f"{options.get('file_raw')}.csv", "w") as out:
                # File case save to disk and prevent execute on DB
                out.write(response_obj.text)
            return

        # Create dataframe empty
        df_member_empty = pd.DataFrame(
            {
                "prom_code": pd.Series(
                    dtype="string",
                ),
                "deposit": pd.Series(
                    dtype=np.float32,
                ),
                "registered_count": pd.Series(
                    dtype=np.uint32,
                ),
                "cpa_count": pd.Series(
                    dtype=np.uint32,
                ),
                "first_deposit_count": pd.Series(
                    dtype=np.uint32,
                ),
                "net_revenue": pd.Series(
                    dtype=np.float32,
                ),
                "revenue_share": pd.Series(
                    dtype=np.float32,
                ),
                "fixed_income": pd.Series(
                    dtype=np.float32,
                ),
            },
        )

        dfs_member = [df_member_empty]
        # Income CPA
        # Get all "Date," index

        indexes = [i.start() for i in re.finditer("Date,", response_obj.text)]
        indexes_iter = iter(indexes)

        index_start_i = next(indexes_iter, None)
        while True:
            if (index_start_i is None):
                break
            index_end_i = next(indexes_iter, None)

            data_str_i = response_obj.text[index_start_i:index_end_i]
            data_io = StringIO(data_str_i)

            # Define vars
            if ("Income CPA" in data_str_i):
                cols_to_use = [
                    "Campaign",
                    "Total Deposits",
                    "NRC",
                    "NDC",
                    "Qualified NDCs",
                    "Net revenue Total",
                    "Income Revshare",
                    "Income CPA",
                ]
                cols_dtype = {
                    "Campaign": "string",
                    "Total Deposits": np.float32,
                    "NRC": np.uint32,
                    "Qualified NDCs": np.uint32,
                    "NDC": np.uint32,
                    "Net revenue Total": np.float32,
                    "Income Revshare": np.float32,
                    "Income CPA": np.float32,
                }
                cols_rename = {
                    "Campaign": "prom_code",
                    "Total Deposits": "deposit",
                    "NRC": "registered_count",
                    "Qualified NDCs": "cpa_count",
                    "NDC": "first_deposit_count",
                    "Net revenue Total": "net_revenue",
                    "Income Revshare": "revenue_share",
                    "Income CPA": "fixed_income",
                }
                # Date,Channel,Pay period,Customer group,Campaign,Impressions,
                # Clicks,NRC,NDC,First Deposit,Total Deposits,Qualified NDCs,
                # NDCs from Country 1,NDCs from Country 2,NDCs from Country 3,
                # NDCs from Country 4,NDCs from Country 5,Gamewin Sportsbook,
                # Costs Sportsbook,Admin fee Sportsbook,Net revenue Sportsbook,
                # Gamewin Casino,Costs Casino,Admin fee Casino,
                # Net revenue Casino,Gamewin Poker,Costs Poker,Admin fee Poker,
                # Net revenue Poker,Gamewin Bingo,Costs Bingo,Admin fee Bingo,
                # Net revenue Bingo,Gamewin Generic,Costs Generic,
                # Admin fee Generic,Net revenue Generic,Gamewin Total,
                # Costs Total,Admin fee Total,Net revenue Total,
                # Income Revshare,Income CPA,Income CPL,Income
                #
                # Var meaning
                # New Depositing Customer = NDC and New Registered Customer = NRC
            else:
                cols_to_use = [
                    "Campaign",
                    "Total Deposits",
                    "NRC",
                    "NDC",
                    "Net revenue Total",
                    "Income",
                ]
                cols_dtype = {
                    "Campaign": "string",
                    "Total Deposits": np.float32,
                    "NRC": np.uint32,
                    "NDC": np.uint32,
                    "Net revenue Total": np.float32,
                    "Income": np.float32,
                }
                cols_rename = {
                    "Campaign": "prom_code",
                    "Total Deposits": "deposit",
                    "NRC": "registered_count",
                    "NDC": "first_deposit_count",
                    "Net revenue Total": "net_revenue",
                    "Income": "revenue_share",
                }
                # Date,Channel,Pay period,Customer group,Campaign,Impressions,
                # Clicks,NRC,NDC,First Deposit,Total Deposits,
                # Gamewin Sportsbook,Costs Sportsbook,Admin fee Sportsbook,
                # Net revenue Sportsbook,Gamewin Casino,Costs Casino,
                # Admin fee Casino,Net revenue Casino,Gamewin Poker,
                # Costs Poker,Admin fee Poker,Net revenue Poker,Gamewin Bingo,
                # Costs Bingo,Admin fee Bingo,Net revenue Bingo,
                # Gamewin Generic,Costs Generic,Admin fee Generic,
                # Net revenue Generic,Gamewin Total,Costs Total,
                # Admin fee Total,Net revenue Total,Income
                #
                # Var meaning
                # New Depositing Customer = NDC and New Registered Customer = NRC

            df_member_i = pd.read_csv(
                filepath_or_buffer=data_io,
                sep=",",
                usecols=cols_to_use,
                dtype=cols_dtype,
            )[cols_to_use]
            df_member_i.rename(
                inplace=True,
                columns=cols_rename,
            )
            dfs_member.append(df_member_i)

            # Start index is the next of previous loop
            index_start_i = index_end_i

        # Concat all dataframes
        df_member = pd.concat(
            objs=dfs_member,
            sort=False,
            join="outer",
            ignore_index=True,
        )
        # Fill possible nans at df merge
        df_member.fillna(
            {
                "deposit": 0.0,
                "registered_count": 0,
                "cpa_count": 0,
                "first_deposit_count": 0,
                "net_revenue": 0.0,
                "revenue_share": 0.0,
                "fixed_income": 0.0,
            },
        )

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df_member.to_csv(
                path_or_buf=f"{options.get('file')}-no_group.csv",
                index=False,
                encoding="utf-8",
            )

        # Group dataframe
        df_member = df_member.groupby(
            by=["prom_code"],
            as_index=False,
        ).sum()

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df_member.to_csv(
                path_or_buf=f"{options.get('file')}-group.csv",
                index=False,
                encoding="utf-8",
            )
            # return

        # Remove data with value all 0
        df_member.drop(
            labels=df_member[
                df_member.eval(
                    "(deposit == 0) &"
                    "(registered_count == 0) &"
                    "(first_deposit_count == 0) &"
                    "(cpa_count == 0) &"
                    "(net_revenue == 0) &"
                    "(revenue_share == 0) &"
                    "(fixed_income == 0)",
                    engine="numexpr",
                )
            ].index,
            inplace=True,
        )

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df_member.to_csv(
                path_or_buf=f"{options.get('file')}-group_non_cero.csv",
                index=False,
                encoding="utf-8",
            )
            return

        if (df_member.empty):
            logger.warning(
                f"Data not found for campaign_title {campaign_title} from_date {from_date_str} to_date {to_date_str}"
                f" at requested url"
                f"Request url: {url}\n"
                "Data obtained\n"
                f"{response_obj.text}"
            )
            return

        if(from_datetime != to_datetime):
            logger.error("Date from and to are equal this data cannot be used for update on DB")
            return

        # Get related link from prom_codes and campaign, QUERY
        filters = (
            Q(prom_code__in=df_member.prom_code.unique()),
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
            Q(created_at=from_datetime.date()),
        )
        betenlace_daily_reports = BetenlaceDailyReport.objects.filter(*filters)

        filters = (
            Q(betenlace_daily_report__in=betenlace_daily_reports),
        )
        partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(*filters)

        # Get the last Fx value
        filters = (
            Q(created_at__gte=from_datetime),
        )
        fx_partner = FxPartner.objects.filter(*filters).order_by("created_at").first()

        if(fx_partner is None):
            # Get just next from supplied date
            filters = (
                Q(created_at__lte=from_datetime),
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
        keys = {key: index for index, key in enumerate(df_member.columns.values)}

        df_member.loc[np.isnan(df_member.fixed_income.values), "fixed_income"] = 0

        for row in tqdm(zip(*df_member.to_dict('list').values())):
            """
            """
            # Get link according to prom_code of current loop
            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)
            if not link:
                logger.warning(
                    f"Link with prom_code=\"{row[keys.get('prom_code')]}\" and campaign=\"{campaign_title}\" "
                    "not found on database"
                )
                continue

            try:
                # Get current entry of member report based on link (prom_code)
                betenlace_cpa = link.betenlacecpa
            except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
                logger.error(f"Betenlace CPA entry not found for link with prom_code={row[keys.get('prom_code')]}")
                continue

            data_i = {
                "deposit": row[keys.get("deposit")],
                # "stake": row[keys.get("stake")],
                "fixed_income": row[keys.get("fixed_income")],
                "net_revenue": row[keys.get("net_revenue")],
                "revenue_share": row[keys.get("revenue_share")],
                "registered_count": row[keys.get("registered_count")],
                "cpa_count": row[keys.get("cpa_count")],
                "first_deposit_count": row[keys.get("first_deposit_count")],
            }

            # Betenlace Month
            if(update_month):
                betenlace_cpa = self.betenlace_month_update(
                    data=data_i,
                    betenlace_cpa=betenlace_cpa,
                )
                member_reports_betenlace_month_update.append(betenlace_cpa)

            # Betenlace Daily
            betenlace_daily = next(
                filter(
                    lambda betenlace_daily: (
                        betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                        betenlace_daily.created_at == from_datetime.date()
                    ),
                    betenlace_daily_reports,
                ),
                None,
            )

            if(betenlace_daily):
                betenlace_daily = self.betenlace_daily_update(
                    data=data_i,
                    betenlace_daily=betenlace_daily,
                    campaign=campaign,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_update.append(betenlace_daily)
            else:
                betenlace_daily = self.betenlace_daily_create(
                    from_date=from_datetime.date(),
                    data=data_i,
                    betenlace_cpa=betenlace_cpa,
                    campaign=campaign,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_create.append(betenlace_daily)

            partner_link_accumulated = link.partner_link_accumulated
            # When partner have not assigned the link must be continue to next loop
            if(partner_link_accumulated is None):
                continue

            # Validate if link has relationship with partner and if has verify if status is equal to status campaign
            if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                if(campaign.status == Campaign.Status.INACTIVE) and (to_datetime.date() >= campaign.last_inactive_at.date()):
                    msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                    logger.warning(msg)
                    continue
            elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                msg = f"link with prom_code {partner_link_accumulated.prom_code} has custom status inactive"
                logger.warning(msg)
                continue

            # Tracker
            if(data_i.get("cpa_count") > settings.MIN_CPA_TRACKER_DAY):
                cpa_count = math.floor(data_i.get("cpa_count")*partner_link_accumulated.tracker)
            else:
                cpa_count = data_i.get("cpa_count")

            tracked_data = self.get_tracker_values(
                data=data_i,
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
                    from_date=from_datetime.date(),
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
                        # "wagering_count",
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
                        # "wagering_count",
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
                        # "tracker_wagering_count",
                        "deposit",
                        "registered_count",
                        "first_deposit_count",
                        # "wagering_count",
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
        data,
        partner_link_accumulated,
    ):
        tracked_data = {}
        if (data.get("deposit") is not None):
            tracked_data["deposit"] = data.get("deposit")*partner_link_accumulated.tracker_deposit

        if (data.get("registered_count") is not None):
            if(data.get("registered_count") > 1):
                tracked_data["registered_count"] = math.floor(
                    data.get("registered_count")*partner_link_accumulated.tracker_registered_count
                )
            else:
                tracked_data["registered_count"] = data.get("registered_count")

        if (data.get("first_deposit_count") is not None):
            if(data.get("first_deposit_count") > 1):
                tracked_data["first_deposit_count"] = math.floor(
                    data.get("first_deposit_count")*partner_link_accumulated.tracker_first_deposit_count
                )
            else:
                tracked_data["first_deposit_count"] = data.get("first_deposit_count")

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
            # wagering_count=tracked_data.get("wagering_count"),

            tracker=partner_link_accumulated.tracker,
            tracker_deposit=partner_link_accumulated.tracker_deposit,
            tracker_registered_count=partner_link_accumulated.tracker_registered_count,
            tracker_first_deposit_count=partner_link_accumulated.tracker_first_deposit_count,
            # tracker_wagering_count=partner_link_accumulated.tracker_wagering_count,

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
        # partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

        partner_link_daily.deposit = tracked_data.get("deposit")
        partner_link_daily.registered_count = tracked_data.get("registered_count")
        partner_link_daily.first_deposit_count = tracked_data.get("first_deposit_count")
        # partner_link_daily.wagering_count = tracked_data.get("wagering_count")

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
        data,
        betenlace_cpa,
        campaign,
        fx_partner,
    ):
        betenlace_daily = BetenlaceDailyReport(
            betenlace_cpa=betenlace_cpa,

            currency_condition=campaign.currency_condition,

            deposit=data.get("deposit"),

            net_revenue=data.get("net_revenue"),
            revenue_share=data.get("revenue_share"),

            currency_fixed_income=campaign.currency_fixed_income,

            fixed_income=data.get("fixed_income"),
            fixed_income_unitary=(
                data.get("fixed_income") / data.get("cpa_count")
                if data.get("cpa_count") != 0
                else
                campaign.fixed_income_unitary
            ),

            fx_partner=fx_partner,

            registered_count=data.get("registered_count"),
            cpa_count=data.get("cpa_count"),
            first_deposit_count=data.get("first_deposit_count"),
            created_at=from_date,
        )

        return betenlace_daily

    def betenlace_daily_update(
        self,
        data,
        betenlace_daily,
        campaign,
        fx_partner,
    ):
        betenlace_daily.deposit = data.get("deposit")
        # betenlace_daily.stake = data.get("stake")
        betenlace_daily.net_revenue = data.get("net_revenue")
        betenlace_daily.revenue_share = data.get("revenue_share")

        betenlace_daily.fixed_income_unitary = (
            data.get('fixed_income')/data.get("cpa_count")
            if data.get("cpa_count") != 0
            else
            campaign.fixed_income_unitary
        )
        betenlace_daily.fixed_income = data.get("fixed_income")

        betenlace_daily.fx_partner = fx_partner

        betenlace_daily.registered_count = data.get("registered_count")
        betenlace_daily.cpa_count = data.get("cpa_count")
        betenlace_daily.first_deposit_count = data.get("first_deposit_count")

        return betenlace_daily

    def betenlace_month_update(
        self,
        data,
        betenlace_cpa,
    ):
        betenlace_cpa.deposit += data.get("deposit")
        # betenlace_cpa.stake += data.get("stake")
        betenlace_cpa.fixed_income += data.get("fixed_income")
        betenlace_cpa.net_revenue += data.get("net_revenue")
        betenlace_cpa.revenue_share += data.get("revenue_share")
        betenlace_cpa.registered_count += data.get("registered_count")
        betenlace_cpa.cpa_count += data.get("cpa_count")
        betenlace_cpa.first_deposit_count += data.get("first_deposit_count")
        return betenlace_cpa
