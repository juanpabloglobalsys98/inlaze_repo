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
            "-fd",
            "--fromdate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            help="Determine date from for get data of commisions",
        )
        parser.add_argument(
            "-td",
            "--todate",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            help="Determine date to for get data of commisions"
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
            choices=("False", "True",),
            default="True",
            help="Full update or not the month accumulated data",
        )
        parser.add_argument(
            "-ddaire",
            "--do_daily_report",
            choices=("False", "True",),
            default="True",
            help="Get data range of days day per day (True) or get range days in one call (False)",
        )
        parser.add_argument(
            "-ors",
            "--only_positive_rs",
            choices=("False", "True",),
            default="False",
            help="Only positive Revenue share for cpa calculation",
        )
        parser.add_argument(
            "-cd",
            "--cpa_date",
            default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
            help="Date where the calculated cpas are stored",
        )

    def handle(self, *args, **options):
        """
        Get data from API of bookmaker Betsson with CSV files using 
        the pandas module with high performance, on command use tqdm for 
        progress bar.

        This gets account report and calculated according to certain quantity
        of cpa_condition_from_rs generate cpa by every punter. Also the other
        data generated on account report will stored.

        Member report is getted and stored information day per day, cpas to sum
        is based on account report BUT the cpa data is stored on supplied cpa 
        date

        The acces key of bettson is via OAuth2 with Client id and client secret
        authentication

        # CSV columns Account report
        - prom_code : `string`
            Equivalent to raw var "Campaign" used on Model 
            `Link` and `MemberReport (Month, daily) for betenlace and 
            partners`, this is the key that identifies a certain promotional 
            link
        - registered_at : `string`
            Date when the punter has registered on Bookmaker
        - first_deposit_at : `bool/string`
            Date when the punter perform the first deposit. This is calculated 
            with var has_first_deposit raw var "NDC", this value have a Bool
            behaviour according to filtered date
        - deposit : `np.float32`
            Equivalent raw var "Total Deposits" used on Models `BetenlaceCPA`, 
            `BetenlaceDailyReport`, quantity of deposited money by punters
        - stake : `np.float32`
            This value IS NOT supplied by Betsson, quantity of wagered money by 
            punters
        - net_revenue : `np.float32`
            Equivalent to raw var "Net revenue Total", used on Models `BetenlaceCPA`, 
            `BetenlaceDailyReport`, Net revenue of bookmaker from punters
        - revenue_share : `np.float32`
            Equivalent to raw var "Income", used on Models `BetenlaceCPA`, 
            `BetenlaceDailyReport`, shared money by bookmaker to betenlace.
        - cpa_count : `np.uint32`
            Quantity of cpa triggered on campaign, This value is calculated 
            based on "revenue_share" for each cpa_condition_from_rs is one cpa. 
            WARNING data could compromised if Betsson changes conditions

        # CSV columns Member Report
        - prom_code : `string`
            Equivalent to raw var "Campaign" used on Model 
            `Link` and `MemberReport (Month, daily) for betenlace and 
            partners`, this is the key that identifies a certain promotional 
            link
        - deposit : `np.float32`
            Equivalent raw var "Total Deposits" used on Models `BetenlaceCPA`, 
            `BetenlaceDailyReport`, quantity of deposited money by punters
        - stake : `np.float32`
            This value IS NOT supplied by Betsson, quantity of wagered money by 
            punters
        - registered_count : `np.uint32`
            Equivalent to raw var "NRC", used on Models `BetenlaceCPA`, 
            `BetenlaceDailyReport`, Count of punters that are registered
        - first_deposit_count : `np.uint32`
            Equivalent to raw var "NRC" used on Models `BetenlaceCPA`, 
            `BetenlaceDailyReport`, count of punters that make a first deposit
        - net_revenue : `np.float32`
            Equivalent to raw var "Net revenue Total", used on Models `BetenlaceCPA`, 
            `BetenlaceDailyReport`, Net revenue of bookmaker from punters
        - revenue_share : `np.float32`
            Equivalent to raw var "Income", used on Models `BetenlaceCPA`, 
            `BetenlaceDailyReport`, shared money by bookmaker to betenlace.
        - cpa_count : `np.uint32`
            Quantity of cpa triggered on campaign, This value is calculated 
            based on "revenue_share" for each cpa_condition_from_rs is one cpa. 
            WARNING data could compromised if Betsson changes conditions
        - wagering_count : `np.uint32`
            This value IS NOT supplied by Betsson, used on Models 
            `BetenlaceCPA`, `BetenlaceDailyReport`, count of players that make 
            a bet
        """
        logger.info(
            "Making call to API Member Betsson\n"
            f"Campaign Title -> {options.get('campaign')}\n"
            f"From date -> {options.get('fromdate')}\n"
            f"To date -> {options.get('todate')}\n"
            f"Cpa date -> {options.get('cpa_date')}\n"
            f"File to save raw -> {options.get('file_raw')}\n"
            f"File to save -> {options.get('file')}\n"
            f"update month -> {options.get('update_month')}\n"
            f"Do daily report -> {options.get('do_daily_report')}\n"
            f"only positive rs -> {options.get('only_positive_rs')}"
        )
        from_date_str = options.get("fromdate")
        to_date_str = options.get("todate")
        cpa_date_str = options.get("cpa_date")
        campaign_title = options.get("campaign")
        update_month = eval(options.get("update_month"))
        do_daily_report = eval(options.get("do_daily_report"))
        only_positive_rs = eval(options.get("only_positive_rs"))

        try:
            from_datetime = make_aware(datetime.strptime(from_date_str, "%Y-%m-%d"))
            to_datetime = make_aware(datetime.strptime(to_date_str, "%Y-%m-%d"))
            if from_datetime > to_datetime:
                logger.error(f"from_date=\"{from_datetime}\" is greather than to_date=\"{to_datetime}\"")
                return
        except:
            logger.error("from_date or to_date have bad format or value. Expected format\"AAAA-mm-dd\"")
            return

        try:
            cpa_datetime = make_aware(datetime.strptime(cpa_date_str, "%Y-%m-%d"))
        except:
            logger.error("cpa_date have bad format. Expected format \"AAAA-mm-dd\"")
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

        if (campaign_title == "betsson col"):
            betsson_channel_id = settings.API_ACCOUNT_MEMBER_REPORT_BETSSON_COL_CHANNEL_ID
            betsson_client_id = settings.API_ACCOUNT_MEMBER_REPORT_BETSSON_COL_CLIENT_ID
            bettson_client_secret = settings.API_ACCOUNT_MEMBER_REPORT_BETSSON_COL_CLIENT_SECRET
            cpa_condition_from_rs = settings.API_ACCOUNT_MEMBER_REPORT_BETSSON_COL_RS_FOR_CPA
        else:
            logger.error(f"Campaign with title \"{campaign_title}\" undefined settings vars")
            return

        # If any var is None or empty prevent execution
        if (
            not all(
                (
                    betsson_channel_id,
                    betsson_client_id,
                    bettson_client_secret,
                    cpa_condition_from_rs,
                )
            )
        ):
            logger.error(
                f"Campaign with title \"{campaign_title}\" have a undefined settings vars, interpreted vars\n"
                f"betsson_channel_id -> \"{betsson_channel_id}\"\n"
                f"betsson_client_id -> \"{betsson_client_id}\"\n"
                f"bettson_client_secret -> \"{bettson_client_secret}\"\n"
                f"cpa_condition_from_revenue_share -> \"{cpa_condition_from_rs}\"\n"
            )
            return

            # Get OAuth2
        url = "https://affiliates.betssongroupaffiliates.com/oauth/access_token"
        body = {
            "client_id": betsson_client_id,
            "client_secret": bettson_client_secret,
            "grant_type": "client_credentials",
            "scope": "r_user_stats",
        }

        response_obj = requests.post(url=url, data=body)

        if (response_obj.status_code != 200):
            logger.error(
                "Status code is not 200 at try to get Authorization from API, check credendials and connection status\n\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"response status:\n{response_obj.status_code}\n"
                f"response text:\n{response_obj.text}\n\n"
                f"if problem still check traceback:\n{''.join(e)}")
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
                f"if problem still check traceback:\n{''.join(e)}"
            )

        # Retrieve Oauth2 and create requried Auth headers
        token_type = response.get("token_type")
        access_token = response.get("access_token")

        authorization = f"{token_type} {access_token}"
        headers = {
            "Authorization": authorization,
        }

        # Control flags
        db_update = True
        if (options.get('file') is not None or options.get('file_raw') is not None):
            db_update = False

        # --- Initialize values for db write
        cpa_by_prom_code_iter = {}

        # Acumulators bulk create and update
        account_reports_update = {}
        account_reports_create = {}

        member_reports_betenlace_month_update = {}
        member_reports_daily_betenlace_update = {}
        member_reports_daily_betenlace_create = {}

        member_reports_partner_month_update = {}
        member_reports_daily_partner_update = {}
        member_reports_daily_partner_create = {}

        # Campaign data
        currency_condition = campaign.currency_condition
        currency_condition_str = currency_condition.lower()
        currency_fixed_income = campaign.currency_fixed_income
        currency_fixed_income_str = currency_fixed_income.lower()

        fixed_income_campaign = campaign.fixed_income_unitary

        # --- Create dates ---
        dates = []
        if (do_daily_report):
            from_date = from_datetime.date()
            delta_date = to_datetime.date() - from_date
            for delta_days_i in range(delta_date.days+1):
                date_i = from_date + timedelta(days=delta_days_i)
                date_i_str = date_i.strftime("%Y-%m-%d")
                dates.append(
                    (
                        make_aware(datetime.combine(date_i, datetime.min.time())),  # from_date
                        date_i_str,  # from_date_str
                        date_i_str,  # to_date_str
                    )
                )
        else:
            # Force leading zero leading
            dates.append(
                (
                    # date for save in db in full range is end time
                    to_datetime,
                    from_datetime.strftime("%Y-%m-%d"),
                    to_datetime.strftime("%Y-%m-%d"),
                )
            )

        # --- Calculate Account and Member data according to input dates ---
        for db_datetime_i, from_date_str_i, to_date_str_i in tqdm(dates, position=0, desc="main"):
            logger.info(
                "Execute iteration\n"
                f"From Date i -> {from_date_str_i}\n"
                f"To Date i -> {to_date_str_i}"
            )
            df_account, df_member, prom_codes, links = self.get_data_iter(
                options=options,
                from_date_str=from_date_str_i,
                to_date_str=to_date_str_i,
                campaign_title=campaign_title,
                campaign=campaign,
                betsson_channel_id=betsson_channel_id,
                headers=headers,
                db_update=db_update,
            )

            # If any var is None or db_update flag is false prevent DB execution
            if (
                not db_update or
                not all(
                    (
                        df_account is not None,
                        df_member is not None,
                        prom_codes is not None,
                        links is not None,
                    )
                )
            ):
                logger.debug("Some var is null or db update is False. Force Next loop")
                continue

            self.account_report_iter(
                campaign_title=campaign_title,
                only_positive_rs=only_positive_rs,
                from_datetime=db_datetime_i,
                cpa_datetime=cpa_datetime,
                campaign=campaign,
                cpa_condition_from_rs=cpa_condition_from_rs,
                cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                account_reports_update=account_reports_update,
                account_reports_create=account_reports_create,
                currency_condition=currency_condition,
                currency_fixed_income=currency_fixed_income,
                fixed_income_campaign=fixed_income_campaign,
                df_account=df_account,
                prom_codes=prom_codes,
                links=links,
            )

            self.member_report_iter(
                campaign_title=campaign_title,
                update_month=update_month,
                from_datetime=db_datetime_i,
                cpa_datetime=cpa_datetime,
                campaign=campaign,
                member_reports_betenlace_month_update=member_reports_betenlace_month_update,
                member_reports_daily_betenlace_update=member_reports_daily_betenlace_update,
                member_reports_daily_betenlace_create=member_reports_daily_betenlace_create,
                member_reports_daily_partner_update=member_reports_daily_partner_update,
                member_reports_daily_partner_create=member_reports_daily_partner_create,
                currency_condition=currency_condition,
                currency_condition_str=currency_condition_str,
                currency_fixed_income=currency_fixed_income,
                currency_fixed_income_str=currency_fixed_income_str,
                fixed_income_campaign=fixed_income_campaign,
                df_member=df_member,
                links=links,
            )

        if (not db_update):
            logger.warning("Stop forced, DB update prevented for db_update false")
            return

        # --- Set accumulated cpas to determinated cpa date ---
        # Get related link from prom_codes and campaign, QUERY
        filters = (
            Q(prom_code__in=cpa_by_prom_code_iter.keys()),
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

        # Get member reports from previous links, QUERY
        filters = (
            Q(betenlace_cpa__pk__in=betenlacecpas_pk),
            Q(created_at=cpa_datetime.date()),
        )
        betenlace_daily_reports = BetenlaceDailyReport.objects.filter(*filters)

        filters = (
            Q(betenlace_daily_report__in=betenlace_daily_reports),
        )
        partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(*filters)

        # Get the Fx the same update day or the next day
        filters = (
            Q(created_at__gte=cpa_datetime),
        )
        fx_partner = FxPartner.objects.filter(*filters).order_by("created_at").first()

        if(fx_partner is None):
            filters = (
                Q(created_at__lte=cpa_datetime),
            )
            fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        # If still none prevent execution
        if(fx_partner is None):
            logger.error("Undefined fx_partner on DB")
            return

        fx_partner_percentage = fx_partner.fx_percentage

        for prom_code_i in tqdm(cpa_by_prom_code_iter.keys(), position=0, desc="cpa"):
            # Get link according to prom_code of current loop
            link = next(filter(lambda link: link.prom_code == prom_code_i, links), None)
            if not link:
                logger.warning(
                    f"Link with prom_code=\"{prom_code_i}\" and campaign=\"{campaign_title}\" "
                    "not found on database"
                )
                continue

            try:
                # Get current entry of member report based on link (prom_code)
                betenlace_cpa = link.betenlacecpa
            except link._meta.model.betenlacecpa.RelatedObjectDoesNotExist:
                logger.error(f"Betenlace CPA entry not found for link with prom_code={prom_code_i}")
                continue

            # Generate data from account report by prom_code
            cpa_count = len(cpa_by_prom_code_iter.get(prom_code_i))

            betenlace_month_key = f"{betenlace_cpa.pk}"

            # Betenlace Month
            if(update_month):
                if (betenlace_month_key in member_reports_betenlace_month_update.keys()):
                    betenlace_cpa = member_reports_betenlace_month_update.get(betenlace_month_key)

                betenlace_cpa = self.betenlace_month_update_cpa(
                    betenlace_cpa=betenlace_cpa,
                    cpa_count=cpa_count,
                    fixed_income_campaign=fixed_income_campaign,
                )
                member_reports_betenlace_month_update[betenlace_month_key] = betenlace_cpa

             # Reset control vars
            has_in_dicto_betenlace_update = False
            has_in_dicto_betenlace_create = False

            betenlace_daily_key = f"{betenlace_cpa.pk}-{cpa_datetime.date()}"

            has_in_dicto_betenlace_update = betenlace_daily_key in member_reports_daily_betenlace_update.keys()
            if (not has_in_dicto_betenlace_update):
                has_in_dicto_betenlace_create = betenlace_daily_key in member_reports_daily_betenlace_create.keys()

            if (not has_in_dicto_betenlace_update and not has_in_dicto_betenlace_create):
                # Betenlace Daily
                betenlace_daily = next(
                    filter(
                        lambda betenlace_daily: (
                            betenlace_daily.betenlace_cpa_id == betenlace_cpa.pk and
                            betenlace_daily.created_at == cpa_datetime.date()
                        ),
                        betenlace_daily_reports,
                    ),
                    None,
                )

             # Case already scheduled for update / create
            if (has_in_dicto_betenlace_update or has_in_dicto_betenlace_create):
                if (has_in_dicto_betenlace_update):
                    betenlace_daily = member_reports_daily_betenlace_update.get(betenlace_daily_key)
                elif (has_in_dicto_betenlace_create):
                    betenlace_daily = member_reports_daily_betenlace_create.get(betenlace_daily_key)

                betenlace_daily = self.betenlace_daily_update_cpa(
                    betenlace_daily=betenlace_daily,
                    fixed_income_campaign=fixed_income_campaign,
                    cpa_count=cpa_count,
                    fx_partner=fx_partner,
                )

                if (has_in_dicto_betenlace_update):
                    member_reports_daily_betenlace_update[betenlace_daily_key] = betenlace_daily
                elif (has_in_dicto_betenlace_create):
                    member_reports_daily_betenlace_create[betenlace_daily_key] = betenlace_daily
            elif (betenlace_daily is not None):
                betenlace_daily = self.betenlace_daily_update_cpa(
                    betenlace_daily=betenlace_daily,
                    fixed_income_campaign=fixed_income_campaign,
                    cpa_count=cpa_count,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_update[betenlace_daily_key] = betenlace_daily
            else:
                betenlace_daily = self.betenlace_daily_create_cpa(
                    betenlace_cpa=betenlace_cpa,
                    from_date=cpa_datetime.date(),
                    fixed_income_campaign=fixed_income_campaign,
                    cpa_count=cpa_count,
                    currency_condition=currency_condition,
                    currency_fixed_income=currency_fixed_income,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_create[betenlace_daily_key] = betenlace_daily

            # Partner Month
            # Get current partner that have the current link
            partner_link_accumulated = link.partner_link_accumulated

            # When partner have not assigned the link must be continue to next loop
            if(partner_link_accumulated is None):
                continue

            # Validate if link has relationship with partner and if has verify if status is equal to status campaign
            if partner_link_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                if(campaign.status == Campaign.Status.INACTIVE and cpa_datetime.date() >= campaign.last_inactive_at.date()):
                    msg = f"link with prom_code {partner_link_accumulated.prom_code} has status campaign inactive"
                    logger.warning(msg)
                    continue
            elif (partner_link_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                msg = f"link with prom_code {partner_link_accumulated.prom_code} has custom status inactive"
                logger.warning(msg)
                continue

            # Tracker
            if(cpa_count > settings.MIN_CPA_TRACKER_DAY):
                cpa_count_new = math.floor(cpa_count*partner_link_accumulated.tracker)
            else:
                cpa_count_new = cpa_count

            # verify if cpa_count had a change from tracker calculation
            if (cpa_count > cpa_count_new):
                # Reduce -1 additional for enum behavior
                diff_count = (cpa_count - cpa_count_new) - 1

                for enum, account_instance_i in enumerate(
                        reversed(cpa_by_prom_code_iter.get(prom_code_i))):
                    # Remove cpa partner
                    account_instance_i.cpa_partner = 0
                    if (enum >= diff_count):
                        break

            # Fx Currency Fixed income
            partner_currency_str = partner_link_accumulated.currency_local.lower()
            fx_fixed_income_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_fixed_income_str,
                partner_currency_str=partner_currency_str,
            )

            fixed_income_partner_unitary = fixed_income_campaign * partner_link_accumulated.percentage_cpa
            fixed_income_partner_unitary_local = (
                fixed_income_campaign *
                partner_link_accumulated.percentage_cpa *
                fx_fixed_income_partner
            )

            # Fx Currency Condition
            fx_condition_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_condition_str,
                partner_currency_str=partner_currency_str,
            )

            partner_month_key = f"{betenlace_cpa.pk}"

            if(update_month):
                if (partner_month_key in member_reports_partner_month_update.keys()):
                    betenlace_cpa = member_reports_partner_month_update.get(partner_month_key)

                partner_link_accumulated = self.partner_link_month_update_cpa(
                    partner_link_accumulated=partner_link_accumulated,
                    cpa_count=cpa_count_new,
                    fixed_income_partner=cpa_count_new * fixed_income_partner_unitary,
                    fixed_income_partner_local=cpa_count_new * fixed_income_partner_unitary_local,
                )
                member_reports_partner_month_update[partner_month_key] = partner_link_accumulated

            # Reset control vars
            has_in_dicto_partner_update = False
            has_in_dicto_partner_create = False

            partner_daily_key = f"{betenlace_cpa.pk}-{from_datetime.date()}"

            has_in_dicto_partner_update = partner_daily_key in member_reports_daily_partner_update.keys()
            if (not has_in_dicto_partner_update):
                has_in_dicto_partner_create = partner_daily_key in member_reports_daily_partner_create.keys()

            if (not has_in_dicto_partner_update and not has_in_dicto_partner_create):
                # Partner Daily
                partner_link_daily = next(
                    filter(
                        lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id == betenlace_daily.id,
                        partner_link_dailies_reports,
                    ),
                    None,
                )

            if (has_in_dicto_partner_update or has_in_dicto_partner_create):
                if (has_in_dicto_partner_update):
                    partner_link_daily = member_reports_daily_partner_update.get(partner_daily_key)
                elif (has_in_dicto_partner_create):
                    partner_link_daily = member_reports_daily_partner_create.get(partner_daily_key)

                partner_link_daily = self.partner_link_daily_update_cpa(
                    cpa_count=cpa_count_new,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    partner_link_daily=partner_link_daily,
                    partner=partner_link_accumulated.partner,
                    betenlace_daily=betenlace_daily,
                )

                if (has_in_dicto_partner_update):
                    member_reports_daily_partner_update[partner_daily_key] = partner_link_daily
                elif (has_in_dicto_partner_create):
                    member_reports_daily_partner_create[partner_daily_key] = partner_link_daily
            elif(partner_link_daily is not None):
                partner_link_daily = self.partner_link_daily_update_cpa(
                    cpa_count=cpa_count_new,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    partner_link_daily=partner_link_daily,
                    partner=partner_link_accumulated.partner,
                    betenlace_daily=betenlace_daily,
                )
                member_reports_daily_partner_update[partner_daily_key] = partner_link_daily
            else:
                partner_link_daily = self.partner_link_daily_create_cpa(
                    from_date=cpa_datetime.date(),
                    campaign=campaign,
                    betenlace_daily=betenlace_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    cpa_count=cpa_count_new,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    partner=partner_link_accumulated.partner,
                )
                member_reports_daily_partner_create[partner_daily_key] = partner_link_daily
        with transaction.atomic(using=DB_USER_PARTNER):
            # Account case
            if(account_reports_create.values()):
                AccountReport.objects.bulk_create(
                    objs=account_reports_create.values(),
                )
            if(account_reports_update.values()):
                AccountReport.objects.bulk_update(
                    objs=account_reports_update.values(),
                    fields=(
                        "deposit",
                        "fixed_income",
                        "net_revenue",
                        "revenue_share",

                        "cpa_betenlace",
                        "cpa_partner",

                        "registered_at",
                        "first_deposit_at",
                        "cpa_at",
                    ),
                )

            # Member case
            if(member_reports_betenlace_month_update.values()):
                BetenlaceCPA.objects.bulk_update(
                    objs=member_reports_betenlace_month_update.values(),
                    fields=(
                        "deposit",
                        "net_revenue",
                        "revenue_share",
                        "fixed_income",
                        "cpa_count",
                        "registered_count",
                        "first_deposit_count",
                    ),
                )

            if(member_reports_daily_betenlace_update.values()):
                BetenlaceDailyReport.objects.bulk_update(
                    objs=member_reports_daily_betenlace_update.values(),
                    fields=(
                        "deposit",
                        "net_revenue",
                        "revenue_share",
                        "fixed_income",
                        "fixed_income_unitary",
                        "fx_partner",
                        "cpa_count",
                        "registered_count",
                        "first_deposit_count",
                    ),
                )

            if(member_reports_daily_betenlace_create.values()):
                BetenlaceDailyReport.objects.bulk_create(
                    objs=member_reports_daily_betenlace_create.values(),
                )

            if(member_reports_partner_month_update.values()):
                PartnerLinkAccumulated.objects.bulk_update(
                    objs=member_reports_partner_month_update.values(),
                    fields=(
                        "cpa_count",
                        "fixed_income",
                        "fixed_income_local",
                    ),
                )

            if(member_reports_daily_partner_update.values()):
                PartnerLinkDailyReport.objects.bulk_update(
                    objs=member_reports_daily_partner_update.values(),
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

            if(member_reports_daily_partner_create.values()):
                PartnerLinkDailyReport.objects.bulk_create(
                    objs=member_reports_daily_partner_create.values(),
                )

    def get_data_iter(
        self,
        options,
        from_date_str,
        to_date_str,
        campaign_title,
        campaign,
        betsson_channel_id,
        headers,
        db_update,
    ):
        # --- Retrieve data ---

        # ---- Account report case ----
        url = (
            f"https://affiliates.betssongroupaffiliates.com/statistics-ext.php?p={betsson_channel_id}&"
            f"d1={from_date_str}&d2={to_date_str}&pdd=customer&"
            "cg=&c=&m=&o=&s=&jd1=&jd2=&cjd=1&cc=1&mode=csv&sbm=1&dnl=1"
        )

        response_obj = requests.get(url=url, headers=headers)

        if (
            (response_obj.text == "" or "No data" in response_obj.text) or
            not ("Campaign" in response_obj.text or "Join date" in response_obj.text)
        ):
            logger.warning(
                f"Data not found for campaign_title {campaign_title} from_date {from_date_str} to_date {to_date_str}"
                f" at account report from requested url"
                f"Request url: {url}\n"
                "Data obtained\n"
                f"{response_obj.text}"
            )
            return None, None, None, None

        data_io = StringIO(response_obj.text)

        # Create dataframe
        cols_to_use = [
            "Campaign",
            "Customer",
            "Join date",
            "Total Deposits",
            "Net revenue Total",
            "Income",
            "NDC",
        ]
        df_account = pd.read_csv(
            filepath_or_buffer=data_io,
            sep=",",
            usecols=cols_to_use,
            dtype={
                "Campaign": "string",
                "Customer": "string",
                "Join date": "string",
                "Total Deposits": np.float32,
                "Net revenue Total": np.float32,
                "Income": np.float32,
                "NDC": np.bool8,
            },
        )[cols_to_use]

        df_account.rename(
            inplace=True,
            columns={
                "Campaign": "prom_code",
                "Customer": "punter_id",
                "Join date": "registered_at",
                "Total Deposits": "deposit",
                "Net revenue Total": "net_revenue",
                "Income": "revenue_share",
                "NDC": "has_first_deposit",
            },
        )

        # Account report vars
        # Channel,Pay period,Customer group,Customer,Join date,Campaign group,
        # Campaign,Impressions,Clicks,NRC,NDC,First Deposit,Total Deposits,
        # Gamewin Sportsbook,Costs Sportsbook,Admin fee Sportsbook,
        # Net revenue Sportsbook,Gamewin Casino,Costs Casino,Admin fee Casino,
        # Net revenue Casino,Gamewin Poker,Costs Poker,Admin fee Poker,
        # Net revenue Poker,Gamewin Bingo,Costs Bingo,Admin fee Bingo,
        # Net revenue Bingo,Gamewin Generic,Costs Generic,Admin fee Generic,
        # Net revenue Generic,Gamewin Total,Costs Total,Admin fee Total,
        # Net revenue Total,Income

        # Remove data about Unkown punters
        df_account.drop(
            labels=df_account[df_account.eval("(punter_id == 'Unknown')", engine="numexpr")].index,
            inplace=True,
        )

        if (df_account.empty):
            logger.warning(
                f"Data not found for campaign_title {campaign_title} from_date {from_date_str} to_date {to_date_str}"
                f" at requested url"
                f"Request url: {url}\n"
                "Data obtained\n"
                f"{response_obj.text}"
            )
            return None, None, None, None

        # File case save to disk
        if(options.get("file")):
            df_account.to_csv(
                path_or_buf=f"{options.get('file')}_{from_date_str}_account.csv",
                index=False,
                encoding="utf-8",
            )

        if(options.get("file_raw")):
            with open(f"{options.get('file_raw')}_{from_date_str}_account.csv", "w") as out:
                out.write(response_obj.text)

        # ---- Member report case ----
        url = (
            f"https://affiliates.betssongroupaffiliates.com/statistics.php?p={betsson_channel_id}&"
            f"d1={from_date_str}&d2={to_date_str}&cg=&c=&m=&o=&s=&sd=1&sc=1&mode=csv&sbm=1&dnl=1"
        )

        response_obj = requests.get(url=url, headers=headers)

        if (
            (
                response_obj.text == "" or "No data" in response_obj.text) or
            not ("Campaign" in response_obj.text or "Join date" in response_obj.text)
        ):
            logger.warning(
                f"Data not found for campaign_title {campaign_title} from_date {from_date_str} to_date {to_date_str}"
                f" at requested url"
                f"Request url: {url}\n"
                "Data obtained\n"
                f"{response_obj.text}"
            )
            # Temp force stop
            # return

        data_io = StringIO(response_obj.text)

        # Create dataframe
        cols_to_use = [
            "Campaign",
            "Total Deposits",
            "NRC",
            "NDC",
            "Net revenue Total",
            "Income",
        ]
        df_member = pd.read_csv(
            filepath_or_buffer=data_io,
            sep=",",
            usecols=cols_to_use,
            dtype={
                "Campaign": "string",
                "Total Deposits": np.float32,
                "NRC": np.uint32,
                "NDC": np.uint32,
                "Net revenue Total": np.float32,
                "Income": np.float32,
            },
        )[cols_to_use]

        df_member.rename(
            inplace=True,
            columns={
                "Campaign": "prom_code",
                "Total Deposits": "deposit",
                "NRC": "registered_count",
                "NDC": "first_deposit_count",
                "Net revenue Total": "net_revenue",
                "Income": "revenue_share",
            },
        )

        # Member report vars
        # Date,Channel,Pay period,Customer group,Campaign,Impressions,Clicks,
        # NRC,NDC,First Deposit,Total Deposits,Gamewin Sportsbook,
        # Costs Sportsbook,Admin fee Sportsbook,Net revenue Sportsbook,
        # Gamewin Casino,Costs Casino,Admin fee Casino,Net revenue Casino,
        # Gamewin Poker,Costs Poker,Admin fee Poker,Net revenue Poker,
        # Gamewin Bingo,Costs Bingo,Admin fee Bingo,Net revenue Bingo,
        # Gamewin Generic,Costs Generic,Admin fee Generic,Net revenue Generic,
        # Gamewin Total,Costs Total,Admin fee Total,Net revenue Total,Income

        # NRC -> registered count
        # NDC -> first deposit count
        # Total Deposits -> deposit
        # Net revenue Total -> Net revenue
        # Income -> Revenue Share
        # Campaign -> Prom code

        # Remove data with value all 0
        df_member.drop(
            labels=df_member[
                df_member.eval(
                    "(deposit == 0) &"
                    "(registered_count == 0) &"
                    "(first_deposit_count == 0) &"
                    "(net_revenue == 0) &"
                    "(revenue_share == 0)",
                    engine="numexpr",
                )
            ].index,
            inplace=True,
        )

        if (df_member.empty):
            logger.warning(
                f"Data not found for campaign_title {campaign_title} from_date {from_date_str} to_date {to_date_str}"
                f" at requested url"
                f"Request url: {url}\n"
                "Data obtained\n"
                f"{response_obj.text}"
            )
            # Temp force stop
            # return

        # File case save to disk
        if(options.get("file")):
            logger.debug(f"Create member file \"{options.get('file')}_{from_date_str}_member.csv\"")
            df_member.to_csv(
                path_or_buf=f"{options.get('file')}_{from_date_str}_member.csv",
                index=False,
                encoding="utf-8",
            )

        if(options.get("file_raw")):
            logger.debug(f"Create member file raw \"{options.get('file_raw')}_{from_date_str}_member.csv\"")
            with open(f"{options.get('file_raw')}_{from_date_str}_member.csv", "w") as out:
                out.write(response_obj.text)

        if(not db_update):
            logger.debug("prevent DB calculation, force next loop")
            return None, None, None, None

        # --- Get data from DB ---
        prom_codes = set(df_account.prom_code.unique()) | set(df_member.prom_code.unique())

        # Get related link from prom_codes and campaign, QUERY
        filters = (
            Q(prom_code__in=prom_codes),
            Q(campaign_id=campaign.id),
        )
        links = Link.objects.filter(
            *filters,
        ).select_related(
            "partner_link_accumulated",
            "partner_link_accumulated__partner",
            "betenlacecpa",
        )

        return df_account, df_member, prom_codes, links

    def account_report_iter(
        self,
        campaign_title,
        only_positive_rs,
        from_datetime,
        cpa_datetime,
        campaign,
        cpa_condition_from_rs,
        cpa_by_prom_code_iter,
        account_reports_update,
        account_reports_create,
        currency_condition,
        currency_fixed_income,
        fixed_income_campaign,
        df_account,
        prom_codes,
        links,
    ):
        links_pk = links.values_list("pk", flat=True)

        # Get account reports from previous links and punter_id, QUERY
        filters = (
            Q(link__in=links_pk),
            Q(punter_id__in=df_account.punter_id.unique()),
        )
        account_reports = AccountReport.objects.filter(*filters)

        # Set keys by index based on colum names of Dataframe
        keys = {key: index for index, key in enumerate(df_account.columns.values)}

        # Dictionary with current applied sum of cpa's by prom_code, add only new cpas
        for prom_code in prom_codes:
            if (not prom_code in cpa_by_prom_code_iter):
                cpa_by_prom_code_iter[prom_code] = []

        for row in tqdm(zip(*df_account.to_dict('list').values()), position=1, desc="account"):
            """
            - row_id
            - prom_code
            - punter_id
            - revenue_share
            - registered_at
            - first_deposit_at
            """
            link = next(filter(lambda link: link.prom_code == row[keys.get("prom_code")], links), None)

            if not link:
                logger.warning(f"Link with prom_code={row[keys.get('prom_code')]} and campaign={campaign_title}")
                continue

            # Reset control vars
            has_in_dicto_account_update = False
            has_in_dicto_account_create = False

            account_dicto_key = f"{link.pk}-{row[keys.get('punter_id')]}"

            # Check if punter has already for db update
            has_in_dicto_account_update = account_dicto_key in account_reports_update.keys()

            # Check if punter has already for db create
            if (not has_in_dicto_account_update):
                has_in_dicto_account_create = account_dicto_key in account_reports_create.keys()

            # Get current entry of account report based on link and punter_id
            if (not has_in_dicto_account_update and not has_in_dicto_account_create):
                account_report = next(
                    filter(
                        lambda account_report_i: account_report_i.link_id == link.pk and
                        account_report_i.punter_id == row[keys.get("punter_id")],
                        account_reports
                    ),
                    None,
                )

            # Get current partner that have the current link
            partner_link_accumulated = (
                None
                if (
                    campaign.status == Campaign.Status.INACTIVE
                    and
                    cpa_datetime.date() >= campaign.last_inactive_at.date()
                )
                else
                link.partner_link_accumulated
            )

            # Case already scheduled for update / create
            if (has_in_dicto_account_update or has_in_dicto_account_create):
                if (has_in_dicto_account_update):
                    account_report = account_reports_update.get(account_dicto_key)
                if (has_in_dicto_account_create):
                    account_report = account_reports_create.get(account_dicto_key)

                account_report = self.account_report_update(
                    keys=keys,
                    row=row,
                    from_date=from_datetime.date(),
                    partner_link_accumulated=partner_link_accumulated,
                    account_report=account_report,
                    cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                    cpa_condition_from_rs=cpa_condition_from_rs,
                    fixed_income_campaign=fixed_income_campaign,
                    only_positive_rs=only_positive_rs,
                )
                if (has_in_dicto_account_update):
                    account_reports_update[account_dicto_key] = account_report
                if (has_in_dicto_account_create):
                    account_reports_create[account_dicto_key] = account_report
            elif (account_report is not None):
                # Case and exist entry
                account_report = self.account_report_update(
                    keys=keys,
                    row=row,
                    from_date=from_datetime.date(),
                    partner_link_accumulated=partner_link_accumulated,
                    account_report=account_report,
                    cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                    cpa_condition_from_rs=cpa_condition_from_rs,
                    fixed_income_campaign=fixed_income_campaign,
                    only_positive_rs=only_positive_rs,
                )
                account_reports_update[account_dicto_key] = account_report
            else:
                # Case new entry
                account_report = self.account_report_create(
                    row=row,
                    keys=keys,
                    link=link,
                    currency_condition=currency_condition,
                    currency_fixed_income=currency_fixed_income,
                    partner_link_accumulated=partner_link_accumulated,
                    from_date=from_datetime.date(),
                    cpa_by_prom_code_iter=cpa_by_prom_code_iter,
                    cpa_condition_from_rs=cpa_condition_from_rs,
                    fixed_income_campaign=fixed_income_campaign,
                    only_positive_rs=only_positive_rs,
                )
                account_reports_create[account_dicto_key] = account_report

    def account_report_update(
        self,
        keys,
        row,
        from_date,
        partner_link_accumulated,
        account_report,
        cpa_by_prom_code_iter,
        cpa_condition_from_rs,
        fixed_income_campaign,
        only_positive_rs,
    ):
        """
        Update account report data from row data like
        - first_deposit_at
        - revenue_share
        - prom_code

        prom_code is used to get the related link on database and sum iter 
        count for easy tracker management, with revenue_share are calculated 
        the net_revenue with `revenue_share_percentage` value, registered at 
        must be alredy defined at punter data creation.
        """
        # Check registrationdate
        if (not pd.isna(row[keys.get("registered_at")])):
            account_report.registered_at = make_aware(datetime.strptime(row[keys.get("registered_at")], "%Y-%m-%d"))

        # Check first_deposit_at
        if (row[keys.get("has_first_deposit")]):
            account_report.first_deposit_at = from_date

        if (only_positive_rs):
            account_report.revenue_share += (
                0 if row[keys.get("revenue_share")] < 0
                else
                row[keys.get("revenue_share")]
            )
        else:
            account_report.revenue_share += row[keys.get("revenue_share")]

        account_report.deposit += row[keys.get("deposit")]

        account_report.net_revenue += row[keys.get("net_revenue")]

        # Force new Related partner if cpa count are not determinated yet
        if(not account_report.cpa_betenlace):
            account_report.partner_link_accumulated = partner_link_accumulated

        # condition from revenue share and not already cpa
        if (account_report.revenue_share >= cpa_condition_from_rs and not account_report.cpa_betenlace):
            account_report.cpa_betenlace = 1
            account_report.cpa_at = from_date
            # This bookmaker pay only Revenue share
            account_report.fixed_income = 0

            # Temp have value 1, later will removed
            account_report.cpa_partner = 0 if partner_link_accumulated is None else 1

            if not account_report in cpa_by_prom_code_iter[row[keys.get("prom_code")]]:
                cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(account_report)

        return account_report

    def account_report_create(
        self,
        row,
        keys,
        link,
        currency_condition,
        currency_fixed_income,
        partner_link_accumulated,
        from_date,
        cpa_by_prom_code_iter,
        cpa_condition_from_rs,
        fixed_income_campaign,
        only_positive_rs,
    ):
        """
        Create account report data from row data like
        - first_deposit_at
        - revenue_share
        - registered_at
        - prom_code

        prom_code is used to get the related link on database and sum iter 
        count for easy tracker management, with revenue_share are calculated 
        the net_revenue
        """
        # Check registrationdate null
        if (not pd.isna(row[keys.get("registered_at")])):
            registered_at = make_aware(datetime.strptime(row[keys.get("registered_at")], "%Y-%m-%d"))
        else:
            registered_at = None

        # Check registrationdate null
        if (row[keys.get("has_first_deposit")]):
            first_deposit_at = from_date
        else:
            first_deposit_at = None

        cpa_count = 0
        # condition from revenue share
        if (row[keys.get("revenue_share")] >= cpa_condition_from_rs):
            cpa_count = 1

        if (only_positive_rs):
            revenue_share = (
                0 if row[keys.get("revenue_share")] < 0
                else
                row[keys.get("revenue_share")]
            )
        else:
            revenue_share = row[keys.get("revenue_share")]

        account_report = AccountReport(
            partner_link_accumulated=partner_link_accumulated,
            punter_id=row[keys.get("punter_id")],

            deposit=row[keys.get("deposit")],

            # This bookmaker pay only Revenue share
            fixed_income=0,

            net_revenue=row[keys.get("net_revenue")],
            revenue_share=revenue_share,
            currency_condition=currency_condition,
            currency_fixed_income=currency_fixed_income,
            cpa_betenlace=cpa_count,
            cpa_partner=(0 if partner_link_accumulated is None else cpa_count),
            first_deposit_at=first_deposit_at,
            link=link,
            registered_at=registered_at,
            created_at=from_date,
        )

        if(cpa_count):
            # Case when cpa is True or 1
            account_report.cpa_at = from_date
            cpa_by_prom_code_iter[row[keys.get("prom_code")]].append(account_report)
        return account_report

    def member_report_iter(
        self,
        campaign_title,
        update_month,
        from_datetime,
        cpa_datetime,
        campaign,
        member_reports_betenlace_month_update,
        member_reports_daily_betenlace_update,
        member_reports_daily_betenlace_create,
        member_reports_daily_partner_update,
        member_reports_daily_partner_create,
        currency_condition,
        currency_condition_str,
        currency_fixed_income,
        currency_fixed_income_str,
        fixed_income_campaign,
        df_member,
        links,
    ):
        # --- Continue for Member report ---
        betenlacecpas_pk = links.values_list("betenlacecpa__pk", flat=True)

        # Get member reports from previous links, QUERY
        filters = (
            Q(betenlace_cpa__pk__in=betenlacecpas_pk),
            Q(created_at=from_datetime.date()),
        )
        betenlace_daily_reports = BetenlaceDailyReport.objects.filter(*filters)

        filters = (
            Q(betenlace_daily_report__in=betenlace_daily_reports),
        )
        partner_link_dailies_reports = PartnerLinkDailyReport.objects.filter(*filters)

        # Get the Fx the same update day or the next day
        filters = (
            Q(created_at__gte=from_datetime),
        )
        fx_partner = FxPartner.objects.filter(*filters).order_by("created_at").first()

        if(fx_partner is None):
            filters = (
                Q(created_at__lte=from_datetime),
            )
            fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        # If still none prevent execution
        if(fx_partner is None):
            logger.error("Undefined fx_partner on DB")
            return

        fx_partner_percentage = fx_partner.fx_percentage

        # Set keys by index based on colum names of Dataframe
        keys = {key: index for index, key in enumerate(df_member.columns.values)}

        for row in tqdm(zip(*df_member.to_dict('list').values()), position=2, desc="member"):
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

            betenlace_month_key = f"{betenlace_cpa.pk}"

            # Betenlace Month
            if(update_month):
                if (betenlace_month_key in member_reports_betenlace_month_update.keys()):
                    betenlace_cpa = member_reports_betenlace_month_update.get(betenlace_month_key)

                betenlace_cpa = self.betenlace_month_update_no_cpa(
                    keys=keys,
                    row=row,
                    betenlace_cpa=betenlace_cpa,
                )
                member_reports_betenlace_month_update[betenlace_month_key] = betenlace_cpa

            # Reset control vars
            has_in_dicto_betenlace_update = False
            has_in_dicto_betenlace_create = False

            betenlace_daily_key = f"{betenlace_cpa.pk}-{from_datetime.date()}"

            has_in_dicto_betenlace_update = betenlace_daily_key in member_reports_daily_betenlace_update.keys()
            if (not has_in_dicto_betenlace_update):
                has_in_dicto_betenlace_create = betenlace_daily_key in member_reports_daily_betenlace_create.keys()

            if (not has_in_dicto_betenlace_update and not has_in_dicto_betenlace_create):
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

            # Case already scheduled for update / create
            if (has_in_dicto_betenlace_update or has_in_dicto_betenlace_create):
                if (has_in_dicto_betenlace_update):
                    betenlace_daily = member_reports_daily_betenlace_update.get(betenlace_daily_key)
                elif (has_in_dicto_betenlace_create):
                    betenlace_daily = member_reports_daily_betenlace_create.get(betenlace_daily_key)

                betenlace_daily = self.betenlace_daily_update_no_cpa(
                    keys=keys,
                    row=row,
                    betenlace_daily=betenlace_daily,
                    fixed_income_campaign=fixed_income_campaign,
                    fx_partner=fx_partner,
                )

                if (has_in_dicto_betenlace_update):
                    member_reports_daily_betenlace_update[betenlace_daily_key] = betenlace_daily
                elif (has_in_dicto_betenlace_create):
                    member_reports_daily_betenlace_create[betenlace_daily_key] = betenlace_daily
            elif (betenlace_daily is not None):
                betenlace_daily = self.betenlace_daily_update_no_cpa(
                    keys=keys,
                    row=row,
                    betenlace_daily=betenlace_daily,
                    fixed_income_campaign=fixed_income_campaign,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_update[betenlace_daily_key] = betenlace_daily
            else:
                betenlace_daily = self.betenlace_daily_create_no_cpa(
                    keys=keys,
                    row=row,
                    betenlace_cpa=betenlace_cpa,
                    from_date=from_datetime.date(),
                    fixed_income_campaign=fixed_income_campaign,
                    currency_condition=currency_condition,
                    currency_fixed_income=currency_fixed_income,
                    fx_partner=fx_partner,
                )
                member_reports_daily_betenlace_create[betenlace_daily_key] = betenlace_daily

            # Partner Month
            # Get current partner that have the current link
            partner_link_accumulated = (
                None
                if (
                    campaign.status == Campaign.Status.INACTIVE
                    and
                    cpa_datetime.date() >= campaign.last_inactive_at.date()
                )
                else
                link.partner_link_accumulated
            )

            # When partner have not assigned the link must be continue to next loop
            if(partner_link_accumulated is None):
                continue

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

            fixed_income_partner_unitary = fixed_income_campaign * partner_link_accumulated.percentage_cpa
            fixed_income_partner_unitary_local = (
                fixed_income_campaign *
                partner_link_accumulated.percentage_cpa *
                fx_fixed_income_partner
            )

            # Fx Currency Condition
            fx_condition_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_condition_str,
                partner_currency_str=partner_currency_str,
            )

            # Reset control vars
            has_in_dicto_partner_update = False
            has_in_dicto_partner_create = False

            partner_daily_key = f"{betenlace_cpa.pk}-{from_datetime.date()}"

            has_in_dicto_partner_update = partner_daily_key in member_reports_daily_partner_update.keys()
            if (not has_in_dicto_partner_update):
                has_in_dicto_partner_create = partner_daily_key in member_reports_daily_partner_create.keys()

            if (not has_in_dicto_partner_update and not has_in_dicto_partner_create):
                # Partner Daily
                partner_link_daily = next(
                    filter(
                        lambda partner_link_daily: partner_link_daily.betenlace_daily_report_id == betenlace_daily.id,
                        partner_link_dailies_reports,
                    ),
                    None,
                )

            if (has_in_dicto_partner_update or has_in_dicto_partner_create):
                if (has_in_dicto_partner_update):
                    partner_link_daily = member_reports_daily_partner_update.get(partner_daily_key)
                elif (has_in_dicto_partner_create):
                    partner_link_daily = member_reports_daily_partner_create.get(partner_daily_key)

                partner_link_daily = self.partner_link_daily_update_no_cpa(
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    partner_link_daily=partner_link_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    partner=partner_link_accumulated.partner,
                    betenlace_daily=betenlace_daily,
                )

                if (has_in_dicto_partner_update):
                    member_reports_daily_partner_update[partner_daily_key] = partner_link_daily
                elif (has_in_dicto_partner_create):
                    member_reports_daily_partner_create[partner_daily_key] = partner_link_daily
            elif(partner_link_daily is not None):
                partner_link_daily = self.partner_link_daily_update_no_cpa(
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    partner_link_daily=partner_link_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    partner=partner_link_accumulated.partner,
                    betenlace_daily=betenlace_daily,
                )
                member_reports_daily_partner_update[partner_daily_key] = partner_link_daily
            else:
                partner_link_daily = self.partner_link_daily_create_no_cpa(
                    from_date=from_datetime.date(),
                    campaign=campaign,
                    betenlace_daily=betenlace_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    partner=partner_link_accumulated.partner,
                )
                member_reports_daily_partner_create[partner_daily_key] = partner_link_daily

    def betenlace_month_update_no_cpa(
        self,
        keys,
        row,
        betenlace_cpa,
    ):
        """
        Update Member Current Month report data from row data like
        - deposit
        - first_deposit_at
        - revenue_share
        - registered_count
        - first_deposit_count

        revenue_share calculated from net_revenue.

        The results are sum
        """
        betenlace_cpa.deposit += row[keys.get("deposit")]

        betenlace_cpa.net_revenue += row[keys.get("net_revenue")]
        betenlace_cpa.revenue_share += row[keys.get("revenue_share")]

        betenlace_cpa.registered_count += row[keys.get('registered_count')]
        betenlace_cpa.first_deposit_count += row[keys.get('first_deposit_count')]
        return betenlace_cpa

    def betenlace_daily_update_no_cpa(
        self,
        keys,
        row,
        betenlace_daily,
        fixed_income_campaign,
        fx_partner,
    ):
        """
        Update Member daily report data from row data like
        - deposit
        - first_deposit_at
        - revenue_share
        - registered_count
        - first_deposit_count

        Cpa count will calculated later
        """

        betenlace_daily.deposit = row[keys.get("deposit")]

        betenlace_daily.net_revenue = row[keys.get("net_revenue")]
        betenlace_daily.revenue_share = row[keys.get("revenue_share")]

        # This bookmaker pay only Revenue share
        betenlace_daily.fixed_income = 0
        betenlace_daily.fixed_income_unitary = fixed_income_campaign

        betenlace_daily.fx_partner = fx_partner

        betenlace_daily.registered_count = row[keys.get('registered_count')]
        betenlace_daily.first_deposit_count = row[keys.get('first_deposit_count')]

        return betenlace_daily

    def betenlace_daily_create_no_cpa(
        self,
        keys,
        row,
        betenlace_cpa,
        from_date,
        fixed_income_campaign,
        currency_condition,
        currency_fixed_income,
        fx_partner,
    ):
        """
        Create Member daily report data from row data like
        - deposit
        - first_deposit_at
        - revenue_share
        - registered_count
        - first_deposit_count
        """
        betenlace_daily = BetenlaceDailyReport(
            betenlace_cpa=betenlace_cpa,

            deposit=row[keys.get("deposit")],

            currency_condition=currency_condition,

            net_revenue=row[keys.get("net_revenue")],
            revenue_share=row[keys.get("revenue_share")],

            currency_fixed_income=currency_fixed_income,

            # This bookmaker pay only Revenue share
            fixed_income=0,
            fixed_income_unitary=fixed_income_campaign,

            fx_partner=fx_partner,

            registered_count=row[keys.get("registered_count")],
            first_deposit_count=row[keys.get("first_deposit_count")],

            created_at=from_date,
        )

        return betenlace_daily

    def partner_link_daily_update_no_cpa(
        self,
        tracked_data,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner_unitary_local,
        partner_link_daily,
        partner_link_accumulated,
        partner,
        betenlace_daily,
    ):
        """
        Update Member Daily report for Partner data with respective
        - fixed_income_unitary
        - fx_book_local
        - fx_percentage
        - fixed_income_unitary_local
        """
        partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary

        partner_link_daily.fx_book_local = fx_fixed_income_partner
        partner_link_daily.fx_book_net_revenue_local = fx_condition_partner
        partner_link_daily.fx_percentage = fx_partner_percentage

        partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

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
        partner_link_daily.net_revenue_adviser_percentage = partner.net_revenue_adviser_percentage

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
        partner_link_daily.net_revenue_referred_percentage = partner.net_revenue_referred_percentage

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

    def partner_link_daily_create_no_cpa(
        self,
        from_date,
        campaign,
        betenlace_daily,
        partner_link_accumulated,
        tracked_data,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner_unitary_local,
        partner,
    ):
        """
        Create Member Daily report for Partner data with respective
        - fixed_income_unitary
        - fx_book_local
        - fx_book_net_revenue_local
        - fx_percentage
        - fixed_income_unitary_local

        # Relations data
        - betenlace_daily
        - partner_link_accumulated

        # Currencies
        - currency_fixed_income
        - currency_local

        # Others
        - tracker
        - created_at
        """
        # Calculate Adviser payment

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
            fixed_income_unitary=fixed_income_partner_unitary,

            currency_local=partner_link_accumulated.currency_local,
            fx_book_local=fx_fixed_income_partner,
            fx_book_net_revenue_local=fx_condition_partner,
            fx_percentage=fx_partner_percentage,

            fixed_income_unitary_local=fixed_income_partner_unitary_local,

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

            net_revenue_adviser=net_revenue_adviser,
            net_revenue_adviser_local=net_revenue_adviser_local,

            # referred base data
            referred_by=partner.referred_by,
            fixed_income_referred_percentage=partner.fixed_income_referred_percentage,
            net_revenue_referred_percentage=partner.net_revenue_referred_percentage,

            net_revenue_referred=net_revenue_referred,
            net_revenue_referred_local=net_revenue_referred_local,

            created_at=from_date,
        )

        return partner_link_daily

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

        # if (keys.get("wagering_count") is not None):
        #     if(row[keys.get("wagering_count")] > 1):
        #         tracked_data["wagering_count"] = math.floor(
        #             row[keys.get("wagering_count")]*partner_link_accumulated.tracker_wagering_count
        #         )
        #     else:
        #         tracked_data["wagering_count"] = row[keys.get("wagering_count")]

        return tracked_data

    def calc_fx(
        self,
        fx_partner,
        fx_partner_percentage,
        currency_from_str,
        partner_currency_str,
    ):
        """
        Calculate Fx conversion according to campaign currency and partner 
        currency. if both currency are same fx will value 1 in another case 
        get respective value multiply by fx_percentage
        """
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

    def betenlace_month_update_cpa(
        self,
        betenlace_cpa,
        cpa_count,
        fixed_income_campaign,
    ):
        """
        Update only cpas for month case cpas 

        The results are sum
        """
        # This bookmaker pay only Revenue share
        betenlace_cpa.fixed_income += 0
        betenlace_cpa.cpa_count += cpa_count
        return betenlace_cpa

    def betenlace_daily_update_cpa(
        self,
        betenlace_daily,
        fixed_income_campaign,
        cpa_count,
        fx_partner,
    ):
        """
        Update Member daily report data from row data like
        - deposit
        - first_deposit_at
        - revenue_share
        - registered_count
        - first_deposit_count
        - wagering_count

        revenue_share calculated from net_revenue
        """

        fixed_income = cpa_count * fixed_income_campaign

        # This bookmaker pay only Revenue share
        if (betenlace_daily.fixed_income is None):
            betenlace_daily.fixed_income = 0

        betenlace_daily.fixed_income += 0
        betenlace_daily.fixed_income_unitary = (
            fixed_income / cpa_count
            if cpa_count != 0
            else
            fixed_income_campaign
        )

        betenlace_daily.fx_partner = fx_partner

        if (betenlace_daily.cpa_count is None):
            betenlace_daily.cpa_count = 0
        betenlace_daily.cpa_count += cpa_count

        return betenlace_daily

    def betenlace_daily_create_cpa(
        self,
        betenlace_cpa,
        from_date,
        fixed_income_campaign,
        cpa_count,
        currency_condition,
        currency_fixed_income,
        fx_partner,
    ):
        """
        """
        betenlace_daily = BetenlaceDailyReport(
            betenlace_cpa=betenlace_cpa,

            currency_condition=currency_condition,
            currency_fixed_income=currency_fixed_income,

            # This bookmaker pay only Revenue share
            fixed_income=0,
            fixed_income_unitary=0,

            fx_partner=fx_partner,

            cpa_count=cpa_count,

            created_at=from_date,
        )

        return betenlace_daily

    def partner_link_month_update_cpa(
        self,
        partner_link_accumulated,
        cpa_count,
        fixed_income_partner,
        fixed_income_partner_local,
    ):
        """
        Update Member Current Month report for Partner data with respective
        - cpa_count
        - fixed_income_partner
        - fixed_income_partner_local
        """
        partner_link_accumulated.cpa_count += cpa_count
        partner_link_accumulated.fixed_income += fixed_income_partner
        partner_link_accumulated.fixed_income_local += fixed_income_partner_local

        return partner_link_accumulated

    def partner_link_daily_update_cpa(
        self,
        cpa_count,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner_unitary_local,
        partner_link_daily,
        partner,
        betenlace_daily,
    ):
        """
        Update Member Daily report for Partner data with respective
        - fixed_income
        - fixed_income_unitary
        - fx_book_local
        - fx_percentage
        - fixed_income_unitary_local
        - cpa_count
        """
        partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary

        partner_link_daily.fx_book_local = fx_fixed_income_partner
        partner_link_daily.fx_book_net_revenue_local = fx_condition_partner
        partner_link_daily.fx_percentage = fx_partner_percentage

        partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

        if (partner_link_daily.cpa_count is None):
            partner_link_daily.cpa_count = 0

        partner_link_daily.cpa_count += cpa_count

        partner_link_daily.fixed_income = partner_link_daily.cpa_count * fixed_income_partner_unitary
        partner_link_daily.fixed_income_local = (
            partner_link_daily.cpa_count * fixed_income_partner_unitary_local
        )

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

    def partner_link_daily_create_cpa(
        self,
        from_date,
        campaign,
        betenlace_daily,
        partner_link_accumulated,
        cpa_count,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner_unitary_local,
        partner,
    ):
        """
        Create Member Daily report for Partner data with respective
        - fixed_income
        - fixed_income_unitary
        - fx_book_local
        - fx_percentage
        - fixed_income_unitary_local
        - cpa_count

        ### Relations data
        - betenlace_daily
        - partner_link_accumulated

        ### Currencies
        - currency_fixed_income
        - currency_local
        - tracker
        - created_at
        """
        fixed_income_partner = cpa_count * fixed_income_partner_unitary
        fixed_income_partner_local = cpa_count * fixed_income_partner_unitary_local

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
            tracker=partner_link_accumulated.tracker,

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
