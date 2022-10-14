import json
import logging
import sys
import traceback

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
            default="galera.bet br",
            choices=("galera.bet br",),
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
            choices=["False", "True"],
            default="True",
            help="Full update or not the month accumulated data",
        )

    def handle(self, *args, **options):
        logger.info(
            "Making call to API Galera\n"
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
            from_date = make_aware(datetime.strptime(from_date_str, "%Y-%m-%d"))
            to_date = make_aware(datetime.strptime(to_date_str, "%Y-%m-%d"))
            if from_date > to_date:
                logger.error(f"from_date=\"{from_date}\" is greather than to_date=\"{to_date}\"")
                return
        except:
            logger.error("from_date or to_date have bad format. Expected format\"AAAA-mm-dd\"")
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

        if (campaign_title == "galera.bet br"):
            galera_password = settings.API_GALERABET_BR_PASSWORD
            galera_username = settings.API_GALERABET_BR_USERNAME
            revenue_share_percentage = settings.API_GALERABET_BR_RS_PERCENTAGE

        # Login into galera
        try:
            url = "https://glraff.com/global/api/User/signIn"
            body = {
                "password": galera_password,
                "username": galera_username,
            }
            response_login = requests.post(
                url=url,
                json=body,
            )
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            logger.error(
                f"Something is wrong at login into Galera\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"response dict: {response_login.text}\n\n"
                f"if problem persist check traceback:\n\n{''.join(e)}"
            )
            return

        if response_login.status_code != 200:
            logger.error(
                "Not status 200 when try to login into Galera, check credentials\n"
                f"Response text: {response_login.text}"
            )
            return

        # Get cookie auth
        try:
            set_cookie = response_login.headers.get("set-cookie")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                etype=exc_type,
                value=exc_value,
                tb=exc_traceback,
            )
            logger.error(
                "Something is wrong at get data from API, check if current username, password and status of server\n"
                f"request url: {url}\n"
                f"request body: {body}\n"
                f"response status:\n{response_login.status_code}\n"
                f"response text:\n{response_login.text}\n\n"
                f"if problem still check traceback:\n{''.join(e)}"
            )
            return

        # Get data from specific date
        url = "https://glraff.com/global/api/Statistics/getPlayersLinksStatistics"
        headers = {
            "Cookie": set_cookie,
        }
        body = {
            "filter": {
                "date": {
                    "action": "between",
                    "from": from_date_str,
                    "to": to_date_str,
                    "valueLabel": f"{from_date_str} - {to_date_str}",
                },
            },
            # Limit -1 means ALL data
            "limit": -1,
            "start": 0,
        }

        try:
            response_obj = requests.post(
                url=url,
                json=body,
                headers=headers,
            )
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
                f"response dict: {response_obj.text}\n\n"
                f"if problem persist check traceback:\n\n{''.join(e)}"
            )
            return

        if(response_obj.status_code != 200):
            logger.error(
                f"request not success with code: {response_obj.status_code}, message: {response_obj.text}"
            )
            return

        response_data = json.loads(response_obj.text)

        if(response_data.get("result") == "ex"):
            logger.error(
                f"request result exception. \n\n {response_obj.text}"
            )
            return

        if(options.get("file_raw")):
            with open(f"{options.get('file_raw')}.json", "w") as out:
                json.dump(response_data.get("result").get("records"), out)
            return

        #  ['affiliateId' 'name' 'linkId' 'createDate' 'marketingSourceName'
        #  'website' 'marketingSourceId' 'clickLink' 'signUp' 'ratio' 'playersCount'
        #  'deposits' 'turnover' 'profitness' 'commissions' 'grossRevenue'
        #  'netRevenue' 'NDDACC' 'NDACC']

        cols_to_use = [
            "name",
            # "signUp",
            # "NDACC",
            "deposits",
            "turnover",
            "netRevenue",
        ]
        df = pd.DataFrame(data=response_data.get("result").get("records"))
        df = df[cols_to_use]
        df = df.astype(
            {
                "name": "string",
                # "signUp": np.uint32,
                # "NDACC": np.uint32,
                "deposits": np.float32,
                "turnover": np.float32,
                "netRevenue": np.float32,
            },
            copy=False,
        )
        df.rename(
            inplace=True,
            columns={
                "name": "prom_code",
                # "signUp": "registered_count",
                # "NDACC": "first_deposit_count",
                "deposits": "deposit",
                "turnover": "stake",
                "netRevenue": "net_revenue",
            },
        )

        if(options.get("file")):
            # File case save to disk and prevent execute on DB
            df.to_csv(
                path_or_buf=options.get("file"),
                index=False,
                encoding="utf-8",
            )
            return

        # Remove data with ALL zeros
        df.drop(
            labels=df[
                df.eval(
                    # "(registered_count == 0) & "
                    # "(first_deposit_count == 0) & "
                    "(deposit == 0) & "
                    "(stake == 0) & "
                    "(net_revenue == 0)",
                    engine="numexpr",
                )
            ].index,
            inplace=True,
        )

        # Check if dataframe was empty
        if (df.empty):
            logger.warning(
                f"Data not found at requested url from_date \"{from_date_str}\" to_date \"{to_date_str}\"\n"
                f"Request url: {url}\n\n"
            )
            return

        if(from_date != to_date):
            logger.error("Date from and to are not equal this data cannot be used for update on DB")
            return

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
        member_reports_betenlace_month_update = []  # BetenlaceCpa
        member_reports_daily_betenlace_update = []  # Betenlacedailyreport update
        member_reports_daily_betenlace_create = []  # Betenlacedailyreport create

        member_reports_daily_partner_update = []  # Partner_link_daily_report update
        member_reports_daily_partner_create = []  # Partner_link_daily_report create

        keys = {key: index for index, key in enumerate(df.columns.values)}

        for row in tqdm(zip(*df.to_dict('list').values())):
            """
            - prom_code
            - registered_count
            - first_deposit_count
            - deposit
            - stake
            - net_revenue
            """
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

            # Betenlace Daily -  Betenlace Month
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
                betenlace_daily, betenlace_cpa = self.betenlace_daily_update(
                    keys=keys,
                    row=row,
                    betenlace_daily=betenlace_daily,
                    fx_partner=fx_partner,
                    betenlace_cpa=betenlace_cpa,
                    revenue_share_percentage=revenue_share_percentage,
                    update_month=update_month,
                )
                member_reports_daily_betenlace_update.append(betenlace_daily)
                member_reports_betenlace_month_update.append(betenlace_cpa)
            else:
                betenlace_daily, betenlace_cpa = self.betenlace_daily_create(
                    from_date=from_date.date(),
                    keys=keys,
                    row=row,
                    betenlace_cpa=betenlace_cpa,
                    campaign=campaign,
                    fx_partner=fx_partner,
                    revenue_share_percentage=revenue_share_percentage,
                    update_month=update_month,
                )
                member_reports_daily_betenlace_create.append(betenlace_daily)
                member_reports_betenlace_month_update.append(betenlace_cpa)

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

            tracked_data = self.get_tracker_values(
                keys=keys,
                row=row,
                partner_link_accumulated=partner_link_accumulated,
            )

            # Get currency local from partner link accumulated
            partner_currency_str = partner_link_accumulated.currency_local.lower()

            # Fx Currency Fixed income
            fx_fixed_income_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_fixed_income_str,
                partner_currency_str=partner_currency_str,
            )

            # Calculate fixed income for partner
            fixed_income_partner_unitary = campaign.fixed_income_unitary * partner_link_accumulated.percentage_cpa
            fixed_income_partner = 0
            fixed_income_partner_unitary_local = (
                fixed_income_partner_unitary *
                fx_fixed_income_partner
            )
            fixed_income_partner_local = 0

            # Fx Currency Condition
            fx_condition_partner = self.calc_fx(
                fx_partner=fx_partner,
                fx_partner_percentage=fx_partner_percentage,
                currency_from_str=currency_condition_str,
                partner_currency_str=partner_currency_str,
            )

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
                partner_link_daily = self.partner_link_daily_update(
                    tracked_data=tracked_data,
                    fx_fixed_income_partner=fx_fixed_income_partner,
                    fx_condition_partner=fx_condition_partner,
                    fx_partner_percentage=fx_partner_percentage,
                    fixed_income_partner_unitary=fixed_income_partner_unitary,
                    fixed_income_partner_unitary_local=fixed_income_partner_unitary_local,
                    partner_link_daily=partner_link_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    betenlace_daily=betenlace_daily,
                    partner=partner_link_accumulated.partner,
                )
                member_reports_daily_partner_update.append(partner_link_daily)
            else:
                partner_link_daily = self.partner_link_daily_create(
                    from_date=from_date.date(),
                    campaign=campaign,
                    betenlace_daily=betenlace_daily,
                    partner_link_accumulated=partner_link_accumulated,
                    cpa_count=0,
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
                        "stake",

                        "net_revenue",
                        "revenue_share",

                        # "registered_count",
                        # "first_deposit_count",
                    ),
                )

            if(member_reports_daily_betenlace_update):
                BetenlaceDailyReport.objects.bulk_update(
                    objs=member_reports_daily_betenlace_update,
                    fields=(
                        "deposit",
                        "stake",

                        "net_revenue",
                        "revenue_share",

                        "fixed_income_unitary",

                        "fx_partner",

                        # "registered_count",
                        # "first_deposit_count",
                    ),
                )

            if(member_reports_daily_betenlace_create):
                BetenlaceDailyReport.objects.bulk_create(objs=member_reports_daily_betenlace_create)

            if(member_reports_daily_partner_update):
                PartnerLinkDailyReport.objects.bulk_update(
                    objs=member_reports_daily_partner_update,
                    fields=(
                        # "fixed_income",
                        # "fixed_income_unitary",

                        # "fx_book_local",
                        "fx_book_net_revenue_local",
                        "fx_percentage",

                        # "fixed_income_local",
                        # "fixed_income_unitary_local",
                        # "cpa_count",
                        # "percentage_cpa",
                        # "tracker",
                        "tracker_deposit",
                        # "tracker_registered_count",
                        # "tracker_first_deposit_count",
                        # "tracker_wagering_count",
                        "deposit",
                        # "registered_count",
                        # "first_deposit_count",
                        # "wagering_count",

                        "adviser_id",

                        # "fixed_income_adviser",
                        # "fixed_income_adviser_local",

                        "net_revenue_adviser",
                        "net_revenue_adviser_local",

                        "fixed_income_adviser_percentage",
                        "net_revenue_adviser_percentage",

                        "referred_by",
                        # "fixed_income_referred",
                        # "fixed_income_referred_local",
                        "net_revenue_referred",
                        "net_revenue_referred_local",
                        "fixed_income_referred_percentage",
                        "net_revenue_referred_percentage",
                    ),
                )

            if(member_reports_daily_partner_create):
                PartnerLinkDailyReport.objects.bulk_create(objs=member_reports_daily_partner_create)

    def get_tracker_values(
        self,
        keys,
        row,
        partner_link_accumulated,
    ):
        tracked_data = {}
        if (keys.get("deposit") is not None):
            tracked_data["deposit"] = row[keys.get("deposit")]*partner_link_accumulated.tracker_deposit

        # if (keys.get("registered_count") is not None):
        #     if(row[keys.get("registered_count")] > 1):
        #         tracked_data["registered_count"] = math.floor(
        #             row[keys.get("registered_count")]*partner_link_accumulated.tracker_registered_count
        #         )
        #     else:
        #         tracked_data["registered_count"] = row[keys.get("registered_count")]

        # if (keys.get("first_deposit_count") is not None):
        #     if(row[keys.get("first_deposit_count")] > 1):
        #         tracked_data["first_deposit_count"] = math.floor(
        #             row[keys.get("first_deposit_count")]*partner_link_accumulated.tracker_first_deposit_count
        #         )
        #     else:
        #         tracked_data["first_deposit_count"] = row[keys.get("first_deposit_count")]

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

    def betenlace_daily_update(
        self,
        keys,
        row,
        betenlace_daily,
        fx_partner,
        betenlace_cpa,
        revenue_share_percentage,
        update_month,
    ):

        revenue_share_calculate = row[keys.get('net_revenue')] * revenue_share_percentage

        if(update_month):
            # get difference from current daily to update previous to update
            difference_deposit = row[keys.get('deposit')] - (betenlace_daily.deposit or 0)
            difference_stake = row[keys.get('stake')] - (betenlace_daily.stake or 0)
            difference_net_revenue = row[keys.get('net_revenue')] - (betenlace_daily.net_revenue or 0)
            difference_revenue_share = revenue_share_calculate - (betenlace_daily.revenue_share or 0)
            # difference_registered_count = row[keys.get('registered_count')] - (betenlace_daily.registered_count or 0)
            # difference_first_deposit_count = (
            #     row[keys.get('first_deposit_count')] - (betenlace_daily.first_deposit_count or 0)
            # )

            # Betenlace Month update
            betenlace_cpa.deposit += difference_deposit
            betenlace_cpa.stake += difference_stake
            betenlace_cpa.net_revenue += difference_net_revenue
            betenlace_cpa.revenue_share += difference_revenue_share
            # betenlace_cpa.registered_count += difference_registered_count
            # betenlace_cpa.first_deposit_count += difference_first_deposit_count

        # Betenlace daily update
        betenlace_daily.deposit = row[keys.get('deposit')]
        betenlace_daily.stake = row[keys.get('stake')]
        betenlace_daily.net_revenue = row[keys.get('net_revenue')]
        # Revenue share is calculated from net revenue according to current revenue share value
        betenlace_daily.revenue_share = (betenlace_daily.net_revenue*revenue_share_percentage)

        betenlace_daily.fx_partner = fx_partner

        # betenlace_daily.registered_count = row[keys.get('registered_count')]
        # betenlace_daily.first_deposit_count = row[keys.get('first_deposit_count')]
        return betenlace_daily, betenlace_cpa

    def betenlace_daily_create(
        self,
        from_date,
        keys,
        row,
        betenlace_cpa,
        campaign,
        fx_partner,
        revenue_share_percentage,
        update_month,
    ):

        if(update_month):
            # Betenlace month update
            betenlace_cpa.deposit += row[keys.get('deposit')]
            betenlace_cpa.stake += row[keys.get('stake')]
            betenlace_cpa.net_revenue += row[keys.get('net_revenue')]
            betenlace_cpa.revenue_share += row[keys.get('net_revenue')]*revenue_share_percentage
            # betenlace_cpa.registered_count += row[keys.get('registered_count')]
            # betenlace_cpa.first_deposit_count += row[keys.get('first_deposit_count')]

        # Betenlace Daily
        betenlace_daily = BetenlaceDailyReport(
            betenlace_cpa=betenlace_cpa,

            currency_condition=campaign.currency_condition,
            deposit=row[keys.get('deposit')],
            stake=row[keys.get('stake')],

            net_revenue=row[keys.get('net_revenue')],
            # Revenue share is calculated based on net_revenue
            revenue_share=row[keys.get('net_revenue')]*revenue_share_percentage,

            currency_fixed_income=campaign.currency_fixed_income,
            fixed_income=0,
            fixed_income_unitary=campaign.fixed_income_unitary,

            fx_partner=fx_partner,

            cpa_count=0,
            # registered_count=row[keys.get('registered_count')],
            # first_deposit_count=row[keys.get('first_deposit_count')],
            created_at=from_date,
        )

        return betenlace_daily, betenlace_cpa

    def partner_link_daily_update(
        self,
        tracked_data,
        fx_fixed_income_partner,
        fx_condition_partner,
        fx_partner_percentage,
        fixed_income_partner_unitary,
        fixed_income_partner_unitary_local,
        partner_link_daily,
        partner_link_accumulated,
        betenlace_daily,
        partner,
    ):

        partner_link_daily.fx_book_local = fx_fixed_income_partner
        partner_link_daily.fx_book_net_revenue_local = fx_condition_partner
        partner_link_daily.fx_percentage = fx_partner_percentage

        partner_link_daily.fixed_income_unitary = fixed_income_partner_unitary
        partner_link_daily.fixed_income_unitary_local = fixed_income_partner_unitary_local

        # partner_link_daily.percentage_cpa = partner_link_accumulated.percentage_cpa

        # partner_link_daily.tracker = partner_link_accumulated.tracker
        partner_link_daily.tracker_deposit = partner_link_accumulated.tracker_deposit
        # partner_link_daily.tracker_registered_count = partner_link_accumulated.tracker_registered_count
        # partner_link_daily.tracker_first_deposit_count = partner_link_accumulated.tracker_first_deposit_count
        # partner_link_daily.tracker_wagering_count = partner_link_accumulated.tracker_wagering_count

        partner_link_daily.deposit = tracked_data.get("deposit")
        # partner_link_daily.registered_count = tracked_data.get("registered_count")
        # partner_link_daily.first_deposit_count = tracked_data.get("first_deposit_count")
        # partner_link_daily.wagering_count = tracked_data.get("wagering_count")

        # Calculate Adviser payment
        partner_link_daily.adviser_id = partner.adviser_id
        partner_link_daily.fixed_income_adviser_percentage = partner.fixed_income_adviser_percentage
        partner_link_daily.net_revenue_adviser_percentage = partner.net_revenue_adviser_percentage

        # Update fixed income with current cpa value
        if (partner_link_daily.cpa_count is not None):
            partner_link_daily.fixed_income = partner_link_daily.fixed_income_unitary * partner_link_daily.cpa_count
            partner_link_daily.fixed_income_local = partner_link_daily.fixed_income_unitary_local * partner_link_daily.cpa_count

            # Adviser case
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

        # Update fixed income with current cpa value
        if (partner_link_daily.cpa_count is not None):
            partner_link_daily.fixed_income = partner_link_daily.fixed_income_unitary * partner_link_daily.cpa_count
            partner_link_daily.fixed_income_local = partner_link_daily.fixed_income_unitary_local * partner_link_daily.cpa_count

            # referred case
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
            # registered_count=tracked_data.get("registered_count"),
            # first_deposit_count=tracked_data.get("first_deposit_count"),
            # wagering_count=tracked_data.get("wagering_count"),

            tracker=partner_link_accumulated.tracker,
            tracker_deposit=partner_link_accumulated.tracker_deposit,
            # tracker_registered_count=partner_link_accumulated.tracker_registered_count,
            # tracker_first_deposit_count=partner_link_accumulated.tracker_first_deposit_count,
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
