import logging
import pandas as pd
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import (
    BetenlaceCPA,
    BetenlaceDailyReport,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import (
    Q,
    Value,
)
from django.db.models.aggregates import Sum
from django.utils import timezone
from django.utils.timezone import datetime
from tqdm import tqdm
from django.db.models.functions import Coalesce

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def handle(self, *args, **options):
        """
        """
        timenow = timezone.now()
        month_start = datetime(year=timenow.year, month=timenow.month, day=1)

        betenlace_cpa = BetenlaceCPA.objects.all()
        betenlace_cpa_update = []
        for betenlace_cpa_i in betenlace_cpa:
            filters = [Q(created_at__gte=month_start), Q(betenlace_cpa=betenlace_cpa_i)]
            betenlace_daily_reports = BetenlaceDailyReport.objects.filter(
                *filters).aggregate(
                deposit__sum=Coalesce(Sum("deposit"), 0.0),
                stake__sum=Coalesce(Sum("stake"), 0.0),
                fixed_income__sum=Coalesce(Sum("fixed_income"), 0.0),
                net_revenue__sum=Coalesce(Sum("net_revenue"), 0.0),
                revenue_share__sum=Coalesce(Sum("revenue_share"), 0.0),
                registered_count__sum=Coalesce(Sum("registered_count"), 0),
                cpa_count__sum=Coalesce(Sum("cpa_count"), 0),
                first_deposit_count__sum=Coalesce(Sum("first_deposit_count"), 0),
                wagering_count__sum=Coalesce(Sum("wagering_count"), 0))

            betenlace_cpa_i.deposit = betenlace_daily_reports.get("deposit__sum")
            betenlace_cpa_i.stake = betenlace_daily_reports.get("stake__sum")
            betenlace_cpa_i.fixed_income = betenlace_daily_reports.get("fixed_income__sum")
            betenlace_cpa_i.net_revenue = betenlace_daily_reports.get("net_revenue__sum")
            betenlace_cpa_i.revenue_share = betenlace_daily_reports.get("revenue_share__sum")
            betenlace_cpa_i.registered_count = betenlace_daily_reports.get("registered_count__sum")
            betenlace_cpa_i.cpa_count = betenlace_daily_reports.get("cpa_count__sum")
            betenlace_cpa_i.first_deposit_count = betenlace_daily_reports.get("first_deposit_count__sum")
            betenlace_cpa_i.wagering_count = betenlace_daily_reports.get("wagering_count__sum")

            betenlace_cpa_update.append(betenlace_cpa_i)

        partner_cpa = PartnerLinkAccumulated.objects.all()
        partner_cpa_update = []
        for partner_cpa_i in partner_cpa:
            filters = [Q(created_at__gte=month_start), Q(partner_link_accumulated=partner_cpa_i)]
            partner_daily_reports = PartnerLinkDailyReport.objects.filter(
                *filters).aggregate(
                fixed_income__sum=Coalesce(Sum("fixed_income"), 0.0),
                fixed_income_local__sum=Coalesce(Sum("fixed_income_local"), 0.0),
                cpa_count__sum=Coalesce(Sum("cpa_count"), 0),
            )

            partner_cpa_i.fixed_income = partner_daily_reports.get("fixed_income__sum")
            partner_cpa_i.fixed_income_local = partner_daily_reports.get("fixed_income_local__sum")
            partner_cpa_i.cpa_count = partner_daily_reports.get("cpa_count__sum")

            partner_cpa_update.append(partner_cpa_i)

        with transaction.atomic(using=DB_USER_PARTNER):
            if(betenlace_cpa_update):
                BetenlaceCPA.objects.bulk_update(betenlace_cpa_update, (
                    "deposit",
                    "stake",
                    "fixed_income",
                    "net_revenue",
                    "revenue_share",
                    "registered_count",
                    "cpa_count",
                    "first_deposit_count",
                    "wagering_count",
                ))

            if(partner_cpa_update):
                PartnerLinkAccumulated.objects.bulk_update(partner_cpa_update, (
                    "fixed_income",
                    "fixed_income_local",
                    "cpa_count",
                ))
