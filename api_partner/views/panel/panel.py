import numpy as np
import pandas as pd
import pytz
from api_partner.helpers import (
    IsActive,
    IsFullRegister,
    IsNotBanned,
    IsBasicInfoValid,
    fx_conversion_usd_partner_daily_cases,
)
from api_partner.models import PartnerLinkDailyReport
from cerberus import Validator
from core.helpers import CurrencyAll
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.db import models
from django.db.models import (
    Case,
    Q,
    Sum,
)
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class PanelPartnerAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
    )

    def get(self, request):
        """
        Send data about resume of last 30 days and last month of ALL campaings
        of partner of current session

        ### Parameters
        No params

        ### Return data
        #### Case with data
        - data: list of values of grouped records
        - currency_local: currency of partner of current session
        #### Case without data
        - data: empty list
        - currency_local: currency of partner of current session
        """
        validator = Validator(
            schema={},
        )

        if not validator.validate(
            document=request.query_params,
        ):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get partner of current session
        user = request.user

        # Today with Settings timezone
        today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE)).date()

        month_before = today + relativedelta(months=-1) + relativedelta(day=1)
        last_30_days = today + relativedelta(days=-30)

        # Get all data from the most earliest date
        earliest_date = month_before if month_before < last_30_days else last_30_days

        filters = (
            Q(created_at__gte=earliest_date),
            Q(partner_link_accumulated__partner=user.partner),
        )

        # Define values to group
        values = [
            "currency_local",
            "created_at",
        ]

        # Get fx conversion cases to usd
        fx_conversion_cases = fx_conversion_usd_partner_daily_cases(
            Sum,
            include_stake=False,
        )

        annotate = {
            **fx_conversion_cases,
            "fixed_income_local": Sum("fixed_income_local"),
            "click_count": Sum("betenlace_daily_report__click_count"),
            "registered_count": Sum("registered_count"),
            "cpa_count": Sum("cpa_count"),
            "first_deposit_count": Sum("first_deposit_count"),
        }

        # Get data from DB with aggregation to groub by
        partner_daily_values = PartnerLinkDailyReport.objects.select_related(
            "betenlace_daily_report",
            "betenlace_daily_report__fx_partner",
        ).filter(
            *filters,
        ).values(
            *values,
        ).annotate(
            **annotate,
        )

        if not partner_daily_values:
            return Response(
                data={
                    "data": (),
                },
                status=status.HTTP_200_OK,
            )

        # Use pandas for fix group by for currency condition conversion
        partner_daily_values_df = pd.DataFrame(
            data=partner_daily_values,
            columns=(
                "deposit_usd",
                "fixed_income_local",
                "click_count",
                "registered_count",
                "cpa_count",
                "first_deposit_count",
                "currency_local",
                "created_at",
            )
        )

        partner_daily_values_df = partner_daily_values_df.groupby(
            by=values,
            as_index=False,
            dropna=True,
        ).agg(
            {
                "deposit_usd": np.sum,
                "click_count": np.sum,
                "registered_count": np.sum,
                "first_deposit_count": np.sum,
                "cpa_count": np.sum,
                "fixed_income_local": np.sum,
            }
        )
        return Response(
            data={
                "data": partner_daily_values_df.to_dict("records"),
                "currency_local": CurrencyAll.USD,
            },
            status=status.HTTP_200_OK,
        )
