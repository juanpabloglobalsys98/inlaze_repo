import logging

import numpy as np
import pandas as pd
from api_partner.helpers import (
    GetAllmemberReport,
    IsActive,
    IsFullRegister,
    IsNotBanned,
    IsTerms,
    IsBasicInfoValid,
    fx_conversion_usd_partner_daily_cases,
)
from api_partner.models import PartnerLinkDailyReport
from api_partner.serializers import (
    MemberReportConsolidatedSer,
    MemberReportGroupedSer,
    MemberReportSer,
)
from cerberus import Validator
from core.helpers import (
    CountryCampaign,
    to_bool,
)
from django.db import models
from django.db.models import (
    Case,
    F,
    Q,
    Sum,
    Value,
)
from django.db.models.functions import Concat
from django.utils.timezone import datetime
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class MemberReportFromPartnerAPI(APIView, GetAllmemberReport):
    """
        Returning member report from a logged user
    """
    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsBasicInfoValid,
        IsTerms
    )

    def get(self, request):
        """
            Return partner's member report

            #Body

           -  since_date : "str"
                Param to define since date to return date
           -  until_date : "str"
                Param to define until date to return date
           -  campaign : "str"
                Param to define campaign to return date
           -  country_campaign : "str"
                Param to define country campaign to return date
           -  group_by_campaign : "str"
                Param to define group_by_campaign to group data
           -  group_by_month : "str"
                Param to define group_by_month to group data
           -  lim : "int"
           -  offs : "int"

        """

        validator = Validator(
            schema={
                "since_date": {
                    "required": False,
                    "type": "string",
                },
                "until_date": {
                    "required": False,
                    "type": "string",
                },
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                'country_campaign': {
                    'required': False,
                    'type': 'string',
                    'allowed': CountryCampaign.values,
                },
                "group_by_campaign": {
                    "required": False,
                    "type": "boolean",
                    "default": "False",
                    "coerce": to_bool,
                },
                "group_by_month": {
                    "required": False,
                    "type": "boolean",
                    "default": "False",
                    "coerce": to_bool,
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "default": "created_at",
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
            },
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

        # Get user of current session
        user = request.user

        # Force filters only if user have related the reports
        filters = [
            Q(partner_link_accumulated__partner__user=user),
        ]

        # Add extra filters
        if 'since_date' in validator.document:
            filters.append(
                Q(created_at__gte=datetime.strptime(validator.document.get("since_date"), "%Y-%m-%d")),
            )

        if 'until_date' in validator.document:
            filters.append(
                Q(created_at__lte=datetime.strptime(validator.document.get("until_date"), "%Y-%m-%d")),
            )

        if 'campaign' in validator.document:
            filters.append(
                Q(campaign_title__exact=validator.document.get("campaign")),
            )

        if 'country_campaign' in validator.document:
            filters.append(
                Q(partner_link_accumulated__campaign__countries__icontains=validator.document.get("country_campaign")),
            )

        order_by = validator.document.get("sort_by")

        values = [
            'currency_local',
        ]

        # Case for group by
        if (validator.document.get('group_by_month') or validator.document.get('group_by_campaign')):
            # Setup conversion cases according to possible currency conditions
            # values of bookmakers defined on CurrencyCondition enumerator
            fx_conversion_cases = fx_conversion_usd_partner_daily_cases(
                model_func=Sum,
                include_stake=False,
            )
            annotate_filters = {
                **fx_conversion_cases,
                "fixed_income_local": Sum("fixed_income_local"),
                "click_count": Sum("betenlace_daily_report__click_count"),
                "cpa_count": Sum("cpa_count"),
                "first_deposit_count": Sum("first_deposit_count"),
                "registered_count": Sum("registered_count"),
                "wagering_count": Sum("wagering_count"),
            }

            # Define values for pandas dataframe
            # Define columns to use
            cols_to_use = [
                "deposit_usd",
                "fixed_income_local",
                "click_count",
                "cpa_count",
                "currency_local",
                "first_deposit_count",
                "registered_count",
                "wagering_count",
            ]
            # Define aggreations for group by
            agg_pd = {
                "deposit_usd": np.sum,
                "click_count": np.sum,
                "cpa_count": np.sum,
                "first_deposit_count": np.sum,
                "fixed_income_local": np.sum,
                "registered_count": np.sum,
                "wagering_count": np.sum,
            }
            # Define astype converter
            astype_df = {
                "deposit_usd": np.float32,
                "click_count": np.uint32,
                "cpa_count": np.int32,
                "currency_local": "string",
                "first_deposit_count": np.int32,
                "fixed_income_local": np.int32,
                "registered_count": np.int32,
                "wagering_count": np.int32,
            }

            if (validator.document.get('group_by_month')):
                # Add to list of values to group by month and year
                values.append("created_at__month")
                values.append("created_at__year")

                # Add values for load data with pandas
                cols_to_use.append("created_at__month")
                cols_to_use.append("created_at__year")

                # Add astype for pandas
                astype_df["created_at__month"] = np.int32
                astype_df["created_at__year"] = np.int32

            if (validator.document.get('group_by_campaign')):
                # Add to list of values to group by campaign title
                values.append('campaign_title')

                # Add values for load data with pandas
                cols_to_use.append("campaign_title")

                # Add astype for pandas
                astype_df["campaign_title"] = "string"

            # Get data form Database
            partner_link_daily = PartnerLinkDailyReport.objects.select_related(
                "betenlace_daily_report",
                "betenlace_daily_report__fx_partner",
            ).annotate(
                campaign_title=(
                    Concat(
                        "partner_link_accumulated__campaign__bookmaker__name",
                        Value(" "),
                        "partner_link_accumulated__campaign__title",
                    )
                    # Only if group by campaign concatenate
                    if validator.document.get("group_by_campaign")
                    # in another case set empty string
                    else
                    Value("")
                ),
            ).values(
                *values,
            ).filter(
                *filters,
            ).annotate(
                **annotate_filters,
            )

            # Use pandas for fix group by for currency condition conversion
            partner_link_daily_df = pd.DataFrame(
                data=partner_link_daily,
                columns=cols_to_use,
            )
            # Fill nan
            partner_link_daily_df.currency_local.fillna('', inplace=True)
            partner_link_daily_df.fillna(0, inplace=True)

            # Convert to expected types
            partner_link_daily_df = partner_link_daily_df.astype(astype_df)

            # Calculate sort by
            if ("month" == order_by or "-month" == order_by):
                if "-" == order_by[0]:
                    df_ascending = [
                        False,
                        False,
                    ]
                else:
                    df_ascending = [
                        True,
                        True,
                    ]
                df_order_by = [
                    "created_at__year",
                    "created_at__month",
                ]

            else:
                if "-" == order_by[0]:
                    df_ascending = [
                        False,
                    ]
                    df_order_by = [
                        order_by[1:],
                    ]
                else:
                    df_ascending = [
                        True,
                    ]
                    df_order_by = [
                        order_by,
                    ]

            # Group by with pandas dataframe
            partner_link_daily_df = partner_link_daily_df.groupby(
                by=values,
                as_index=False,
                dropna=True,
            ).agg(
                agg_pd,
            ).sort_values(
                # if minus is on first element of order_by str set string without
                # that minus
                by=df_order_by,
                # if minus is on first element of order_by str order descending
                ascending=df_ascending,
                kind="stable",
            )

            # Paginate data with list of dictionaries from grouped dataframe
            member_pag = self.paginate_queryset(
                queryset=partner_link_daily_df.to_dict("records"),
                request=request,
                view=self
            )

            member_ser = MemberReportGroupedSer(
                instance=member_pag,
                many=True,
            )

            return Response(
                data={
                    "data": member_ser.data,
                },
                headers={
                    "count": self.count,
                    "access-control-expose-headers": "count,next,previous",
                },
                status=status.HTTP_200_OK,
            )
        # Case not group by
        else:
            # Setup conversion cases according to possible currency conditions
            # values of bookmakers defined on CurrencyCondition enumerator
            fx_conversion_cases = fx_conversion_usd_partner_daily_cases(
                include_stake=False,
                model_func=F,
            )

            partner_link_daily = PartnerLinkDailyReport.objects.using("default").select_related(
                "betenlace_daily_report",
                "betenlace_daily_report__fx_partner",
            ).annotate(
                campaign_title=Concat(
                    'partner_link_accumulated__campaign__bookmaker__name',
                    Value(' '),
                    'partner_link_accumulated__campaign__title'
                ),
                **fx_conversion_cases,
                click_count=F("betenlace_daily_report__click_count"),
            ).filter(
                *filters,
            ).order_by(
                F(order_by[1:]).desc(nulls_last=True)
                if "-" == order_by[0]
                else
                F(order_by).asc(nulls_first=True),
            )

            member_pag = self.paginate_queryset(
                queryset=partner_link_daily,
                request=request,
                view=self,
            )

            member_ser = MemberReportSer(
                instance=member_pag,
                many=True,
            )

            return Response(
                data={
                    "data": member_ser.data,
                },
                headers={
                    "count": self.count,
                    "access-control-expose-headers": "count,next,previous",
                },
                status=status.HTTP_200_OK,
            )


class MemberReportConsolidateAPI(APIView):
    """ 
        Returning member report consolidate from a logged user 
    """
    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsBasicInfoValid,
        IsTerms,
    )

    def get(self, request):
        """ 
            Returning member report consolidate data 

            #Params

           -  since_date : "str"
                Param to define since date to return records
           -  until_date : "str"
                Param to define until date to return records
           -  campaign : "str"
                Param to define campaign to return records 
           -  country_campaign : "str"
                Param to define country campaign to return records
        """
        validator = Validator(
            schema={
                "since_date": {
                    "required": False,
                    "type": "string",
                },
                "until_date": {
                    "required": False,
                    "type": "string",
                },
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "country_campaign": {
                    "required": False,
                    "type": "string",
                    "allowed": CountryCampaign.values,
                },
            },
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

        user = request.user

        filters = [
            Q(partner_link_accumulated__partner__user=user),
        ]

        if ('since_date' in validator.document):
            filters.append(
                Q(created_at__gte=datetime.strptime(validator.document.get("since_date"), "%Y-%m-%d")),
            )

        if ('until_date' in validator.document):
            filters.append(
                Q(created_at__lte=datetime.strptime(validator.document.get("until_date"), "%Y-%m-%d")),
            )

        if ('campaign' in validator.document):
            filters.append(
                Q(campaign_title__exact=validator.document.get("campaign")),
            )

        if 'country_campaign' in validator.document:
            filters.append(
                Q(partner_link_accumulated__campaign__countries__icontains=validator.document.get("country_campaign")),
            )

        # Setup conversion cases according to possible currency conditions
        # values of bookmakers defined on CurrencyCondition enumerator
        fx_conversion_cases = fx_conversion_usd_partner_daily_cases(
            include_stake=False,
            model_func=Sum,
        )

        anotate_filters = {
            **fx_conversion_cases,
            "click_count": Sum("betenlace_daily_report__click_count"),
            "first_deposit_count": Sum('first_deposit_count'),
            "registered_count": Sum('registered_count'),
            "cpa_count": Sum('cpa_count'),
            "fixed_income_local": Sum('fixed_income_local'),
            "wagering_count": Sum('wagering_count'),
        }

        partner = PartnerLinkDailyReport.objects.annotate(
            campaign_title=Concat(
                "partner_link_accumulated__campaign__bookmaker__name",
                Value(" "),
                "partner_link_accumulated__campaign__title",
            ),
        ).values(
            "currency_local",
        ).filter(
            *filters,
        ).annotate(
            **anotate_filters,
        )

        # Use pandas for fix group by
        partner_link_daily_df = pd.DataFrame(
            data=partner,
            columns=(
                "currency_local",
                "deposit_usd",
                "click_count",
                "first_deposit_count",
                "registered_count",
                "cpa_count",
                "fixed_income_local",
                "wagering_count",
            ),
        )

        # Group by again to force currency_condition agrupation in a correct
        # way
        partner_link_daily_df = partner_link_daily_df.groupby(
            by=[
                "currency_local",
            ],
            as_index=False,
        ).agg(
            {
                "deposit_usd": np.sum,
                "click_count": np.sum,
                "first_deposit_count": np.sum,
                "registered_count": np.sum,
                "cpa_count": np.sum,
                "fixed_income_local": np.sum,
                "wagering_count": np.sum,
            },
        )

        # Fill nan
        partner_link_daily_df.currency_local.fillna('', inplace=True)
        partner_link_daily_df.fillna(0, inplace=True)

        # Convert to expected types
        partner_link_daily_df = partner_link_daily_df.astype(
            {
                "deposit_usd": np.float32,
                "click_count": np.uint32,
                "cpa_count": np.int32,
                "currency_local": "string",
                "first_deposit_count": np.int32,
                "fixed_income_local": np.int32,
                "registered_count": np.int32,
                "wagering_count": np.int32,
            }
        )

        partner_link_daily_ser = MemberReportConsolidatedSer(
            instance=partner_link_daily_df.to_dict("records"),
            many=True,
        )

        return Response(
            data={
                "data": partner_link_daily_ser.data,
            },
            status=status.HTTP_200_OK,
        )
