import numpy as np
import pandas as pd
from api_admin.helpers import (
    fx_conversion_specific_adviser_daily_cases,
    report_visualization_limit,
)
from api_admin.models import SearchPartnerLimit
from api_admin.paginators import GetAllMemberReportMultiFx
from api_admin.serializers import (
    MemberReportConsolidatedMultiFxSer,
    MembertReportGroupMultiFxSer,
    MemeberReportMultiFxSer,
)
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import BetenlaceDailyReport
from cerberus import Validator
from core.helpers import (
    CountryCampaign,
    CountryPartner,
    CurrencyPartner,
    HavePermissionBasedView,
    request_cfg,
)
from django.conf import settings
from django.db import models
from django.db.models import (
    Case,
    F,
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils.timezone import datetime
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

# Global var for member report limitation, from main member report
CODENAME_GEN = "member report api-get"
CODENAME = "member report multi fx api-get"


class MemberReportMultiFxAPI(APIView, GetAllMemberReportMultiFx):
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Returning member report data
        """
        # Force default DB routes to Partner
        request_cfg.is_partner = True

        validator = Validator(
            schema={
                "since_date": {
                    "required": True,
                    "type": "string",
                },
                "until_date": {
                    "required": True,
                    "type": "string",
                },
                "currency_convert": {
                    "required": True,
                    "type": "string",
                    "default": "orig",
                    "allowed": CurrencyPartner.values + ["orig"],
                },
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "partner": {
                    "required": False,
                    "type": "string",
                },
                "bookmaker": {
                    "required": False,
                    "type": "string",
                },
                "prom_code": {
                    "required": False,
                    "type": "string",
                },
                "country_campaign": {
                    "required": False,
                    "type": "string",
                    "allowed": CountryCampaign.values,
                },
                "country_partner": {
                    "required": False,
                    "type": "string",
                    "allowed": CountryPartner.values,
                },
                "group_by_campaign": {
                    "required": False,
                    "type": "string",
                },
                "group_by_month": {
                    "required": False,
                    "type": "string",
                },
                "group_by_prom_code": {
                    "required": False,
                    "type": "string",
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
                "sort_by": {
                    "required": True,
                    "type": "string",
                    "default": "-created_at",
                },
            },
        )

        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        admin = request.user

        filters = []
        if not admin.is_superuser:
            filters__search_partner_limit = (
                Q(rol=admin.rol),
                Q(codename=CODENAME_GEN),
            )
            # Limit admin search to a specifics partners
            searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters__search_partner_limit).first()
            if (
                not searchpartnerlimit or
                searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED
            ):
                filters.append(
                    Q(partnerlinkdailyreport__partner_link_accumulated__partner__adviser_id=admin.pk)
                )

        if "since_date" in validator.document:
            filters.append(
                Q(created_at__gte=datetime.strptime(validator.document.get("since_date"), "%Y-%m-%d"))
            )

        if "until_date" in validator.document:
            filters.append(
                Q(created_at__lte=datetime.strptime(validator.document.get("until_date"), "%Y-%m-%d"))
            )

        if "bookmaker" in validator.document:
            filters.append(Q(betenlace_cpa__link__campaign__bookmaker__id=validator.document.get("bookmaker")))

        if "campaign" in validator.document:
            filters.append(
                Q(campaign_title__icontains=validator.document.get("campaign"))
            )

        if "prom_code" in validator.document:
            filters.append(Q(betenlace_cpa__link__prom_code__iexact=validator.document.get("prom_code")))

        if "partner" in validator.document:
            filters.append(
                Q(
                    partnerlinkdailyreport__partner_link_accumulated__partner__user__id=validator.document.
                    get("partner")
                )
            )

        if "country_campaign" in validator.document:
            filters.append(
                Q(betenlace_cpa__link__campaign__countries__icontains=validator.document.get("country_campaign")))

        if "country_partner" in validator.document:
            filters.append(
                Q(
                    partnerlinkdailyreport__partner_link_accumulated__partner__additionalinfo__country=validator.document.
                    get("country_partner")
                )
            )

        # Order by var used on pandas
        order_by = validator.document.get("sort_by")

        # Grouped cases
        group_by_month = "group_by_month" in validator.document
        group_by_campaign = "group_by_campaign" in validator.document
        group_by_prom_code = "group_by_prom_code" in validator.document

        # Make report visualization fields if is_superuser so can be show all fields
        if admin.is_superuser:
            member_group = set(MembertReportGroupMultiFxSer._declared_fields.keys())
            member_filter = set(MemeberReportMultiFxSer.Meta.fields)
            report_visualization = list(member_filter.union(member_group))
        # show just fields that the user's role has assigned
        else:
            report_visualization = report_visualization_limit(
                admin=admin,
                permission_codename=CODENAME,
            )
            if (not report_visualization):
                return Response(
                    data={
                        "error": settings.FORBIDDEN_NOT_ALLOWED,
                        "details": {
                            "report_visualization": [
                                _("This user does not has permission to visualization"),
                            ],
                        },
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )

        if (group_by_month or group_by_campaign or group_by_prom_code):
            # Flag to determine if is a group by case at least by one of the next
            # cases:
            # - Month
            # - Campaign
            # - Prom Code

            # All values that will used into dataframe
            values, df_groupby, qset_annotates, df_astype = self._grouped_cases_df_create_vars()
            self._grouped_cases_df_recalculate_vars(
                group_by_month=group_by_month,
                group_by_campaign=group_by_campaign,
                group_by_prom_code=group_by_prom_code,
                values=values,
                df_groupby=df_groupby,
                annotates=qset_annotates,
                df_astype=df_astype,
                currency_convert=validator.document.get("currency_convert"),
            )

            # Calculate sort by
            if ("created_at" == order_by or "-created_at" == order_by):
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

            # If currency convert is different to orig, recalculate with cases
            if (validator.document.get("currency_convert") != "orig"):
                # Get cases for Fx conversion from fixed_incomes to
                # currency_convert for annotates
                fx_conversion_cases = fx_conversion_specific_adviser_daily_cases(
                    model_func=F,
                    currency_to=validator.document.get("currency_convert"),
                )
                qset_annotates |= {
                    **fx_conversion_cases
                }
            # Case orig currency
            else:
                qset_annotates |= {
                    "deposit_fxc": F("deposit"),
                    "deposit_partner_fxc": F("partnerlinkdailyreport__deposit"),
                    "stake_fxc": F("stake"),
                    "net_revenue_fxc": F("net_revenue"),
                    "revenue_share_fxc": F("revenue_share"),
                    "fixed_income_fxc": F("fixed_income"),
                    "fixed_income_unitary_fxc": F("fixed_income_unitary"),
                    "fixed_income_partner_fxc": F("partnerlinkdailyreport__fixed_income"),
                    "fixed_income_partner_unitary_fxc": F("partnerlinkdailyreport__fixed_income_unitary"),
                    "fixed_income_adviser_fxc": F("partnerlinkdailyreport__fixed_income_adviser"),
                    "net_revenue_adviser_fxc": F("partnerlinkdailyreport__net_revenue_adviser"),
                    "fixed_income_referred_fxc": F("partnerlinkdailyreport__fixed_income_referred"),
                    "net_revenue_referred_fxc": F("partnerlinkdailyreport__net_revenue_referred"),
                }

            bet_daily_qset = BetenlaceDailyReport.objects.using(DB_USER_PARTNER).annotate(
                campaign_title=Concat(
                    "betenlace_cpa__link__campaign__bookmaker__name",
                    Value(" "),
                    "betenlace_cpa__link__campaign__title",
                ),
                prom_code=F("betenlace_cpa__link__prom_code"),

                fixed_income_local=F("partnerlinkdailyreport__fixed_income_local"),
                fixed_income_unitary_local=F("partnerlinkdailyreport__fixed_income_unitary_local"),

                registered_count_partner=F("partnerlinkdailyreport__registered_count"),
                first_deposit_count_partner=F("partnerlinkdailyreport__first_deposit_count"),
                wagering_count_partner=F("partnerlinkdailyreport__wagering_count"),

                currency_local=F("partnerlinkdailyreport__currency_local"),
                cpa_partner=F("partnerlinkdailyreport__cpa_count"),

                fx_book_local=F("partnerlinkdailyreport__fx_book_local"),

                percentage_cpa=F("partnerlinkdailyreport__percentage_cpa"),
                tracker=F("partnerlinkdailyreport__tracker"),
                tracker_deposit=F("partnerlinkdailyreport__tracker_deposit"),
                tracker_registered_count=F("partnerlinkdailyreport__tracker_registered_count"),
                tracker_first_deposit_count=F("partnerlinkdailyreport__tracker_first_deposit_count"),
                tracker_wagering_count=F("partnerlinkdailyreport__tracker_wagering_count"),

                adviser_id=F("partnerlinkdailyreport__adviser_id"),

                fixed_income_adviser_local=F("partnerlinkdailyreport__fixed_income_adviser_local"),
                net_revenue_adviser_local=F("partnerlinkdailyreport__net_revenue_adviser_local"),

                fixed_income_adviser_percentage=F("partnerlinkdailyreport__fixed_income_adviser_percentage"),
                net_revenue_adviser_percentage=F("partnerlinkdailyreport__net_revenue_adviser_percentage"),

                referred_by_id=F("partnerlinkdailyreport__referred_by_id"),
                fixed_income_referred_local=F("partnerlinkdailyreport__fixed_income_referred_local"),
                net_revenue_referred_local=F("partnerlinkdailyreport__net_revenue_referred_local"),
                fixed_income_referred_percentage=F("partnerlinkdailyreport__fixed_income_referred_percentage"),
                net_revenue_referred_percentage=F("partnerlinkdailyreport__net_revenue_referred_percentage"),
                **qset_annotates,
            ).select_related(
                "partnerlinkdailyreport",
                "betenlace_cpa",
                "fx_partner",
            ).filter(
                *filters,
            )

            bet_daily_values = bet_daily_qset.values(*values)

            if not bet_daily_values:
                return Response(
                    data={},
                    status=status.HTTP_200_OK,
                )

            bet_daily_df = pd.DataFrame(bet_daily_values)

            # Dictionary with columns of type string
            # Temporary all currencies local is USD, prevent separate by null
            # cases
            df_fillna_str = {"currency_local": "USD"}
            if ("currency_condition" in bet_daily_df.columns):
                df_fillna_str["currency_condition"] = ""
            if ("currency_fixed_income" in bet_daily_df.columns):
                df_fillna_str["currency_fixed_income"] = ""

            bet_daily_df = bet_daily_df.fillna(df_fillna_str).fillna(0)

            # Force to correct data types
            bet_daily_df = bet_daily_df.astype(
                dtype=df_astype,
                copy=False,
            )

            # Fill zeros with NaN for exclude cases for mean calculation
            df_replace_columns = [
                "fixed_income_unitary_fxc",
                "fixed_income_partner_unitary_fxc",
                "fixed_income_unitary_local",
                "fx_book_local",
                "percentage_cpa",
                "tracker",
                "tracker_deposit",
                "tracker_registered_count",
                "tracker_first_deposit_count",
                "tracker_wagering_count",
                "fixed_income_adviser_percentage",
                "net_revenue_adviser_percentage",
                "fixed_income_referred_percentage",
                "net_revenue_referred_percentage",
            ]
            bet_daily_df[df_replace_columns] = bet_daily_df[df_replace_columns].replace(0, np.NaN)

            if bet_daily_df.empty:
                return Response(
                    data={},
                    status=status.HTTP_200_OK,
                )

            bet_daily_df = bet_daily_df.groupby(
                df_groupby,
                as_index=False,
                dropna=False,
            ).agg(
                {
                    "deposit_fxc": np.sum,
                    "deposit_partner_fxc": np.sum,
                    "stake_fxc": np.sum,

                    "net_revenue_fxc": np.sum,
                    "revenue_share_fxc": np.sum,

                    "cpa_count": np.sum,
                    "fixed_income_fxc": np.sum,
                    "fixed_income_unitary_fxc": np.mean,

                    "cpa_partner": np.sum,
                    "fixed_income_partner_fxc": np.sum,
                    "fixed_income_partner_unitary_fxc": np.mean,
                    "fixed_income_local": np.sum,
                    "fixed_income_unitary_local": np.mean,

                    "click_count": np.sum,
                    "registered_count": np.sum,
                    "registered_count_partner": np.sum,
                    "first_deposit_count": np.sum,
                    "first_deposit_count_partner": np.sum,
                    "wagering_count": np.sum,
                    "wagering_count_partner": np.sum,

                    "fx_book_local": np.mean,

                    "percentage_cpa": np.mean,
                    "tracker": np.mean,
                    "tracker_deposit": np.mean,
                    "tracker_registered_count": np.mean,
                    "tracker_first_deposit_count": np.mean,
                    "tracker_wagering_count": np.mean,

                    "adviser_id": lambda x: set(x),
                    "fixed_income_adviser_fxc": np.sum,
                    "fixed_income_adviser_local": np.sum,
                    "net_revenue_adviser_fxc": np.sum,
                    "net_revenue_adviser_local": np.sum,
                    "fixed_income_adviser_percentage": np.mean,
                    "net_revenue_adviser_percentage": np.mean,

                    "referred_by_id": lambda x: set(x),
                    "fixed_income_referred_fxc": np.sum,
                    "fixed_income_referred_local": np.sum,
                    "net_revenue_referred_fxc": np.sum,
                    "net_revenue_referred_local": np.sum,
                    "fixed_income_referred_percentage": np.mean,
                    "net_revenue_referred_percentage": np.mean,
                },
            ).sort_values(
                by=df_order_by,
                ascending=df_ascending,
                kind="stable",
            )

            # If nan persist
            bet_daily_df.fillna(0, inplace=True)

            bet_daily_pag = self.paginate_queryset(
                queryset=bet_daily_df.to_dict("records"),
                request=request,
                view=self,
            )

            betenlace_ser = MembertReportGroupMultiFxSer(
                instance=bet_daily_pag,
                many=True,
                context={
                    "permissions": report_visualization,
                },
            )

            return Response(
                data={
                    "member": betenlace_ser.data,
                },
                headers={
                    "count": self.count,
                    "access-control-expose-headers": "count,next,previous",
                },
                status=status.HTTP_200_OK,
            )
        # Case not grouped
        else:
            # If currency convert is different to orig, recalculate with cases
            if (validator.document.get("currency_convert") != "orig"):
                # Get cases for Fx conversion from fixed_incomes to USD for annotates
                fx_conversion_cases = fx_conversion_specific_adviser_daily_cases(
                    model_func=F,
                    currency_to=validator.document.get("currency_convert"),
                )
                qset_annotates = fx_conversion_cases
            # Currency to is orig, without changes
            else:
                qset_annotates = {
                    "deposit_fxc": F("deposit"),
                    "deposit_partner_fxc": F("partnerlinkdailyreport__deposit"),
                    "stake_fxc": F("stake"),
                    "net_revenue_fxc": F("net_revenue"),
                    "revenue_share_fxc": F("revenue_share"),
                    "fixed_income_fxc": F("fixed_income"),
                    "fixed_income_unitary_fxc": F("fixed_income_unitary"),
                    "fixed_income_partner_fxc": F("partnerlinkdailyreport__fixed_income"),
                    "fixed_income_partner_unitary_fxc": F("partnerlinkdailyreport__fixed_income_unitary"),
                    "fixed_income_adviser_fxc": F("partnerlinkdailyreport__fixed_income_adviser"),
                    "net_revenue_adviser_fxc": F("partnerlinkdailyreport__net_revenue_adviser"),
                    "fixed_income_referred_fxc": F("partnerlinkdailyreport__fixed_income_referred"),
                    "net_revenue_referred_fxc": F("partnerlinkdailyreport__net_revenue_referred"),
                }

            bet_daily = BetenlaceDailyReport.objects.using(DB_USER_PARTNER).annotate(
                id_partner=F("partnerlinkdailyreport__partner_link_accumulated__partner__pk"),
                partner_name=Concat(
                    "partnerlinkdailyreport__partner_link_accumulated__partner__user__first_name",
                    Value(" "),
                    "partnerlinkdailyreport__partner_link_accumulated__partner__user__second_name",
                    Value(" "),
                    "partnerlinkdailyreport__partner_link_accumulated__partner__user__last_name",
                    Value(" "),
                    "partnerlinkdailyreport__partner_link_accumulated__partner__user__second_last_name",
                ),
                campaign_title=Concat(
                    "betenlace_cpa__link__campaign__bookmaker__name",
                    Value(" "),
                    "betenlace_cpa__link__campaign__title",
                ),
                prom_code=F("betenlace_cpa__link__prom_code"),
                fixed_income_local=F("partnerlinkdailyreport__fixed_income_local"),
                fixed_income_unitary_local=F("partnerlinkdailyreport__fixed_income_unitary_local"),

                registered_count_partner=F("partnerlinkdailyreport__registered_count"),
                first_deposit_count_partner=F("partnerlinkdailyreport__first_deposit_count"),
                wagering_count_partner=F("partnerlinkdailyreport__wagering_count"),

                currency_local=F("partnerlinkdailyreport__currency_local"),
                cpa_partner=F("partnerlinkdailyreport__cpa_count"),

                fx_book_local=F("partnerlinkdailyreport__fx_book_local"),

                percentage_cpa=F("partnerlinkdailyreport__percentage_cpa"),
                tracker=F("partnerlinkdailyreport__tracker"),
                tracker_deposit=F("partnerlinkdailyreport__tracker_deposit"),
                tracker_registered_count=F("partnerlinkdailyreport__tracker_registered_count"),
                tracker_first_deposit_count=F("partnerlinkdailyreport__tracker_first_deposit_count"),
                tracker_wagering_count=F("partnerlinkdailyreport__tracker_wagering_count"),

                adviser_id=F("partnerlinkdailyreport__adviser_id"),
                fixed_income_adviser_local=F("partnerlinkdailyreport__fixed_income_adviser_local"),
                net_revenue_adviser_local=F("partnerlinkdailyreport__net_revenue_adviser_local"),
                fixed_income_adviser_percentage=F("partnerlinkdailyreport__fixed_income_adviser_percentage"),
                net_revenue_adviser_percentage=F("partnerlinkdailyreport__net_revenue_adviser_percentage"),

                referred_by_id=F("partnerlinkdailyreport__referred_by_id"),
                fixed_income_referred_local=F("partnerlinkdailyreport__fixed_income_referred_local"),
                net_revenue_referred_local=F("partnerlinkdailyreport__net_revenue_referred_local"),
                fixed_income_referred_percentage=F("partnerlinkdailyreport__fixed_income_referred_percentage"),
                net_revenue_referred_percentage=F("partnerlinkdailyreport__net_revenue_referred_percentage"),

                **qset_annotates,
            ).select_related(
                "partnerlinkdailyreport",
                "betenlace_cpa",
                "fx_partner",
            ).filter(
                *filters,
            ).order_by(
                F(order_by[1:]).desc(nulls_last=True)
                if "-" == order_by[0]
                else
                F(order_by).asc(nulls_first=True),
            )

            bet_daily_pag = self.paginate_queryset(bet_daily, request, view=self)

            bet_daily_ser = MemeberReportMultiFxSer(
                instance=bet_daily_pag,
                many=True,
                context={
                    "permissions": report_visualization,
                },
            )

            return Response(
                data={
                    "member": bet_daily_ser.data
                }, headers={
                    "count": self.count,
                    "access-control-expose-headers": "count,next,previous",
                },
                status=status.HTTP_200_OK,
            )

    def _grouped_cases_df_recalculate_vars(
        self,
        group_by_month,
        group_by_campaign,
        group_by_prom_code,
        values,
        df_groupby,
        annotates,
        df_astype,
        currency_convert,
    ):
        if (group_by_month):
            values.append("created_at__year")
            values.append("created_at__month")

            df_groupby.append("created_at__year")
            df_groupby.append("created_at__month")

            df_astype["created_at__year"] = np.uint32
            df_astype["created_at__month"] = np.uint32

        if (group_by_campaign):
            values.append("campaign_title")
            df_groupby.append("campaign_title")
            df_astype["campaign_title"] = "string"

        if (group_by_prom_code):
            values.append('id_partner')
            values.append('prom_code')
            values.append("partner_name")

            df_groupby.append("prom_code")
            df_groupby.append("partner_name")

            annotates["id_partner"] = F("partnerlinkdailyreport__partner_link_accumulated__partner__pk")
            annotates["partner_name"] = Concat(
                "partnerlinkdailyreport__partner_link_accumulated__partner__user__first_name",
                Value(" "),
                "partnerlinkdailyreport__partner_link_accumulated__partner__user__second_name",
                Value(" "),
                "partnerlinkdailyreport__partner_link_accumulated__partner__user__last_name",
                Value(" "),
                "partnerlinkdailyreport__partner_link_accumulated__partner__user__second_last_name",
            )

            df_astype["id_partner"] = np.uint32
            df_astype["prom_code"] = "string"
            df_astype["partner_name"] = "string"

        # Case orig data without fx conversion
        if currency_convert == "orig":
            values.append("currency_condition")
            values.append("currency_fixed_income")

            df_astype["currency_condition"] = "string"
            df_astype["currency_fixed_income"] = "string"

            df_groupby.append("currency_condition")
            df_groupby.append("currency_fixed_income")

    def _grouped_cases_df_create_vars(self):
        values = [
            "deposit_fxc",
            "deposit_partner_fxc",
            "stake_fxc",

            "net_revenue_fxc",
            "revenue_share_fxc",

            "cpa_count",
            "fixed_income_fxc",
            "fixed_income_unitary_fxc",

            "registered_count",
            "registered_count_partner",
            "first_deposit_count",
            "first_deposit_count_partner",
            "wagering_count",
            "wagering_count_partner",
            "click_count",

            "currency_local",
            "cpa_partner",
            "fixed_income_partner_fxc",
            "fixed_income_partner_unitary_fxc",
            "fixed_income_local",
            "fixed_income_unitary_local",

            "fx_book_local",

            "percentage_cpa",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",

            "adviser_id",
            "fixed_income_adviser_fxc",
            "fixed_income_adviser_local",
            "net_revenue_adviser_fxc",
            "net_revenue_adviser_local",
            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",

            "referred_by_id",
            "fixed_income_referred_fxc",
            "fixed_income_referred_local",
            "net_revenue_referred_fxc",
            "net_revenue_referred_local",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",
        ]

        # Group by vars for DF
        filter_group = [
            "currency_local",
        ]

        # Annotates for DF grouped case
        qset_annotates = {
        }

        # Define df astype for grouped case
        df_astype = {
            "deposit_fxc": np.float32,
            "deposit_partner_fxc": np.float32,
            "stake_fxc": np.float32,

            "net_revenue_fxc": np.float32,
            "revenue_share_fxc": np.float32,

            "currency_local": "string",

            "cpa_count": np.int32,
            "fixed_income_fxc": np.float32,
            "fixed_income_unitary_fxc": np.float32,

            "registered_count": np.int32,
            "registered_count_partner": np.int32,
            "first_deposit_count": np.int32,
            "first_deposit_count_partner": np.int32,
            "wagering_count": np.int32,
            "wagering_count_partner": np.int32,
            "click_count": np.int32,

            "cpa_partner": np.int32,
            "fixed_income_partner_fxc": np.float32,
            "fixed_income_partner_unitary_fxc": np.float32,
            "fixed_income_local": np.float32,
            "fixed_income_unitary_local": np.float32,

            "percentage_cpa": np.float32,
            "tracker": np.float32,
            "tracker_deposit": np.float32,
            "tracker_registered_count": np.float32,
            "tracker_first_deposit_count": np.float32,
            "tracker_wagering_count": np.float32,

            "adviser_id": np.uint32,
            "fixed_income_adviser_fxc": np.float32,
            "fixed_income_adviser_local": np.float32,
            "net_revenue_adviser_fxc": np.float32,
            "net_revenue_adviser_local": np.float32,
            "fixed_income_adviser_percentage": np.float32,
            "net_revenue_adviser_percentage": np.float32,

            "referred_by_id": np.uint32,
            "fixed_income_referred_fxc": np.float32,
            "fixed_income_referred_local": np.float32,
            "net_revenue_referred_fxc": np.float32,
            "net_revenue_referred_local": np.float32,
            "fixed_income_referred_percentage": np.float32,
            "net_revenue_referred_percentage": np.float32,
        }

        return values, filter_group, qset_annotates, df_astype


class MemberMultiFxConsolidatedAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ Returning member report data """
        validator = Validator(
            schema={
                "since_date": {
                    "required": True,
                    "type": "string",
                },
                "until_date": {
                    "required": False,
                    "type": "string",
                },
                "currency_convert": {
                    "required": True,
                    "type": "string",
                    "default": "orig",
                    "allowed": CurrencyPartner.values + ["orig"],
                },
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "partner": {
                    "required": False,
                    "type": "string",
                },
                "bookmaker": {
                    "required": False,
                    "type": "string",
                },
                "prom_code": {
                    "required": False,
                    "type": "string",
                },
                'country_campaign': {
                    'required': False,
                    'type': 'string',
                    'allowed': CountryCampaign.values,
                },
                'country_partner': {
                    'required': False,
                    'type': 'string',
                    'allowed': CountryPartner.values,
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

        admin = request.user

        filters = (Q(rol=admin.rol), Q(codename=CODENAME_GEN),)
        searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters).first()

        filters = []
        if (
            (
                not searchpartnerlimit or
                searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED
            ) and
            not admin.is_superuser
        ):
            filters.append(
                Q(
                    partnerlinkdailyreport__partner_link_accumulated__partner__adviser_id=admin.pk,
                )
            )
        if 'since_date' in validator.document:
            filters.append(
                Q(created_at__gte=datetime.strptime(validator.document.get("since_date"), "%Y-%m-%d"))
            )
        if 'until_date' in validator.document:
            filters.append(
                Q(created_at__lte=datetime.strptime(validator.document.get("until_date"), "%Y-%m-%d"))
            )
        if 'campaign' in validator.document:
            filters.append(
                Q(campaign_title__icontains=validator.document.get("campaign"))
            )
        if 'partner' in validator.document:
            filters.append(
                Q(partnerlinkdailyreport__partner_link_accumulated__partner__user_id=validator.document.get("partner")
                  )
            )
        if 'bookmaker' in validator.document:
            filters.append(
                Q(
                    betenlace_cpa__link__campaign__bookmaker__id=validator.document.get("bookmaker")
                )
            )
        if 'prom_code' in validator.document:
            filters.append(Q(betenlace_cpa__link__prom_code__iexact=validator.document.get("prom_code")))

        if 'country_campaign' in validator.document:
            filters.append(
                Q(betenlace_cpa__link__campaign__countries__icontains=validator.document.get("country_campaign")))

        if 'country_partner' in validator.document:
            filters.append(
                Q(betenlace_cpa__link__partner_link_accumulated__partner__additionalinfo__country=validator
                    .document.get("country_partner"))
            )

        # Values from
        values = [
            "deposit_fxc",
            "deposit_partner_fxc",
            "stake_fxc",

            "net_revenue_fxc",
            "revenue_share_fxc",

            "fixed_income_fxc",
            "fixed_income_unitary_fxc",

            "cpa_count",
            "registered_count",
            "registered_count_partner",
            "first_deposit_count",
            "first_deposit_count_partner",
            "wagering_count",
            "wagering_count_partner",
            "click_count",

            "currency_local",
            "cpa_partner",
            "fixed_income_partner_fxc",
            "fixed_income_partner_unitary_fxc",
            "fixed_income_local",
            "fixed_income_unitary_local",

            "fx_book_local",

            "percentage_cpa",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",

            "adviser_id",
            "fixed_income_adviser_fxc",
            "fixed_income_adviser_local",
            "net_revenue_adviser_fxc",
            "net_revenue_adviser_local",
            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",

            "referred_by_id",
            "fixed_income_referred_fxc",
            "fixed_income_referred_local",
            "net_revenue_referred_fxc",
            "net_revenue_referred_local",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",
        ]

        # astype for df convsersion
        df_astype = {
            "deposit_fxc": np.float32,
            "deposit_partner_fxc": np.float32,
            "stake_fxc": np.float32,

            "net_revenue_fxc": np.float32,
            "revenue_share_fxc": np.float32,

            "cpa_count": np.int32,
            "fixed_income_fxc": np.float32,
            "fixed_income_unitary_fxc": np.float32,

            "currency_local": "string",

            "cpa_partner": np.int32,
            "fixed_income_partner_fxc": np.float32,
            "fixed_income_partner_unitary_fxc": np.float32,
            "fixed_income_local": np.float32,
            "fixed_income_unitary_local": np.float32,

            "click_count": np.int32,

            "registered_count": np.int32,
            "registered_count_partner": np.int32,
            "first_deposit_count": np.int32,
            "first_deposit_count_partner": np.int32,
            "wagering_count": np.int32,
            "wagering_count_partner": np.int32,

            "percentage_cpa": np.float32,
            "tracker": np.float32,
            "tracker_deposit": np.float32,
            "tracker_registered_count": np.float32,
            "tracker_first_deposit_count": np.float32,
            "tracker_wagering_count": np.float32,

            "adviser_id": np.uint32,
            "fixed_income_adviser_fxc": np.float32,
            "fixed_income_adviser_local": np.float32,
            "net_revenue_adviser_fxc": np.float32,
            "net_revenue_adviser_local": np.float32,
            "fixed_income_adviser_percentage": np.float32,
            "net_revenue_adviser_percentage": np.float32,

            "referred_by_id": np.uint32,
            "fixed_income_referred_fxc": np.float32,
            "fixed_income_referred_local": np.float32,
            "net_revenue_referred_fxc": np.float32,
            "net_revenue_referred_local": np.float32,
            "fixed_income_referred_percentage": np.float32,
            "net_revenue_referred_percentage": np.float32,
        }

        # df group by
        df_group = [
            "currency_local",
        ]

        # If currency convert is different to orig, recalculate with cases
        if (validator.document.get("currency_convert") != "orig"):
            # Get cases for Fx conversion from fixed_incomes to
            # currency_convert for annotates
            fx_conversion_cases = fx_conversion_specific_adviser_daily_cases(
                model_func=F,
                currency_to=validator.document.get("currency_convert"),
            )
            qset_annotates = fx_conversion_cases
        else:
            qset_annotates = {
                "deposit_fxc": F("deposit"),
                "deposit_partner_fxc": F("partnerlinkdailyreport__deposit"),
                "stake_fxc": F("stake"),
                "net_revenue_fxc": F("net_revenue"),
                "revenue_share_fxc": F("revenue_share"),
                "fixed_income_fxc": F("fixed_income"),
                "fixed_income_unitary_fxc": F("fixed_income_unitary"),
                "fixed_income_partner_fxc": F("partnerlinkdailyreport__fixed_income"),
                "fixed_income_partner_unitary_fxc": F("partnerlinkdailyreport__fixed_income_unitary"),
                "fixed_income_adviser_fxc": F("partnerlinkdailyreport__fixed_income_adviser"),
                "net_revenue_adviser_fxc": F("partnerlinkdailyreport__net_revenue_adviser"),
                "fixed_income_referred_fxc": F("partnerlinkdailyreport__fixed_income_referred"),
                "net_revenue_referred_fxc": F("partnerlinkdailyreport__net_revenue_referred"),
            }

            # Add currencies of bookmakers to get grouped cases
            values.append("currency_condition")
            values.append("currency_fixed_income")

            # Add astype for currency condition and fixed_income
            df_astype["currency_condition"] = "string"
            df_astype["currency_fixed_income"] = "string"

            df_group.append("currency_condition")
            df_group.append("currency_fixed_income")

        bet_daily_values = BetenlaceDailyReport.objects.using(DB_USER_PARTNER).select_related(
            "partnerlinkdailyreport",
            "betenlace_cpa",
            "fx_partner",
        ).annotate(
            campaign_title=Concat(
                "betenlace_cpa__link__campaign__bookmaker__name",
                Value(" "),
                "betenlace_cpa__link__campaign__title",
            ),
            # Setup vars with name for serializer
            fixed_income_local=F("partnerlinkdailyreport__fixed_income_local"),
            fixed_income_unitary_local=F("partnerlinkdailyreport__fixed_income_unitary_local"),

            registered_count_partner=F("partnerlinkdailyreport__registered_count"),
            first_deposit_count_partner=F("partnerlinkdailyreport__first_deposit_count"),
            wagering_count_partner=F("partnerlinkdailyreport__wagering_count"),

            currency_local=F("partnerlinkdailyreport__currency_local"),
            cpa_partner=F("partnerlinkdailyreport__cpa_count"),

            fx_book_local=F("partnerlinkdailyreport__fx_book_local"),

            percentage_cpa=F("partnerlinkdailyreport__percentage_cpa"),
            tracker=F("partnerlinkdailyreport__tracker"),
            tracker_deposit=F("partnerlinkdailyreport__tracker_deposit"),
            tracker_registered_count=F("partnerlinkdailyreport__tracker_registered_count"),
            tracker_first_deposit_count=F("partnerlinkdailyreport__tracker_first_deposit_count"),
            tracker_wagering_count=F("partnerlinkdailyreport__tracker_wagering_count"),

            adviser_id=F("partnerlinkdailyreport__adviser_id"),
            fixed_income_adviser_local=F("partnerlinkdailyreport__fixed_income_adviser_local"),
            net_revenue_adviser_local=F("partnerlinkdailyreport__net_revenue_adviser_local"),
            fixed_income_adviser_percentage=F("partnerlinkdailyreport__fixed_income_adviser_percentage"),
            net_revenue_adviser_percentage=F("partnerlinkdailyreport__net_revenue_adviser_percentage"),

            referred_by_id=F("partnerlinkdailyreport__referred_by_id"),
            fixed_income_referred_local=F("partnerlinkdailyreport__fixed_income_referred_local"),
            net_revenue_referred_local=F("partnerlinkdailyreport__net_revenue_referred_local"),
            fixed_income_referred_percentage=F("partnerlinkdailyreport__fixed_income_referred_percentage"),
            net_revenue_referred_percentage=F("partnerlinkdailyreport__net_revenue_referred_percentage"),
            **qset_annotates,
        ).filter(
            *filters,
        ).values(
            *values,
        )

        if not bet_daily_values:
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )

        bet_daily_df = pd.DataFrame(bet_daily_values)

        df_fillna_str = {"currency_local": "USD"}
        if ("currency_condition" in bet_daily_df.columns):
            df_fillna_str["currency_condition"] = ""
        if ("currency_fixed_income" in bet_daily_df.columns):
            df_fillna_str["currency_fixed_income"] = ""

        bet_daily_df = bet_daily_df.fillna(df_fillna_str).fillna(0)

        # Force to correct data types
        bet_daily_df = bet_daily_df.astype(
            df_astype,
            copy=False,
        )

        # Fill zeros with NaN for exclude cases for mean calculation
        df_replace_columns = [
            "fixed_income_unitary_fxc",
            "fixed_income_partner_unitary_fxc",
            "fixed_income_unitary_local",
            "fx_book_local",
            "percentage_cpa",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",
            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",
        ]
        bet_daily_df[df_replace_columns] = bet_daily_df[df_replace_columns].replace(0, np.NaN)

        if bet_daily_df.empty:
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )

        bet_daily_df = bet_daily_df.groupby(
            by=df_group,
            as_index=False,
            dropna=False,
        ).agg(
            {
                "deposit_fxc": np.sum,
                "deposit_partner_fxc": np.sum,
                "stake_fxc": np.sum,

                "net_revenue_fxc": np.sum,
                "revenue_share_fxc": np.sum,

                "cpa_count": np.sum,
                "fixed_income_fxc": np.sum,
                "fixed_income_unitary_fxc": np.mean,

                "cpa_partner": np.sum,
                "fixed_income_partner_fxc": np.sum,
                "fixed_income_partner_unitary_fxc": np.mean,
                "fixed_income_local": np.sum,
                "fixed_income_unitary_local": np.mean,

                "click_count": np.sum,
                "registered_count": np.sum,
                "registered_count_partner": np.sum,
                "first_deposit_count": np.sum,
                "first_deposit_count_partner": np.sum,
                "wagering_count": np.sum,
                "wagering_count_partner": np.sum,

                "fx_book_local": np.mean,

                "percentage_cpa": np.mean,
                "tracker": np.mean,
                "tracker_deposit": np.mean,
                "tracker_registered_count": np.mean,
                "tracker_first_deposit_count": np.mean,
                "tracker_wagering_count": np.mean,

                "adviser_id": lambda x: set(x),
                "fixed_income_adviser_fxc": np.sum,
                "fixed_income_adviser_local": np.sum,
                "net_revenue_adviser_fxc": np.sum,
                "net_revenue_adviser_local": np.sum,
                "fixed_income_adviser_percentage": np.mean,
                "net_revenue_adviser_percentage": np.mean,

                "referred_by_id": lambda x: set(x),
                "fixed_income_referred_fxc": np.sum,
                "fixed_income_referred_local": np.sum,
                "net_revenue_referred_fxc": np.sum,
                "net_revenue_referred_local": np.sum,
                "fixed_income_referred_percentage": np.mean,
                "net_revenue_referred_percentage": np.mean,
            },
        )

        # If nan persist
        bet_daily_df.fillna(0, inplace=True)

        # Make report visualization fields  if is_superuser so can be show all fields
        if admin.is_superuser:
            member_group = set(MembertReportGroupMultiFxSer._declared_fields.keys())
            member_filter = set(MemeberReportMultiFxSer.Meta.fields)
            report_visualization = list(member_filter.union(member_group))
        else:  # else show just fields that the user's role has assigned
            report_visualization = report_visualization_limit(
                admin=admin,
                permission_codename=CODENAME,
            )
            if (not report_visualization):
                return Response(
                    data={
                        "error": settings.FORBIDDEN_NOT_ALLOWED,
                        "details": {
                            "report_visualization": [
                                _("This user does not has permission to visualization"),
                            ],
                        },
                    },
                    status=status.HTTP_403_FORBIDDEN,
                )

        betenlace_daily_ser = MemberReportConsolidatedMultiFxSer(
            instance=bet_daily_df.to_dict("records"),
            many=True,
            context={
                "permissions": report_visualization,
            },
        )

        return Response(
            data={
                "total_records": betenlace_daily_ser.data,
            },
            status=status.HTTP_200_OK,
        )
