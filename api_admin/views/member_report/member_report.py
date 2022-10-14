import numpy as np
import pandas as pd
from api_admin.helpers import (
    fx_conversion_usd_adviser_daily_cases,
    report_visualization_limit,
)
from api_admin.helpers.routers_db import DB_ADMIN
from api_admin.models import SearchPartnerLimit
from api_admin.paginators import GetAllMemberReport
from api_admin.serializers import (
    BookmakerSerializer,
    CampaignAccountReportSerializer,
    FilterMemeberReportSer,
    MemberReportAdviserSer,
    MembertReportConsoliSer,
    MembertReportGroupSer,
    ParnertAssignSer,
)
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import (
    BetenlaceDailyReport,
    Bookmaker,
    Campaign,
    Partner,
)
from cerberus import Validator
from core.helpers import (
    CountryCampaign,
    CountryPartner,
    HavePermissionBasedView,
    request_cfg,
)
from core.models import User
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


class MemberReportAPI(APIView, GetAllMemberReport):

    """
        Class view with resources to return member report
    """

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
            Returning member report data

            #Body
           -  since_date : "str"
                Param to define since date return membert report records
           -  until_date : "str"
                Param to define until date return membert report records
           -  campaign : "str"
                Param to filter and return by name campaign name
           -  partner : "str"
                Param to filter and return by partner id 
           -  adviser_id : "str"
                Param to filter and return by adviser id 
           -  bookmaker :
                Param to filter and return by bookmaker
           -  prom_code
                Param to filter and return by prom_code
           -  country_campaign
                Param to filter and return by country_campaign
           -  country_partner
                Param to filter and return by country_partner
           -  group_by_campaign
                Param to group and return by country_partner
           -  group_by_month
                Param to group and return by group_by_month
           -  group_by_prom_code
                Param to group and return by group_by_prom_code

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
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "partner": {
                    "required": False,
                    "type": "string",
                },
                "adviser_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "bookmaker": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
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
        codename = "member report api-get"

        if not admin.is_superuser:
            filters_search_partner_limit = (
                Q(rol=admin.rol),
                Q(codename=codename),
            )
            # Limit admin search to a specifics partners
            searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters_search_partner_limit).first()
            if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED):
                filters.append(
                    Q(partnerlinkdailyreport__adviser_id=admin.pk)
                )

        if 'adviser_id' in validator.document:
            filters.append(
                Q(partnerlinkdailyreport__adviser_id=validator.document.get("adviser_id"))
            )

        if 'since_date' in validator.document:
            filters.append(
                Q(created_at__gte=datetime.strptime(validator.document.get("since_date"), "%Y-%m-%d"))
            )

        if 'until_date' in validator.document:
            filters.append(
                Q(created_at__lte=datetime.strptime(validator.document.get("until_date"), "%Y-%m-%d"))
            )

        if 'bookmaker' in validator.document:
            filters.append(Q(betenlace_cpa__link__campaign__bookmaker__id=validator.document.get("bookmaker")))

        if 'campaign' in validator.document:
            filters.append(
                Q(campaign_title__icontains=validator.document.get("campaign"))
            )

        if 'prom_code' in validator.document:
            filters.append(Q(betenlace_cpa__link__prom_code__iexact=validator.document.get("prom_code")))

        if 'partner' in validator.document:
            filters.append(
                Q(
                    partnerlinkdailyreport__partner_link_accumulated__partner__user__id=validator.document.
                    get("partner")
                )
            )

        if 'country_campaign' in validator.document:
            filters.append(
                Q(betenlace_cpa__link__campaign__countries__icontains=validator.document.get("country_campaign")))

        if 'country_partner' in validator.document:
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
            member_group = set(MembertReportGroupSer._declared_fields.keys())
            member_filter = set(FilterMemeberReportSer.Meta.fields)
            report_visualization = list(member_filter.union(member_group))
        # show just fields that the user's role has assigned
        else:
            report_visualization = report_visualization_limit(
                admin=admin,
                permission_codename=codename,
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

        if group_by_month or group_by_campaign or group_by_prom_code:
            # Flag to determine if is a group by case at least by one of the next
            # cases:
            # - Month
            # - Campaign
            # - Prom Code

            # All values that will used into dataframe
            values, df_groupby, annotates, df_astype = self._grouped_cases_df_create_vars()
            self._grouped_cases_df_recalculate_vars(
                group_by_month=group_by_month,
                group_by_campaign=group_by_campaign,
                group_by_prom_code=group_by_prom_code,
                values=values,
                df_groupby=df_groupby,
                annotates=annotates,
                df_astype=df_astype,
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
            # Get cases for Fx conversion from fixed_incomes to USD for annotates
            fx_conversion_cases = fx_conversion_usd_adviser_daily_cases(
                model_func=F,
            )

            bet = BetenlaceDailyReport.objects.using(DB_USER_PARTNER).annotate(
                **fx_conversion_cases,
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

                cpa_partner=F("partnerlinkdailyreport__cpa_count"),
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
                **annotates,
            ).select_related(
                "partnerlinkdailyreport",
                "betenlace_cpa",
                "fx_partner",
            ).filter(
                *filters,
            )

            bet_daily_values = bet.values(*values)

            if not bet_daily_values:
                return Response(
                    data={},
                    status=status.HTTP_200_OK,
                )

            bet_daily_df = pd.DataFrame(bet_daily_values)
            bet_daily_df.fillna(0, inplace=True)

            # Force to correct data types
            bet_daily_df = bet_daily_df.astype(
                df_astype,
                copy=False,
            )

            # Fill zeros with NaN for exclude cases for mean calculation
            df_replace_columns = [
                "fixed_income_unitary_usd",
                "fixed_income_unitary_local",
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
                    "deposit_usd": np.sum,
                    "deposit_partner_usd": np.sum,
                    "stake_usd": np.sum,

                    "net_revenue_usd": np.sum,
                    "revenue_share_usd": np.sum,

                    "cpa_count": np.sum,
                    "fixed_income_usd": np.sum,
                    "fixed_income_unitary_usd": np.mean,

                    "click_count": np.sum,
                    "registered_count": np.sum,
                    "registered_count_partner": np.sum,
                    "first_deposit_count": np.sum,
                    "first_deposit_count_partner": np.sum,
                    "wagering_count": np.sum,
                    "wagering_count_partner": np.sum,

                    "cpa_partner": np.sum,
                    "fixed_income_local": np.sum,
                    "fixed_income_unitary_local": np.mean,

                    "percentage_cpa": np.mean,
                    "tracker": np.mean,
                    "tracker_deposit": np.mean,
                    "tracker_registered_count": np.mean,
                    "tracker_first_deposit_count": np.mean,
                    "tracker_wagering_count": np.mean,


                    "adviser_id": lambda x: set(x),
                    "fixed_income_adviser_local": np.sum,
                    "net_revenue_adviser_local": np.sum,
                    "fixed_income_adviser_percentage": np.mean,
                    "net_revenue_adviser_percentage": np.mean,

                    "referred_by_id": lambda x: set(x),
                    "fixed_income_referred_local": np.sum,
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

            betenlace_ser = MembertReportGroupSer(
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
                }, status=status.HTTP_200_OK,
            )
        # Case not grouped
        else:
            # Get cases for Fx conversion from fixed_incomes to USD for annotates
            fx_conversion_cases = fx_conversion_usd_adviser_daily_cases(
                model_func=F,
            )

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
                **fx_conversion_cases,

                fixed_income_local=F("partnerlinkdailyreport__fixed_income_local"),
                fixed_income_unitary_local=F("partnerlinkdailyreport__fixed_income_unitary_local"),

                registered_count_partner=F("partnerlinkdailyreport__registered_count"),
                first_deposit_count_partner=F("partnerlinkdailyreport__first_deposit_count"),
                wagering_count_partner=F("partnerlinkdailyreport__wagering_count"),

                cpa_partner=F("partnerlinkdailyreport__cpa_count"),
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

            bet_daily_pag = self.paginate_queryset(
                queryset=bet_daily,
                request=request,
                view=self,
            )

            bet_daily_ser = FilterMemeberReportSer(
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

    def _grouped_cases_df_create_vars(self):
        values = [
            "deposit_usd",
            "deposit_partner_usd",
            "stake_usd",
            "net_revenue_usd",
            "revenue_share_usd",

            "fixed_income_usd",
            "fixed_income_unitary_usd",
            "cpa_count",

            "registered_count",
            "registered_count_partner",
            "first_deposit_count",
            "first_deposit_count_partner",
            "wagering_count",
            "wagering_count_partner",
            "click_count",

            "cpa_partner",
            "fixed_income_local",
            "fixed_income_unitary_local",

            "percentage_cpa",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",

            "adviser_id",
            "fixed_income_adviser_local",
            "net_revenue_adviser_local",
            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",

            "referred_by_id",
            "fixed_income_referred_local",
            "net_revenue_referred_local",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",
        ]

        # Group by vars for DF
        filter_group = [
        ]

        # Annotates for DF grouped case
        annotates_filt = {
        }

        # Define df astype for grouped case
        df_astype = {
            "deposit_usd": np.float32,
            "deposit_partner_usd": np.float32,
            "stake_usd": np.float32,

            "net_revenue_usd": np.float32,
            "revenue_share_usd": np.float32,

            "cpa_count": np.int32,
            "fixed_income_usd": np.float32,
            "fixed_income_unitary_usd": np.float32,
            "registered_count": np.int32,
            "registered_count_partner": np.int32,
            "first_deposit_count": np.int32,
            "first_deposit_count_partner": np.int32,
            "wagering_count": np.int32,
            "wagering_count_partner": np.int32,
            "click_count": np.int32,

            "cpa_partner": np.int32,
            "fixed_income_local": np.float32,
            "fixed_income_unitary_local": np.float32,

            "percentage_cpa": np.float32,
            "tracker": np.float32,
            "tracker_deposit": np.float32,
            "tracker_registered_count": np.float32,
            "tracker_first_deposit_count": np.float32,
            "tracker_wagering_count": np.float32,

            "adviser_id": np.uint32,
            "fixed_income_adviser_local": np.float32,
            "net_revenue_adviser_local": np.float32,
            "fixed_income_adviser_percentage": np.float32,
            "net_revenue_adviser_percentage": np.float32,

            "referred_by_id": np.uint32,
            "fixed_income_referred_local": np.float32,
            "net_revenue_referred_local": np.float32,
            "fixed_income_referred_percentage": np.float32,
            "net_revenue_referred_percentage": np.float32,
        }

        return values, filter_group, annotates_filt, df_astype


class MemberConsolidated(APIView):

    """
        Class view with resources to return data consolidated

    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ 
            Returning member report data consolidated


            #Body
           -  since_date : "str"
                Param to define since date return membert report records
           -  until_date : "str"
                Param to define until date return membert report records
           -  campaign : "str"
                Param to filter and return by name campaign name
           -  partner : "str"
                Param to filter and return by partner id 
           -  adviser_id : "str"
                Param to filter and return by adviser id 
           -  bookmaker :
                Param to filter and return by bookmaker
           -  prom_code
                Param to filter and return by prom_code
           -  country_campaign
                Param to filter and return by country_campaign
           -  country_partner
                Param to filter and return by country_partner
        """
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
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "partner": {
                    "required": False,
                    "type": "string",
                },
                "adviser_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
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
        codename = "member report api-get"
        filters = (
            Q(rol=admin.rol),
            Q(codename=codename),
        )
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
                    partnerlinkdailyreport__adviser_id=admin.pk,
                )
            )

        if 'adviser_id' in validator.document:
            filters.append(
                Q(partnerlinkdailyreport__adviser_id=validator.document.get("adviser_id"))
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

        fx_conversion_cases = fx_conversion_usd_adviser_daily_cases(
            model_func=F,
        )

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
            **fx_conversion_cases,

            fixed_income_local=F("partnerlinkdailyreport__fixed_income_local"),
            fixed_income_unitary_local=F("partnerlinkdailyreport__fixed_income_unitary_local"),

            registered_count_partner=F("partnerlinkdailyreport__registered_count"),
            first_deposit_count_partner=F("partnerlinkdailyreport__first_deposit_count"),
            wagering_count_partner=F("partnerlinkdailyreport__wagering_count"),

            cpa_partner=F("partnerlinkdailyreport__cpa_count"),
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
        ).filter(
            *filters,
        ).values(
            "deposit_usd",
            "deposit_partner_usd",
            "stake_usd",

            "net_revenue_usd",
            "revenue_share_usd",

            "fixed_income_usd",
            "fixed_income_unitary_usd",

            "cpa_count",
            "registered_count",
            "registered_count_partner",
            "first_deposit_count",
            "first_deposit_count_partner",
            "wagering_count",
            "wagering_count_partner",
            "click_count",

            "cpa_partner",
            "fixed_income_local",
            "fixed_income_unitary_local",

            "percentage_cpa",
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",

            "adviser_id",
            "fixed_income_adviser_local",
            "net_revenue_adviser_local",
            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",

            "referred_by_id",
            "fixed_income_referred_local",
            "net_revenue_referred_local",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",
        )

        if not bet_daily_values:
            return Response(
                data={},
                status=status.HTTP_200_OK,
            )

        bet_daily_df = pd.DataFrame(bet_daily_values)
        bet_daily_df.fillna(0, inplace=True)

        bet_daily_df = bet_daily_df.astype(
            {
                "deposit_usd": np.float32,
                "deposit_partner_usd": np.float32,
                "stake_usd": np.float32,

                "net_revenue_usd": np.float32,
                "revenue_share_usd": np.float32,

                "fixed_income_usd": np.float32,
                "fixed_income_unitary_usd": np.float32,

                "fixed_income_local": np.float32,
                "fixed_income_unitary_local": np.float32,

                "click_count": np.int32,
                "cpa_count": np.int32,
                "cpa_partner": np.int32,
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
                "fixed_income_adviser_local": np.float32,
                "net_revenue_adviser_local": np.float32,
                "fixed_income_adviser_percentage": np.float32,
                "net_revenue_adviser_percentage": np.float32,

                "referred_by_id": np.uint32,
                "fixed_income_referred_local": np.float32,
                "net_revenue_referred_local": np.float32,
                "fixed_income_referred_percentage": np.float32,
                "net_revenue_referred_percentage": np.float32,



            }, copy=False
        )

        # Fill zeros with NaN for exclude cases for mean calculation
        df_replace_columns = [
            "fixed_income_unitary_usd",
            "fixed_income_unitary_local",
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

        bet_daily_df = bet_daily_df.agg(
            {
                "deposit_usd": np.sum,
                "deposit_partner_usd": np.sum,
                "stake_usd": np.sum,

                "net_revenue_usd": np.sum,
                "revenue_share_usd": np.sum,

                "cpa_count": np.sum,
                "fixed_income_usd": np.sum,
                "fixed_income_unitary_usd": np.mean,

                "cpa_partner": np.sum,
                "fixed_income_local": np.sum,
                "fixed_income_unitary_local": np.mean,

                "click_count": np.sum,
                "registered_count": np.sum,
                "registered_count_partner": np.sum,
                "first_deposit_count": np.sum,
                "first_deposit_count_partner": np.sum,
                "wagering_count": np.sum,
                "wagering_count_partner": np.sum,

                "percentage_cpa": np.mean,
                "tracker": np.mean,
                "tracker_deposit": np.mean,
                "tracker_registered_count": np.mean,
                "tracker_first_deposit_count": np.mean,
                "tracker_wagering_count": np.mean,


                "adviser_id": lambda x: set(x),
                "fixed_income_adviser_local": np.sum,
                "net_revenue_adviser_local": np.sum,
                "fixed_income_adviser_percentage": np.mean,
                "net_revenue_adviser_percentage": np.mean,

                "referred_by_id": lambda x: set(x),
                "fixed_income_referred_local": np.sum,
                "net_revenue_referred_local": np.sum,
                "fixed_income_referred_percentage": np.mean,
                "net_revenue_referred_percentage": np.mean,
            },
        )

        # If nan persist
        bet_daily_df.fillna(0, inplace=True)

        # Make report visualization fields  if is_superuser so can be show all fields
        if admin.is_superuser:
            member_group = set(MembertReportGroupSer._declared_fields.keys())
            member_filter = set(FilterMemeberReportSer.Meta.fields)
            report_visualization = list(member_filter.union(member_group))
        else:  # else show just fields that the user's role has assigned
            report_visualization = report_visualization_limit(
                admin=admin,
                permission_codename=codename,
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

        betenlace_daily_ser = MembertReportConsoliSer(
            bet_daily_df,
            context={
                "permissions": report_visualization,
            },
        )

        return Response(
            data={
                "total_records": [betenlace_daily_ser.data],
            },
            status=status.HTTP_200_OK,
        )


class MemberReportAdviserCampaignAPI(APIView):
    permission_classes = [
        IsAuthenticated,
    ]

    def get(self, request):
        """ 
            Returning campaigns from member report
        """
        admin = request.user
        campaigns = Campaign.objects.annotate(
            name=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        ).filter(
            link_to_campaign__partner_link_accumulated__partner__adviser_id=admin.id,
        )
        return Response(
            data={
                "camapings": CampaignAccountReportSerializer(
                    campaigns, many=True
                ).data,
            },
            status=status.HTTP_200_OK
        )


class MemberReportPartnersAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ 
            Returning partners related to adviser 
        """
        validator = Validator(
            schema={
                "partner_id": {
                    "required": False,
                    "type": "string",
                },
                "full_name": {
                    "required": False,
                    "type": "string",
                },
                "email": {
                    "required": False,
                    "type": "string",
                },
                "identification_type": {
                    "required": False,
                    "type": "string",
                },
                "identification_number": {
                    "required": False,
                    "type": "string",
                },
            },
        )

        admin = request.user

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        codename_partners = "member report partners api-get"
        filters = (
            Q(rol=admin.rol),
            Q(codename=codename_partners),
        )
        searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters).first()
        filters = []

        if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED)\
                and not admin.is_superuser:
            filters.append(
                Q(adviser_id=admin.pk),
            )

        if 'email' in request.query_params:
            user = User.objects.db_manager(DB_USER_PARTNER).filter(
                email__icontains=request.query_params.get("email")
            ).values('id')
            if user:
                filters.append(
                    Q(user_id__in=user),
                )
            else:
                filters.append(
                    Q(user_id=None),
                )

        if 'identification_number' in request.query_params:
            filters.append(
                Q(
                    additionalinfo__identification__istartswith=request.query_params.get("identification_number")
                ),
            )

        if 'identification_type' in request.query_params:
            filters.append(
                Q(
                    additionalinfo__identification_type=request.query_params.get("identification_type"),
                ),
            )

        if 'partner_id' in request.query_params:
            filters.append(
                Q(user__id=request.query_params.get("partner_id")),
            )

        if 'full_name' in request.query_params:
            filters.append(
                Q(full_name__icontains=request.query_params.get("full_name")),
            )

        partners = Partner.objects.annotate(
            full_name=Concat(
                "user__first_name",
                Value(" "),
                "user__second_name",
                Value(" "),
                "user__last_name",
                Value(" "),
                "user__second_last_name",
            ),
            identification_number=F("additionalinfo__identification"),
            identification_type=F("additionalinfo__identification_type"),
            email=F("user__email"),
        ).filter(*filters)[:5]

        parnertassignser = ParnertAssignSer(partners, many=True)

        return Response(
            data={
                "count": partners.count(),
                "partners": parnertassignser.data
            },
            status=status.HTTP_200_OK,
        )


class MemberReportAdviserAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ 
            Return the search adviser in member report

            #Body
           -  adviser_id : "int"
                Param to identify adviser to return member report data
           -  full_name : "str"
                Param to define adviser's name into member report data
           -  email : "str"
                Param to define adviser's email into member report data

        """
        validator = Validator(
            schema={
                'adviser_id': {
                    'required': False,
                    'type': 'string',
                },
                'full_name': {
                    'required': False,
                    'type': 'string',
                },
                'email': {
                    'required': False,
                    'type': 'string',
                },
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = []

        if 'adviser_id' in request.query_params:
            filters.append(
                Q(pk=request.query_params.get("adviser_id"))
            )

        if 'full_name' in request.query_params:
            filters.append(
                Q(full_name__icontains=request.query_params.get("full_name"))
            )

        if 'email' in request.query_params:
            filters.append(
                Q(email__icontains=request.query_params.get("email"))
            )

        advisers = User.objects.annotate(
            full_name=Concat(
                "first_name",
                Value(" "),
                "second_name",
                Value(" "),
                "last_name",
                Value(" "),
                "second_last_name",
            ),
        ).using(DB_ADMIN).filter(*filters)[:5]

        member_report_ser = MemberReportAdviserSer(advisers, many=True)

        return Response(
            data={
                "count": advisers.count(),
                "advisers": member_report_ser.data
            },
            status=status.HTTP_200_OK,
        )


class MemberReportBookmakerAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        admin = request.user
        codename_bookmaker = "member report bookmaker api-get"
        filters = (
            Q(rol=admin.rol),
            Q(codename=codename_bookmaker),
        )
        # Limit admin search to a specifics partners
        searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters).first()
        filters = []
        if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED)\
                and not admin.is_superuser:
            filters.append(
                Q(
                    campaign_to_bookmaker__link_to_campaign__partner_link_accumulated__partner__adviser_id=admin.pk,
                ),
            )

        bookmaker = Bookmaker.objects.using(DB_USER_PARTNER).filter(*filters).order_by("-name").distinct("name")
        return Response(
            data={
                "bookmakers": BookmakerSerializer(bookmaker, many=True).data
            },
            status=status.HTTP_200_OK
        )


class MemberReportCampaignAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ Return all campaigns from advisers """

        codename_campaign = "member report campaign api-get"
        admin = request.user
        filters = (
            Q(rol=admin.rol),
            Q(codename=codename_campaign),
        )
        # Limit admin search to a specifics partners
        searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters).first()
        filters = []
        if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED)\
                and not admin.is_superuser:
            filters.append(
                Q(
                    link_to_campaign__partner_link_accumulated__partner__adviser_id=admin.pk,
                )
            )

        filters_annotate = {
            "name": Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        }
        campaigns = Campaign.objects.annotate(
            **filters_annotate
        ).filter(*filters).order_by("name").distinct("name")

        return Response(
            data={
                "campaigns": CampaignAccountReportSerializer(campaigns, many=True).data
            },
            status=status.HTTP_200_OK,
        )
