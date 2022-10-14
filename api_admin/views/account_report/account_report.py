import ast

from api_admin.models import (
    ReportVisualization,
    SearchPartnerLimit,
)
from api_admin.serializers import (
    AccountReportTotalCount,
    AcountReportAdminSerializers,
    BookmakerSerializer,
    CampaignAccountReportSerializer,
    ParnertAssignSer,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    GetAccountsReports,
)
from api_partner.models import (
    AccountReport,
    Bookmaker,
    Campaign,
    Partner,
)
from cerberus import Validator
from core.helpers import HavePermissionBasedView
from core.helpers.path_route_db import request_cfg
from core.models import User
from django.conf import settings
from django.db.models import (
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


class AccountReportCampaignAPI(APIView):
    """
        Class has method that return all campaigns in DB
    """
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ Return all campaigns from advisers """

        codename_campaign = "account report campaign api-get"  # codename to SearchLimit model
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
                ),
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


class AccountReportPartnersAPI(APIView):
    """
        Class with method to return partner into account report filter
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
            Method that return partners in account report filter
        """
        request_cfg.is_partner = True
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

        codename = "account report partners api-get"

        admin = request.user

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(rol=admin.rol),
            Q(codename=codename),
        )
        searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters).first()
        filters = []

        if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED)\
                and not admin.is_superuser:
            filters.append(
                Q(adviser_id=admin.pk),
            )

        if "email" in request.query_params:
            user = User.objects.db_manager(DB_USER_PARTNER).filter(
                email__icontains=request.query_params.get("email"),
            ).values("id")
            if user:
                filters.append(
                    Q(user_id__in=user),
                )
            else:
                filters.append(
                    Q(user_id=None),
                )

        if "identification_number" in request.query_params:
            filters.append(
                Q(
                    additionalinfo__identification__istartswith=request.query_params.get("identification_number"),
                ),
            )

        if "identification_type" in request.query_params:
            filters.append(
                Q(
                    additionalinfo__identification_type=request.query_params.get(
                        "identification_type"),
                ),
            )

        if "partner_id" in request.query_params:
            filters.append(
                Q(user__id=request.query_params.get("partner_id")),
            )

        if "full_name" in request.query_params:
            filters.append(
                Q(full_name__icontains=request.query_params.get("full_name")),
            )

        partner = Partner.objects.annotate(
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
        partner_serializer = ParnertAssignSer(partner, many=True)

        return Response(
            data={
                "count": partner.count(),
                "partners": partner_serializer.data,
            },
            status=status.HTTP_200_OK,
        )


class AccountReportAPI(APIView, GetAccountsReports):
    """
        Class View with method get, this class contain the method that return all account report records
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
           Return account report records with filters

           #Body
           -  since_date : "str"
                Param to define since date to return records
           -  until_date : "str"
                Param to define until date to return records
           -  since_cpa_date : "str"
                Param to define until date to cpa return records
           -  until_cpa_date : "str"
                Param to define until date to cpa return records
           -  punter_id : "int"
                Param to define punter specific records to return 
           -  cpa : "int"
                Param to define number cpas records to return 
           -  campaign : "str"
                Param to define campaign records to return 
           -  partner_id : "int"
                Param to define partner specific records to return 
           -  cpa_betenlace : "int"
                Param to define punter specific records to return 
           -  bookmaker : "str"
                Param to define bookmaker specific records to return
           -  sort_by : "str"
                Param to sort data
           -  lim : "int"
           -  offs : "int"
        """
        request_cfg.is_partner = True

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
                "since_cpa_date": {
                    "required": False,
                    "type": "string",
                },
                "until_cpa_date": {
                    "required": False,
                    "type": "string",
                },
                "punter_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "cpa": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "partner_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "cpa_betenlace": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "bookmaker": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                },
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
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

        admin = request.user
        codename_account = "account report api-get"

        filters = []
        searchpartnerlimit = SearchPartnerLimit.objects.filter(
            Q(
                rol=admin.rol,
            ),
            Q(
                codename=codename_account,
            ),
        ).first()

        if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED)\
                and not admin.is_superuser:
            filters.append(
                Q(
                    partner_link_accumulated__partner__adviser_id=admin.pk,
                ),
            )

        if "since_date" in request.query_params and \
                "until_date" in request.query_params:
            filters.append(
                Q(
                    registered_at__gte=datetime.strptime(
                        request.query_params.get("since_date"),
                        "%Y-%m-%d"
                    ),
                ),
            )
            filters.append(
                Q(
                    registered_at__lte=datetime.strptime(
                        request.query_params.get("until_date"),
                        "%Y-%m-%d"
                    ),
                ),
            )

        if "since_cpa_date" in request.query_params and \
                "until_cpa_date" in request.query_params:
            filters.append(
                Q(
                    cpa_at__gte=datetime.strptime(
                        request.query_params.get("since_cpa_date"),
                        "%Y-%m-%d"
                    ),
                ),
            )
            filters.append(
                Q(
                    cpa_at__lte=datetime.strptime(
                        request.query_params.get("until_cpa_date"),
                        "%Y-%m-%d"
                    ),
                ),
            )

        if "punter_id" in request.query_params:
            filters.append(
                Q(
                    punter_id__icontains=request.query_params.get("punter_id"),
                ),
            )

        if "campaign" in request.query_params:
            filters.append(
                Q(
                    campaign_title__icontains=request.query_params.get("campaign"),
                ),
            )

        if "partner_id" in request.query_params:
            partner = Partner.objects.using(DB_USER_PARTNER).filter(
                user_id=request.query_params.get("partner_id"),
            ).first()

            filters.append(
                Q(
                    partner_link_accumulated__partner=partner,
                ),
            )

        if "cpa_betenlace" in request.query_params:
            filters.append(
                Q(
                    cpa_betenlace=request.query_params.get("cpa_betenlace"),
                ),
            )

        if "cpa" in request.query_params:
            filters.append(
                Q(
                    cpa_partner=request.query_params.get("cpa"),
                ),
            )

        if "bookmaker" in request.query_params:
            filters.append(
                Q(
                    link__campaign__bookmaker__id=request.query_params.get("bookmaker"),
                ),
            )

        sort_by = "-created_at"
        if "sort_by" in request.query_params:
            sort_by = request.query_params.get("sort_by")

        account_report = AccountReport.objects.using(DB_USER_PARTNER).annotate(
            campaign_title=Concat(
                "link__campaign__bookmaker__name",
                Value(" "),
                "link__campaign__title",
            ),
            partner_name=Concat(
                "partner_link_accumulated__partner__user__first_name",
                Value(" "),
                "partner_link_accumulated__partner__user__last_name",
            ),
            prom_code=F(
                "link__prom_code",
            ),
        ).filter(
            *filters
        ).order_by(
            sort_by
        )

        if admin.is_superuser:
            reportvisualization = set(AcountReportAdminSerializers.Meta.fields)
        else:
            reportvisualization = ReportVisualization.objects.filter(
                Q(rol=admin.rol),
                Q(permission__codename=codename_account),
            ).first()

            if not reportvisualization:
                return Response({
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "report_visualization": [
                            _("This user doesnt has permission to visualitazion"),
                        ],
                    },
                },
                    status=status.HTTP_403_FORBIDDEN,
                )
            reportvisualization = ast.literal_eval(reportvisualization.values_can_view)

        accounts_pag = self.paginate_queryset(
            account_report, request, view=self
        )

        account_serializer = AcountReportAdminSerializers(
            accounts_pag,
            many=True,
            context={
                "permissions": reportvisualization,
            },
        )

        return Response(
            data={
                "accounts": account_serializer.data,
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )


class AccountReportSumAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
           Return account report records with filters

           #Body
           -  since_date : "str"
                Param to define since date to return records
           -  until_date : "str"
                Param to define until date to return records
           -  since_cpa_date : "str"
                Param to define until date to cpa return records
           -  until_cpa_date : "str"
                Param to define until date to cpa return records
           -  punter_id : "int"
                Param to define punter specific records to return 
           -  cpa : "int"
                Param to define number cpas records to return 
           -  campaign : "str"
                Param to define campaign records to return 
           -  partner_id : "int"
                Param to define partner specific records to return 
           -  cpa_betenlace : "int"
                Param to define punter specific records to return 
           -  bookmaker : "str"
                Param to define bookmaker specific records to return
           -  sort_by : "str"
                Param to sort data
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
                "since_cpa_date": {
                    "required": False,
                    "type": "string",
                },
                "punter_id": {
                    "required": False,
                    "type": "string",
                },
                "until_cpa_date": {
                    "required": False,
                    "type": "string",
                },
                "cpa_betenlace": {
                    "required": False,
                    "type": "string",
                },
                "cpa": {
                    "required": False,
                    "type": "string",
                },
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "partner_id": {
                    "required": False,
                    "type": "string",
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                },
                "bookmaker": {
                    "required": False,
                    "type": "string",
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

        admin = request.user

        codename_account = "account report api-get"

        filters = (
            Q(
                rol=admin.rol,
            ),
            Q(
                codename=codename_account,
            ),
        )
        searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters).first()

        filters = []
        if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED)\
                and not admin.is_superuser:
            filters.append(
                Q(
                    partner_link_accumulated__partner__adviser_id=admin.pk,
                ),
            )

        if "since_date" in request.query_params and \
                "until_date" in request.query_params:
            filters.append(
                Q(
                    registered_at__gte=datetime.strptime(
                        request.query_params.get("since_date"),
                        "%Y-%m-%d"
                    ),
                ),
            )
            filters.append(
                Q(
                    registered_at__lte=datetime.strptime(
                        request.query_params.get("until_date"
                                                 ),
                        "%Y-%m-%d"
                    ),
                ),
            )

        if "since_cpa_date" in request.query_params and \
                "until_cpa_date" in request.query_params:
            filters.append(
                Q(
                    cpa_at__gte=datetime.strptime(
                        request.query_params.get("since_cpa_date"),
                        "%Y-%m-%d"
                    ),
                ),
            )
            filters.append(
                Q(
                    cpa_at__lte=datetime.strptime(
                        request.query_params.get("until_cpa_date"),
                        "%Y-%m-%d"
                    ),
                ),
            )

        if "punter_id" in request.query_params:
            filters.append(
                Q(
                    punter_id__icontains=request.query_params.get("punter_id"),
                ),
            )

        if "campaign" in request.query_params:
            campaign = Campaign.objects.annotate(
                campaign_title=Concat(
                    "bookmaker__name",
                    Value(" "),
                    "title",
                ),
            ).using(DB_USER_PARTNER).filter(
                campaign_title=request.query_params.get("campaign"),
            ).first()
            if not campaign:
                return Response({
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "campaign": [
                            "Field not found",
                        ],
                    },
                },
                    status=status.HTTP_404_NOT_FOUND
                )

            filters.append(
                Q(
                    link__campaign=campaign,
                ),
            )

        if "partner_id" in request.query_params:
            partner = Partner.objects.using(DB_USER_PARTNER).filter(
                user_id=request.query_params.get("partner_id")
            ).first()

            filters.append(
                Q(
                    partner_link_accumulated__partner=partner,
                ),
            )

        if "cpa" in request.query_params:
            filters.append(
                Q(
                    cpa_partner=request.query_params.get("cpa"),
                ),
            )

        if "cpa_betenlace" in request.query_params:
            filters.append(
                Q(
                    cpa_betenlace=1,
                ),
            )

        if "bookmaker" in request.query_params:
            filters.append(
                Q(
                    link__campaign__bookmaker__id=request.query_params.get("bookmaker"),
                ),
            )

        account_report = AccountReport.objects.filter(*filters).values(
            "currency_fixed_income",
        ).annotate(
            Sum("deposit"),
            Sum("stake"),
            Sum("cpa_partner"),
            Sum("cpa_betenlace"),
        )

        acount_serializer = AccountReportTotalCount(account_report, many=True)

        return Response(
            data={
                "total_records": acount_serializer.data,
            },
            status=status.HTTP_200_OK,
        )


class AccountBookmakerAPI(APIView):

    """
        Class to define bookmaker filter into account report
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """
            Return all bookmaker into account report filter
        """
        admin = request.user
        codename_bookmaker = "account bookmaker api-get"
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
                    campaign_to_bookmaker__link_to_campaign__accountreport_to_link__partner_link_accumulated__partner__adviser_id=admin.pk,
                ),
            )

        bookmaker = Bookmaker.objects.using(DB_USER_PARTNER).filter(*filters).order_by("-name").distinct("name")
        return Response(
            data={
                "bookmakers": BookmakerSerializer(bookmaker, many=True).data,
            },
            status=status.HTTP_200_OK
        )
