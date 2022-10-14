from api_partner.helpers import (
    GetAccountsReports,
    IsActive,
    IsFullRegister,
    IsNotBanned,
    IsTerms,
    fx_conversion_usd_account_cases,
)
from api_partner.models import (
    AccountReport,
    Campaign,
    Partner,
    PartnerLinkAccumulated,
)
from api_partner.serializers import (
    AccountReportSer,
    AccountReportTotalCountSer,
    CampaignAccountSer,
)
from cerberus import Validator
from core.helpers import to_date
from django.conf import settings
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


class CampaignsAccountReportAPI(APIView):
    """ Return campaigns that an user have or had """
    permission_classes = [
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsTerms,
    ]

    def get(self, request):
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

        # Get relations of partner with campaings (current and old relations)
        partners_accumulated = PartnerLinkAccumulated.objects.annotate(
            id_campaign=F("pk"),
            name=Concat(
                'campaign__bookmaker__name',
                Value(' '),
                'campaign__title'
            ),
        ).filter(
            partner__pk=request.user.pk,
        )

        campaigns = CampaignAccountSer(
            partners_accumulated,
            many=True,
        )

        return Response(
            data={
                "count": partners_accumulated.count(),
                "campaigns": campaigns.data,
            },
            status=status.HTTP_200_OK,
        )


class AccountReportAPI(APIView, GetAccountsReports):
    """ 
        Return account report from an logged user 
    """
    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsActive,
        IsTerms,
    )

    def get(self, request):
        """
            Resource that return account report records related to a user

            #Params

           -  since_date : "str"
                Param to define since date to return records
           -  until_date : "str"
                Param to define until date to return records
           -  since_cpa_date : "str"
                Param to define since cpa date to return records
           -  punter_id : "int"
                Param to define punter to return records
           -  until_cpa_date : "str"
                Param to define until cpa date to return records
           -  cpa : "int"
                Param to define cpa number to return records
           -  campaign : "str"
                Param to define campaign to return records
           -  lim
           -  offs
           -  sort_by : "str"
                Param to define sort by to return records
        """
        validator = Validator(
            schema={
                "since_date": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
                },
                "until_date": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
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
                "cpa": {
                    "required": False,
                    "type": "string",
                },
                "campaign": {
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
                    "required": False,
                    "type": "string",
                    "default": "created_at",
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

        # Get partner of current session
        partner = request.user.partner

        partners_accumulated = PartnerLinkAccumulated.objects.filter(
            partner=partner,
        )

        filters = [
            Q(partner_link_accumulated__in=partners_accumulated),
        ]

        # filter for registration date
        if (
            'since_date' in validator.document and
            'until_date' in validator.document
        ):
            filters.append(
                Q(registered_at__gte=validator.document.get("since_date")),
            )
            filters.append(
                Q(registered_at__lte=validator.document.get("until_date")),
            )

        # filter for cpa date
        if (
            'since_cpa_date' in validator.document and
            'until_cpa_date' in validator.document
        ):
            filters.append(
                Q(cpa_at__gte=validator.document.get("since_cpa_date")),
            )
            filters.append(
                Q(cpa_at__lte=validator.document.get("until_cpa_date")),
            )

        if 'punter_id' in validator.document:
            filters.append(
                Q(punter_id__icontains=validator.document.get("punter_id")),
            )

        # verify if supplied campaign exist
        # This filter MUST BE change later
        if 'campaign' in validator.document:
            campaign = Campaign.objects.annotate(
                campaign_name=Concat(
                    'bookmaker__name',
                    Value(' '),
                    'title',
                )
            ).filter(
                Q(campaign_name__icontains=validator.document.get("campaign")),
            ).first()
            if not campaign:
                return Response(
                    data={
                        "error": settings.NOT_FOUND_CODE,
                        "details": {
                            "campaign": [
                                _("Campaign not found"),
                            ],
                        },
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            filters.append(
                Q(link__campaign=campaign),
            )

        if 'cpa' in validator.document:
            filters.append(
                Q(cpa_partner=validator.document.get("cpa")),
            )

        order_by = validator.document.get("sort_by")

        account_report = AccountReport.objects.annotate(
            campaign_title=Concat(
                'link__campaign__bookmaker__name', Value(' '),
                'link__campaign__title',
            ),
            prom_code=F("link__prom_code"),
        ).filter(*filters).order_by(
            order_by,
        )
        accounts_pag = self.paginate_queryset(
            queryset=account_report,
            request=request,
            view=self,
        )
        account_serializer = AccountReportSer(
            instance=accounts_pag,
            many=True,
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
    """ Returning Account report Sum from an logged user"""
    permission_classes = [
        IsAuthenticated,
        IsNotBanned,
    ]

    def get(self, request):
        validator = Validator({
            'since_date': {
                'required': False,
                'type': 'string'
            },
            'until_date': {
                'required': False,
                'type': 'string'
            },
            'since_cpa_date': {
                'required': False,
                'type': 'string'
            },
            'campaign': {
                'required': False,
                'type': 'string'
            },
            'until_cpa_date': {
                'required': False,
                'type': 'string'
            },
            'punter_id': {
                'required': False,
                'type': 'string'
            },
            'cpa': {
                'required': False,
                'type': 'string'
            },
        })

        if not validator.validate(request.query_params):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        user = request.user
        partner = Partner.objects.filter(
            user_id=user.id
        ).first()

        partners_accumulated = PartnerLinkAccumulated.objects.filter(
            partner=partner
        ).first()

        filters = [
            Q(partner_link_accumulated=partners_accumulated)
        ]

        if 'since_date' in request.query_params and \
                'until_date' in request.query_params:
            filters.append(
                Q(created_at__gte=datetime.strptime(
                    request.query_params.get("since_date"),
                    "%Y-%m-%d"
                )
                )
            )
            filters.append(
                Q(created_at__lte=datetime.strptime(
                    request.query_params.get("until_date"),
                    "%Y-%m-%d"
                ))
            )

        if 'since_cpa_date' in request.query_params and \
                'until_cpa_date' in request.query_params:
            filters.append(
                Q(cpa_at__gte=datetime.strptime(
                    request.query_params.get("since_cpa_date"),
                    "%Y-%m-%d"
                )
                )
            )
            filters.append(
                Q(cpa_at__lte=datetime.strptime(
                    request.query_params.get("until_cpa_date"),
                    "%Y-%m-%d"
                ))
            )

        if 'punter_id' in request.query_params:
            filters.append(
                Q(punter_id__icontains=request.query_params.get("punter_id"))
            )

        if 'campaign' in request.query_params:
            campaign = Campaign.objects.annotate(
                campaign_name=Concat(
                    'bookmaker__name',
                    Value(' '),
                    'title'
                )
            ).filter(
                Q(campaign_name=request.query_params.get("campaign"))
            ).first()
            if not campaign:
                return Response({
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "campaign": [
                            "Campaign not found",
                            request.query_params.get("campaign")
                        ]
                    }
                }, status=status.HTTP_404_NOT_FOUND)

            filters.append(
                Q(
                    link__campaign=campaign
                )
            )

        if 'cpa' in request.query_params:
            filters.append(
                Q(
                    cpa_partner=request.query_params.get("cpa")
                )
            )

        account_report = AccountReport.objects.filter(*filters).values(
            'currency_fixed_income'
        ).annotate(
            Sum('cpa_partner')
        )

        acount_serializer = AccountReportTotalCountSer(account_report, many=True)

        return Response({
            "total_records": acount_serializer.data
        }, status=status.HTTP_200_OK)
