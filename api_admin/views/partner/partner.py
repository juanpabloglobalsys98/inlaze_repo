from api_admin.serializers import (
    PartnerFromAdviserSerializer,
    PartnerViewCampaignSerializer,
)
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import (
    Campaign,
    Partner,
    PartnerLinkAccumulated,
)
from api_partner.serializers import CampaignPartnerSerializer
from cerberus import Validator
from core.helpers.path_route_db import request_cfg
from core.models import User
from django.conf import settings
from django.db.models import (
    Max,
    Q,
    Sum,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class PartnerCampaignAPI(APIView):
    permission_classes = [
        IsAuthenticated
    ]

    def get(self, request):
        validator = Validator({
            'partner_id': {
                'required': False,
                'type': 'string'
            },
            'partner_full_name': {
                'required': False,
                'type': 'string'
            },
            'email': {
                'required': False,
                'type': 'string'
            },
            'phone': {
                'required': False,
                'type': 'string'
            }
        })

        if not validator.validate(request.query_params):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        filters = []
        if 'partner_id' in request.query_params:
            filters.append(
                Q(user_id=request.query_params.get("partner_id"))
            )

        if 'partner_full_name' in request.query_params:
            filters.append(
                Q(full_name=request.query_params.get("partner_full_name"))
            )

        if 'email' in request.query_params:
            user = User.objects.using(DB_USER_PARTNER).filter(
                email__icontains=request.query_params.get("email")
            ).first()
            if user:
                filters.append(
                    Q(user=user)
                )
            else:
                filters.append(
                    Q(user=None)
                )

        if 'phone' in request.query_params:
            user = User.objects.using(DB_USER_PARTNER).filter(
                phone__icontains=request.query_params.get("phone")
            ).first()
            if user:
                filters.append(
                    Q(user_id=user.id)
                )
            else:
                filters.append(
                    Q(user_id=None)
                )

        partners = Partner.objects.filter(*filters)
        return Response({
            "partners": PartnerFromAdviserSerializer(partners, many=True).data
        }, status=status.HTTP_200_OK)


class CampaignPartnerAPI(APIView):

    """
        Class View that return partners in campaign's section
    """

    permission_classes = [
        IsAuthenticated,
    ]

    def get(self, request):
        """
            This method return partner search based in filters

            #Params
           -  partner_id : "int"
                Param to identify partner
           -  campaign_id : "int"
                Param to identify campaign
        """

        request_cfg.is_partner = True
        validator = Validator(
            schema={
                'partner_id': {
                    'required': True,
                    'type': 'integer',
                    'coerce': int
                },
                'campaign_id': {
                    'required': True,
                    'type': 'integer',
                    'coerce': int
                },
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = Partner.objects.filter(
            user_id=request.query_params.get("partner_id")
        ).first()

        if not partner:
            return Response(
                data={
                    "message": _("Not found"),
                    "error": "Partner not found"
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        campaign = Campaign.objects.filter(
            id=request.query_params.get("campaign_id")
        ).first()

        if not campaign:
            return Response({
                "message": _("Not found"),
                "error": "Campaign not found",
            },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_link = PartnerLinkAccumulated.objects.filter(
            partner=partner,
            campaign=campaign
        ).first()

        if not partner_link:
            return Response({
                "message": _("Not found"),
                "error": "Partner Links not found",
            },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_link_daily = partner_link.Partnerlinkdailyreport_to_partnerlinkaccumulated.all()
        count = partner_link_daily.aggregate(
            cpa_count_total=Sum('betenlace_daily_report__cpa_count'),
            registered_count_total=Sum('betenlace_daily_report__registered_count'),
        )
        max_date = partner_link_daily.filter(
            cpa_count__gt=0,
        ).aggregate(
            cpa_count_max=Max('created_at'),
        ).get("cpa_count_max")

        return Response(
            data={
                "cpa_count": count.get("cpa_count_total"),
                "last_capa_at": max_date,
                "registered_count": count.get("registered_count_total")
            },
            status=status.HTTP_200_OK
        )


class PartnerCampaignStatusAPI(APIView):

    """
        Class view with resource to update the campaign's status
    """

    permission_classes = [
        IsAuthenticated,
    ]

    def patch(self, request):
        """
            Method to update campaign's status to an partner
        """

        validator = Validator(
            schema={
                'user_id': {
                    'required': True,
                    'type': 'integer',
                },
                'campaign_status': {
                    'required': True,
                    'type': 'integer',
                    'allowed': Partner.CampaignStatus.values,
                },
            })

        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(user_id=validator.document.get("user_id")),
        )
        partner = Partner.objects.filter(*filters).first()
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "partner": [
                            _("Partner not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        data = {
            "campaign_status": validator.document.get("campaign_status")
        }
        partnerviewcampaignserializer = PartnerViewCampaignSerializer(
            instance=partner,
            data=data,
        )
        if partnerviewcampaignserializer.is_valid():
            partnerviewcampaignserializer.save()
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partnerviewcampaignserializer.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            data={
                "msg": "Partner status campaign view was updated successfully!!",
            },
            status=status.HTTP_200_OK,
        )
