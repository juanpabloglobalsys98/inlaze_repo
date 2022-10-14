
from api_admin.helpers import (
    DefaultPAG,
    create_history,
)
from api_admin.models import (
    LevelPercentageBase,
    SearchPartnerLimit,
)
from api_admin.paginators import GetAllRelationPartnerCampaigns
from api_admin.serializers import (
    ParnertAssignSer,
    PartnerInRelationshipCampaign,
    PartnerLinkAccumHistoricalSER,
    PartnerLinkAccumSER,
    PartnerLinkAccumulatedAdminSerializer,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerAccumStatusCHO,
    PartnerLevelCHO,
)
from api_partner.helpers.choices.partner_link_accum_status import PartnerAccumUpdateReasonCHO
from api_partner.models import (
    Campaign,
    HistoricalPartnerLinkAccum,
    Partner,
    PartnerLinkAccumulated,
)
from cerberus import Validator
from core.helpers import (
    CurrencyAll,
    HavePermissionBasedView,
    StandardErrorHandler,
    to_int,
)
from core.helpers.path_route_db import request_cfg
from core.models import User
from django.conf import settings
from django.db.models import (
    F,
    Prefetch,
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class RelationPartnerCampaignAPI(APIView, GetAllRelationPartnerCampaigns):

    """
        Class view that have the logic to return relation between partners and campaigns
    """

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
            This method returns relationship of partner and campaigns associted with it

            #Body
           -  partner_id : "int"
                Param to identify the partner
           -  campaign_status : "str"
                Param to filter by campaign_status
           -  order_by : "str"
                Param to order data by this self
           -  lim : "int"
           -  offs : "int"

        """
        request_cfg.is_partner = True
        validator = Validator(
            schema={
                "partner_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "campaign_status": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Partner.CampaignStatus.values
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "allowed": (
                        PartnerInRelationshipCampaign.Meta.fields +
                        tuple(["-"+i for i in PartnerInRelationshipCampaign.Meta.fields])
                    ),
                    "default": "user_id",
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
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        order_by = validator.document.get("order_by")

        filters_partner = []
        filters_partnerlinkaccumulated = []
        if "campaign_status" in validator.document:
            filters_partner.append(
                Q(campaign_status=validator.document.get("campaign_status")),
            )

        if "partner_id" in validator.document:
            filters_partner.append(
                Q(user_id=validator.document.get("partner_id"))
            )

        partners = Partner.objects.using(
            DB_USER_PARTNER
        ).annotate(
            name=Concat(
                "user__first_name",
                Value(" "),
                "user__second_name",
                Value(" "),
                "user__last_name",
                Value(" "),
                "user__second_last_name",
            ),
            email=F("user__email"),
            identification_type=F("additionalinfo__identification_type"),
            identification=F("additionalinfo__identification"),
        ).filter(
            *filters_partner
        ).distinct()

        partnerlinkaccumulated = PartnerLinkAccumulated.objects.using(
            DB_USER_PARTNER
        ).filter(
            *filters_partnerlinkaccumulated
        )

        partners_with_campaigns = partners.using(
            DB_USER_PARTNER
        ).prefetch_related(
            Prefetch(
                lookup="partnerlinkaccumulated_to_partner",
                queryset=partnerlinkaccumulated,
                to_attr="partnerlinkaccumulated"
            )
        ).order_by(
            F(order_by[1:]).desc(nulls_last=True)
            if "-" == order_by[0]
            else
            F(order_by).asc(nulls_first=True),
        )

        partner_relationship_partner_campaign = self.paginate_queryset(
            queryset=partners_with_campaigns,
            request=request,
            view=self,
        )

        partnerinrelationshipcampaign_serializer = PartnerInRelationshipCampaign(
            partner_relationship_partner_campaign,
            many=True,
        )

        return Response(
            data={
                "data": partnerinrelationshipcampaign_serializer.data
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request):
        """
            Assign link to partner
        """
        validator = Validator(
            schema={
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "campaign_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "status": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerAccumStatusCHO.values
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(campaign__id=validator.document.get("campaign_id")),
            Q(partner__user_id=validator.document.get("user_id")),
        )
        data = {
            "status": validator.document.get("status")
        }

        partnerlinkaccumulated = PartnerLinkAccumulated.objects.filter(*filters).first()
        if not partnerlinkaccumulated:
            # Create relationship
            # -Get partner by user_id param
            filters = (
                Q(
                    user__id=validator.document.get("user_id"),
                ),
            )
            partner = Partner.objects.filter(*filters).first()
            if not partner:
                return Response(
                    {
                        "error": settings.NOT_FOUND_CODE,
                        "detail": {
                            "Partner": [
                                _("Partner not found"),
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
            # -Get campaign by campaign_id param
            filters = (
                Q(
                    id=validator.document.get("campaign_id"),
                ),
            )
            campaign = Campaign.objects.filter(*filters).first()
            level_percentages = LevelPercentageBase.objects.all().order_by("-created_at").first()
            partnerlinkaccumulated = PartnerLinkAccumulated(
                partner=partner,
                campaign=campaign,
                percentage_cpa=level_percentages.percentages.get(str(partner.level))*campaign.default_percentage,
                currency_fixed_income=campaign.currency_fixed_income,
                currency_local=CurrencyAll.USD,
                status=validator.document.get("status"),
                is_assigned=False,
            )
        elif partnerlinkaccumulated.status == PartnerAccumStatusCHO.INACTIVE:
            filters = (
                Q(
                    user__id=validator.document.get("user_id"),
                ),
            )
            partner = Partner.objects.filter(*filters).first()
            filters = (
                Q(
                    id=validator.document.get("campaign_id"),
                ),
            )
            campaign = Campaign.objects.filter(*filters).first()
            level_percentages = LevelPercentageBase.objects.all().order_by("-created_at").first()
            partnerlinkaccumulated.percentage_cpa = level_percentages.percentages.get(
                str(partner.level)) * campaign.default_percentage
            partnerlinkaccumulated.status = validator.document.get("status")
            partnerlinkaccumulated.is_percentage_custom = False

            partnerlinkaccumulated.save()

        partnerlinkaccumulatedadminserializer = PartnerLinkAccumulatedAdminSerializer(
            partnerlinkaccumulated,
            data=data,
            partial=True
        )
        if partnerlinkaccumulatedadminserializer.is_valid():
            partnerlinkaccumulatedadminserializer.save()
        else:
            return Response({
                "error": settings.SERIALIZER_ERROR_CODE,
                "details": partnerlinkaccumulatedadminserializer.errors
            },
                status=status.HTTP_400_BAD_REQUEST
            )

        filters = (
            Q(partner__user_id=validator.document.get("user_id")),
            Q(campaign__id=validator.document.get("campaign_id")),
        )
        partner_accumulated = PartnerLinkAccumulated.objects.filter(*filters).first()
        create_history(
            instance=partner_accumulated,
            update_reason=PartnerAccumUpdateReasonCHO.PARTNER_REQUEST,
            adviser=request.user.id,
        )

        return Response(
            data={
                "msg": _("Partnerlink status was updated successfully"),
            },
            status=status.HTTP_200_OK,
        )


CODENAME_LINK_ASSIGN_GET = "link assign api-get"


class PartnerLinkAcumulatedGetPartnerAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """ Return partners to assign or remove links """
        # DB routing for Default models to Partner
        request_cfg.is_partner = True

        validator = Validator(
            schema={
                "partner_id": {
                    "required": False,
                    "type": "string",
                    "coerce": to_int,
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

        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        admin = request.user

        filters = (
            Q(rol=admin.rol),
            Q(codename=CODENAME_LINK_ASSIGN_GET),
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
            filters.append(Q(adviser_id=admin.pk))

        if "email" in validator.document:
            filters_user = (
                Q(email__icontains=validator.document.get("email")),
            )
            user = User.objects.db_manager(DB_USER_PARTNER).filter(
                *filters_user,
            ).first()
            if user:
                filters.append(Q(user_id=user.id))
            else:
                filters.append(Q(user_id=None))

        if 'identification_number' in validator.document:
            filters.append(
                Q(additionalinfo__identification__istartswith=validator.document.get("identification_number"))
            )

        if 'identification_type' in validator.document:
            filters.append(Q(additionalinfo__identification_type=validator.document.get("identification_type")))

        if 'partner_id' in validator.document:
            filters.append(
                Q(user__id=validator.document.get("partner_id"))
            )

        if 'full_name' in validator.document:
            filters.append(
                Q(full_name__icontains=validator.document.get("full_name"))
            )

        partner = Partner.objects.annotate(
            full_name=Concat(
                "user__first_name",
                Value(" "),
                "user__second_name",
                Value(" "),
                "user__last_name",
                Value(" "),
                "user__second_last_name"
            ),
            identification_number=F("additionalinfo__identification"),
            identification_type=F("additionalinfo__identification_type"),
            email=F("user__email"),
        ).filter(*filters)[:5]

        partner_serializer = ParnertAssignSer(instance=partner, many=True)

        return Response(
            data={
                "count": partner.count(),
                "partners": partner_serializer.data,
            },
            status=status.HTTP_200_OK,
        )


class GetUserCampaignSpecificAPI(APIView):

    """
        Class view that has method to return partners in section partnerlink relation
    """

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
            Method that return filter partners in relationship partner and campaign

            #Body
           -  partner_id : "int"
                Param to define search partner by id
           -  full_name : "str"
                Param to define search partner by name
           -  email : "str"
                Param to define search partner by email
           -  identification_type : "str"
                Param to define search partner by identification type
           -  identification_number : "str"
                Param to define search partner by identification_number
        """

        validator = Validator(
            schema={
                "partner_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
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

        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        admin = request.user

        filters = (
            Q(rol=admin.rol),
            Q(codename=CODENAME_LINK_ASSIGN_GET),
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
            filters.append(Q(adviser_id=admin.pk))

        if "email" in validator.document:
            filters_user = (
                Q(email__icontains=validator.document.get("email")),
            )
            user = User.objects.db_manager(DB_USER_PARTNER).filter(
                *filters_user,
            ).first()
            if user:
                filters.append(Q(user_id=user.id))
            else:
                filters.append(Q(user_id=None))

        if 'identification_number' in validator.document:
            filters.append(
                Q(additionalinfo__identification__istartswith=validator.document.get("identification_number"))
            )

        if 'identification_type' in validator.document:
            filters.append(Q(additionalinfo__identification_type=validator.document.get("identification_type")))

        if 'partner_id' in validator.document:
            filters.append(
                Q(user__id=validator.document.get("partner_id"))
            )

        if 'full_name' in validator.document:
            filters.append(
                Q(full_name__icontains=validator.document.get("full_name"))
            )

        partner = Partner.objects.annotate(
            full_name=Concat(
                "user__first_name",
                Value(" "),
                "user__second_name",
                Value(" "),
                "user__last_name",
                Value(" "),
                "user__second_last_name"
            ),
            identification_number=F("additionalinfo__identification"),
            identification_type=F("additionalinfo__identification_type"),
            email=F("user__email"),
        ).filter(*filters)[:5]

        partner_serializer = ParnertAssignSer(instance=partner, many=True)

        return Response(
            data={
                "count": partner.count(),
                "partners": partner_serializer.data,
            },
            status=status.HTTP_200_OK,
        )


class PartnerLinkAccumAPI(APIView, DefaultPAG):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def post(self, request):
        validator = Validator(
            schema={
                "filter": {
                    "required": True,
                    "type": "dict",
                    "default": {},
                    "schema": {
                        "id": {
                            "required": False,
                            "type": "integer",
                        },
                        "partner_id": {
                            "required": False,
                            "type": "integer",
                        },
                    }
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "id",
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

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        order_by = validator.document.get("order_by")

        query = Q(**validator.document.get("filter"))

        partnerlink = PartnerLinkAccumulated.objects.using(DB_USER_PARTNER).annotate(
            campaign_title=Concat(
                "campaign__bookmaker__name",
                Value(" "),
                "campaign__title",
            ),
        ).filter(query).order_by(order_by)

        if not partnerlink:
            return Response(
                data={
                    "partner_link_accum": [],
                },
                status=status.HTTP_200_OK
            )

        user_paginated = self.paginate_queryset(
            queryset=partnerlink,
            request=request,
            view=self,
        )

        partner_accum_ser = PartnerLinkAccumSER(
            instance=user_paginated,
            many=True,
            partial=True,
        )

        return Response(
            data={
                "partner_accum": partner_accum_ser.data,
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )


class PartnerLinkAccumHistoricAPI(APIView, DefaultPAG):

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def post(self, request):
        validator = Validator(
            schema={
                "filter": {
                    "required": True,
                    "type": "dict",
                    "default": {},
                    "schema": {
                        "id": {
                            "required": False,
                            "type": "integer",
                        },
                        "partner_link_accum_id": {
                            "required": False,
                            "type": "integer",
                        },
                    }
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "id",
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

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        order_by = validator.document.get("order_by")

        query = Q(**validator.document.get("filter"))

        history = HistoricalPartnerLinkAccum.objects.using(DB_USER_PARTNER).filter(query).order_by(order_by)

        if not history:
            return Response(
                data={
                    "history": [],
                },
                status=status.HTTP_200_OK
            )

        user_paginated = self.paginate_queryset(
            queryset=history,
            request=request,
            view=self,
        )

        code_ser = PartnerLinkAccumHistoricalSER(
            instance=user_paginated,
            many=True,
            partial=True,
        )

        return Response(
            data={
                "history": code_ser.data,
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )
