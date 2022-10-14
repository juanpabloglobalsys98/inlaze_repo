import logging
import sys
import traceback
from api_admin.helpers.partner_accum_history import create_history

import pytz
from api_admin.helpers import recalculate_temperature
from api_admin.models import (
    LevelPercentageBase,
    SearchPartnerLimit,
)
from api_admin.paginators import GetAllLinks
from api_admin.serializers import (
    BetenlacecpaSerializer,
    LinkAdviserPartnerSerializer,
    LinkSpecificSerializer,
    LinkTableSer,
    LinkUpdateSer,
    ParnertAssignSer,
    PartnerLinkAccumManageLinkSer,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerAccumUpdateReasonCHO,
    PartnerLevelCHO,
)
from api_partner.models import (
    BetenlaceDailyReport,
    Bookmaker,
    Campaign,
    FxPartner,
    Link,
    Partner,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from api_partner.models.reports_management.betenlace_cpa import BetenlaceCPA
from cerberus import Validator
from core.helpers import (
    CurrencyAll,
    HavePermissionBasedView,
    to_bool,
    to_float,
    to_int,
)
from core.helpers.path_route_db import request_cfg
from core.models import User
from django.conf import settings
from django.db import (
    IntegrityError,
    transaction,
)
from django.db.models import (
    F,
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)

CODENAME_LINK_GET = "link api-get"
CODENAME_LINK_ASSIGN_GET = "link assign api-get"


class LinkAPI(APIView, GetAllLinks):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):

        validator = Validator(
            schema={
                "partner_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "bookmaker_id": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "campaign_title": {
                    "required": False,
                    "type": "string",
                },
                "prom_code": {
                    "required": False,
                    "type": "string",
                },
                "percentage_cpa": {
                    "required": False,
                    "type": "float",
                    "coerce": to_float,
                },
                "tracker": {
                    "required": False,
                    "type": "float",
                    "coerce": to_float,
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "default": "-created_at",
                },
                "status": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": Link.Status.values,
                },
                "is_percentage_custom": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "partner_level": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "lim": {
                    "required": False,
                },
                "offs": {
                    "required": False,
                },
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        admin = request.user
        searchpartnerlimit = SearchPartnerLimit.objects.filter(
            Q(rol=admin.rol), Q(codename=CODENAME_LINK_ASSIGN_GET)).first()
        filters = []

        if (
            (
                not searchpartnerlimit or
                searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED
            ) and
                not admin.is_superuser
        ):
            filters.append(
                Q(partner_link_accumulated__partner__adviser_id=admin.pk) |
                Q(partner_link_accumulated__isnull=True)
            )

        if "partner_id" in validator.document:
            filters.append(
                Q(partner_link_accumulated__partner_id=validator.document.get("partner_id"))
            )

        if "bookmaker_id" in validator.document:
            bookmaker_filters = (
                Q(id=validator.document.get("bookmaker_id")),
            )
            bookmaker = Bookmaker.objects.using(
                DB_USER_PARTNER,
            ).only(
                "id",
            ).filter(
                *bookmaker_filters,
            ).first()
            if bookmaker is None:
                return Response(
                    data={
                        "error": settings.NOT_FOUND_CODE,
                        "detail": {
                            "bookmaker_id": [
                                "Bookmaker not found",
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

            filters.append(
                Q(campaign__bookmaker_id=bookmaker.pk)
            )

        if "campaign_title" in validator.document:
            filters.append(
                Q(campaign_title__icontains=validator.document.get("campaign_title"))
            )

        if "percentage_cpa" in validator.document:
            filters.append(
                Q(partner_link_accumulated__percentage_cpa__lt=validator.document.get("percentage_cpa"))
            )

        if "tracker" in validator.document:
            filters.append(
                Q(partner_link_accumulated__tracker__lt=validator.document.get("tracker"))
            )

        if "prom_code" in validator.document:
            filters.append(
                Q(prom_code__icontains=validator.document.get("prom_code"))
            )

        if "status" in validator.document:
            filters.append(
                Q(status=validator.document.get("status"))
            )

        if "is_percentage_custom" in validator.document:
            filters.append(
                Q(partner_link_accumulated__is_percentage_custom=validator.document.get("is_percentage_custom"))
            )

        sort_by = validator.document.get("sort_by")

        # Force db_route to partner DB
        request_cfg.is_partner = True

        annotates = {
            "bookmaker_name": F("campaign__bookmaker__name"),
            "campaign_title": Concat(
                "campaign__bookmaker__name",
                Value(" "),
                "campaign__title",
            ),
        }

        links = Link.objects.using(DB_USER_PARTNER).select_related(
            "partner_link_accumulated",
            "partner_link_accumulated__partner__additionalinfo",
            "partner_link_accumulated__partner__user",
            "campaign",
        ).annotate(
            **annotates,
        ).filter(
            *filters,
        ).order_by(
            sort_by,
        )

        fx_partner = FxPartner.objects.all().order_by("-created_at").first()
        percentages = LevelPercentageBase.objects.all().order_by("-created_at").first()

        links_pag = self.paginate_queryset(
            queryset=links,
            request=request,
            view=self,
        )

        links_serializer = LinkTableSer(
            instance=links_pag,
            context={
                "fx_partner": fx_partner,
                "percentages": percentages,
            },
            many=True,
        )

        return Response(
            data={
                "links": links_serializer.data,
            }, headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """ Create an link in DB based an campaign """

        validator = Validator({
            'campaign': {
                'required': True,
                'type': 'integer'
            },
            'links': {
                'required': True,
                'type': 'list',
                'schema': {
                    'type': 'dict',
                    'schema': {
                        'prom_code': {
                            'required': True,
                            'type': 'string'
                        },
                        'url': {
                            'required': True,
                            'type': 'string'
                        },
                        'status': {
                            'required': False,
                            'type': 'integer',
                            'allowed': [
                                Link.Status.UNAVAILABLE,
                                Link.Status.AVAILABLE
                            ]
                        }
                    }
                }
            }
        })

        if not validator.validate(request.data):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        links_to_update = []
        links_to_create = []
        filters = (
            Q(id=request.data.get("campaign")),
        )
        # Get campaign
        campaign = Campaign.objects.filter(*filters).first()
        for data in request.data.get("links"):
            data['campaign'] = campaign
            filters = (
                Q(prom_code=data.get("prom_code")),
                Q(campaign__pk=campaign.pk),
            )
            link_to_update = Link.objects.filter(*filters).first()
            if (link_to_update):
                links_to_update.append(
                    Link(
                        pk=link_to_update.pk,
                        **data,
                    )
                )
            else:
                filters = (
                    Q(url=data.get("url")),
                )
                link_to_validate = Link.objects.filter(*filters).first()
                if link_to_validate:
                    return Response({
                        "error": settings.CONFLICT_CODE,
                        "details": {
                            "Link": [
                                "Link is already in this campaign",
                                data.get("url")
                            ]
                        }
                    }, status=status.HTTP_409_CONFLICT)
                links_to_create.append(Link(**data))

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        try:
            Link.objects.bulk_create(
                objs=links_to_create,
            )
            Link.objects.bulk_update(
                objs=links_to_update,
                fields=[
                    "url",
                ],
            )
            for link in links_to_create:
                BetenlaceCPA.objects.create(
                    currency_condition=link.campaign.currency_condition,
                    currency_fixed_income=link.campaign.currency_fixed_income,
                    link=link
                )
            recalculate_temperature(campaign)
            return Response({
                "msg": "Links were created succesfully"
            }, status=status.HTTP_201_CREATED)
        except IntegrityError as e:
            # transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)
            # exc_type, exc_value, exc_traceback = sys.exc_info()
            # e = traceback.format_exception(
            #     exc_type, exc_value, exc_traceback)
            logger.error((
                "Something is wrong when try create an link"
                f"check traceback:\n\n{e}"
            ))
            e = str(e)
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "url": [
                        e[e.index("DETAIL:"):]
                    ]
                }
            },
                status=status.HTTP_403_FORBIDDEN,
            )
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error((
                "Something is wrong when try create an link"
                f"check traceback:\n\n{''.join(e)}"
            ))
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "non_fields_errors": [
                        _("Partner dont exists in DB")
                    ]
                }
            },
                status=status.HTTP_403_FORBIDDEN,
            )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        validator = Validator(
            schema={
                "links": {
                    "required": True,
                    "type": "list",
                    "schema": {
                        "type": "dict",
                        "schema": {
                            "id": {
                                "required": True,
                                "type": "integer",
                            },
                            "url": {
                                "required": False,
                                "type": "string",
                            },
                            "status": {
                                "required": False,
                                "type": "integer",
                                "allowed": [
                                    Link.Status.UNAVAILABLE,
                                    Link.Status.AVAILABLE,
                                ],
                            },
                            "prom_code": {
                                "required": False,
                                "type": "string",
                            },
                            "percentage_cpa": {
                                "required": False,
                                "type": "float",
                            },
                            "tracker": {
                                "required": False,
                                "type": "float",
                            },
                            "tracker_deposit": {
                                "required": False,
                                "type": "float",
                            },
                            "tracker_registered_count": {
                                "required": False,
                                "type": "float",
                            },
                            "tracker_first_deposit_count": {
                                "required": False,
                                "type": "float",
                            },
                            "tracker_wagering_count": {
                                "required": False,
                                "type": "float",
                            },
                        },
                    },
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        level = LevelPercentageBase.objects.order_by("-created_at").first()

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        for data in request.data.get("links"):
            link = Link.objects.select_related(
                "partner_link_accumulated",
                "partner_link_accumulated__campaign",
            ).filter(
                id=data.get("id"),
            ).first()

            if link is not None:
                link_ser = LinkUpdateSer(instance=link, data=data, partial=True)

                if (link_ser.is_valid()):
                    link_ser.save()
                else:
                    transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)

                    return Response(
                        data={
                            "error": settings.SERIALIZER_ERROR_CODE,
                            "detail": link_ser.errors,
                        },
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                # This is a BAD procedure
                partner_accumulated = link.partner_link_accumulated

                # validate incoming data, then update percentage_cpa and change bool

                if (partner_accumulated is not None):
                    partner_accum_ser = PartnerLinkAccumManageLinkSer(
                        instance=partner_accumulated,
                        data=data,
                        partial=True,
                        context={
                            "level": level,
                        }
                    )
                    if (partner_accum_ser.is_valid()):
                        partner_accum_ser.save()
                        create_history(
                            instance=partner_accumulated,
                            update_reason=PartnerAccumUpdateReasonCHO.ADVISER_ASSIGN,
                            adviser=request.user.id,
                        )
                    else:
                        transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)
                        return Response(
                            data={
                                "error": settings.SERIALIZER_ERROR_CODE,
                                "detail": partner_accum_ser.errors,
                            },
                            status=status.HTTP_400_BAD_REQUEST,
                        )
            else:
                transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)
                return Response(
                    data={
                        "error": settings.NOT_FOUND_CODE,
                        "detail": {
                            "links": [
                                _("Link not found"),
                                link.id,
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
        recalculate_temperature(link.campaign)
        return Response(
            data={
                "msg": "Link were updated succesfully"
            },
            status=status.HTTP_200_OK,
        )


class LinkByCampaign(APIView, GetAllLinks):

    """
        Class view with resource to management links by campaign
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView
    ]

    def get(self, request):
        """
            Returning links filter by campaign_id

            #Body
           -  campaign_id : "int"
                Param to identify campaign
           -  lim : "int"
           -  offs : "int"


        """
        validator = Validator(
            schema={
                "campaign_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "lim": {
                    "required": False,
                },
                "offs": {
                    "required": False,
                },
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(id=validator.document.get("campaign_id")),
        )
        campaign = Campaign.objects.filter(*filters).first()

        if campaign is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "campaign": [
                            _("Campaign not found"),
                            validator.document.get("campaign_id"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        annotates = {
            "bookmaker_name": F("campaign__bookmaker__name"),
            "campaign_title": Concat(
                "campaign__bookmaker__name",
                Value(" "),
                "campaign__title",
            ),
        }

        filters = (
            Q(campaign=campaign),
        )
        links = Link.objects.select_related(
            "partner_link_accumulated",
            "partner_link_accumulated__partner__additionalinfo",
            "partner_link_accumulated__partner__user",
            "campaign",
        ).annotate(
            **annotates,
        ).filter(
            *filters,
        )

        fx_partner = FxPartner.objects.all().order_by("-created_at").first()
        percentages = LevelPercentageBase.objects.all().order_by("-created_at").first()

        links_pag = self.paginate_queryset(
            queryset=links,
            request=request,
            view=self,
        )
        links_serializer = LinkTableSer(
            instance=links_pag,
            many=True,
            context={
                "fx_partner": fx_partner,
                "percentages": percentages,
            },
        )

        return Response(
            data={
                "links": links_serializer.data,
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )


CODENAME_LINK_ASSIGN_GET = "link assign api-get"


class LinkAssignAPI(APIView):

    """
        Class view to allow assign an link to partner
    """

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
            Return partners to assign or remove links

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
        # DB routing for Default models to Partner
        request_cfg.is_partner = True

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
                filters.append(
                    Q(user_id=user.id),
                )
            else:
                filters.append(
                    Q(user_id=None),
                )

        if 'identification_number' in validator.document:
            filters.append(
                Q(additionalinfo__identification__istartswith=validator.document.get("identification_number")),
            )

        if 'identification_type' in validator.document:
            filters.append(
                Q(additionalinfo__identification_type=validator.document.get("identification_type")),
            )

        if 'partner_id' in validator.document:
            filters.append(
                Q(user__id=validator.document.get("partner_id")),
            )

        if 'full_name' in validator.document:
            filters.append(
                Q(full_name__icontains=validator.document.get("full_name")),
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

        partner_serializer = ParnertAssignSer(instance=partner, many=True)

        return Response(
            data={
                "count": partner.count(),
                "partners": partner_serializer.data,
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request):
        """ Assing Link """
        validator = Validator(
            schema={
                "id_link": {
                    "required": True,
                    "type": "integer",
                },
                "id_partner": {
                    "required": True,
                    "type": "integer",
                },
            },
        )

        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(user_id=validator.document.get("id_partner")),
        )
        partner = Partner.objects.using(DB_USER_PARTNER).filter(*filters).first()

        if partner is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "partner": [
                            _("Partner not found"),
                            validator.document.get("id_partner"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(id=validator.document.get("id_link")),
        )
        link = Link.objects.using(DB_USER_PARTNER).select_related(
            "partner_link_accumulated",
            "campaign",
        ).filter(*filters).first()

        if link is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "link": [
                            _("Link not found"),
                            validator.document.get("id_link"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Verify if already have a user with partner link on same link
        if (link.partner_link_accumulated is not None):
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "link": [
                            _("Link already assigned"),
                            validator.document.get("id_link"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        campaign = link.campaign

        filters = (
            Q(partner=partner),
            Q(campaign_id=campaign.pk),
        )
        partner_link = PartnerLinkAccumulated.objects.using(DB_USER_PARTNER).filter(*filters).first()

        if partner_link is not None and partner_link.is_assigned:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "link": [
                            _("This user already have an link"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))
        level = LevelPercentageBase.objects.order_by("-created_at").first()
        partner.level

        # Update case
        if partner_link is not None:

            link.partner_link_accumulated = partner_link
            link.status = Link.Status.ASSIGNED
            link.save()

            partner_link.prom_code = link.prom_code
            partner_link.is_assigned = True
            partner_link.currency_fixed_income = campaign.currency_fixed_income

            # Currency local for all partners will now USD always
            partner_link.currency_local = CurrencyAll.USD
            partner_link.assigned_at = today

            # Default percentages
            level_percentage = level.percentages.get(str(partner.level))
            partner_link.percentage_cpa = campaign.default_percentage * level_percentage
            partner_link.is_percentage_custom = False
            partner_link.partner_level = partner.level
            if partner.level == PartnerLevelCHO.BASIC:
                partner_link.tracker = 1
                partner_link.tracker_deposit = 1
                partner_link.tracker_registered_count = 1
                partner_link.tracker_first_deposit_count = 1
                partner_link.tracker_wagering_count = 1
            else:
                partner_link.tracker = campaign.tracker
                partner_link.tracker_deposit = campaign.tracker_deposit
                partner_link.tracker_registered_count = campaign.tracker_registered_count
                partner_link.tracker_first_deposit_count = campaign.tracker_first_deposit_count
                partner_link.tracker_wagering_count = campaign.tracker_wagering_count

            partner_link.save()

        # Create case
        else:
            if partner.level is None:
                level_percentage = level.percentages.get("0")
                partner.level = 0

            else:
                level_percentage = level.percentages.get(str(partner.level))

            if not partner.level == PartnerLevelCHO.BASIC:
                partner_link = PartnerLinkAccumulated.objects.create(
                    partner=partner,
                    campaign=campaign,
                    prom_code=link.prom_code,
                    is_assigned=True,
                    cpa_count=0,
                    fixed_income=0,
                    currency_fixed_income=campaign.currency_fixed_income,
                    fixed_income_local=0,

                    # Currency local for all partners will now USD always
                    currency_local=CurrencyAll.USD,

                    # Default percentages

                    percentage_cpa=campaign.default_percentage * level_percentage,
                    is_percentage_custom=False,
                    partner_level=partner.level,

                    tracker=campaign.tracker,
                    tracker_deposit=campaign.tracker_deposit,
                    tracker_registered_count=campaign.tracker_registered_count,
                    tracker_first_deposit_count=campaign.tracker_first_deposit_count,
                    tracker_wagering_count=campaign.tracker_wagering_count,

                    assigned_at=today,
                )

            else:
                partner_link = PartnerLinkAccumulated.objects.create(
                    partner=partner,
                    campaign=campaign,
                    prom_code=link.prom_code,
                    is_assigned=True,
                    cpa_count=0,
                    fixed_income=0,
                    currency_fixed_income=campaign.currency_fixed_income,
                    fixed_income_local=0,

                    # Currency local for all partners will now USD always
                    currency_local=CurrencyAll.USD,

                    # Default percentages

                    percentage_cpa=campaign.default_percentage * level_percentage,
                    is_percentage_custom=False,
                    partner_level=partner.level,

                    tracker=1,
                    tracker_deposit=1,
                    tracker_registered_count=1,
                    tracker_first_deposit_count=1,
                    tracker_wagering_count=1,

                    assigned_at=today,
                )

        link.partner_link_accumulated = partner_link
        link.status = Link.Status.ASSIGNED
        link.save()

        create_history(
            instance=partner_link,
            update_reason=PartnerAccumUpdateReasonCHO.PARTNER_REQUEST,
            adviser=None,
        )

        # Get today betenlace daily of determinated link
        filters = (
            Q(created_at=today.date()),
            Q(betenlace_cpa_id=link.id),
        )
        betenlace_daily = BetenlaceDailyReport.objects.filter(*filters).first()

        # Data of betenlace on same day is only with clicks, force relation
        if (betenlace_daily is not None):
            # if betenlace daily has a partner link daily delete
            if hasattr(betenlace_daily, 'partnerlinkdailyreport'):
                betenlace_daily.partnerlinkdailyreport.delete()

            PartnerLinkDailyReport.objects.create(
                partner_link_accumulated=partner_link,
                betenlace_daily_report=betenlace_daily,
                adviser_id=partner_link.partner.adviser_id,
                currency_fixed_income=campaign.currency_fixed_income,
                # Currency local for all partners is USD
                currency_local=partner_link.currency_local,
                created_at=today.date(),
            )

        recalculate_temperature(campaign)
        return Response(
            data={
                "msg": "Link was assigned",
            },
            status=status.HTTP_200_OK,
        )


class LinkUnassignAPI(APIView):
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """ Unassing Link """
        validator = Validator(
            schema={
                "id_link": {
                    "required": True,
                    "type": "integer",
                },
                "status": {
                    "required": True,
                    "type": "integer",
                    "allowed": [
                        1,
                        3,
                    ],
                },
            },
        )

        if not validator.validate(document=request.data):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (
            Q(id=validator.document.get("id_link")),
        )
        link = Link.objects.filter(*filters).first()
        if link is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "link": [
                            "Link not found",
                            request.data.get("id_link"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        partner_link_accumulated = link.partner_link_accumulated

        if partner_link_accumulated is not None:
            # Mark flag of partner link accumulated to False
            partner_link_accumulated.is_assigned = False
            partner_link_accumulated.save()
            create_history(
                instance=partner_link_accumulated,
                update_reason=PartnerAccumUpdateReasonCHO.ADVISER_UNASSIGN,
                adviser=request.user.id,
            )

            # Delete data of partner link daily with created at today
            # Get current time in Time zone of settings
            today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))
            filters = (
                Q(created_at=today.date()),
                Q(partner_link_accumulated=partner_link_accumulated),
            )
            # Delete Partner Link daily report of today
            PartnerLinkDailyReport.objects.filter(*filters).delete()

            link.partner_link_accumulated = None
            link.status = validator.document.get("status")
            link.save()

            recalculate_temperature(link.campaign)

            transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "msg": "Links was unassigned succesfully",
                },
                status=status.HTTP_200_OK,
            )

        return Response(
            data={
                "error": settings.BAD_REQUEST_CODE,
                "detail": {
                    "link": [
                        _("Link does not have an partner_link associated"),
                        link.id,
                    ],
                },
            },
            status=status.HTTP_400_BAD_REQUEST,
        )


class LinksAdviserPartner(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ Return links that an partner has """
        validator = Validator(
            schema={
                "partner_id": {
                    "required": False,
                    "type": "string",
                },
                "bookmaker": {
                    "required": False,
                    "type": "string",
                },
                "campaign": {
                    "required": False,
                    "type": "string",
                },
                "percentage_cpa": {
                    "required": False,
                    "type": "string",
                },
                "tracker": {
                    "required": False,
                    "type": "string",
                },
                "sort_by": {
                    "required": True,
                    "type": "string",
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

        filters = []
        if 'partner_id' in request.query_params:
            partner = Partner.objects.filter(
                user_id=request.query_params.get("partner_id")
            ).first()
            if not partner:
                return Response(
                    data={
                        "error": settings.NOT_FOUND_CODE,
                        "details": {
                            "partner": [
                                "Partner not found",
                                request.query_params.get("partner_id"),
                            ],
                        },
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )
            filters.append(
                Q(partner_link_accumulated__partner=partner)
            )

        if 'bookmaker' in request.query_params:
            bookmaker = Bookmaker.objects.filter(
                id=request.query_params.get("bookmaker")
            ).first()
            if not bookmaker:
                return Response({
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "bookmaker": [
                            "Bookmaker not found",
                            request.query_params.get("bookmaker"),
                        ],
                    },
                },
                    status=status.HTTP_404_NOT_FOUND,
                )
            filters.append(
                Q(campaign__bookmaker=bookmaker),
            )

        if 'campaign' in request.query_params:
            campaign = Campaign.objects.filter(
                id=request.query_params.get("bookmaker")
            ).first()
            if not campaign:
                return Response({
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "campaign": [
                            "Campaign not found",
                            request.query_params.get("bookmaker"),
                        ],
                    },
                },
                    status=status.HTTP_404_NOT_FOUND,
                )
            filters.append(
                Q(campaign=campaign),
            )

        if 'percentage_cpa' in request.query_params:
            filters.append(
                Q(partner_link_accumulated__percentage_cpa__lte=request.query_params.get("percentage_cpa")),
            )

        if 'tracker' in request.query_params:
            filters.append(
                Q(partner_link_accumulated__tracker=request.query_params.get("tracker")),
            )

        links = Link.objects.using(DB_USER_PARTNER).filter(*filters).order_by(request.query_params.get("sort_by"))

        return Response(
            data={
                "links": LinkAdviserPartnerSerializer(links, many=True).data,
            },
            status=status.HTTP_200_OK,
        )
