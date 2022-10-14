import pytz
from api_admin.models import SearchPartnerLimit
from api_admin.paginators import GetAllCpas
from api_admin.serializers import (
    ClicksManagementSerializer,
    PartnerMemeberReportSerializer,
)
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerAccumStatusCHO,
)
from api_partner.models import (
    BetenlaceDailyReport,
    FxPartner,
    Link,
    Partner,
    PartnerLinkAccumulated,
    PartnerLinkDailyReport,
)
from api_partner.models.reports_management.betenlace_daily_report import (
    BetenlaceDailyReport,
)
from api_partner.models.reports_management.campaign import Campaign
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    timezone_customer,
)
from core.helpers.path_route_db import request_cfg
from core.models import User
from django.conf import settings
from django.db import transaction
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.utils import timezone
from django.utils.timezone import (
    datetime,
    timedelta,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class CpaManagementAPI(APIView, GetAllCpas):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        request_cfg.is_partner = True
        """ Returning all data from click tracking """
        validator = Validator(
            schema={
                'campaign': {
                    'required': False,
                    'type': 'string',
                },
                'prom_code': {
                    'required': False,
                    'type': 'string',
                },
                'partner': {
                    'required': False,
                    'type': 'string',
                },
                'only_partner': {
                    'required': False,
                    'type': 'string',
                },
                'lim': {
                    'required': False,
                    'type': 'string',
                },
                'offs': {
                    'required': False,
                    'type': 'string',
                },
                'sort_by': {
                    'required': False,
                    'type': 'string',
                },
            },
        )

        admin = request.user

        if not validator.validate(document=request.query_params):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        codename_get = "cpa management api-get"
        filters = (
            Q(rol=admin.rol),
            Q(codename=codename_get),
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
            filters_partners = (
                Q(adviser_id=admin.pk),
            )
            partners = Partner.objects.using(DB_USER_PARTNER).filter(*filters_partners)
            filters.append(Q(partner_link_accumulated__partner__in=partners))

        if 'campaign' in request.query_params:
            filters.append(Q(campaign_title__icontains=request.query_params.get("campaign")))

        if 'prom_code' in request.query_params:
            filters.append(Q(prom_code__icontains=request.query_params.get("prom_code")))

        if 'partner' in request.query_params:
            filters.append(Q(partner_link_accumulated__partner__user__id=request.query_params.get("partner")))

        if 'only_partner' in request.query_params:
            filters.append(~Q(partner_link_accumulated=None))

        sort_by = "-created_at"
        if 'sort_by' in request.query_params:
            sort_by = request.query_params.get("sort_by")

        links = Link.objects.annotate(
            campaign_title=Concat(
                "campaign__bookmaker__name",
                Value(" "),
                "campaign__title",
            ),
        ).filter(
            *filters,
        ).order_by(sort_by)

        links_pag = self.paginate_queryset(links, request, view=self)

        clicks_management = ClicksManagementSerializer(instance=links_pag, many=True)

        return Response(
            data={
                "clicks": clicks_management.data,
            },
            headers={
                "count": self.count,
                "access-control-expose-headers": "count,next,previous",
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def put(self, request):
        """ Updating or creating CPA partner """
        validator = Validator(
            schema={
                'data': {
                    'required': True,
                    'type': 'list',
                    'schema': {
                        'type': 'dict',
                        'schema': {
                            'id_link': {
                                'required': True,
                                'type': 'integer',
                            },
                            'cpa_partner': {
                                'required': True,
                                'type': 'integer',
                            },
                            'cpa_betenlace': {
                                'required': True,
                                'type': 'integer',
                            },
                            'registered_count': {
                                'required': True,
                                'type': 'integer',
                            },
                            'deposit': {
                                'required': True,
                                'type': 'float',
                            },
                            'stake': {
                                'required': True,
                                'type': 'float',
                            },
                            'revenue_share': {
                                'required': True,
                                'type': 'float',
                            },
                            'first_deposit_count': {
                                'required': True,
                                'type': 'integer',
                            },
                            'wagering_count': {
                                'required': True,
                                'type': 'integer',
                            },
                            'net_revenue': {
                                'required': True,
                                'type': 'float',
                            },
                            "deposit_partner": {
                                "required": True,
                                "type": "float",
                            },
                            "registered_count_partner": {
                                "required": True,
                                "type": "integer",
                            },
                            "first_deposit_count_partner": {
                                "required": True,
                                "type": "integer",
                            },
                            "wagering_count_partner": {
                                "required": True,
                                "type": "integer",
                            },
                        },
                    },
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

        today = timezone_customer(timezone.now()).date() - timedelta(days=1)
        fx_created_at = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE)) - timedelta(days=1)
        fx_created_at = fx_created_at.replace(minute=0, hour=0, second=0, microsecond=0)

        # Get the last Fx value
        filters = (
            Q(created_at__gte=fx_created_at),
        )
        tax_fx_today = FxPartner.objects.filter(*filters).order_by("created_at").first()

        if(tax_fx_today is None):
            # Get just next from supplied date
            filters = (
                Q(created_at__lte=fx_created_at),
            )
            tax_fx_today = FxPartner.objects.filter(*filters).order_by("-created_at").first()

        if not tax_fx_today:
            return Response(
                data={
                    "error": settings.INTERNAL_SERVER_ERROR,
                    "details": {
                        "not_field_erros": [
                            _("tax fx is not in DB"),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        fx_partner_percentage = tax_fx_today.fx_percentage

        codename_put = "cpa management api-get"

        admin = request.user
        searchpartnerlimit = SearchPartnerLimit.objects.filter(Q(rol=admin.rol), Q(codename=codename_put)).first()
        if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED)\
                and not admin.is_superuser:
            partners_ids = Partner.objects.using(DB_USER_PARTNER).filter(
                Q(adviser_id=admin.pk)
            ).values_list(
                "user__id",
                flat=True,
            )
        else:
            partners_ids = Partner.objects.using(DB_USER_PARTNER).all().values_list("user__id", flat=True)

        for data_cpas in request.data.get("data"):
            if "cpa_partner" in data_cpas:
                if not "cpa_betenlace" in data_cpas:
                    return Response(
                        data={
                            "error": settings.BAD_REQUEST_CODE,
                            "details": {
                                "not_field_erros": [
                                    "cpa_betenlace field is required",
                                ],
                            },
                        },
                        status=status.HTTP_400_BAD_REQUEST
                    )

            if data_cpas.get("cpa_betenlace") < data_cpas.get("cpa_partner"):
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "details": {
                            "not_field_erros": [
                                "cpa_betenlace must be greater than cpa_partner",
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get link object
            link = Link.objects.filter(id=data_cpas.get("id_link")).first()
            if not link:
                return Response(
                    data={
                        "error": settings.NOT_FOUND_CODE,
                        "details": {
                            "not_field_erros": [
                                "Link not found",
                                data_cpas.get("id_link"),
                            ],
                        },
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

            # Get partner user_id that is related with this link and validate if admin can edit data
            if link.partner_link_accumulated:
                partner_id = link.partner_link_accumulated.partner.user_id

                if not partner_id in partners_ids:
                    return Response(
                        data={
                            "error": settings.BAD_REQUEST_CODE,
                            "details": {
                                "not_field_erros": [
                                    _(f"Adviser dont has permission to edit cpas from {partner_id} user"),
                                ],
                            },
                        },
                        status=status.HTTP_404_NOT_FOUND,
                    )

            last_link = link.betenlacecpa.Betenlacedailyreport_to_BetenlaceCPA.filter(created_at=today).first()
            last_inactive_at = link.campaign.last_inactive_at.date()
            exception = None
            if last_link:
                # Create or update
                if last_link.created_at == today:
                    """ Update """
                    status_res, exception = self._update_data(
                        bet_daily=last_link,
                        data=data_cpas,
                        tax_fx_today=tax_fx_today,
                        last_inactive_at=last_inactive_at,
                        today=today,
                    )
                else:
                    """ Create """
                    status_res, exception = self._create_data(
                        link=link,
                        data=data_cpas,
                        tax_fx_today=tax_fx_today,
                        last_inactive_at=last_inactive_at,
                        today=today,
                    )
            else:
                # Create
                status_res, exception = self._create_data(
                    link=link,
                    data=data_cpas,
                    tax_fx_today=tax_fx_today,
                    last_inactive_at=last_inactive_at,
                    today=today,
                )

            if not status_res:
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "details": {
                            "not_field_erros": [
                                exception,
                            ],
                        },
                    },
                    status=status.HTTP_404_NOT_FOUND,
                )

        return Response(
            data={
                "msg": "Cpas updated succesfully",
            },
            status=status.HTTP_200_OK,
        )

    def _update_data(
        self,
        bet_daily,
        data,
        tax_fx_today,
        last_inactive_at,
        today,
    ):
        fx_partner_percentage = tax_fx_today.fx_percentage

        # BetenlaceDaily
        cpa_daily_registered_count = bet_daily.registered_count or 0
        cpa_daily_before_cpa_count = bet_daily.cpa_count or 0
        cpa_daily_fixed_income_before = bet_daily.fixed_income or 0
        cpa_daily_deposit_before = bet_daily.deposit or 0
        cpa_daily_stake_before = bet_daily.stake or 0
        cpa_daily_revenue_share_before = bet_daily.revenue_share or 0
        cpa_daily_first_deposit_count_before = bet_daily.first_deposit_count or 0
        cpa_daily_wagering_count_before = bet_daily.wagering_count or 0
        cpa_daily_net_revenue = bet_daily.net_revenue or 0

        bet_daily.cpa_count = data.get("cpa_betenlace")
        bet_daily.registered_count = data.get("registered_count")
        bet_daily.fixed_income_unitary = bet_daily.betenlace_cpa.link.campaign.fixed_income_unitary
        bet_daily.fixed_income = (bet_daily.fixed_income_unitary * data.get("cpa_betenlace"))
        bet_daily.deposit = data.get("deposit")
        bet_daily.stake = data.get("stake")
        bet_daily.revenue_share = data.get("revenue_share")
        bet_daily.first_deposit_count = data.get("first_deposit_count")
        bet_daily.wagering_count = data.get("wagering_count")
        bet_daily.net_revenue = data.get("net_revenue")
        bet_daily.fx_partner = tax_fx_today
        bet_daily.save()

        betenlace_cpa = bet_daily.betenlace_cpa
        betenlace_cpa.cpa_count += (data.get("cpa_betenlace") - cpa_daily_before_cpa_count)
        betenlace_cpa.registered_count += (data.get("registered_count") - cpa_daily_registered_count)
        betenlace_cpa.fixed_income += (bet_daily.fixed_income - cpa_daily_fixed_income_before)
        betenlace_cpa.deposit += (data.get("deposit") - cpa_daily_deposit_before)
        betenlace_cpa.stake += (data.get("stake") - cpa_daily_stake_before)
        betenlace_cpa.revenue_share += (data.get("revenue_share") - cpa_daily_revenue_share_before)
        betenlace_cpa.first_deposit_count += (
            data.get("first_deposit_count") - cpa_daily_first_deposit_count_before
        )

        betenlace_cpa.wagering_count += (data.get("wagering_count") - cpa_daily_wagering_count_before)
        betenlace_cpa.net_revenue += (data.get("net_revenue") - cpa_daily_net_revenue)
        betenlace_cpa.save()

        if hasattr(bet_daily, 'partnerlinkdailyreport'):  # verificar si hay un partner link asociado
            partner_report = bet_daily.partnerlinkdailyreport

            partner_daily_before_cpa_count = partner_report.cpa_count or 0
            partner_daily_fixed_income_before = partner_report.fixed_income or 0
            partner_daily_fixed_income_local__before = partner_report.fixed_income_local or 0

            partner_report.fixed_income_unitary = (
                bet_daily.betenlace_cpa.link.campaign.fixed_income_unitary *
                partner_report.partner_link_accumulated.percentage_cpa
            )

            partner_accumulated = partner_report.partner_link_accumulated

            partner_report.fixed_income = partner_report.fixed_income_unitary * data.get("cpa_partner")
            partner_report.cpa_count = data.get("cpa_partner")
            partner_report.percentage_cpa = partner_accumulated.percentage_cpa

            partner_report.deposit = data.get("deposit_partner")
            partner_report.registered_count = data.get("registered_count_partner")
            partner_report.first_deposit_count = data.get("first_deposit_count_partner")
            partner_report.wagering_count = data.get("wagering_count_partner")

            partner_report.tracker = partner_accumulated.tracker
            partner_report.tracker_deposit = partner_accumulated.tracker_deposit
            partner_report.tracker_registered_count = partner_accumulated.tracker_registered_count
            partner_report.tracker_first_deposit_count = partner_accumulated.tracker_first_deposit_count
            partner_report.tracker_wagering_count = partner_accumulated.tracker_wagering_count

            # Fx currency fixed income
            if partner_report.currency_local == partner_report.currency_fixed_income:
                partner_report.fx_book_local = 1
                partner_report.fixed_income_local = partner_report.fixed_income
                partner_report.fixed_income_unitary_local = partner_report.fixed_income_unitary
            else:
                partner_report.fx_book_local = eval(
                    f"tax_fx_today.fx_{partner_report.currency_fixed_income.lower()}_{partner_report.currency_local.lower()}"
                ) * fx_partner_percentage
                partner_report.fixed_income_unitary_local = (
                    partner_report.fixed_income_unitary * partner_report.fx_book_local
                )
                partner_report.fixed_income_local = partner_report.fixed_income_unitary_local * data.get(
                    "cpa_partner")

            # Fx Currency condition
            if partner_report.currency_local == bet_daily.currency_condition:
                partner_report.fx_book_net_revenue_local = 1
            else:
                partner_report.fx_book_net_revenue_local = eval(
                    f"tax_fx_today.fx_{bet_daily.currency_condition.lower()}_{partner_report.currency_local.lower()}"
                ) * fx_partner_percentage

            # Calculate Adviser payment with CURRENT related partner default data
            partner = partner_accumulated.partner
            partner_report.adviser_id = partner.adviser_id
            partner_report.fixed_income_adviser_percentage = partner.fixed_income_adviser_percentage
            partner_report.net_revenue_adviser_percentage = partner.net_revenue_adviser_percentage

            if (partner.fixed_income_adviser_percentage is None):
                partner_report.fixed_income_adviser = None
                partner_report.fixed_income_adviser_local = None
            else:
                partner_report.fixed_income_adviser = (
                    partner_report.fixed_income *
                    partner.fixed_income_adviser_percentage
                )
                partner_report.fixed_income_adviser_local = (
                    partner_report.fixed_income_adviser *
                    partner_report.fx_book_local
                )

            if (partner.net_revenue_adviser_percentage is None):
                partner_report.net_revenue_adviser = None
                partner_report.net_revenue_adviser_local = None
            else:
                partner_report.net_revenue_adviser = (
                    bet_daily.net_revenue * partner.net_revenue_adviser_percentage
                    if bet_daily.net_revenue is not None
                    else
                    0
                )
                partner_report.net_revenue_adviser_local = (
                    partner_report.net_revenue_adviser * partner_report.fx_book_net_revenue_local
                )

            partner_report.referred_by = partner.referred_by
            partner_report.fixed_income_referred_percentage = partner.fixed_income_referred_percentage
            partner_report.net_revenue_referred_percentage = partner.net_revenue_referred_percentage

            if (partner.fixed_income_referred_percentage is None):
                partner_report.fixed_income_referred = None
                partner_report.fixed_income_referred_local = None
            else:
                partner_report.fixed_income_referred = (
                    partner_report.fixed_income *
                    partner.fixed_income_referred_percentage
                )
                partner_report.fixed_income_referred_local = (
                    partner_report.fixed_income_referred *
                    partner_report.fx_book_local
                )

            if (partner.net_revenue_referred_percentage is None):
                partner_report.net_revenue_referred = None
                partner_report.net_revenue_referred_local = None
            else:
                partner_report.net_revenue_referred = (
                    bet_daily.net_revenue * partner.net_revenue_referred_percentage
                    if bet_daily.net_revenue is not None
                    else
                    0
                )
                partner_report.net_revenue_referred_local = (
                    partner_report.net_revenue_referred * partner_report.fx_book_net_revenue_local
                )

            partner_report.save()

            partner_accumulated.cpa_count += (data.get("cpa_partner") - partner_daily_before_cpa_count)
            partner_accumulated.fixed_income += (partner_report.fixed_income - partner_daily_fixed_income_before)
            partner_accumulated.fixed_income_local += (
                partner_report.fixed_income_local - partner_daily_fixed_income_local__before
            )
            partner_accumulated.save()
        else:
            partner_accumulated = bet_daily.betenlace_cpa.link.partner_link_accumulated
            if partner_accumulated:
                # Validate if link has relationship with partner and if has verify if status is equal to status campaign
                if partner_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                    # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                    if(partner_accumulated.campaign.status == Campaign.Status.INACTIVE) and (today >= partner_accumulated.campaign.last_inactive_at.date()):
                        return True, None
                elif (partner_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                    return True, None

                fixed_income_unitary = (
                    bet_daily.betenlace_cpa.link.campaign.fixed_income_unitary *
                    partner_accumulated.percentage_cpa
                )
                fixed_income = fixed_income_unitary * data.get("cpa_partner")

                data_to_add = {
                    "partner_link_accumulated": partner_accumulated,
                    "betenlace_daily_report": bet_daily,
                    "fixed_income_unitary": fixed_income_unitary,
                    "fixed_income": fixed_income,
                    "currency_local": partner_accumulated.currency_local,
                    "currency_fixed_income": bet_daily.betenlace_cpa.link.campaign.currency_fixed_income,
                    "cpa_count": data.get("cpa_partner"),
                    "deposit": data.get("deposit_partner"),
                    "registered_count": data.get("registered_count_partner"),
                    "first_deposit_count": data.get("first_deposit_count_partner"),
                    "wagering_count": data.get("wagering_count_partner"),
                    "tracker": partner_accumulated.tracker,
                    "tracker_deposit": partner_accumulated.tracker_deposit,
                    "tracker_registered_count": partner_accumulated.tracker_registered_count,
                    "tracker_first_deposit_count": partner_accumulated.tracker_first_deposit_count,
                    "tracker_wagering_count": partner_accumulated.tracker_wagering_count,
                    "percentage_cpa": partner_accumulated.percentage_cpa,
                    "created_at": today,
                }

                # Fx Currency Fixed income
                tax_new = tax_fx_today
                currency_fixed_income = data_to_add.get("currency_fixed_income")
                currency_local = data_to_add.get("currency_local")
                if data_to_add.get("currency_local") == data_to_add.get("currency_fixed_income"):
                    data_to_add["fx_book_local"] = 1
                    data_to_add["fixed_income_local"] = fixed_income
                    data_to_add["fixed_income_unitary_local"] = fixed_income_unitary
                else:
                    tax_fx_today_new = eval(f"tax_new.fx_{currency_fixed_income.lower()}_{currency_local.lower()}")
                    data_to_add["fx_book_local"] = tax_fx_today_new * fx_partner_percentage
                    data_to_add["fixed_income_unitary_local"] = (
                        fixed_income_unitary * data_to_add["fx_book_local"]
                    )
                    data_to_add["fixed_income_local"] = data_to_add["fixed_income_unitary_local"] * \
                        data.get("cpa_partner")

                # Fx Currency Condition
                if data_to_add.get("currency_local") == bet_daily.currency_condition:
                    data_to_add["fx_book_net_revenue_local"] = 1
                else:
                    tax_fx_today_new_condition = eval(
                        f"tax_new.fx_{bet_daily.currency_condition.lower()}_{currency_local.lower()}")
                    data_to_add["fx_book_net_revenue_local"] = tax_fx_today_new_condition * fx_partner_percentage

                # Calculate Adviser payment
                partner = partner_accumulated.partner
                data_to_add["adviser_id"] = partner.adviser_id
                data_to_add["fixed_income_adviser_percentage"] = partner.fixed_income_adviser_percentage
                data_to_add["net_revenue_adviser_percentage"] = partner.net_revenue_adviser_percentage

                if (partner.fixed_income_adviser_percentage is None):
                    data_to_add["fixed_income_adviser"] = None
                    data_to_add["fixed_income_adviser_local"] = None
                else:
                    data_to_add["fixed_income_adviser"] = (
                        data_to_add.get("fixed_income") *
                        partner.fixed_income_adviser_percentage
                    )
                    data_to_add["fixed_income_adviser_local"] = (
                        data_to_add.get("fixed_income_adviser") *
                        data_to_add.get("fx_book_local")
                    )

                if (partner.net_revenue_adviser_percentage is None):
                    data_to_add["net_revenue_adviser"] = None
                    data_to_add["net_revenue_adviser_local"] = None
                else:
                    data_to_add["net_revenue_adviser"] = (
                        bet_daily.net_revenue * partner.net_revenue_adviser_percentage
                        if bet_daily.net_revenue is not None
                        else
                        0
                    )
                    data_to_add["net_revenue_adviser_local"] = (
                        data_to_add.get("net_revenue_adviser") * data_to_add.get("fx_book_net_revenue_local")
                    )

                 # Calculate referred payment
                partner = partner_accumulated.partner
                data_to_add["referred_by"] = partner.referred_by
                data_to_add["fixed_income_referred_percentage"] = partner.fixed_income_referred_percentage
                data_to_add["net_revenue_referred_percentage"] = partner.net_revenue_referred_percentage

                if (partner.fixed_income_referred_percentage is None):
                    data_to_add["fixed_income_referred"] = None
                    data_to_add["fixed_income_referred_local"] = None
                else:
                    data_to_add["fixed_income_referred"] = (
                        data_to_add.get("fixed_income") *
                        partner.fixed_income_referred_percentage
                    )
                    data_to_add["fixed_income_referred_local"] = (
                        data_to_add.get("fixed_income_referred") *
                        data_to_add.get("fx_book_local")
                    )

                if (partner.net_revenue_referred_percentage is None):
                    data_to_add["net_revenue_referred"] = None
                    data_to_add["net_revenue_referred_local"] = None
                else:
                    data_to_add["net_revenue_referred"] = (
                        bet_daily.net_revenue * partner.net_revenue_referred_percentage
                        if bet_daily.net_revenue is not None
                        else
                        0
                    )
                    data_to_add["net_revenue_referred_local"] = (
                        data_to_add.get("net_revenue_referred") * data_to_add.get("fx_book_net_revenue_local")
                    )

                PartnerLinkDailyReport.objects.create(**data_to_add)

                partner_accumulated.cpa_count += data.get("cpa_partner")
                partner_accumulated.fixed_income += data_to_add.get("fixed_income_local")
                partner_accumulated.fixed_income_local += data_to_add.get("fixed_income_unitary_local")
                partner_accumulated.save()

        return True, None

    def _create_data(
        self,
        link,
        data,
        tax_fx_today,
        last_inactive_at,
        today,
    ):
        fx_partner_percentage = tax_fx_today.fx_percentage
        # BetenlaceDaily
        data_to_create_betdaily = {
            "cpa_count": data.get("cpa_betenlace"),
            "registered_count": data.get("registered_count"),
            "fixed_income_unitary": link.campaign.fixed_income_unitary,
            "betenlace_cpa": link.betenlacecpa,
            "currency_condition": link.campaign.currency_condition,
            "currency_fixed_income": link.campaign.currency_fixed_income,
            "deposit": data.get("deposit"),
            "stake": data.get("stake"),
            "net_revenue": data.get("net_revenue"),
            "revenue_share": data.get("revenue_share"),
            "first_deposit_count": data.get("first_deposit_count"),
            "wagering_count": data.get("wagering_count"),
            "fx_partner": tax_fx_today,
            "created_at": today
        }
        data_to_create_betdaily["fixed_income"] = data_to_create_betdaily.get(
            "fixed_income_unitary") * data.get("cpa_betenlace")
        bet_daily = BetenlaceDailyReport.objects.create(**data_to_create_betdaily)

        # Update month of betenlace
        betenlace_cpa = link.betenlacecpa
        betenlace_cpa.cpa_count += data.get("cpa_betenlace")
        betenlace_cpa.registered_count += data.get("registered_count")
        betenlace_cpa.fixed_income += data_to_create_betdaily["fixed_income"]
        betenlace_cpa.deposit += data_to_create_betdaily["deposit"]
        betenlace_cpa.stake += data_to_create_betdaily["stake"]
        betenlace_cpa.net_revenue += data_to_create_betdaily["net_revenue"]
        betenlace_cpa.revenue_share += data_to_create_betdaily["revenue_share"]
        betenlace_cpa.first_deposit_count += data_to_create_betdaily["first_deposit_count"]
        betenlace_cpa.wagering_count += data_to_create_betdaily["wagering_count"]
        betenlace_cpa.save()

        partner_accumulated = link.partner_link_accumulated
        if partner_accumulated:

            # Validate if link has relationship with partner and if has verify if status is equal to status campaign
            if partner_accumulated.status == PartnerAccumStatusCHO.BY_CAMPAIGN:
                # Validate if campaign status is equal to INACTIVE and last inactive at is great tha
                if(partner_accumulated.campaign.status == Campaign.Status.INACTIVE) and (today >= partner_accumulated.campaign.last_inactive_at.date()):
                    return True, None
            elif (partner_accumulated.status == PartnerAccumStatusCHO.INACTIVE):
                return True, None

            fixed_income_unitary = link.campaign.fixed_income_unitary *\
                partner_accumulated.percentage_cpa
            fixed_income = fixed_income_unitary * data.get("cpa_partner")

            data_to_add = {
                "partner_link_accumulated": partner_accumulated,
                "betenlace_daily_report": bet_daily,
                "fixed_income_unitary": fixed_income_unitary,
                "fixed_income": fixed_income,
                "currency_local": partner_accumulated.currency_local,
                "currency_fixed_income": link.campaign.currency_fixed_income,
                "cpa_count": data.get("cpa_partner"),
                "percentage_cpa": partner_accumulated.percentage_cpa,

                "deposit": data.get("deposit_parter"),
                "registered_count": data.get("registered_count_partner"),
                "first_deposit_count": data.get("first_deposit_count_partner"),
                "wagering_count": data.get("wagering_count_partner"),

                "tracker": partner_accumulated.tracker,
                "tracker_deposit": partner_accumulated.tracker_deposit,
                "tracker_registered_count": partner_accumulated.tracker_registered_count,
                "tracker_first_deposit_count": partner_accumulated.tracker_first_deposit_count,
                "tracker_wagering_count": partner_accumulated.tracker_wagering_count,

                "created_at": today
            }
            currency_fixed_income = data_to_add.get("currency_fixed_income")
            currency_local = data_to_add.get("currency_local")

            # Fx currency fixed income
            tax_new = tax_fx_today
            if data_to_add.get("currency_local") == data_to_add.get("currency_fixed_income"):
                data_to_add["fx_book_local"] = 1
                data_to_add["fixed_income_local"] = fixed_income
                data_to_add["fixed_income_unitary_local"] = fixed_income_unitary
            else:
                tax_fx_today_new = eval(f"tax_new.fx_{currency_fixed_income.lower()}_{currency_local.lower()}")
                data_to_add["fx_book_local"] = tax_fx_today_new * fx_partner_percentage
                data_to_add["fixed_income_unitary_local"] = (
                    fixed_income_unitary * data_to_add["fx_book_local"]
                )
                data_to_add["fixed_income_local"] = data_to_add["fixed_income_unitary_local"] * \
                    data.get("cpa_partner")

            # Fx Currency condition
            if data_to_add.get("currency_local") == data_to_create_betdaily.get("currency_condition"):
                data_to_add["fx_book_net_revenue_local"] = 1
            else:
                data_to_add["fx_book_net_revenue_local"] = eval(
                    f"tax_fx_today.fx_{data_to_create_betdaily.get('currency_condition').lower()}_{data_to_add.get('currency_local').lower()}"
                ) * fx_partner_percentage

            # Calculate Adviser payment
            partner = partner_accumulated.partner
            data_to_add["adviser_id"] = partner.adviser_id
            data_to_add["fixed_income_adviser_percentage"] = partner.fixed_income_adviser_percentage
            data_to_add["net_revenue_adviser_percentage"] = partner.net_revenue_adviser_percentage

            if (partner.fixed_income_adviser_percentage is None):
                data_to_add["fixed_income_adviser"] = None
                data_to_add["fixed_income_adviser_local"] = None
            else:
                data_to_add["fixed_income_adviser"] = (
                    data_to_add.get("fixed_income") *
                    partner.fixed_income_adviser_percentage
                )
                data_to_add["fixed_income_adviser_local"] = (
                    data_to_add.get("fixed_income_adviser") *
                    data_to_add.get("fx_book_local")
                )

            if (partner.net_revenue_adviser_percentage is None):
                data_to_add["net_revenue_adviser"] = None
                data_to_add["net_revenue_adviser_local"] = None
            else:
                data_to_add["net_revenue_adviser"] = (
                    bet_daily.net_revenue * partner.net_revenue_adviser_percentage
                    if bet_daily.net_revenue is not None
                    else
                    0
                )
                data_to_add["net_revenue_adviser_local"] = (
                    data_to_add.get("net_revenue_adviser") * data_to_add.get("fx_book_net_revenue_local")
                )
            # Calculate referred payment
            partner = partner_accumulated.partner
            data_to_add["referred_by"] = partner.referred_by
            data_to_add["fixed_income_referred_percentage"] = partner.fixed_income_referred_percentage
            data_to_add["net_revenue_referred_percentage"] = partner.net_revenue_referred_percentage

            if (partner.fixed_income_referred_percentage is None):
                data_to_add["fixed_income_referred"] = None
                data_to_add["fixed_income_referred_local"] = None
            else:
                data_to_add["fixed_income_referred"] = (
                    data_to_add.get("fixed_income") *
                    partner.fixed_income_referred_percentage
                )
                data_to_add["fixed_income_referred_local"] = (
                    data_to_add.get("fixed_income_referred") *
                    data_to_add.get("fx_book_local")
                )

            if (partner.net_revenue_referred_percentage is None):
                data_to_add["net_revenue_referred"] = None
                data_to_add["net_revenue_referred_local"] = None
            else:
                data_to_add["net_revenue_referred"] = (
                    bet_daily.net_revenue * partner.net_revenue_referred_percentage
                    if bet_daily.net_revenue is not None
                    else
                    0
                )
                data_to_add["net_revenue_referred_local"] = (
                    data_to_add.get("net_revenue_referred") * data_to_add.get("fx_book_net_revenue_local")
                )

            PartnerLinkDailyReport.objects.create(**data_to_add)

            partner_accumulated.cpa_count += data.get("cpa_partner")
            partner_accumulated.fixed_income += data_to_add.get("fixed_income_local")
            partner_accumulated.fixed_income_local += data_to_add.get("fixed_income_unitary_local")
            partner_accumulated.save()
        return True, None


CODENAME_CPA_PARTNER = "cpa partners api-get"


class CpaPartnersAPI(APIView):

    """

    """

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        validator = Validator({
            'partner_id': {
                'required': False,
                'type': 'string'
            },
            'full_name': {
                'required': False,
                'type': 'string'
            },
            'email': {
                'required': False,
                'type': 'string'
            },
            'identification_type': {
                'required': False,
                'type': 'string'
            },
            'identification_number': {
                'required': False,
                'type': 'string'
            }
        })

        admin = request.user

        if not validator.validate(request.query_params):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            },
                status=status.HTTP_400_BAD_REQUEST
            )

        searchpartnerlimit = SearchPartnerLimit.objects.filter(
            Q(rol=admin.rol),
            Q(codename=CODENAME_CPA_PARTNER)
        ).first()

        filters = []

        if (not searchpartnerlimit or searchpartnerlimit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED)\
                and not admin.is_superuser:
            filters.append(Q(adviser_id=admin.pk))

        if 'email' in request.query_params:
            user = User.objects.db_manager(DB_USER_PARTNER).filter(
                email__icontains=request.query_params.get("email")
            ).values('id')
            if user:
                filters.append(
                    Q(user_id__in=user)
                )
            else:
                filters.append(
                    Q(user_id=None)
                )

        if 'identification_number' in request.query_params:
            filters.append(Q(additionalinfo__identification__istartswith=request.query_params.get(
                "identification_number")))

        if 'identification_type' in request.query_params:
            filters.append(Q(additionalinfo__identification_type=request.query_params.get(
                "identification_type")))

        if 'partner_id' in request.query_params:
            filters.append(
                Q(user__id=request.query_params.get("partner_id"))
            )

        if 'full_name' in request.query_params:
            filters.append(
                Q(full_name__icontains=request.query_params.get("full_name"))
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
            )
        ).filter(*filters)[:5]

        return Response({
            'count': partners.count(),
            "partners": PartnerMemeberReportSerializer(partners, many=True).data
        }, status=status.HTTP_200_OK)
