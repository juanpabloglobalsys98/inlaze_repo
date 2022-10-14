from api_admin.helpers import (
    CampaignsPaginator,
    ClickPaginator,
    PartnersForClicPaginator,
)
from api_admin.serializers import CampaignBasicSer
from api_log.helpers import DB_HISTORY
from api_log.models import ClickTracking
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import Campaign
from api_partner.serializers.authentication import (
    PartnerBillingDetailSerializer,
)
from api_partner.serializers.reports_management.click_tracking import (
    ClickTrackingSerializer,
)
from api_partner.serializers.reports_management.partner_accumulated import (
    PartnerLinkAccumulatedBasicSerializer,
)
from cerberus import Validator
from core.helpers import StandardErrorHandler
from core.models import User
from django.conf import settings
from django.db.models.query_utils import Q
from django.utils.timezone import (
    datetime,
    make_aware,
    timedelta,
)
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class PartnersForClicksAPI(APIView, PartnersForClicPaginator):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets an admin gets all partners to know who review their clicks
        """

        validator = Validator(
            {
                "email": {
                    "required": False,
                    "nullable": True,
                    "type": "string"
                },
                "identification": {
                    "required": False,
                    "nullable": True,
                    "type": "string"
                },
                "identification_type": {
                    "required": False,
                    "nullable": True,
                    "type": "string"
                },
                "full_name": {
                    "required": False,
                    "nullable": True,
                    "type": "string"
                },
                "user_id": {
                    "required": False,
                    "nullable": True,
                    "type": "integer",
                    "coerce": int
                },
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        validator.normalized(request.data)

        # filters
        user_id = request.query_params.get("user_id")
        email = request.query_params.get("email")
        identification = request.query_params.get("identification")
        identification_type = request.query_params.get("identification_type")
        full_name = request.query_params.get("full_name")

        filters = []
        if email:
            filters.append(Q(user__email__icontains=email))
        if identification:
            filters.append(Q(additionalinfo__identification__icontains=identification))
        if identification_type:
            filters.append(Q(additionalinfo__identification_type=identification_type))
        if full_name:
            filters.append(Q(full_name__icontains=full_name))
        if user_id:
            filters.append(Q(user_id=user_id))

        partners = PartnerBillingDetailSerializer().get_partners(filters=filters, database=DB_USER_PARTNER)

        if partners:
            partners = self.paginate_queryset(partners, request, view=self)
            partners = PartnerBillingDetailSerializer(instance=partners, many=True)

        return Response(
            data={"partners": partners.data if partners else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if partners else None
        )


class ClicksAPI(APIView, ClickPaginator):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets an admin gets all clicks details from links obtained in a campaign
        """

        def to_datetime(s): return make_aware(datetime.strptime(s, '%Y-%m-%d'))
        validator = Validator(
            {
                "campaign_id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int
                },
                "partner_id": {
                    "required": False,
                    "nullable": True,
                    "type": "integer",
                    "coerce": int
                },
                "created_at_from": {
                    "required": True,
                    "type": "datetime",
                    "coerce": to_datetime
                },
                "created_at_to": {
                    "required": True,
                    "type": "datetime",
                    "coerce": to_datetime
                },
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler)

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        campaign_id = validator.document.get("campaign_id")
        campaign = Campaign.objects.db_manager(DB_USER_PARTNER).filter(id=campaign_id).first()
        if not campaign:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"campaign_id": [_("There is not such campaign in the system")]}
                }, status=status.HTTP_404_NOT_FOUND
            )

        partner_id = validator.document.get("partner_id")
        created_at_from = validator.document.get("created_at_from")
        created_at_to = validator.document.get("created_at_to")

        delta = created_at_to - created_at_from
        if delta.days < 0:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {"created_at": [_("'created_at_from' cannot be greater than 'created_at_to'.")]}
                }, status=status.HTTP_400_BAD_REQUEST
            )

        if delta.days > int(settings.MAX_CLIC_DAYS):
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {"created_at": [_("The maximum number of days allowed has been exceeded.")]}
                }, status=status.HTTP_400_BAD_REQUEST
            )

        def get_clicks_by_partner_link_accumulated(partners, partner_links_accumulated, campaign, filters):
            bookmaker = campaign.bookmaker
            clicks = []
            start = 0
            end = 0
            formated_link = bookmaker.name + " " + campaign.title
            for partner_link_accumulated in partner_links_accumulated:
                partner = partners.get(partner_link_accumulated.partner_id)
                filters.append(Q(partner_link_accumulated_id=partner_link_accumulated.pk))
                clicks.extend(ClickTracking.objects.using(DB_HISTORY).filter(*filters))
                end = len(clicks)

                for i in range(start, end):
                    clicks[i].partner_full_name = partner.first_name + " " + partner.second_name + " " + \
                        partner.last_name + " " + partner.second_last_name if partner else "undefined"
                    clicks[i].formated_link = formated_link + " " + partner_link_accumulated.prom_code
                start = end

            return clicks

        def add_links_to_clicks(links, clicks, campaign):
            bookmaker = campaign.bookmaker
            formated_link = bookmaker.name + " " + campaign.title
            for click in clicks:
                click.formated_link = formated_link + " " + links.get(click.link_id).prom_code

        def add_partners_and_links(clicks, campaign, link, partner):
            bookmaker = campaign.bookmaker
            partner_full_name = partner.first_name + " " + partner.second_name + " " + \
                partner.last_name + " " + partner.second_last_name if partner else "undefined"
            formated_link = bookmaker.name + " " + campaign.title + " " + link.prom_code
            for click in clicks:
                click.partner_full_name = partner_full_name
                click.formated_link = formated_link

        filters = [Q(created_at__range=[created_at_from, created_at_to + timedelta(days=1)])]
        if partner_id is None:
            links = campaign.link_to_campaign.all()
            link_ids = links.values_list('id', flat=True)
            links = {link.id: link for link in links}
            clicks = ClickTrackingSerializer().get_by_links_without_partner_link_accumulated(link_ids, filters)
            add_links_to_clicks(links, clicks, campaign)
        elif partner_id:
            partner_link_accumulated = PartnerLinkAccumulatedBasicSerializer(
            ).get_by_partner_and_campaign(partner_id, campaign_id, DB_USER_PARTNER)

            if not len(partner_link_accumulated):
                return Response(
                    data={
                        "error": settings.NOT_FOUND_CODE,
                        "details": {"partner_id": [_("This partner does not have that campaign")]}
                    }, status=status.HTTP_404_NOT_FOUND)

            partner = User.objects.using(DB_USER_PARTNER).filter(pk=partner_id).first()
            link = partner_link_accumulated[0].link_to_partner_link_accumulated
            filters.append(Q(partner_link_accumulated_id=partner_link_accumulated[0].pk))
            clicks = ClickTracking.objects.using(DB_HISTORY).filter(
                *filters,
            ).order_by("created_at")
            add_partners_and_links(clicks, campaign, link, partner)
        else:
            partner_links_accumulated = PartnerLinkAccumulatedBasicSerializer().get_by_campaign(campaign_id, DB_USER_PARTNER)
            partners_ids = partner_links_accumulated.values_list("partner_id", flat=True)
            partners = User.objects.using(DB_USER_PARTNER).filter(pk__in=partners_ids)
            partners = {partner.id: partner for partner in partners}
            clicks = get_clicks_by_partner_link_accumulated(partners, partner_links_accumulated, campaign, filters)

        clicks = self.paginate_queryset(clicks, request, view=self)
        clicks = ClickTrackingSerializer(instance=clicks, many=True)
        return Response(
            data={"clicks": clicks.data if clicks else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if clicks else None
        )
