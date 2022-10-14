
from api_log.helpers import DB_HISTORY
from api_log.models import ClickTracking
from api_partner.helpers import (
    DB_USER_PARTNER,
    CampaignsPaginator,
    ClickPaginator,
    IsTerms,
)
from api_partner.models import Campaign
from api_partner.serializers.reports_management.campaign import (
    CampaignPartnerBasicSER,
)
from api_partner.serializers.reports_management.click_tracking import (
    ClickTrackingBasicSerializer,
)
from api_partner.serializers.reports_management.partner_accumulated import (
    PartnerLinkAccumulatedBasicSerializer,
)
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
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


class CampaignsForClicksAPI(APIView, CampaignsPaginator):

    permission_classes = (
        IsAuthenticated,
        IsTerms,
    )

    def get(self, request):
        """
        Get the campaigns where the user is subscribed
        """

        validator = Validator(
            {
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

        partner = request.user.partner
        partner_links_accumulated = set(partner.partnerlinkaccumulated_to_partner.values_list('campaign', flat=True))
        filters = (
            Q(id__in=partner_links_accumulated),
        )
        campaigns = Campaign.objects.db_manager(DB_USER_PARTNER).annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            ),
        ).filter(*filters).order_by("campaign_title")

        campaigns = CampaignPartnerBasicSER(instance=campaigns, many=True)

        return Response(
            data={
                "campaigns": campaigns.data if campaigns else []
            },
            status=status.HTTP_200_OK,
        )


class ClicksAPI(APIView, ClickPaginator):

    permission_classes = (
        IsAuthenticated,
        IsTerms,
    )

    def get(self, request):
        """
        Get the clicks over the link for a specific campaign where the user is subscribed
        """

        def to_datetime(s): return make_aware(datetime.strptime(s, '%Y-%m-%d'))
        validator = Validator(
            {
                "campaign_id": {
                    "required": True,
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

        partner_id = request.user.id
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
        filters = [Q(created_at__range=[created_at_from, created_at_to + timedelta(days=1)])]
        partner_link_accumulated = PartnerLinkAccumulatedBasicSerializer(
        ).get_by_partner_and_campaign(partner_id, campaign_id, DB_USER_PARTNER)

        if not partner_link_accumulated:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"partner_id": [_("You don't have that campaign")]}
                }, status=status.HTTP_404_NOT_FOUND)

        filters.append(Q(partner_link_accumulated_id=partner_link_accumulated[0].pk))
        clicks = ClickTracking.objects.using(DB_HISTORY).filter(
            *filters,
        ).order_by("created_at")
        clicks = self.paginate_queryset(clicks, request, view=self)
        clicks = ClickTrackingBasicSerializer(instance=clicks, many=True)
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
