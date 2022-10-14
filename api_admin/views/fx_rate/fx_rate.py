from api_admin.helpers import FXratePaginator
from api_partner.helpers import DB_USER_PARTNER
from api_partner.serializers import FxPartnerSerializer
from cerberus import Validator
from core.helpers import StandardErrorHandler
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

from core.helpers import HavePermissionBasedView


class FXRateAPI(APIView, FXratePaginator):
    """
    UserApi View retreive and edit own Staff user data
    """
    permission_classes = (IsAuthenticated, HavePermissionBasedView)

    def get(self, request):
        """
        Lets an admin gets partner fx rate
        """
        sort_by_regex = "\-?fx_eur_cop|\-?fx_eur_mxn|\-?fx_eur_usd|\-?fx_eur_brl|" + \
            "\-?fx_eur_pen|\-?fx_usd_cop|\-?fx_usd_mxn|\-?fx_usd_eur" + \
            "|\-?fx_usd_brl|\-?fx_usd_pen|\-?fx_cop_usd|\-?fx_cop_mxn|\-?fx_cop_eur|\-?fx_cop_brl" + \
            "|\-?fx_cop_pen"

        def to_date(s): return make_aware(datetime.strptime(s, '%Y-%m-%d'))
        validator = Validator(
            {
                "creation_date_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_date,
                },
                "creation_date_to": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_date
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
                },
                "sort_by": {
                    "required": False,
                    "type": "string",
                    "regex": sort_by_regex
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        # filters
        creation_date_from = validator.document.get("creation_date_from")
        creation_date_to = validator.document.get("creation_date_to")
        sort_by = request.query_params.get("sort_by")
        if not sort_by:
            sort_by = "-id"

        filters = []
        if creation_date_from and creation_date_to:
            filters.append(Q(created_at__range=[creation_date_from, creation_date_to + timedelta(days=1)]))

        fx_partner = FxPartnerSerializer().get_fx(filters, sort_by, DB_USER_PARTNER)

        if fx_partner:
            fx_partner = self.paginate_queryset(fx_partner, request, view=self)
            fx_partner = FxPartnerSerializer(instance=fx_partner, many=True)

        return Response(
            data={"fx_partner": fx_partner.data if fx_partner else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if fx_partner else None
        )
