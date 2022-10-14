from datetime import timedelta

from api_admin.helpers import FXratePercentagePaginator
from api_partner.helpers import DB_USER_PARTNER
from api_partner.serializers import FxPartnerPercentageSerializer
from cerberus import Validator
from core.helpers import HavePermissionBasedView, StandardErrorHandler
from django.conf import settings
from django.db.models.query_utils import Q
from django.utils.timezone import datetime, make_aware
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class FXRatePercentageAPI(APIView, FXratePercentagePaginator):

    permission_classes = (
        IsAuthenticated, 
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin get fx partner rate percentage 
        """
        sort_by_regex = "\-?id|\-?percentage_fx|\-?updated_at"

        def to_date(s): return make_aware(datetime.strptime(s, "%Y-%m-%d"))
        validator = Validator(
            {
                "updated_date_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_date,
                },
                "updated_date_to": {
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
        updated_date_from = validator.document.get("updated_date_from")
        updated_date_to = validator.document.get("updated_date_to")

        sort_by = request.query_params.get("sort_by")
        if not sort_by:
            sort_by = "-id"

        filters = []
        if updated_date_from and updated_date_to:
            filters.append(Q(updated_at__range=[updated_date_from, updated_date_to + timedelta(days=1)]))

        fx_partner_percentage = FxPartnerPercentageSerializer().get_fx_percentage(filters, sort_by, DB_USER_PARTNER)

        if fx_partner_percentage:
            fx_partner_percentage = self.paginate_queryset(fx_partner_percentage, request, view=self)
            fx_partner_percentage = FxPartnerPercentageSerializer(instance=fx_partner_percentage, many=True)

        return Response(
            data={"fx_partner_percentage": fx_partner_percentage.data if fx_partner_percentage else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if fx_partner_percentage else None
        )

    def put(self, request):
        """
        Lets an admin updates fx partner rate percentage 
        """
        validator = Validator(
            {
                "percentage_fx": {
                    "required": True,
                    "type": "number",
                    "min": 0.0,
                    "max": 1.0,
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        # filters
        fx_partner_percentage = FxPartnerPercentageSerializer().get_latest(DB_USER_PARTNER)
        if fx_partner_percentage:
            day_today = datetime.now().day
            day_latest = fx_partner_percentage.updated_at.day

        if fx_partner_percentage and day_today == day_latest:
            serialized_fx_partner_percentage = FxPartnerPercentageSerializer(
                instance=fx_partner_percentage, data=validator.document)
        else:
            serialized_fx_partner_percentage = FxPartnerPercentageSerializer(data=validator.document)

        if serialized_fx_partner_percentage.is_valid():
            serialized_fx_partner_percentage.save()
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_fx_partner_percentage.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        return Response(status=status.HTTP_200_OK)


class LatestFXRatePercentageAPI(APIView):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets an admin gets fx percentage
        """
        # filters
        fx_partner_percentage = FxPartnerPercentageSerializer().get_latest(DB_USER_PARTNER)

        if fx_partner_percentage:
            fx_partner_percentage = FxPartnerPercentageSerializer(instance=fx_partner_percentage)

        return Response(
            data={"fx_partner_percentage": fx_partner_percentage.data if fx_partner_percentage else []},
            status=status.HTTP_200_OK
        )
