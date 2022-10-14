from api_admin.helpers import MinWithdrawalPartnerMoneyPaginator
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerLevelCHO,
)
from api_partner.models import MinWithdrawalPartnerMoney
from api_partner.serializers import MinWithdrawalPartnerMoneySerializer
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    StandardErrorHandler,
)
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


class MinWithdrawalPartnerMoneyAPI(APIView, MinWithdrawalPartnerMoneyPaginator):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin gets the partner's minimum withdrawal
        """
        def to_date(s): return make_aware(datetime.strptime(s, '%Y-%m-%d'))
        validator = Validator(
            {
                "created_at_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_date,
                },
                "created_at_to": {
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
                    "default": "-created_at",
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        # filters
        created_at_from = validator.document.get("created_at_from")
        created_at_to = validator.document.get("created_at_to")
        sort_by = request.query_params.get("sort_by")
        if not sort_by:
            sort_by = "-id"

        filters = []
        if created_at_from and created_at_to:
            filters.append(Q(created_at__range=[created_at_from, created_at_to + timedelta(days=1)]))

        min_withdrawal_partner_money = MinWithdrawalPartnerMoneySerializer(
        ).min_withdrawal_partner_money(filters, sort_by, DB_USER_PARTNER)

        if min_withdrawal_partner_money:
            min_withdrawal_partner_money = self.paginate_queryset(min_withdrawal_partner_money, request, view=self)
            min_withdrawal_partner_money = MinWithdrawalPartnerMoneySerializer(
                instance=min_withdrawal_partner_money, many=True)

        return Response(
            data={"min_withdrawal_partner_money": min_withdrawal_partner_money.data
                  if min_withdrawal_partner_money else[]},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if min_withdrawal_partner_money else None
        )

    def post(self, request):
        """
        Lets an admin updates the partner's minimum withdrawal
        """
        validator = Validator(
            {
                "min_usd_by_level": {
                    "required": False,
                    "type": "dict",
                    "schema": {
                            str(key): {
                                "required": False,
                                "type": "float",
                            }
                        for key in PartnerLevelCHO.values
                    },
                },


            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        min_usd_by_level = validator.document.get("min_usd_by_level")
        old_min_usd = MinWithdrawalPartnerMoney.objects.all().order_by("-created_at").first()
        min_usd_by_level = old_min_usd.min_usd_by_level | min_usd_by_level
        data = {
            "min_usd_by_level": min_usd_by_level,
            "created_by": request.user.id,
        }
        # filters
        min_withdrawal_partner_money = MinWithdrawalPartnerMoneySerializer(
            data=data
        )

        if min_withdrawal_partner_money.is_valid():
            min_withdrawal_partner_money.save()
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": min_withdrawal_partner_money.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        return Response(status=status.HTTP_200_OK)


class LatestMinWithdrawalPartnerMoneyAPI(APIView):
    """
    UserApi View retreive and edit own Staff user data
    """
    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin gets the partner's latest minimum withdrawal
        """

        min_withdrawal_partner_money = MinWithdrawalPartnerMoneySerializer().get_latest(DB_USER_PARTNER)

        if min_withdrawal_partner_money:
            min_withdrawal_partner_money = MinWithdrawalPartnerMoneySerializer(instance=min_withdrawal_partner_money)

        return Response(
            data={"min_withdrawal_partner_money": min_withdrawal_partner_money.data
                  if min_withdrawal_partner_money else[]},
            status=status.HTTP_200_OK)
