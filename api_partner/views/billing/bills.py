from api_partner.helpers import (
    DB_USER_PARTNER,
    BillsPaginator,
    IsBasicInfoValid,
    IsEmailValid,
    IsNotBanned,
    IsNotToBeVerified,
    IsTerms,
)
from api_partner.models import WithdrawalPartnerMoney
from api_partner.serializers import (
    BillCSVForPartnerSerializer,
    BillZipForPartnerSerializer,
    OwnCompanySerializer,
    WithdrawalPartnerMoneyForPartnerDetailsSerializer,
    WithdrawalPartnerMoneyForPartnerTableSer,
)
from cerberus import Validator
from core.helpers import (
    StandardErrorHandler,
    to_date,
    to_datetime_from,
    to_datetime_to,
)
from django.conf import settings
from django.db.models import (
    F,
    Prefetch,
    Q,
    Value,
)
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


class BillDetailsAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified,
        IsTerms,
    )

    def get(self, request):
        """
        Get the user's bills following any sort rule or filter
        """

        validator = Validator(
            {
                "id": {
                    "required": True,
                    "type": "integer",
                    "coerce": int,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Partner User id of current session
        partner_user = request.user

        # filters
        id = validator.document.get("id")

        filters = (
            Q(partner_id=partner_user.id),
            Q(id=id),
        )

        withdrawal_partner_money = WithdrawalPartnerMoney.objects.using(
            DB_USER_PARTNER,
        ).annotate(
            level=F("partner__level")
        ).select_related(
            "own_company",
            "bank_account",
        ).prefetch_related(
            "withdrawal_partner_money_accum_set",
            "withdrawal_partner_money_accum_set__fx_partner",
        ).filter(
            *filters,
        ).first()

        if not withdrawal_partner_money:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "id": [
                            _("There is not such bill with that user"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        withdrawal_partner_money_ser = WithdrawalPartnerMoneyForPartnerDetailsSerializer(
            instance=withdrawal_partner_money,
        )

        return Response(
            data={
                "bill": withdrawal_partner_money_ser.data,
            },
            status=status.HTTP_200_OK,
        )


class BillsAPI(APIView, BillsPaginator):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified,
        IsTerms,
    )

    def get(self, request):
        """
        Get the user's bills following any sort rule or filter
        """

        validator = Validator(
            {
                "id": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "creation_date_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime_from,
                },
                "creation_date_to": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime_to,
                },
                "billed_from_at": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
                },
                "billed_to_at": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
                },
                "payment_date_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime_from,
                },
                "payment_date_to": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime_to,
                },
                "status": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                    "allowed": WithdrawalPartnerMoney.Status.values,
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
                    "default": "-id",
                    "regex": ("\-?id|\-?billed_from_at|\-?billed_to_at|\-?status|\-?created_at|\-?payment_at|"
                              "\-?fixed_income_local|\-?bill_rate|\-?cpa_count|\-?status")
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner_user = request.user

        # setting sort_by
        sort_by = validator.document.get("sort_by")

        # filters
        id = validator.document.get("id")
        creation_date_from = validator.document.get("creation_date_from")
        creation_date_to = validator.document.get("creation_date_to")
        billed_from_at = validator.document.get("billed_from_at")
        billed_to_at = validator.document.get("billed_to_at")
        payment_date_from = validator.document.get("payment_date_from")
        payment_date_to = validator.document.get("payment_date_to")
        status_ = validator.document.get("status")

        filters = [Q(partner_id=partner_user.id)]
        if id:
            filters.append(Q(id=id))
        if creation_date_from and creation_date_to:
            filters.append(Q(created_at__range=[creation_date_from, creation_date_to]))
        if billed_from_at and billed_to_at:
            filters.append(
                Q(billed_from_at__gte=billed_from_at, billed_to_at__lte=billed_to_at) |
                Q(billed_from_at__lte=billed_from_at, billed_to_at__gte=billed_to_at)
            )
        if payment_date_from and payment_date_to:
            filters.append(Q(payment_at__range=[payment_date_from, payment_date_to]))
        if status_ is not None:
            filters.append(Q(status=status_))

        withdrawals_partner_money = WithdrawalPartnerMoney.objects.db_manager(
            DB_USER_PARTNER).annotate(
                total_net_income=(
                    F("fixed_income_local") -
                    F("bill_rate") +
                    F("bill_bonus")
                ),
        ).filter(*filters).order_by(sort_by)

        if withdrawals_partner_money:
            withdrawals_partner_money = self.paginate_queryset(withdrawals_partner_money, request, view=self)
            withdrawals_partner_money = WithdrawalPartnerMoneyForPartnerTableSer(
                instance=withdrawals_partner_money, many=True)

        return Response(
            data={
                "bills": withdrawals_partner_money.data if withdrawals_partner_money else []
            },
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if withdrawals_partner_money else None
        )


class MakeBillCSVAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsBasicInfoValid,
        IsEmailValid,
        IsNotToBeVerified
    )

    def get(self, request):
        """
        Get the user's bills to put them in a CSV
        """

        validator = Validator(
            {
                "creation_date_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime_from,
                },
                "creation_date_to": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime_to,
                },
                "billed_from_at": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
                },
                "billed_to_at": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner_user = request.user

        # filters
        creation_date_from = validator.document.get("creation_date_from")
        creation_date_to = validator.document.get("creation_date_to")
        billed_from_at = validator.document.get("billed_from_at")
        billed_to_at = validator.document.get("billed_to_at")

        filters = [Q(partner_id=partner_user.id)]
        if creation_date_from and creation_date_to:
            filters.append(Q(created_at__range=[creation_date_from, creation_date_to]))
        if billed_from_at and billed_to_at:
            filters.append(Q(billed_from_at__gte=billed_from_at, billed_to_at__lte=billed_to_at))

        bills_csv = WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).filter(*filters)

        if bills_csv:
            serialized_bills_csv = BillCSVForPartnerSerializer(instance=bills_csv, many=True)

        return Response(
            data={"bill_csv": serialized_bills_csv.data if bills_csv else []},
            status=status.HTTP_200_OK
        )


class MakeBillZIPAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        IsNotBanned,
        IsNotToBeVerified,
    )

    def get(self, request):
        """
        Get the user's bills to put them in a ZIP
        """

        def to_datetime(s): return make_aware(datetime.strptime(s, '%Y-%m-%d'))
        def to_date(s): return datetime.strptime(s, '%Y-%m-%d')

        validator = Validator(
            {
                "creation_date_from": {
                    "required": False,
                    "type": "date",
                    "coerce": to_datetime,
                },
                "creation_date_to": {
                    "required": False,
                    "type": "date",
                    "coerce": to_datetime,
                },
                "billed_from_at": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
                },
                "billed_to_at": {
                    "required": False,
                    "type": "date",
                    "coerce": to_date,
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        partner = request.user.id

        # filters
        creation_date_from = validator.document.get("creation_date_from")
        creation_date_to = validator.document.get("creation_date_to")
        billed_from_at = validator.document.get("billed_from_at")
        billed_to_at = validator.document.get("billed_to_at")

        filters = [Q(partner=partner)]
        if creation_date_from and creation_date_to:
            filters.append(Q(created_at__range=[creation_date_from, creation_date_to + timedelta(days=1)]))
        if billed_from_at and billed_to_at:
            filters.append(Q(billed_from_at__gte=billed_from_at, billed_to_at__lte=billed_to_at))

        bill_zips = BillZipForPartnerSerializer().get_by_dates_partner(filters, DB_USER_PARTNER)
        own_companies = None
        if bill_zips:
            serialized_bill_zip = BillZipForPartnerSerializer(instance=bill_zips, many=True)
            own_companies = set(bill_zips.values_list("own_company_id", flat=True))
            filters = [Q(id__in=own_companies)]
            own_companies = OwnCompanySerializer().get_by_ids(filters, DB_USER_PARTNER)
            serialized_own_companies = OwnCompanySerializer(instance=own_companies, many=True)

        return Response(
            data={
                "bill details": serialized_bill_zip.data if bill_zips else [],
                "own_companies": serialized_own_companies.data if own_companies else [],
            },
            status=status.HTTP_200_OK
        )
