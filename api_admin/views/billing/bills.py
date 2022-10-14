from api_admin.helpers import BillsPaginator
from api_admin.models import SearchPartnerLimit
from api_partner.helpers import PartnerLevelCHO
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import (
    WithdrawalPartnerMoney,
    WithdrawalPartnerMoneyAccum,
)
from api_partner.serializers import (
    BillCSVSerializer,
    BillZipSerializer,
    OwnCompanySerializer,
    WithdrawalPartnerMoneyAdviserPatchSer,
    WithdrawalPartnerMoneyForAdviserDetailsSer,
    WithdrawalPartnerMoneyForAdviserTableSer,
)
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    StandardErrorHandler,
    check_positive_float,
    to_date,
    to_datetime_from,
    to_datetime_to,
    to_int,
)
from django.conf import settings
from django.db import transaction
from django.db.models import (
    F,
    Prefetch,
    Q,
    Value,
)
from django.db.models.query_utils import Q
from django.utils import timezone
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


class BillsAPI(APIView, BillsPaginator):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin to get a partner's bills using filtering or sort_by rules
        """
        CODENAME = "bills api-get"

        validator = Validator(
            {
                "partner": {
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
                "level": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                    "allowed": PartnerLevelCHO.values,
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
                    "allowed": (
                        WithdrawalPartnerMoneyForAdviserTableSer.Meta.fields +
                        tuple(["-"+i for i in WithdrawalPartnerMoneyForAdviserTableSer.Meta.fields])
                    ),
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

        admin = request.user

        filters = []
        filters_partner_limit = (
            Q(rol=admin.rol),
            Q(codename=CODENAME),
        )
        search_partner_limit = SearchPartnerLimit.objects.filter(*filters_partner_limit).first()

        if (
            (
                not search_partner_limit or
                search_partner_limit.search_type == SearchPartnerLimit.SearchType.ONLY_ASSIGNED
            ) and
                not admin.is_superuser
        ):
            filters.append(Q(partner__adviser_id=admin.pk))

        # setting sort_by
        sort_by = validator.document.get("sort_by")

        # filters
        partner = validator.document.get("partner")
        partner_level = validator.document.get("level")
        creation_date_from = validator.document.get("creation_date_from")
        creation_date_to = validator.document.get("creation_date_to")
        billed_from_at = validator.document.get("billed_from_at")
        billed_to_at = validator.document.get("billed_to_at")
        payment_date_from = validator.document.get("payment_date_from")
        payment_date_to = validator.document.get("payment_date_to")
        status_ = validator.document.get("status")

        if partner:
            filters.append(Q(partner=partner))
        if partner_level:
            filters.append(Q(partner_level=partner))
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

        withdrawals_partner_money = WithdrawalPartnerMoney.objects.using(
            DB_USER_PARTNER,
        ).annotate(
            level=F("partner__level")
        ).filter(
            *filters,
        ).order_by(
            sort_by,
        )

        if withdrawals_partner_money:
            withdrawals_partner_money = self.paginate_queryset(
                queryset=withdrawals_partner_money,
                request=request,
                view=self,
            )
            withdrawals_partner_money = WithdrawalPartnerMoneyForAdviserTableSer(
                instance=withdrawals_partner_money,
                many=True,
            )

        return Response(
            data={
                "bills": withdrawals_partner_money.data if withdrawals_partner_money else [],
            },
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                "count": self.count,
                "next": self.get_next_link(),
                "previous": self.get_previous_link()
            } if withdrawals_partner_money else None,
        )

    def patch(self, request):
        """
        Lets an admin to update a partner's bill info for example:
        * when it was paid or bill_rate needs to be changed
        """

        validator = Validator(
            {
                "withdrawals": {
                    "required": True,
                    "type": "list",
                    "schema": {
                        "type": "dict",
                        "schema": {
                            "id": {
                                "required": True,
                                "type": "integer",
                                "coerce": int
                            },
                            "status": {
                                "required": True,
                                "type": "integer",
                                "coerce": int,
                                "allowed": WithdrawalPartnerMoney.Status.values
                            },
                            "bill_rate": {
                                "required": True,
                                "type": "float",
                                "coerce": float,
                                "check_with": check_positive_float
                            },
                            "bill_bonus": {
                                "required": False,
                                "type": "float",
                                "coerce": float,
                                "check_with": check_positive_float
                            },

                        }
                    }
                },
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        withdrawals_data = validator.document.get("withdrawals")
        withdrawals_count = len(withdrawals_data)

        ids_tuple = (withdrawal_i.get("id") for withdrawal_i in withdrawals_data)

        filters = [Q(id__in=ids_tuple)]
        withdrawals_partner_money = WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).filter(*filters)
        if withdrawals_count != withdrawals_partner_money.count():
            ids_not_exist = set(ids_tuple) - set(withdrawals_partner_money.values("id", flat=True))
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"id": [_(f"Some of bills does not exist on system"), ids_not_exist]}
                },
                status=status.HTTP_404_NOT_FOUND
            )

        today = timezone.now()

        withdrawals_update = []
        for withdrawal_data_i in withdrawals_data:

            withdrawal_i = next(
                filter(
                    lambda withdrawal_i: withdrawal_i.id == withdrawal_data_i.get("id"),
                    withdrawals_partner_money),
                None)

            if (withdrawal_data_i.get("status") == WithdrawalPartnerMoney.Status.PAYED and
                    withdrawal_i.status != WithdrawalPartnerMoney.Status.PAYED):
                withdrawal_i.payment_at = today
            elif (withdrawal_data_i.get("status") != WithdrawalPartnerMoney.Status.PAYED):
                withdrawal_i.payment_at = None

            withdrawal_i.status = withdrawal_data_i.get("status")
            withdrawal_i.bill_rate = withdrawal_data_i.get("bill_rate")
            if withdrawal_data_i.get("bill_bonus") is not None:
                withdrawal_i.bill_bonus = withdrawal_data_i.get("bill_bonus")

            withdrawals_update.append(withdrawal_i)

        with transaction.atomic(using=DB_USER_PARTNER):
            WithdrawalPartnerMoney.objects.bulk_update(
                withdrawals_update,
                ("status", "bill_rate", "bill_bonus", "payment_at")
            )

        return Response(status=status.HTTP_200_OK)


class BillDetailsAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
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

        # filters
        filters = [Q(id=validator.document.get("id"))]

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

        withdrawal_partner_money_ser = WithdrawalPartnerMoneyForAdviserDetailsSer(
            instance=withdrawal_partner_money,
        )

        return Response(
            data={
                "bill": withdrawal_partner_money_ser.data,
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin to update a partner's bill info for example:
        * when it was paid or bill_rate needs to be changed
        """

        validator = Validator(
            schema={
                "id": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "status": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": WithdrawalPartnerMoney.Status.values,
                },
                "bill_rate": {
                    "required": False,
                    "type": "float",
                    "check_with": check_positive_float,
                },
                "bill_bonus": {
                    "required": False,
                    "type": "float",
                    "check_with": check_positive_float,
                },
            },
            error_handler=StandardErrorHandler,
        )

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        _status = validator.document.get("status")
        bill_rate = validator.document.get("bill_rate")
        bill_bonus = validator.document.get("bill_bonus")

        if _status == None and bill_rate == None and bill_bonus == None:
            return Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {"non_field_errors": [_('You need to send at least one field "status" or "bill_rate"')]}
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        id = validator.document.get("id")

        filters = [Q(id=id)]
        withdrawal_partner_money = WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).filter(*filters).first()
        if not withdrawal_partner_money:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"id": [_("There is not such bill in the system")]}
                },
                status=status.HTTP_404_NOT_FOUND
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)

        if (validator.document.get("status") == WithdrawalPartnerMoney.Status.PAYED and
                withdrawal_partner_money.status != WithdrawalPartnerMoney.Status.PAYED):
            validator.document["payment_at"] = timezone.now()
        elif (validator.document.get("status") != WithdrawalPartnerMoney.Status.PAYED):
            validator.document["payment_at"] = None

        serialized_withdrawal_partner_money = WithdrawalPartnerMoneyAdviserPatchSer(
            instance=withdrawal_partner_money, data=validator.document)
        if serialized_withdrawal_partner_money.is_valid():
            serialized_withdrawal_partner_money.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_withdrawal_partner_money.errors
                },
                status=status.HTTP_404_NOT_FOUND
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)


class MakeBillCSVAPI(APIView):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        lets an admin gets partner's bills to build a CSV file by a range criteria
        """
        def to_date(s): return datetime.strptime(s, "%Y-%m-%d")
        def to_datetime(s): return make_aware(datetime.strptime(s, "%Y-%m-%d"))

        validator = Validator(
            {
                "creation_date_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime,
                },
                "creation_date_to": {
                    "required": False,
                    "type": "datetime",
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

        # filters
        creation_date_from = validator.document.get("creation_date_from")
        creation_date_to = validator.document.get("creation_date_to")
        billed_from_at = validator.document.get("billed_from_at")
        billed_to_at = validator.document.get("billed_to_at")

        filters = []
        if creation_date_from and creation_date_to:
            filters.append(Q(created_at__range=[creation_date_from, creation_date_to + timedelta(days=1)]))
        if billed_from_at and billed_to_at:
            filters.append(Q(billed_from_at__gte=billed_from_at, billed_to_at__lte=billed_to_at))

        bills_csv = BillCSVSerializer().get_by_dates_partner(filters, DB_USER_PARTNER)
        if bills_csv:
            serialized_bills_csv = BillCSVSerializer(instance=bills_csv, many=True)

        return Response(
            data={"bill details": serialized_bills_csv.data if bills_csv else []},
            status=status.HTTP_200_OK
        )


class MakeBillZIPAPI(APIView):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        lets an admin gets partner's bills to build a ZIP file by a range criteria
        """

        def to_date(s): return datetime.strptime(s, "%Y-%m-%d")
        def to_datetime(s): return make_aware(datetime.strptime(s, "%Y-%m-%d"))

        validator = Validator(
            {
                "creation_date_from": {
                    "required": False,
                    "type": "datetime",
                    "coerce": to_datetime,
                },
                "creation_date_to": {
                    "required": False,
                    "type": "datetime",
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

        # filters
        creation_date_from = validator.document.get("creation_date_from")
        creation_date_to = validator.document.get("creation_date_to")
        billed_from_at = validator.document.get("billed_from_at")
        billed_to_at = validator.document.get("billed_to_at")

        filters = []
        if creation_date_from and creation_date_to:
            filters.append(Q(created_at__range=[creation_date_from, creation_date_to + timedelta(days=1)]))
        if billed_from_at and billed_to_at:
            filters.append(Q(billed_from_at__gte=billed_from_at, billed_to_at__lte=billed_to_at))

        bills_zip = BillZipSerializer().get_by_dates_partner(filters, DB_USER_PARTNER)
        own_companies = None
        if bills_zip:
            serialized_bill_zip = BillZipSerializer(instance=bills_zip, many=True)
            own_companies = set(bills_zip.values_list("own_company_id", flat=True))
            filters = [Q(id__in=own_companies)]
            own_companies = OwnCompanySerializer().get_by_ids(filters, DB_USER_PARTNER)
            serialized_own_companies = OwnCompanySerializer(instance=own_companies, many=True)

        return Response(
            data={
                "bill details": serialized_bill_zip.data if bills_zip else [],
                "own_companies": serialized_own_companies.data if own_companies else [],
            }, status=status.HTTP_200_OK
        )


class LastBilledDatePartnersAPI(APIView):
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        validator = Validator(
            schema={},
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get the last accum_at on withdrawals
        latest_withdrawal_accum = WithdrawalPartnerMoneyAccum.objects.all().order_by("-accum_at").first()

        if (latest_withdrawal_accum is not None):
            latest_accum_at = latest_withdrawal_accum.accum_at
        else:
            latest_accum_at = None

        return Response(
            data={
                "latest_accum_at": latest_accum_at,
            },
            status=status.HTTP_200_OK,

        )
