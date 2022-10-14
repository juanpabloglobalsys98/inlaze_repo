from io import BytesIO as IO

import pandas as pd
from api_admin.helpers import BillsPaginator
from api_admin.models import SearchPartnerLimit
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import WithdrawalPartnerMoney
from api_partner.serializers import WithdrawalPartnerMoneyForAdviserTableSer
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    StandardErrorHandler,
    to_date,
    to_datetime_from,
    to_datetime_to,
)
from django.conf import settings
from django.db.models.query_utils import Q
from django.http import HttpResponse
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class BillsExportAPI(APIView):

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
        creation_date_from = validator.document.get("creation_date_from")
        creation_date_to = validator.document.get("creation_date_to")
        billed_from_at = validator.document.get("billed_from_at")
        billed_to_at = validator.document.get("billed_to_at")
        payment_date_from = validator.document.get("payment_date_from")
        payment_date_to = validator.document.get("payment_date_to")
        status_ = validator.document.get("status")

        if partner:
            filters.append(Q(partner=partner))
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
        ).filter(
            *filters,
        ).order_by(
            sort_by,
        )

        df = pd.DataFrame(
            withdrawals_partner_money.values(
                "pk",
                "partner_id",
                "first_name",
                "last_name",
                "email",
                "created_at",
                "billed_from_at",
                "billed_to_at",
                "payment_at",
                "fixed_income_usd",
                "fixed_income_eur",
                "fixed_income_cop",
                "fixed_income_mxn",
                "fixed_income_gbp",
                "fixed_income_pen",
                "fixed_income_local",
                "bill_rate",
                "bill_bonus",
                "cpa_count",
                "status",
            ),
        )

        df["created_at"] = pd.to_datetime(df["created_at"]).dt.date
        df["billed_from_at"] = pd.to_datetime(df["billed_from_at"]).dt.date
        df["billed_to_at"] = pd.to_datetime(df["billed_to_at"]).dt.date
        df["payment_at"] = pd.to_datetime(df["payment_at"]).dt.date

        df.rename(
            inplace=True,
            columns={
                "pk": "pk",
                "partner_id": "Partner ID",
                "first_name": "Nombre",
                "last_name": "Apellido",
                "email": "Correo",
                "billed_from_at": "Facturado_desde",
                "billed_to_at": "Facturado_hasta",
                "payment_at": "Fecha_pago",
                "fixed_income_usd": "Ingresos_fijos_USD",
                "fixed_income_eur": "Ingresos_fijos_EUR",
                "fixed_income_cop": "Ingresos_fijos_COP",
                "fixed_income_mxn": "Ingresos_fijos_MXN",
                "fixed_income_gbp": "Ingresos_fijos_GBP",
                "fixed_income_pen": "Ingresos_fijos_PEN",
                "fixed_income_local": "Ingresos_moneda_local",
                "bill_rate": "Gastos_Financieros",
                "bill_bonus": "Bono",
                "cpa_count": "#_cpa",
                "status": "Estado",

            },
        )
        excel_file = IO()
        writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1')

        writer.save()

        excel_file.seek(0)
        response = HttpResponse(
            excel_file.read(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        # set the file name in the Content-Disposition header
        response['Content-Disposition'] = 'attachment; filename=BillingPartner.xlsx'

        return response
