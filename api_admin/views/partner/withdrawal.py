import logging

from api_partner.models import (
    PartnerBankAccount,
    WithdrawalPartnerMoney,
)
from api_partner.serializers import WithdrawalPartnerMoneySerializer
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    StandardErrorHandler,
    to_int,
)
from django.conf import settings
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class WithdrawalPartnerMoneyAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def patch(self, request):
        """
        Edit the bank for a single withdrawal.
        """
        validator = Validator(
            schema={
                "pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "bank_account_pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
            error_handler=StandardErrorHandler,
        )
        if not validator.validate(document=request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        withdrawal_pk = validator.document.get("pk")
        withdrawal = WithdrawalPartnerMoney.objects.filter(pk=withdrawal_pk).select_related(
            "partner",
            "bank_account",
        ).first()
        if withdrawal is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "id": [
                            _("WithdrawalPartnerMoney not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner = withdrawal.partner
        bank_pk = validator.document.get("bank_account_pk")
        # Get the bank account, if it belongs to the withdrawals' partner
        query = Q(pk=bank_pk) & Q(partner=partner)
        bank_account = PartnerBankAccount.objects.filter(query).first()
        if bank_account is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "bank_account_pk": [
                            _("PartnerBankAccount not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        # Check if the bank account is the same associated with the withdrawal
        elif bank_account == withdrawal.bank_account:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "status": [
                            _("Withdrawal already associated with that bank account"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        withdrawal_ser = WithdrawalPartnerMoneySerializer(
            instance=withdrawal,
            data={
                "bank_account": bank_account.id,
            },
            partial=True,
        )
        if not withdrawal_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": withdrawal_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        withdrawal_ser.save()
        return Response(status=status.HTTP_204_NO_CONTENT)
