import logging

from api_admin.helpers import DefaultPAG
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerStatusCHO,
)
from api_partner.models import (
    Partner,
    PartnerBankAccount,
    PartnerBankValidationRequest,
    WithdrawalPartnerMoney,
)
from api_partner.serializers import PartnerBankAccountSER
from cerberus import Validator
from core.helpers import (
    CountryAll,
    HavePermissionBasedView,
    ValidatorFile,
    normalize_capitalize,
    request_cfg,
    str_extra_space_remove,
    to_bool,
    to_int,
)
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class PartnerBankAccountAPI(APIView, DefaultPAG):
    """
    In this view the adviser can add, modify or update bank account for partner.
    """
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def post(self, request):
        """
        this post works like a get, and return the result that the filter found
        you must send a dict, with the filter params
        """
        validator = Validator(
            schema={
                "filter": {
                    "required": True,
                    "type": "dict",
                    "schema": {
                        "partner_id": {
                            "required": True,
                            "type": "integer",
                            "coerce": to_int,
                        },
                    },
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "partner_id",
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if validator.document is None:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "partner_id": [
                        _("There is not partners  in the system"),
                    ],
                },
            },
                status=status.HTTP_404_NOT_FOUND,
            )

        order_by = validator.document.get("order_by")
        query = Q(**validator.document.get("filter"))

        request_cfg.is_partner = True
        banks = PartnerBankAccount.objects.filter(query).order_by(order_by)

        user_paginated = self.paginate_queryset(
            queryset=banks,
            request=request,
            view=self,
        )

        banks_ser = PartnerBankAccountSER(
            instance=user_paginated,
            many=True,
        )
        return Response(
            data={
                "banks": banks_ser.data,
            },
            status=status.HTTP_200_OK,
        )

    def patch(self, request):
        """
        Here, the adviser user can modify the partner bank account
        """
        validator_query = Validator(
            schema={
                "pk": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
        )

        if not validator_query.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_PARAMS,
                    "details": validator_query.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator = ValidatorFile(
            schema={
                "partner": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                },
                "billing_country": {
                    "required": False,
                    "type": "string",
                    "allowed": CountryAll.values,
                },
                "billing_address": {
                    "required": False,
                    "type": "string",
                },
                "billing_city": {
                    "required": False,
                    "type": "string",
                },
                "bank_name": {
                    "required": False,
                    "type": "string",
                },
                "account_type": {
                    "required": False,
                    "type": "integer",
                    "nullable": True,
                    "coerce": to_int,
                    "allowed": PartnerBankAccount.AccountType.values,
                },
                "account_number": {
                    "required": False,
                    "type": "string",
                    "nullable": True,
                },
                "swift_code": {
                    "required": False,
                    "type": "string",
                    "nullable": True,
                },
                "is_primary": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "is_company": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "company_name": {
                    "required": False,
                    "type": "string",
                    "nullable": True,
                },
                "company_reg_number": {
                    "required": False,
                    "type": "string",
                    "nullable": True,
                },
            },
        )
        """
        validatorfile is a cerberus way to verify the files
        """

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE_BODY,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not validator.document:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {
                        "non_field_errors": [
                            _("not input data for patch"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        """
        this case is when the adviser is not changing any file document
        """

        if (res := PartnerBankAccount.verify_account_type(validator.document)):
            return res

        is_primary = validator.document.get("is_primary")
        if is_primary == True:
            query = Q(partner_id=validator.document.get("partner"))
            partner_accounts = PartnerBankAccount.objects.filter(query)
            if partner_accounts is not None:
                partner_accounts.update(is_primary=False)
        """
        if the new bank account is the primary, the other accounts will set into false
        because only can exist one primary account.
        """
        is_active = validator.document.get("is_active")
        partner_id = validator.document.get("partner")
        query = Q(partner_id=partner_id, is_active=True)
        if is_active == False:
            bank_active = PartnerBankAccount.objects.using(DB_USER_PARTNER).filter(query).count()

            if bank_active == 1:
                return Response(
                    data={
                        "error": settings.BAD_REQUEST_CODE,
                        "detail": {
                            "non_field_errors": [
                                _("Only have one bank account, you can't deactivate"),
                            ],
                        },
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )

        pk = validator_query.document.get("pk")
        query = Q(pk=pk)
        bank_account = PartnerBankAccount.objects.using(DB_USER_PARTNER).filter(query).first()

        if bank_account.is_primary == True and is_active == False:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("You can't deactivate primary account"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        bank_ser = PartnerBankAccountSER(
            instance=bank_account,
            data=validator.document,
            partial=True,
        )

        if bank_ser.is_valid():
            bank_ser.save()
            return Response(
                data={},
                status=status.HTTP_204_NO_CONTENT,
            )


class PartnerBankAccountCreateAPI(APIView, DefaultPAG):
    """
    here the adviser can create other bank account
    """
    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def post(self, request):
        request_cfg.is_partner = True
        validator = ValidatorFile(
            schema={
                "partner": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "billing_country": {
                    "required": True,
                    "type": "string",
                    "allowed": CountryAll.values,
                },
                "billing_city": {
                    "required": True,
                    "type": "string",
                    "empty": False,
                    "coerce": normalize_capitalize,
                },
                "billing_address": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "bank_name": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "account_type": {
                    "required": True,
                    "type": "integer",
                    "coerce": to_int,
                    "allowed": PartnerBankValidationRequest.AccountType.values,
                },
                "account_number": {
                    "required": True,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "swift_code": {
                    "required": False,
                    "type": "string",
                    "coerce": str_extra_space_remove,
                },
                "is_company": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
                "company_name": {
                    "required": False,
                    "type": "string",
                    "nullable": True,
                    "coerce": str_extra_space_remove,
                },
                "company_reg_number": {
                    "required": False,
                    "type": "string",
                    "nullable": True,
                    "coerce": str_extra_space_remove,
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if (res := PartnerBankAccount.verify_account_type(validator.document)):
            return res

        partner_pk = validator.document.get("partner")
        partner: Partner = Partner.objects.filter(pk=partner_pk).first()
        if partner is None:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "detail": {
                        "partner": [
                            _("Partner not found"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        bank_accounts_count = partner.bank_accounts.count()
        if bank_accounts_count + 1 > settings.BANK_ACCOUNTS_LIMIT:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _("Bank accounts limit reached"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        has_primary = partner.bank_accounts.filter(is_primary=True).exists()
        if not has_primary and validator.document.get("is_active") == False:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "is_active": [
                            _("Primary bank account can't be inactive"),
                        ]
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        bank_account: PartnerBankAccount = PartnerBankAccount.objects.create(
            partner=partner,
            is_primary=not has_primary,
        )
        bank_account_ser = PartnerBankAccountSER(
            instance=bank_account,
            data=validator.document,
            partial=True,
        )
        if not bank_account_ser.is_valid():
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": bank_account_ser.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if not has_primary:
            partner.bank_status = PartnerStatusCHO.ACCEPTED
            partner.alerts["bank"] = True
        else:
            # Modify secondary account status if partner already has a primary account
            partner.secondary_bank_status = PartnerStatusCHO.ACCEPTED
            partner.alerts["secondary_bank"] = True

        withdrawals = []
        if bank_accounts_count == 0 and bank_account.is_primary:
            # Update withdrawals that have a null bank
            query = Q(partner=partner) & Q(bank_account=None)
            withdrawals = WithdrawalPartnerMoney.objects.filter(query)
            for withdrawal in withdrawals:
                withdrawal.bank_account = bank_account

        with transaction.atomic(using=DB_USER_PARTNER):
            bank_account_ser.save()
            partner.save()
            WithdrawalPartnerMoney.objects.bulk_update(
                objs=withdrawals,
                fields=(
                    "bank_account",
                ),
            )

        return Response(status=status.HTTP_201_CREATED)
