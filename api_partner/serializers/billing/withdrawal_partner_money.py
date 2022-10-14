from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models import WithdrawalPartnerMoney
from api_partner.serializers import PartnerBankAccountSER
from api_partner.serializers.billing.own_company import (
    OwnCompanyPartnerSerializer,
)
from api_partner.serializers.billing.withdrawal_partner_money_accum import (
    WithdrawalPartnerMoneyAccumForAdviserSer,
    WithdrawalPartnerMoneyAccumForPartnerSerializer,
)
from django.db.models import Q
from rest_framework import serializers


class WithdrawalPartnerMoneySerializer(serializers.ModelSerializer):
    """
    Withdrawal partner money serializer with all fields
    """

    class Meta:
        model = WithdrawalPartnerMoney
        fields = "__all__"

    def create(self, validated_data):
        """
        """
        return WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def get_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).delete()


class WithdrawalPartnerMoneyAdviserPatchSer(serializers.ModelSerializer):
    """
    Withdrawal partner money basic serializer with specific fields for updating and querying purposes
    """

    class Meta:
        model = WithdrawalPartnerMoney
        fields = ("id", "status", "bill_rate", "payment_at", "bill_bonus")

    def create(self, validated_data):
        """
        """
        return WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).delete()


class WithdrawalPartnerMoneyOwnBasicSerializer(serializers.ModelSerializer):
    """
    Withdrawal partner money own basic serializer with specific fields for CUD purposes
    """

    class Meta:
        model = WithdrawalPartnerMoney
        exclude = ("first_name", "second_name", "last_name", "second_last_name", "email",
                   "phone", "country", "city", "address", "identification", "identification_type")

    def create(self, validated_data):
        """
        """
        return WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).delete()

    def get_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def get_by_id_partner(self, partner, bill_id, database="default"):
        filters = [Q(partner=partner, id=bill_id)]
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).first()


class WithdrawalPartnerMoneyForPartnerDetailsSerializer(serializers.ModelSerializer):
    """
    Withdrawal partner money for partner serializer with specific fields for querying purpose
    """

    full_name = serializers.CharField(
        source="get_full_name",
        read_only=True,
    )
    details = WithdrawalPartnerMoneyAccumForPartnerSerializer(
        source="withdrawal_partner_money_accum_set",
        many=True,
        read_only=True,
    )
    own_company = OwnCompanyPartnerSerializer(
        read_only=True,
    )
    bank_account = PartnerBankAccountSER(read_only=True)
    level = serializers.IntegerField()

    class Meta:
        model = WithdrawalPartnerMoney
        fields = (
            "id",
            "level",
            "email",
            "phone",
            "country",
            "city",
            "address",
            "identification",
            "identification_type",
            "billed_from_at",
            "billed_to_at",
            "cpa_count",
            "fixed_income_local",
            "bill_rate",
            "bill_bonus",
            "currency_local",
            "status",
            "payment_at",
            "created_at",
            "full_name",
            "details",
            "own_company",
            "bank_account",
        )


class WithdrawalPartnerMoneyForPartnerTableSer(serializers.ModelSerializer):
    """
    Withdrawal partner money for partner serializer with specific fields for querying purpose
    """
    fixed_income_local = serializers.FloatField(source="total_net_income")

    class Meta:
        model = WithdrawalPartnerMoney
        fields = (
            "id",
            "billed_from_at",
            "billed_to_at",
            "cpa_count",
            "fixed_income_local",
            "bill_rate",
            "currency_local",
            "status",
            "payment_at",
            "created_at",
        )

    def get_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)


class WithdrawalPartnerMoneyForAdviserTableSer(serializers.ModelSerializer):
    """
    Withdrawal partner money for partner serializer with specific fields for querying purpose
    """
    full_name = serializers.CharField(source="get_full_name", read_only=True)
    level = serializers.IntegerField()
    # partner_id = serializers.UUIDField(required=False)

    class Meta:
        model = WithdrawalPartnerMoney
        fields = (
            "id",
            "partner_id",
            "full_name",
            "email",
            "billed_from_at",
            "billed_to_at",
            "cpa_count",
            "fixed_income_usd",
            "fixed_income_eur",
            "fixed_income_eur_usd",
            "fixed_income_cop",
            "fixed_income_cop_usd",
            "fixed_income_mxn",
            "fixed_income_mxn_usd",
            "fixed_income_gbp",
            "fixed_income_gbp_usd",
            "fixed_income_pen",
            "fixed_income_pen_usd",
            "fixed_income_local",
            "bill_rate",
            "bill_bonus",
            "currency_local",
            "status",
            "level",
            "payment_at",
            "created_at",
        )

    def get_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)


class WithdrawalPartnerMoneyForAdviserDetailsSer(serializers.ModelSerializer):
    """
    Withdrawal partner money for partner serializer with specific fields for querying purpose
    """
    # partner_id = serializers.UUIDField(required=False)
    # own_company_id = serializers.UUIDField(required=False)

    full_name = serializers.CharField(
        source="get_full_name",
        read_only=True,
    )
    details = WithdrawalPartnerMoneyAccumForAdviserSer(
        source="withdrawal_partner_money_accum_set",
        many=True,
        read_only=True,
    )
    own_company = OwnCompanyPartnerSerializer(
        read_only=True,
    )
    bank_account = PartnerBankAccountSER(read_only=True)
    level = serializers.IntegerField()

    class Meta:
        model = WithdrawalPartnerMoney
        fields = (
            "id",
            "partner_id",
            "own_company_id",
            "full_name",
            "level",
            "email",
            "phone",
            "country",
            "city",
            "address",
            "identification",
            "identification_type",
            "billed_from_at",
            "billed_to_at",
            "cpa_count",
            "fixed_income_usd",
            "fixed_income_eur",
            "fixed_income_eur_usd",
            "fixed_income_cop",
            "fixed_income_cop_usd",
            "fixed_income_mxn",
            "fixed_income_mxn_usd",
            "fixed_income_gbp",
            "fixed_income_gbp_usd",
            "fixed_income_pen",
            "fixed_income_pen_usd",
            "fixed_income_local",
            "bill_rate",
            "bill_bonus",
            "currency_local",
            "status",
            "payment_at",
            "created_at",
            "details",
            "own_company",
            "bank_account",
        )


class BillPDFSerializer(serializers.ModelSerializer):
    """
    Bill pdf serializer with specific fields for querying purposes (adviser)
    """

    own_company = serializers.SerializerMethodField("get_own_company")
    details = serializers.SerializerMethodField("get_details")

    class Meta:
        model = WithdrawalPartnerMoney
        fields = (
            "id",
            "billed_from_at",
            "billed_to_at",
            "cpa_count",
            "fixed_income_usd",
            "fixed_income_eur",
            "fixed_income_cop",
            "fixed_income_local",
            "bill_rate",
            "currency_local",
            "status",
            "payment_at",
            "created_at",
            "partner",
            "own_company",
            "details",
        )

    def get_own_company(self, bill):
        from api_partner.serializers import OwnCompanySerializer

        serialized_own_company = OwnCompanySerializer(instance=bill.own_company)
        return serialized_own_company.data

    def get_details(self, bill):
        from api_partner.serializers import (
            WithdrawalPartnerMoneyAccumSerializer,
        )

        bill_detail = bill.withdrawal_partner_money_accum_set
        serialized_bill_detail = WithdrawalPartnerMoneyAccumSerializer(instance=bill_detail, many=True)
        return serialized_bill_detail.data

    def create(self, validated_data):
        """
        """
        return WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).delete()

    def get_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def get_by_id_partner(self, partner, bill_id, database="default"):
        filters = [Q(partner=partner, id=bill_id)]
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).first()


class BillPDFForPartnerSerializer(serializers.ModelSerializer):
    """
    Bill pdf serializer with specific fields for querying purposes (partner)
    """

    own_company = serializers.SerializerMethodField("get_own_company")
    details = serializers.SerializerMethodField("get_details")

    class Meta:
        model = WithdrawalPartnerMoney
        fields = ("id", "billed_from_at", "billed_to_at", "cpa_count", "fixed_income_local", "bill_rate",
                  "currency_local", "status", "payment_at", "created_at", "partner", "own_company", "details",)

    def get_own_company(self, bill):
        from api_partner.serializers import OwnCompanySerializer

        serialized_own_company = OwnCompanySerializer(instance=bill.own_company)
        return serialized_own_company.data

    def get_details(self, bill):
        from api_partner.serializers import (
            WithdrawalPartnerMoneyAccumForPartnerSerializer,
        )

        bill_detail = bill.withdrawal_partner_money_accum_set
        serialized_bill_detail = WithdrawalPartnerMoneyAccumForPartnerSerializer(instance=bill_detail, many=True)
        return serialized_bill_detail.data

    def create(self, validated_data):
        """
        """
        return WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).delete()

    def get_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def get_by_id_partner(self, partner, bill_id, database="default"):
        filters = [Q(partner=partner, id=bill_id)]
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).first()


class BillCSVSerializer(serializers.ModelSerializer):
    """
    Bill CSV serializer with specific fields for querying purposes (adviser)
    """

    class Meta:
        model = WithdrawalPartnerMoney
        fields = ("id", "billed_from_at", "billed_to_at", "cpa_count", "fixed_income_usd", "fixed_income_eur",
                  "fixed_income_cop", "fixed_income_local", "bill_rate", "currency_local", "status", "payment_at",
                  "created_at", "partner")

    def create(self, validated_data):
        """
        """
        return WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).delete()

    def get_by_dates_partner(self, filters, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters)


class BillCSVForPartnerSerializer(serializers.ModelSerializer):
    """
    Bill CSV serializer with specific fields for querying purposes (partner)
    """

    class Meta:
        model = WithdrawalPartnerMoney
        fields = ("id", "billed_from_at", "billed_to_at", "cpa_count", "fixed_income_local", "bill_rate",
                  "currency_local", "status", "payment_at", "created_at", "partner")

    def create(self, validated_data):
        """
        """
        return WithdrawalPartnerMoney.objects.db_manager(DB_USER_PARTNER).create(**validated_data)

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).delete()

    def get_by_dates_partner(self, filters, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters)


class BillZipSerializer(serializers.ModelSerializer):
    """
    Bill ZIP serializer with specific fields for querying purposes (adviser)
    """

    details = serializers.SerializerMethodField("get_details")

    class Meta:
        model = WithdrawalPartnerMoney
        fields = ("id", "billed_from_at", "billed_to_at", "cpa_count", "fixed_income_usd", "fixed_income_eur",
                  "fixed_income_cop", "fixed_income_local", "bill_rate", "currency_local", "status", "payment_at",
                  "created_at", "partner", "own_company", "details")

    def get_details(self, bill):
        from api_partner.serializers import (
            WithdrawalPartnerMoneyAccumSerializer,
        )

        bill_detail = bill.withdrawal_partner_money_accum_set
        serialized_bill_detail = WithdrawalPartnerMoneyAccumSerializer(instance=bill_detail, many=True)
        return serialized_bill_detail.data

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def get_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def get_by_id_partner(self, partner, bill_id, database="default"):
        filters = [Q(partner=partner, id=bill_id)]
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).first()

    def get_by_dates_partner(self, filters, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters)


class BillZipForPartnerSerializer(serializers.ModelSerializer):
    """
    Bill ZIP serializer with specific fields for querying purposes (partner)
    """

    details = serializers.SerializerMethodField("get_details")

    class Meta:
        model = WithdrawalPartnerMoney
        fields = ("first_name", "second_name", "last_name", "second_last_name", "email", "phone", "country", "city",
                  "address", "identification", "identification_type", "id", "billed_from_at", "billed_to_at",
                  "cpa_count", "fixed_income_local", "bill_rate", "currency_local", "status", "payment_at",
                  "created_at", "partner", "own_company", "details")

    def get_details(self, bill):
        from api_partner.serializers import (
            WithdrawalPartnerMoneyAccumForPartnerSerializer,
        )

        bill_detail = bill.withdrawal_partner_money_accum_set
        serialized_bill_detail = WithdrawalPartnerMoneyAccumForPartnerSerializer(instance=bill_detail, many=True)
        return serialized_bill_detail.data

    def exist(self, id, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(id=id).first()

    def get_withdrawal_partner_money(self, filters, sort_by, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).order_by(sort_by)

    def get_by_id_partner(self, partner, bill_id, database="default"):
        filters = [Q(partner=partner, id=bill_id)]
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters).first()

    def get_by_dates_partner(self, filters, database="default"):
        return WithdrawalPartnerMoney.objects.db_manager(database).filter(*filters)
