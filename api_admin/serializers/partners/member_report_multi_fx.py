from api_partner.models import BetenlaceDailyReport
from rest_framework import serializers


class MemeberReportMultiFxSer(serializers.ModelSerializer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        elements_remove = set(self.fields.keys()) - set(self.context.get("permissions"))
        for i in elements_remove:
            self.fields.pop(i)

    # Campaign detail
    campaign_title = serializers.CharField()
    prom_code = serializers.CharField()

    # Partner detail
    id_partner = serializers.IntegerField()
    partner_name = serializers.CharField()

    # General money data
    deposit_fxc = serializers.FloatField()
    deposit_partner_fxc = serializers.FloatField()
    stake_fxc = serializers.FloatField()

    net_revenue_fxc = serializers.FloatField()
    revenue_share_fxc = serializers.FloatField()

    # Betenlace CPA payment
    fixed_income_fxc = serializers.FloatField()
    fixed_income_unitary_fxc = serializers.FloatField()

    # Partner member data
    registered_count_partner = serializers.IntegerField()
    first_deposit_count_partner = serializers.IntegerField()
    wagering_count_partner = serializers.IntegerField()

    # Partner CPA count and payment
    cpa_partner = serializers.IntegerField()
    fixed_income_partner_fxc = serializers.FloatField()
    fixed_income_partner_unitary_fxc = serializers.FloatField()
    fixed_income_local = serializers.FloatField()
    fixed_income_unitary_local = serializers.FloatField()

    # fx conversion from currency fixed_income of bookmaker to partner currency
    fx_book_local = serializers.FloatField()

    # CPA percentage control to partner
    percentage_cpa = serializers.FloatField()

    # Tracker percentages
    tracker = serializers.FloatField()
    tracker_deposit = serializers.FloatField()
    tracker_registered_count = serializers.FloatField()
    tracker_first_deposit_count = serializers.FloatField()
    tracker_wagering_count = serializers.FloatField()

    # Adviser data
    adviser_id = serializers.IntegerField()

    # Adviser payment
    fixed_income_adviser_fxc = serializers.FloatField()
    fixed_income_adviser_local = serializers.FloatField()
    net_revenue_adviser_fxc = serializers.FloatField()
    net_revenue_adviser_local = serializers.FloatField()

    # Adviser payment control
    fixed_income_adviser_percentage = serializers.FloatField()
    net_revenue_adviser_percentage = serializers.FloatField()

    # referred data
    referred_by_id = serializers.IntegerField()

    # referred payment
    fixed_income_referred_fxc = serializers.FloatField()
    fixed_income_referred_local = serializers.FloatField()
    net_revenue_referred_fxc = serializers.FloatField()
    net_revenue_referred_local = serializers.FloatField()

    # referred payment control
    fixed_income_referred_percentage = serializers.FloatField()
    net_revenue_referred_percentage = serializers.FloatField()

    # Currency of partner
    currency_local = serializers.CharField()

    class Meta:
        model = BetenlaceDailyReport
        fields = (
            "id",

            # Campaign detail
            "campaign_title",
            "prom_code",

            # Partner detail
            "id_partner",
            "partner_name",

            # General money data
            "deposit_fxc",
            "deposit_partner_fxc",
            "stake_fxc",
            "net_revenue_fxc",
            "revenue_share_fxc",

            # General Count data
            "click_count",
            "registered_count",
            "registered_count_partner",
            "first_deposit_count",
            "first_deposit_count_partner",
            "wagering_count",
            "wagering_count_partner",

            # Betenlace CPA count and payment
            "cpa_count",
            "fixed_income_fxc",
            "fixed_income_unitary_fxc",

            # Partner CPA count and payment
            "cpa_partner",
            "fixed_income_partner_fxc",
            "fixed_income_partner_unitary_fxc",
            "fixed_income_local",
            "fixed_income_unitary_local",

            # fx conversion from currency fixed_income of bookmaker to partner currency
            "fx_book_local",

            # CPA percentage control to partner
            "percentage_cpa",

            # Tracker percentages
            "tracker",
            "tracker_deposit",
            "tracker_registered_count",
            "tracker_first_deposit_count",
            "tracker_wagering_count",

            # Adviser data
            "adviser_id",

            # Adviser payment
            "fixed_income_adviser_fxc",
            "fixed_income_adviser_local",
            "net_revenue_adviser_fxc",
            "net_revenue_adviser_local",

            # Adviser payment control
            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",

            # referred data
            "referred_by_id",
            # referred payment
            "fixed_income_referred_fxc",
            "fixed_income_referred_local",
            "net_revenue_referred_fxc",
            "net_revenue_referred_local",

            # referred payment control
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",

            "created_at",

            # Currencies
            "currency_condition",
            "currency_fixed_income",
            "currency_local",
        )


class MembertReportGroupMultiFxSer(serializers.Serializer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        elements_remove = set(self.fields.keys()) - set(self.context.get("permissions"))
        for i in elements_remove:
            self.fields.pop(i)

    created_at__year = serializers.IntegerField(required=False)
    created_at__month = serializers.IntegerField(required=False)

    # Campaign detail
    campaign_title = serializers.CharField(required=False)
    prom_code = serializers.CharField(required=False)

    # Partner detail
    id_partner = serializers.IntegerField(required=False)
    partner_name = serializers.CharField(required=False)

    # General money data
    deposit_fxc = serializers.FloatField()
    deposit_partner_fxc = serializers.FloatField()
    stake_fxc = serializers.FloatField()

    net_revenue_fxc = serializers.FloatField()
    revenue_share_fxc = serializers.FloatField()

    # Betenlace CPA count and payment
    cpa_count = serializers.IntegerField()
    fixed_income_fxc = serializers.FloatField()
    fixed_income_unitary_fxc = serializers.FloatField()

    # Partner CPA count and payment
    cpa_partner = serializers.IntegerField()
    fixed_income_partner_fxc = serializers.FloatField()
    fixed_income_partner_unitary_fxc = serializers.FloatField()
    fixed_income_local = serializers.FloatField()
    fixed_income_unitary_local = serializers.FloatField()

    # General Count data
    click_count = serializers.IntegerField()
    registered_count = serializers.IntegerField()
    registered_count_partner = serializers.IntegerField()
    first_deposit_count = serializers.IntegerField()
    first_deposit_count_partner = serializers.IntegerField()
    wagering_count = serializers.IntegerField()
    wagering_count_partner = serializers.IntegerField()

    # fx conversion from currency fixed_income of bookmaker to partner currency
    fx_book_local = serializers.FloatField()

    # CPA percentage control to partner
    percentage_cpa = serializers.FloatField()
    tracker = serializers.FloatField()
    tracker_deposit = serializers.FloatField()
    tracker_registered_count = serializers.FloatField()
    tracker_first_deposit_count = serializers.FloatField()
    tracker_wagering_count = serializers.FloatField()

    # Adviser data
    adviser_id = serializers.ListField()

    # Adviser payment
    fixed_income_adviser_fxc = serializers.FloatField()
    fixed_income_adviser_local = serializers.FloatField()
    net_revenue_adviser_fxc = serializers.FloatField()
    net_revenue_adviser_local = serializers.FloatField()

    # Adviser payment control
    fixed_income_adviser_percentage = serializers.FloatField()
    net_revenue_adviser_percentage = serializers.FloatField()

    # referred data
    referred_by_id = serializers.ListField()

    # referred payment
    fixed_income_referred_fxc = serializers.FloatField()
    fixed_income_referred_local = serializers.FloatField()
    net_revenue_referred_fxc = serializers.FloatField()
    net_revenue_referred_local = serializers.FloatField()

    # referred payment control
    fixed_income_referred_percentage = serializers.FloatField()
    net_revenue_referred_percentage = serializers.FloatField()

    # Currency of partner
    currency_condition = serializers.CharField(required=False)
    currency_fixed_income = serializers.CharField(required=False)
    currency_local = serializers.CharField()


class MemberReportConsolidatedMultiFxSer(serializers.Serializer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        elements_remove = set(self.fields.keys()) - set(self.context.get("permissions"))
        for i in elements_remove:
            self.fields.pop(i)

    # General money data
    deposit_fxc = serializers.FloatField()
    deposit_partner_fxc = serializers.FloatField()
    stake_fxc = serializers.FloatField()

    net_revenue_fxc = serializers.FloatField()
    revenue_share_fxc = serializers.FloatField()

    # Betenlace CPA count and payment
    cpa_count = serializers.IntegerField()
    fixed_income_fxc = serializers.FloatField()
    fixed_income_unitary_fxc = serializers.FloatField()

    # Partner CPA count and payment
    cpa_partner = serializers.IntegerField()
    fixed_income_partner_fxc = serializers.FloatField()
    fixed_income_partner_unitary_fxc = serializers.FloatField()
    fixed_income_local = serializers.FloatField()
    fixed_income_unitary_local = serializers.FloatField()

    # General Count data
    click_count = serializers.IntegerField()
    registered_count = serializers.IntegerField()
    registered_count_partner = serializers.IntegerField()
    first_deposit_count = serializers.IntegerField()
    first_deposit_count_partner = serializers.IntegerField()
    wagering_count = serializers.IntegerField()
    wagering_count_partner = serializers.IntegerField()

    # fx conversion from currency fixed_income of bookmaker to partner currency
    fx_book_local = serializers.FloatField()

    # CPA percentage control to partner
    percentage_cpa = serializers.FloatField()
    tracker = serializers.FloatField()
    tracker_deposit = serializers.FloatField()
    tracker_registered_count = serializers.FloatField()
    tracker_first_deposit_count = serializers.FloatField()
    tracker_wagering_count = serializers.FloatField()

    # Adviser data
    adviser_id = serializers.ListField()

    # Adviser payment
    fixed_income_adviser_fxc = serializers.FloatField()
    fixed_income_adviser_local = serializers.FloatField()
    net_revenue_adviser_fxc = serializers.FloatField()
    net_revenue_adviser_local = serializers.FloatField()

    # Adviser payment control
    fixed_income_adviser_percentage = serializers.FloatField()
    net_revenue_adviser_percentage = serializers.FloatField()

    # referred data
    referred_by_id = serializers.ListField()

    # referred payment
    fixed_income_referred_fxc = serializers.FloatField()
    fixed_income_referred_local = serializers.FloatField()
    net_revenue_referred_fxc = serializers.FloatField()
    net_revenue_referred_local = serializers.FloatField()

    # referred payment control
    fixed_income_referred_percentage = serializers.FloatField()
    net_revenue_referred_percentage = serializers.FloatField()

    # Currencies
    currency_condition = serializers.CharField(required=False)
    currency_fixed_income = serializers.CharField(required=False)
    currency_local = serializers.CharField()
