import logging

from core.helpers import (
    CurrencyCondition,
    CurrencyFixedIncome,
)
from django.db import models
from django.db.models import (
    Case,
    F,
    Sum,
    When,
)

logger = logging.getLogger(__name__)


def fx_conversion_usd_partner_daily_cases(model_func=Sum, include_stake=True, include_deposit=True):
    """
    Manage fx conversion cases for deposit and stake on ParnterLinkDailyReport
    Setup conversion cases according to possible currency conditions
    values of bookmakers defined on CurrencyCondition enumerator, this convert
    from bookmaker currency to USD, case same currency this will have the same
    value.

    # Params
    - model_func : class/function
        functions for usage on cases operations for group by or aggregations 
        this are defined on `django.db.models` module like Sum, Avg, F and 
        others.
    - include_stake : bool
        determines if stake will proceeded or not
    - include_deposit : bool
        determines if deposit will proceeded or not

    # Warning
    This condition force to group by betenlace_daily_report__currency_condition

    # Returns
    Tuple with `fx_conversion_deposit_cases`, `fx_conversion_stake_cases` where
    is equivalent to:
    - fx_conversion_deposit_cases : list
        Cases of conversion for deposit
    - fx_conversion_stake_cases : list
        Cases of conversion for stake
    """
    currencies_condition = CurrencyCondition.values
    # Remove usd from currency condition list
    currencies_condition.remove(CurrencyCondition.USD)

    fx_conversion_cases = {}

    if (include_deposit):
        fx_conversion_cases["deposit_usd"] = [
            When(
                betenlace_daily_report__currency_condition__exact=CurrencyCondition.USD,
                then=model_func('deposit'),
            ),
        ]

    if (include_stake):
        fx_conversion_cases["stake_usd"] = [
            When(
                betenlace_daily_report__currency_condition__exact=CurrencyCondition.USD,
                then=model_func('betenlace_daily_report__stake'),
            ),
        ]

    for currency_i in currencies_condition:
        # Add conditional case for each currency condition
        # Deposit case
        if(fx_conversion_cases.get("deposit_usd") is not None):
            fx_conversion_cases["deposit_usd"].append(
                When(
                    betenlace_daily_report__currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('deposit') * F(
                            f"betenlace_daily_report__fx_partner__fx_{currency_i.lower()}_usd")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('deposit') * F(
                                f"betenlace_daily_report__fx_partner__fx_{currency_i.lower()}_usd")
                        )

                    ),
                ),
            )
        if(fx_conversion_cases.get("stake_usd") is not None):
            # Stake case
            fx_conversion_cases["stake_usd"].append(
                When(
                    betenlace_daily_report__currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('betenlace_daily_report__stake') * F(
                            f"betenlace_daily_report__fx_partner__fx_{currency_i.lower()}_usd")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('betenlace_daily_report__stake') * F(
                                f"betenlace_daily_report__fx_partner__fx_{currency_i.lower()}_usd")
                        )

                    ),
                ),
            )

    for key_case in fx_conversion_cases.keys():
        fx_conversion_cases[key_case] = Case(
            *fx_conversion_cases.get(key_case),
            default=None,
            output_field=models.FloatField(
                null=True,
            ),
        )

        return fx_conversion_cases


def fx_conversion_usd_account_cases(model_func=Sum, include_stake=True, include_deposit=True):
    """
    Manage fx conversion cases for deposit and stake on AccountReport
    Setup conversion cases according to possible currency conditions
    values of bookmakers defined on CurrencyCondition enumerator, this convert
    from bookmaker currency to USD, case same currency this will have the same
    value.

    ### Params
    - model_func : class/function
        functions for usage on cases operations for group by or aggregations 
        this are defined on `django.db.models` module like Sum, Avg, F and 
        others.
    - include_stake : bool
        determines if stake will proceeded or not
    - include_deposit : bool
        determines if deposit will proceeded or not

    ### Warning
    This condition force to group by betenlace_daily_report__currency_condition

    ### Returns
    Tuple with `fx_conversion_deposit_cases`, `fx_conversion_stake_cases` where
    is equivalent to:
    - fx_conversion_deposit_cases : list
        Cases of conversion for deposit
    - fx_conversion_stake_cases : list
        Cases of conversion for stake
    """
    # Prevent circular import
    from api_partner.models import FxPartner

    # Get current fx
    fx_partner = FxPartner.objects.all().order_by("-created_at").first()

    currencies_condition = CurrencyCondition.values
    # Remove usd from currency condition list
    currencies_condition.remove(CurrencyCondition.USD)

    fx_conversion_deposit_cases = (
        [
            When(
                currency_condition__exact=CurrencyCondition.USD,
                then=model_func('deposit'),
            ),
        ]
        if include_deposit
        else
        None
    )

    fx_conversion_stake_cases = (
        [
            When(
                currency_condition__exact=CurrencyCondition.USD,
                then=model_func('stake'),
            ),
        ]
        if include_stake
        else
        None
    )

    for currency_i in currencies_condition:
        try:
            fx_conversion = eval(f"fx_partner.fx_{currency_i.lower()}_usd")
        except:
            logger.error(f"Fx conversion from {currency_i.lower()} to usd undefined on DB")

        # Add conditional case for each currency condition
        # Deposit case
        if(fx_conversion_deposit_cases is not None):
            fx_conversion_deposit_cases.append(
                When(
                    currency_condition__exact=currency_i,
                    then=model_func('deposit')*fx_conversion,
                ),
            )
        if(fx_conversion_stake_cases is not None):
            # Stake case
            fx_conversion_stake_cases.append(
                When(
                    currency_condition__exact=currency_i,
                    then=model_func('stake')*fx_conversion,
                ),
            )

    return fx_conversion_deposit_cases, fx_conversion_stake_cases


def fx_conversion_campaign_fixed_income_cases(model_func=F, currency_local="USD"):
    """
    Manage fx conversion cases for fixed_income at model Campaign
    Setup conversion cases according to possible currency conditions
    values of bookmakers defined on CurrencyCondition enumerator, this convert
    from bookmaker currency to USD, case same currency this will have the same
    value.

    ### Params
    - model_func : class/function
        functions for usage on cases operations for group by or aggregations 
        this are defined on `django.db.models` module like Sum, Avg, F and 
        others.

    ### Returns
    - fx_conversion_fixed_income_cases : list
        Cases of conversion for deposit
    """
    # Prevent circular import
    from api_partner.models import (
        FxPartner,
        FxPartnerPercentage,
    )

    # Get current fx
    fx_partner = FxPartner.objects.all().order_by("-created_at").first()

    # Get current fx percentage
    fx_partner_percentage = FxPartnerPercentage.objects.all().order_by("-updated_at").first()

    # Set default 95% if fx_partner percentage is not found
    fx_partner_percentage = fx_partner_percentage.percentage_fx if fx_partner_percentage is not None else 0.95

    currencies_fixed_income = CurrencyFixedIncome.values
    if (currency_local == CurrencyFixedIncome.USD):
        fx_conversion_fixed_income_cases = [
            When(
                currency_fixed_income__exact=CurrencyFixedIncome.USD,
                then=model_func("fixed_income_unitary"),
            ),
        ]
        # Remove usd from currency condition list
        currencies_fixed_income.remove(CurrencyFixedIncome.USD)

    for currency_i in currencies_fixed_income:
        try:
            fx_conversion = eval(f"fx_partner.fx_{currency_i.lower()}_{currency_local.lower()}")
        except:
            logger.error(f"Fx conversion from {currency_i.lower()} to {currency_local.lower()} undefined on DB")

        # Add conditional case for each currency condition
        fx_conversion_fixed_income_cases.append(
            When(
                currency_fixed_income__exact=currency_i,
                then=model_func('fixed_income_unitary')*fx_conversion*fx_partner_percentage,
            ),
        )

    return fx_conversion_fixed_income_cases
