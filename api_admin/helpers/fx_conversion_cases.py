import logging

from core.helpers import (
    CurrencyCondition,
    CurrencyFixedIncome,
    CurrencyPartner,
)
from django.db import models
from django.db.models import (
    Avg,
    Case,
    F,
    Sum,
    When,
)

logger = logging.getLogger(__name__)


def fx_conversion_usd_adviser_daily_cases(
    model_func=Sum,
    include_stake=True,
    include_deposit=True,
    include_net_revenue=True,
    include_revenue_share=True,
    include_fixed_income=True,
    include_fixed_income_unitary=True,
):
    """
    Manage fx conversion cases for deposit and stake on BetenlaceDailyReport
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
    - fx_conversion_net_revenue_cases : list
        Cases of conversion for net_revenue
    - fx_conversion_revenue_share_cases : list
        Cases of conversion for revenue_share
    """
    currencies_condition = CurrencyCondition.values
    # Remove usd from currency condition list
    currencies_condition.remove(CurrencyCondition.USD)

    fx_conversion_cases = {}

    if (include_deposit):
        fx_conversion_cases["deposit_usd"] = [
            When(
                currency_condition__exact=CurrencyCondition.USD,
                then=model_func("deposit"),
            ),
        ]
        fx_conversion_cases["deposit_partner_usd"] = [
            When(
                currency_condition__exact=CurrencyCondition.USD,
                then=model_func("partnerlinkdailyreport__deposit"),
            ),
        ]

    if (include_stake):
        fx_conversion_cases["stake_usd"] = [
            When(
                currency_condition__exact=CurrencyCondition.USD,
                then=model_func("stake"),
            ),
        ]

    if (include_net_revenue):
        fx_conversion_cases["net_revenue_usd"] = [
            When(
                currency_condition__exact=CurrencyCondition.USD,
                then=model_func("net_revenue"),
            ),
        ]

    if (include_revenue_share):
        fx_conversion_cases["revenue_share_usd"] = [
            When(
                currency_condition__exact=CurrencyCondition.USD,
                then=model_func("revenue_share"),
            ),
        ]

    if (include_fixed_income):
        fx_conversion_cases["fixed_income_usd"] = [
            When(
                currency_fixed_income__exact=CurrencyFixedIncome.USD,
                then=model_func("fixed_income"),
            ),
        ]

    if (include_fixed_income_unitary):
        fx_conversion_cases["fixed_income_unitary_usd"] = [
            When(
                currency_fixed_income__exact=CurrencyFixedIncome.USD,
                then=(
                    Avg("fixed_income_unitary")
                    if model_func == Sum
                    else
                    model_func("fixed_income_unitary")
                ),
            ),
        ]

    for currency_i in currencies_condition:
        currency_from_lower = currency_i.lower()
        # Add conditional case for each currency condition
        # Deposit case
        if(fx_conversion_cases.get("deposit_usd") is not None):
            fx_conversion_cases["deposit_usd"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('deposit') * F(f"fx_partner__fx_{currency_from_lower}_usd")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('deposit') *
                            F(f"fx_partner__fx_{currency_from_lower}_usd")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("deposit_partner_usd") is not None):
            fx_conversion_cases["deposit_partner_usd"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('partnerlinkdailyreport__deposit') * F(f"fx_partner__fx_{currency_from_lower}_usd")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('partnerlinkdailyreport__deposit') *
                            F(f"fx_partner__fx_{currency_from_lower}_usd")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("stake_usd") is not None):
            # Stake case
            fx_conversion_cases["stake_usd"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('stake') *
                        F(f"fx_partner__fx_{currency_from_lower}_usd")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('stake') *
                            F(f"fx_partner__fx_{currency_from_lower}_usd")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("net_revenue_usd") is not None):
            # Net revenue cases
            fx_conversion_cases["net_revenue_usd"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('net_revenue') *
                        F(f"fx_partner__fx_{currency_from_lower}_usd")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('net_revenue') *
                            F(f"fx_partner__fx_{currency_from_lower}_usd")
                        )

                    ),
                ),
            )

        if(fx_conversion_cases.get("revenue_share_usd") is not None):
            # Revenue share cases
            fx_conversion_cases["revenue_share_usd"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('revenue_share') *
                        F(f"fx_partner__fx_{currency_from_lower}_usd")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('revenue_share') *
                            F(f"fx_partner__fx_{currency_from_lower}_usd")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("fixed_income_usd") is not None):
            # Revenue share cases
            fx_conversion_cases["fixed_income_usd"].append(
                When(
                    currency_fixed_income__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('fixed_income') *
                        F(f"fx_partner__fx_{currency_from_lower}_usd")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('fixed_income') *
                            F(f"fx_partner__fx_{currency_from_lower}_usd")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("fixed_income_unitary_usd") is not None):
            # Revenue share cases
            fx_conversion_cases["fixed_income_unitary_usd"].append(
                When(
                    currency_fixed_income__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('fixed_income_unitary') *
                        F(f"fx_partner__fx_{currency_from_lower}_usd")
                        if model_func == F
                        else
                        # Sum case group
                        Avg(
                            F('fixed_income_unitary') *
                            F(f"fx_partner__fx_{currency_from_lower}_usd")
                        )
                        if model_func == Sum
                        else
                        # Other cases must group
                        model_func(
                            F('fixed_income_unitary') *
                            F(f"fx_partner__fx_{currency_from_lower}_usd")
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


def fx_conversion_specific_adviser_daily_cases(
    model_func=Sum,
    currency_to="USD",
    include_stake=True,
    include_deposit=True,
    include_net_revenue=True,
    include_revenue_share=True,
    include_fixed_income=True,
    include_fixed_income_unitary=True,
    include_fixed_income_partner=True,
    include_fixed_income_partner_unitary=True,
    include_fixed_income_adviser=True,
    include_net_revenue_adviser=True,
    include_fixed_income_referred=True,
    include_net_revenue_referred=True,
):
    """
    Manage fx conversion cases for deposit and stake on BetenlaceDailyReport
    Setup conversion cases according to possible currency conditions
    values of bookmakers defined on CurrencyCondition and CurrencyFixedincome 
    enumerator, this convert from bookmaker currency to supplied currency_to 
    var, case same currency this will have the same value.

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
    - fx_conversion_net_revenue_cases : list
        Cases of conversion for net_revenue
    - fx_conversion_revenue_share_cases : list
        Cases of conversion for revenue_share
    """
    if (not currency_to in CurrencyPartner):
        raise ValueError(f"Currency {currency_to} is not in Enumerator CurrencyPartner")

    currencies_condition = CurrencyCondition.values
    # Remove currency_to from currency condition list
    if (currency_to in currencies_condition):
        currencies_condition.remove(currency_to)

    fx_conversion_cases = {}

    if (include_deposit):
        fx_conversion_cases["deposit_fxc"] = [
            When(
                currency_condition__exact=currency_to,
                then=model_func("deposit"),
            ),
        ]
        fx_conversion_cases["deposit_partner_fxc"] = [
            When(
                currency_condition__exact=currency_to,
                then=model_func("partnerlinkdailyreport__deposit"),
            ),
        ]

    if (include_stake):
        fx_conversion_cases["stake_fxc"] = [
            When(
                currency_condition__exact=currency_to,
                then=model_func("stake"),
            ),
        ]

    if (include_net_revenue):
        fx_conversion_cases["net_revenue_fxc"] = [
            When(
                currency_condition__exact=currency_to,
                then=model_func("net_revenue"),
            ),
        ]

    if (include_revenue_share):
        fx_conversion_cases["revenue_share_fxc"] = [
            When(
                currency_condition__exact=currency_to,
                then=model_func("revenue_share"),
            ),
        ]

    if (include_fixed_income):
        fx_conversion_cases["fixed_income_fxc"] = [
            When(
                currency_fixed_income__exact=currency_to,
                then=model_func("fixed_income"),
            ),
        ]

    if (include_fixed_income_unitary):
        fx_conversion_cases["fixed_income_unitary_fxc"] = [
            When(
                currency_fixed_income__exact=currency_to,
                then=(
                    Avg("fixed_income_unitary")
                    if model_func == Sum
                    else
                    model_func("fixed_income_unitary")
                ),
            ),
        ]

    if (include_fixed_income_partner):
        fx_conversion_cases["fixed_income_partner_fxc"] = [
            When(
                currency_fixed_income__exact=currency_to,
                then=model_func("partnerlinkdailyreport__fixed_income"),
            ),
        ]

    if (include_fixed_income_partner_unitary):
        fx_conversion_cases["fixed_income_partner_unitary_fxc"] = [
            When(
                currency_fixed_income__exact=currency_to,
                then=(
                    Avg("partnerlinkdailyreport__fixed_income_unitary")
                    if model_func == Sum
                    else
                    model_func("partnerlinkdailyreport__fixed_income_unitary")
                ),
            ),
        ]

    if (include_fixed_income_adviser):
        fx_conversion_cases["fixed_income_adviser_fxc"] = [
            When(
                currency_fixed_income__exact=currency_to,
                then=model_func("partnerlinkdailyreport__fixed_income_adviser"),
            ),
        ]

    if (include_net_revenue_adviser):
        fx_conversion_cases["net_revenue_adviser_fxc"] = [
            When(
                currency_condition__exact=currency_to,
                then=model_func("partnerlinkdailyreport__net_revenue_adviser"),
            ),
        ]

    if (include_fixed_income_referred):
        fx_conversion_cases["fixed_income_referred_fxc"] = [
            When(
                currency_fixed_income__exact=currency_to,
                then=model_func("partnerlinkdailyreport__fixed_income_referred"),
            ),
        ]

    if (include_net_revenue_referred):
        fx_conversion_cases["net_revenue_referred_fxc"] = [
            When(
                currency_fixed_income__exact=currency_to,
                then=model_func("partnerlinkdailyreport__net_revenue_referred"),
            ),
        ]

    currency_to_lower = currency_to.lower()
    for currency_i in currencies_condition:
        currency_from_lower = currency_i.lower()
        # Add conditional case for each currency condition
        # Deposit case
        if(fx_conversion_cases.get("deposit_fxc") is not None):
            fx_conversion_cases["deposit_fxc"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('deposit') * F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('deposit') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )

                    ),
                ),
            )

        if(fx_conversion_cases.get("deposit_partner_fxc") is not None):
            fx_conversion_cases["deposit_partner_fxc"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('partnerlinkdailyreport__deposit') * F(
                            f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('partnerlinkdailyreport__deposit') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("stake_fxc") is not None):
            # Stake case
            fx_conversion_cases["stake_fxc"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('stake') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('stake') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("net_revenue_fxc") is not None):
            # Net revenue cases
            fx_conversion_cases["net_revenue_fxc"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('net_revenue') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('net_revenue') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("revenue_share_fxc") is not None):
            # Revenue share cases
            fx_conversion_cases["revenue_share_fxc"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('revenue_share') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('revenue_share') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("fixed_income_fxc") is not None):
            # Fixed income cases
            fx_conversion_cases["fixed_income_fxc"].append(
                When(
                    currency_fixed_income__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('fixed_income') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('fixed_income') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("fixed_income_unitary_fxc") is not None):
            # Fixed income unitary cases
            fx_conversion_cases["fixed_income_unitary_fxc"].append(
                When(
                    currency_fixed_income__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('fixed_income_unitary') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Sum case group
                        Avg(
                            F('fixed_income_unitary') *
                            F(f"fx_partner__fx_{currency_i.lower()}_{currency_to_lower}")
                        )
                        if model_func == Sum
                        else
                        # Other cases must group
                        model_func(
                            F('fixed_income_unitary') *
                            F(f"fx_partner__fx_{currency_i.lower()}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("fixed_income_partner_fxc") is not None):
            # Fixed income partner cases
            fx_conversion_cases["fixed_income_partner_fxc"].append(
                When(
                    currency_fixed_income__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('partnerlinkdailyreport__fixed_income') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('partnerlinkdailyreport__fixed_income') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("fixed_income_partner_unitary_fxc") is not None):
            # Fixed income partner unitary cases
            fx_conversion_cases["fixed_income_partner_unitary_fxc"].append(
                When(
                    currency_fixed_income__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('partnerlinkdailyreport__fixed_income_unitary') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Sum case group
                        Avg(
                            F('partnerlinkdailyreport__fixed_income_unitary') *
                            F(f"fx_partner__fx_{currency_i.lower()}_{currency_to_lower}")
                        )
                        if model_func == Sum
                        else
                        # Other cases must group
                        model_func(
                            F('partnerlinkdailyreport__fixed_income_unitary') *
                            F(f"fx_partner__fx_{currency_i.lower()}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("fixed_income_adviser_fxc") is not None):
            # Fixed income adviser cases
            fx_conversion_cases["fixed_income_adviser_fxc"].append(
                When(
                    currency_fixed_income__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('partnerlinkdailyreport__fixed_income_adviser') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('partnerlinkdailyreport__fixed_income_adviser') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("net_revenue_adviser_fxc") is not None):
            # Fixed income adviser cases
            fx_conversion_cases["net_revenue_adviser_fxc"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('partnerlinkdailyreport__net_revenue_adviser') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('partnerlinkdailyreport__net_revenue_adviser') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("net_revenue_referred_fxc") is not None):
            # Fixed income referred cases
            fx_conversion_cases["net_revenue_referred_fxc"].append(
                When(
                    currency_condition__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('partnerlinkdailyreport__net_revenue_referred') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('partnerlinkdailyreport__net_revenue_referred') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        )
                    ),
                ),
            )

        if(fx_conversion_cases.get("fixed_income_referred_fxc") is not None):
            # Fixed income referred cases
            fx_conversion_cases["fixed_income_referred_fxc"].append(
                When(
                    currency_fixed_income__exact=currency_i,
                    then=(
                        # Case F, this case not group
                        F('partnerlinkdailyreport__fixed_income_referred') *
                        F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
                        if model_func == F
                        else
                        # Other cases must group
                        model_func(
                            F('partnerlinkdailyreport__fixed_income_referred') *
                            F(f"fx_partner__fx_{currency_from_lower}_{currency_to_lower}")
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
