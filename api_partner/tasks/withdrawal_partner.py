import pytz
from api_partner.helpers import (
    DB_USER_PARTNER,
    PartnerStatusCHO,
)
from api_partner.models import (
    AdditionalInfo,
    Campaign,
    FxPartner,
    MinWithdrawalPartnerMoney,
    OwnCompany,
    Partner,
    PartnerBankAccount,
    PartnerLinkDailyReport,
    WithdrawalPartnerMoney,
    WithdrawalPartnerMoneyAccum,
)
from betenlace.celery import app
from celery.utils.log import get_task_logger
from core.helpers import (
    CurrencyAll,
    CurrencyCondition,
    CurrencyFixedIncome,
    CurrencyWithdrawalToUSD,
)
from core.models import User
from dateutil.relativedelta import relativedelta
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

logger_task = get_task_logger(__name__)


@app.task
def withdrawal_partner():
    """
    The Fx rate will be recalculated at the end of the month for the month
    being billed.
    The CPA payment percentage will be recalculated at the end of the month,
    so only on the next billing day will you be able to change the CPA
    percentage.
    """
    # Alerts count
    critical_count = 0
    error_count = 0
    warning_count = 0

    # Definition of function
    today = timezone.now().astimezone(pytz.timezone(settings.TIME_ZONE))
    logger_task.info(
        "Calculating billing\n"
        f"Today -> {today}"
    )

    prev_month_date = today + relativedelta(months=-1) + relativedelta(day=1)
    prev_month_date_last_day = prev_month_date + relativedelta(day=31)

    prev_year = prev_month_date.year
    prev_month = prev_month_date.month

    filters = [
        Q(created_at__month=prev_month),
        Q(created_at__year=prev_year),
        Q(cpa_count__isnull=False),
    ]
    partner_link_dailies = PartnerLinkDailyReport.objects.filter(
        *filters,
    ).select_related(
        "partner_link_accumulated",
        # "partner_link_accumulated__partner",
        "betenlace_daily_report",
    )

    partners_id = set(partner_link_dailies.values_list("partner_link_accumulated__partner_id", flat=True))
    campaigns_id = set(partner_link_dailies.values_list("partner_link_accumulated__campaign_id", flat=True))

    filters = (
        Q(id__in=partners_id),
    )
    users = User.objects.db_manager(DB_USER_PARTNER).filter(*filters).select_related("partner")

    filters = (
        Q(partner_id__in=partners_id),
    )
    additional_infos = AdditionalInfo.objects.db_manager(DB_USER_PARTNER).filter(*filters)

    query = Q(is_primary=True) & Q(partner_id__in=partners_id)
    bank_accounts = PartnerBankAccount.objects.filter(query)

    filters = (
        Q(id__in=campaigns_id),
    )
    campaigns = Campaign.objects.filter(*filters)

    filters = (
        Q(created_at__lte=today),
    )
    own_company = OwnCompany.objects.filter(*filters).order_by("-created_at").first()

    if(own_company is None):
        error_count += 1
        error_msg = "Undefined own_company on DB"
        logger_task.error(error_msg)
        return f"crit:{critical_count} error:{error_count} warn:{warning_count} today:{today.date()} - {error_msg}"

    filters = (
        Q(partner_id__in=partners_id),
        ~Q(status=WithdrawalPartnerMoney.Status.PAYED),
    )
    withdrawals = WithdrawalPartnerMoney.objects.filter(
        *filters,
    ).prefetch_related("withdrawal_partner_money_accum_set")

    # Get the last Fx value
    filters = (
        Q(created_at__lte=today),
    )
    fx_partner = FxPartner.objects.filter(*filters).order_by("-created_at").first()

    if(fx_partner is None):
        error_count += 1
        error_msg = "Undefined fx_partner on DB"
        logger_task.error(error_msg)
        return f"crit:{critical_count} error:{error_count} warn:{warning_count} today:{today.date()} - {error_msg}"

    fx_partner_percentage = fx_partner.fx_percentage

    filters = (
        Q(created_at__lte=today),
    )
    min_withdrawal = MinWithdrawalPartnerMoney.objects.filter(*filters).order_by("-created_at").first()

    if(min_withdrawal is None):
        error_count += 1
        error_msg = "Undefined min_withdrawal on DB"
        logger_task.error(error_msg)
        return f"crit:{critical_count} error:{error_count} warn:{warning_count} today:{today.date()} - {error_msg}"

    withdrawal_partner_create = []
    withdrawal_partner_update = []
    withdrawal_partner_accum_create = []
    withdrawal_partner_accum_update = []
    partner_link_daily_report_update = []
    for partner_id_i in partners_id:
        # daily report of certain Partner
        partner_dailies_iter = list(
            filter(
                lambda partner_link_daily: partner_link_daily.partner_link_accumulated.partner_id == partner_id_i,
                partner_link_dailies,
            )
        )

        # Add normal currencies
        fixed_incomes = {
            f"fixed_income_{currency_i.lower()}": 0 for currency_i in CurrencyFixedIncome.labels
        }

        # Add transitories currencies to usd
        fixed_incomes = fixed_incomes | {
            f"fixed_income_{currency_i.lower()}_usd": 0 for currency_i in CurrencyWithdrawalToUSD.labels
        }

        # Add currencie local
        fixed_incomes["fixed_income_local"] = 0

        cpa_count_partner = 0

        for campaign_id_i in campaigns_id:
            # daily reports of certain campaign and previous Partner
            partner_dailies_campaign_iter = list(
                filter(
                    lambda partner_daily_iter: (
                        partner_daily_iter.partner_link_accumulated.campaign_id == campaign_id_i
                    ),
                    partner_dailies_iter,
                )
            )

            if (not partner_dailies_campaign_iter):
                logger_task.warning(
                    f"Partner id {partner_id_i}, do not have data on daily report of campaign id {campaign_id_i}"
                )
                continue

            campaign = next(
                filter(
                    lambda campaign: campaign.id == campaign_id_i,
                    campaigns,
                ),
                None,
            )

            campaign_currency_fixed_income = campaign.currency_fixed_income

            # Temporary currency condition is same to all incoming data
            # This is used in case payment is not reflected on data and
            # assume different to data
            campaign_currency_condition = campaign.currency_condition

            # Must be same and only one per campaign and partner
            partner_link_accumulated = partner_dailies_campaign_iter[0].partner_link_accumulated

            currency_local = partner_link_accumulated.currency_local

            campaign_currency_fixed_income_str = campaign_currency_fixed_income.lower()
            campaign_currency_condition_str = campaign_currency_condition.lower()
            partner_currency_str = currency_local.lower()

            for partner_daily_i in partner_dailies_campaign_iter:
                # Accumulate cpa
                cpa_count_partner += partner_daily_i.cpa_count

                # Fixed income on Currency of Bookmaker, update daily of days of withdrawal
                partner_daily_i.fixed_income = (
                    campaign.fixed_income_unitary *
                    partner_link_accumulated.percentage_cpa *
                    partner_daily_i.cpa_count
                )
                partner_daily_i.percentage_cpa = partner_link_accumulated.percentage_cpa
                partner_daily_i.fx_percentage = fx_partner_percentage
                partner_daily_i.fixed_income_unitary = (
                    partner_daily_i.fixed_income / partner_daily_i.cpa_count
                    if partner_daily_i.cpa_count != 0
                    else
                    0
                )

                if (not campaign_currency_fixed_income in CurrencyFixedIncome):
                    logger_task.critical(
                        f"Currency {campaign_currency_fixed_income} fixed income not configured on enumerator MUST CHECK"
                    )
                    return

                # Local conversions, Verifying if Local currency are same to
                # payment for fixed income conversions
                if (
                    any(
                        campaign_currency_fixed_income == currency_i and
                        currency_local == currency_i
                        for currency_i in CurrencyFixedIncome
                    )
                ):
                    partner_daily_i.fixed_income_local = partner_daily_i.fixed_income
                    partner_daily_i.fixed_income_unitary_local = (
                        partner_daily_i.fixed_income_local / partner_daily_i.cpa_count
                        if partner_daily_i.cpa_count != 0
                        else
                        0
                    )

                    partner_daily_i.fx_book_local = 1
                    partner_daily_i.fx_percentage = fx_partner_percentage
                else:
                    # USDs conversions
                    fx_book_usd = _calc_fx_no_percentage(
                        fx_partner=fx_partner,
                        currency_from=campaign_currency_fixed_income,
                        currency_to=CurrencyAll.USD,
                        currency_from_str=campaign_currency_fixed_income_str,
                        currency_to_str=CurrencyAll.USD.lower(),
                    )

                    iter_usd = fx_book_usd * partner_daily_i.fixed_income

                    # Add sum to transitory conversion, (all except same
                    # currency usd)
                    if (campaign_currency_fixed_income in CurrencyWithdrawalToUSD):
                        fixed_incomes[f"fixed_income_{campaign_currency_fixed_income_str}_usd"] += iter_usd

                    fx_usd_partner = _calc_fx(
                        fx_partner=fx_partner,
                        fx_partner_percentage=fx_partner_percentage,
                        campaign_currency=CurrencyAll.USD,
                        currency_local=currency_local,
                        campaign_currency_fixed_income_str=CurrencyAll.USD.lower(),
                        partner_currency_str=partner_currency_str,
                    )
                    # Fixed income on Currency of Partner
                    partner_daily_i.fixed_income_local = iter_usd * fx_usd_partner
                    partner_daily_i.fixed_income_unitary_local = (
                        partner_daily_i.fixed_income_local / partner_daily_i.cpa_count
                        if partner_daily_i.cpa_count != 0
                        else
                        0
                    )

                    partner_daily_i.fx_book_local = fx_book_usd * fx_usd_partner
                    partner_daily_i.fx_percentage = fx_partner_percentage

                # Calculate Fx book for adviser and Revenue share partner
                # payment
                if (
                    any(
                        campaign_currency_condition == currency_i and
                        currency_local == currency_i
                        for currency_i in CurrencyCondition
                    )
                ):
                    # Here calculation of USDs for payment for RS and
                    # netrevenue case
                    partner_daily_i.fx_book_net_revenue_local = 1
                else:
                    # USDs conversions
                    fx_book_usd = _calc_fx_no_percentage(
                        fx_partner=fx_partner,
                        currency_from=campaign_currency_condition,
                        currency_to=CurrencyAll.USD,
                        currency_from_str=campaign_currency_condition_str,
                        currency_to_str=CurrencyAll.USD.lower(),
                    )

                    # Here must be calculate USD transition to currency
                    # local for RS and net revenue payment
                    fx_usd_local_net_revenue = _calc_fx(
                        fx_partner=fx_partner,
                        fx_partner_percentage=fx_partner_percentage,
                        from_currency=CurrencyAll.USD,
                        to_currency=currency_local,
                        from_currency_str=CurrencyAll.USD.lower(),
                        to_currency_str=partner_currency_str,
                    )

                    partner_daily_i.fx_book_net_revenue_local = fx_book_usd * fx_usd_local_net_revenue

                # Calculate Adviser payment

                # Update adviser
                # partner = partner_daily_i.partner_link_accumulated.partner
                # partner_daily_i.adviser_id = partner.adviser_id
                # partner_daily_i.fixed_income_adviser_percentage = partner.fixed_income_adviser_percentage
                # partner_daily_i.net_revenue_adviser_percentage = partner.net_revenue_adviser_percentage

                if (partner_daily_i.fixed_income_adviser_percentage is not None):
                    partner_daily_i.fixed_income_adviser = (
                        partner_daily_i.fixed_income *
                        partner_daily_i.fixed_income_adviser_percentage
                    )
                    partner_daily_i.fixed_income_adviser_local = (
                        partner_daily_i.fixed_income_adviser *
                        partner_daily_i.fx_book_local
                    )

                if (partner_daily_i.net_revenue_adviser_percentage is not None):
                    partner_daily_i.net_revenue_adviser = (
                        partner_daily_i.betenlace_daily_report.net_revenue *
                        partner_daily_i.net_revenue_adviser_percentage
                        if partner_daily_i.betenlace_daily_report.net_revenue is not None
                        else
                        0
                    )
                    partner_daily_i.net_revenue_adviser_local = (
                        partner_daily_i.net_revenue_adviser *
                        partner_daily_i.fx_book_net_revenue_local
                    )

                ####
                if (partner_daily_i.fixed_income_referred_percentage is not None):
                    partner_daily_i.fixed_income_referred = (
                        partner_daily_i.fixed_income *
                        partner_daily_i.fixed_income_referred_percentage
                    )
                    partner_daily_i.fixed_income_referred_local = (
                        partner_daily_i.fixed_income_referred *
                        partner_daily_i.fx_book_local
                    )

                if (partner_daily_i.net_revenue_referred_percentage is not None):
                    partner_daily_i.net_revenue_referred = (
                        partner_daily_i.betenlace_daily_report.net_revenue *
                        partner_daily_i.net_revenue_referred_percentage
                        if partner_daily_i.betenlace_daily_report.net_revenue is not None
                        else
                        0
                    )
                    partner_daily_i.net_revenue_referred_local = (
                        partner_daily_i.net_revenue_referred *
                        partner_daily_i.fx_book_net_revenue_local
                    )

                partner_link_daily_report_update.append(partner_daily_i)

                # Sum accum
                fixed_incomes[f"fixed_income_{campaign_currency_fixed_income_str}"] += partner_daily_i.fixed_income
                fixed_incomes[f"fixed_income_local"] += partner_daily_i.fixed_income_local

        # Sum all
        current_fixed_income_usd_total = fixed_incomes.get("fixed_income_usd")

        current_fixed_income_eur_total = fixed_incomes.get("fixed_income_eur")
        current_fixed_income_eur_usd_total = fixed_incomes.get("fixed_income_eur_usd")

        current_fixed_income_cop_total = fixed_incomes.get("fixed_income_cop")
        current_fixed_income_cop_usd_total = fixed_incomes.get("fixed_income_cop_usd")

        current_fixed_income_mxn_total = fixed_incomes.get("fixed_income_mxn")
        current_fixed_income_mxn_usd_total = fixed_incomes.get("fixed_income_mxn_usd")

        current_fixed_income_gbp_total = fixed_incomes.get("fixed_income_gbp")
        current_fixed_income_gbp_usd_total = fixed_incomes.get("fixed_income_gbp_usd")

        current_fixed_income_pen_total = fixed_incomes.get("fixed_income_pen")
        current_fixed_income_pen_usd_total = fixed_incomes.get("fixed_income_pen_usd")

        current_fixed_income_local_total = fixed_incomes.get("fixed_income_local")

        cpa_count_accum = 0

        # Get accumulations
        withdrawal = next(filter(lambda withdrawal: withdrawal.partner_id == partner_id_i, withdrawals), None)
        user = next(filter(lambda user: user.id == partner_id_i, users), None)
        additional_info = next(
            filter(
                lambda additional_info: additional_info.pk == partner_id_i,
                additional_infos,
            ),
            None,
        )
        bank_account = next(
            (bank for bank in bank_accounts if bank.partner_id == partner_id_i),
            None,
        )
        accum_id = None

        if(withdrawal):
            withdrawals_accum = withdrawal.withdrawal_partner_money_accum_set.values(
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
                "cpa_count",
                "accum_at",
                "id",
            )
            for withdrawal_accum_i in withdrawals_accum:
                # Ignore the accum of same current date
                if(
                    withdrawal_accum_i.get("accum_at").month == prev_month and
                    withdrawal_accum_i.get("accum_at").year == prev_year
                ):
                    accum_id = withdrawal_accum_i.get("id")
                    continue
                current_fixed_income_usd_total += withdrawal_accum_i.get("fixed_income_usd")

                current_fixed_income_eur_total += withdrawal_accum_i.get("fixed_income_eur")
                current_fixed_income_eur_usd_total += withdrawal_accum_i.get("fixed_income_eur_usd")

                current_fixed_income_cop_total += withdrawal_accum_i.get("fixed_income_cop")
                current_fixed_income_cop_usd_total += withdrawal_accum_i.get("fixed_income_cop_usd")

                current_fixed_income_mxn_total += withdrawal_accum_i.get("fixed_income_mxn")
                current_fixed_income_mxn_usd_total += withdrawal_accum_i.get("fixed_income_mxn_usd")

                current_fixed_income_gbp_total += withdrawal_accum_i.get("fixed_income_gbp")
                current_fixed_income_gbp_usd_total += withdrawal_accum_i.get("fixed_income_gbp_usd")

                current_fixed_income_pen_total += withdrawal_accum_i.get("fixed_income_pen")
                current_fixed_income_pen_usd_total += withdrawal_accum_i.get("fixed_income_pen_usd")

                current_fixed_income_local_total += withdrawal_accum_i.get("fixed_income_local")
                cpa_count_accum += withdrawal_accum_i.get("cpa_count")

            withdrawal.partner_id = partner_id_i
            withdrawal.own_company = own_company
            withdrawal.bank_account = bank_account
            withdrawal.first_name = user.first_name
            withdrawal.second_name = user.second_name
            withdrawal.last_name = user.last_name
            withdrawal.second_last_name = user.second_last_name
            withdrawal.email = user.email
            withdrawal.phone = user.phone
            withdrawal.identification = additional_info.identification
            withdrawal.identification_type = additional_info.identification_type
            withdrawal.currency_local = currency_local
            withdrawal.cpa_count = cpa_count_accum + cpa_count_partner
            withdrawal.billed_to_at = prev_month_date_last_day
            withdrawal.fixed_income_usd = current_fixed_income_usd_total

            withdrawal.fixed_income_eur = current_fixed_income_eur_total
            withdrawal.fixed_income_eur_usd = current_fixed_income_eur_usd_total

            withdrawal.fixed_income_cop = current_fixed_income_cop_total
            withdrawal.fixed_income_cop_usd = current_fixed_income_cop_usd_total

            withdrawal.fixed_income_mxn = current_fixed_income_mxn_total
            withdrawal.fixed_income_mxn_usd = current_fixed_income_mxn_usd_total

            withdrawal.fixed_income_gbp = current_fixed_income_gbp_total
            withdrawal.fixed_income_gbp_usd = current_fixed_income_gbp_usd_total

            withdrawal.fixed_income_pen = current_fixed_income_pen_total
            withdrawal.fixed_income_pen_usd = current_fixed_income_pen_usd_total

            withdrawal.fixed_income_local = current_fixed_income_local_total

            withdrawal_partner_update.append(withdrawal)
        else:
            # Discard case for none cpas
            if(cpa_count_partner == 0):
                logger_task.warning(
                    f"Discard creation for Partner id {partner_id_i}, don't have cpas for date "
                    f"{prev_month_date}-{prev_month_date_last_day}"
                )
                continue

            withdrawal = WithdrawalPartnerMoney(
                partner_id=partner_id_i,
                own_company=own_company,
                bank_account=bank_account,
                first_name=user.first_name,
                second_name=user.second_name,
                last_name=user.last_name,
                second_last_name=user.second_last_name,
                email=user.email,
                phone=user.phone,
                identification=additional_info.identification,
                identification_type=additional_info.identification_type,
                billed_from_at=prev_month_date,
                billed_to_at=prev_month_date_last_day,
                currency_local=currency_local,
                cpa_count=cpa_count_accum + cpa_count_partner,

                fixed_income_usd=current_fixed_income_usd_total,

                fixed_income_eur=current_fixed_income_eur_total,
                fixed_income_eur_usd=current_fixed_income_eur_usd_total,

                fixed_income_cop=current_fixed_income_cop_total,
                fixed_income_cop_usd=current_fixed_income_cop_usd_total,

                fixed_income_mxn=current_fixed_income_mxn_total,
                fixed_income_mxn_usd=current_fixed_income_mxn_usd_total,

                fixed_income_gbp=current_fixed_income_gbp_total,
                fixed_income_gbp_usd=current_fixed_income_gbp_usd_total,

                fixed_income_pen=current_fixed_income_pen_total,
                fixed_income_pen_usd=current_fixed_income_pen_usd_total,

                fixed_income_local=current_fixed_income_local_total,
            )
            withdrawal_partner_create.append(withdrawal)

        if(accum_id):
            # Update accum at for same month
            filters = (
                Q(id=accum_id),
            )
            withdrawal_accum = WithdrawalPartnerMoneyAccum.objects.filter(*filters).first()
            withdrawal_accum.withdrawal_partner_money = withdrawal
            withdrawal_accum.cpa_count = cpa_count_partner

            withdrawal_accum.fixed_income_usd = fixed_incomes.get("fixed_income_usd")

            withdrawal_accum.fixed_income_eur = fixed_incomes.get("fixed_income_eur")
            withdrawal_accum.fixed_income_eur_usd = fixed_incomes.get("fixed_income_eur_usd")

            withdrawal_accum.fixed_income_cop = fixed_incomes.get("fixed_income_cop")
            withdrawal_accum.fixed_income_cop_usd = fixed_incomes.get("fixed_income_cop_usd")

            withdrawal_accum.fixed_income_mxn = fixed_incomes.get("fixed_income_mxn")
            withdrawal_accum.fixed_income_mxn_usd = fixed_incomes.get("fixed_income_mxn_usd")

            withdrawal_accum.fixed_income_gbp = fixed_incomes.get("fixed_income_gbp")
            withdrawal_accum.fixed_income_gbp_usd = fixed_incomes.get("fixed_income_gbp_usd")

            withdrawal_accum.fixed_income_pen = fixed_incomes.get("fixed_income_pen")
            withdrawal_accum.fixed_income_pen_usd = fixed_incomes.get("fixed_income_pen_usd")

            withdrawal_accum.fx_partner = fx_partner
            withdrawal_accum.fx_percentage = fx_partner_percentage

            withdrawal_accum.fixed_income_local = fixed_incomes.get("fixed_income_local")
            withdrawal_accum.currency_local = currency_local
            withdrawal_accum.partner_level = user.partner.level

            withdrawal_partner_accum_update.append(withdrawal_accum)
        else:
            # Create accum
            withdrawal_accum = WithdrawalPartnerMoneyAccum(
                withdrawal_partner_money=withdrawal,
                cpa_count=cpa_count_partner,

                fixed_income_usd=fixed_incomes.get("fixed_income_usd"),

                fixed_income_eur=fixed_incomes.get("fixed_income_eur"),
                fixed_income_eur_usd=fixed_incomes.get("fixed_income_eur_usd"),

                fixed_income_cop=fixed_incomes.get("fixed_income_cop"),
                fixed_income_cop_usd=fixed_incomes.get("fixed_income_cop_usd"),

                fixed_income_mxn=fixed_incomes.get("fixed_income_mxn"),
                fixed_income_mxn_usd=fixed_incomes.get("fixed_income_mxn_usd"),

                fixed_income_gbp=fixed_incomes.get("fixed_income_gbp"),
                fixed_income_gbp_usd=fixed_incomes.get("fixed_income_gbp_usd"),

                fixed_income_pen=fixed_incomes.get("fixed_income_pen"),
                fixed_income_pen_usd=fixed_incomes.get("fixed_income_pen_usd"),

                fixed_income_local=fixed_incomes.get("fixed_income_local"),

                fx_partner=fx_partner,
                fx_percentage=fx_partner_percentage,

                currency_local=currency_local,
                accum_at=prev_month_date_last_day,
                partner_level=user.partner.level,
            )
            withdrawal_partner_accum_create.append(withdrawal_accum)

        try:
            min_local = eval(f"min_withdrawal.min_{partner_currency_str}")
        except:
            error_count += 1
            error_msg = f"Error at get withdrawal min for partner_id {partner_id_i}"
            logger_task.error(error_msg)
            return f"crit:{critical_count} error:{error_count} warn:{warning_count} today:{today.date()} - {error_msg}"

        partner = user.partner
        if partner.bank_status == PartnerStatusCHO.ACCEPTED:
            if current_fixed_income_local_total >= min_local:
                withdrawal.status = WithdrawalPartnerMoney.Status.TO_PAY
            else:
                withdrawal.status = WithdrawalPartnerMoney.Status.NOT_READY
        else:
            withdrawal.status = WithdrawalPartnerMoney.Status.NO_INFO

    with transaction.atomic(using=DB_USER_PARTNER):
        if(withdrawal_partner_create):
            WithdrawalPartnerMoney.objects.bulk_create(objs=withdrawal_partner_create)

        if(withdrawal_partner_update):
            WithdrawalPartnerMoney.objects.bulk_update(
                objs=withdrawal_partner_update,
                fields=(
                    "own_company",
                    "bank_account",
                    "first_name",
                    "second_name",
                    "last_name",
                    "second_last_name",
                    "email",
                    "phone",
                    "country",
                    "city",
                    "address",
                    "identification",
                    "identification_type",
                    "currency_local",
                    "status",
                    "cpa_count",
                    "billed_to_at",

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
                ),
            )
        if(withdrawal_partner_accum_create):
            WithdrawalPartnerMoneyAccum.objects.bulk_create(objs=withdrawal_partner_accum_create)

        if(withdrawal_partner_accum_update):
            WithdrawalPartnerMoneyAccum.objects.bulk_update(
                objs=withdrawal_partner_accum_update,
                fields=(
                    "withdrawal_partner_money",
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

                    "fx_partner",
                    "fx_percentage",

                    "fixed_income_local",
                    "currency_local",
                    "partner_level",
                ),
            )

        if(partner_link_daily_report_update):
            PartnerLinkDailyReport.objects.bulk_update(
                objs=partner_link_daily_report_update,
                fields=(
                    "fixed_income",
                    "fixed_income_unitary",
                    "fixed_income_local",
                    "fixed_income_unitary_local",
                    "fx_book_local",
                    "fx_book_net_revenue_local",
                    "fx_percentage",
                    "percentage_cpa",
                    "fixed_income_adviser",
                    "fixed_income_adviser_local",
                    "net_revenue_adviser",
                    "net_revenue_adviser_local",
                    "fixed_income_referred",
                    "fixed_income_referred_local",
                    "net_revenue_referred",
                    "net_revenue_referred_local",

                )
            )
        # Update Fx percentage
        fx_partner.fx_percentage = fx_partner_percentage
        fx_partner.save()
        # For accummulated months must be run another command
    return f"crit:{critical_count} error:{error_count} warn:{warning_count} today:{today.date()}"

# Define local functions


def _calc_fx(
    fx_partner,
    fx_partner_percentage,
    campaign_currency,
    currency_local,
    campaign_currency_fixed_income_str,
    partner_currency_str,
):
    if(campaign_currency == currency_local):
        # Special Case for usd at conversion for "local" payment includes fx_percentage
        fx_book_partner = 1 * fx_partner_percentage
    else:
        if(campaign_currency in CurrencyFixedIncome.values):
            fx_book_partner = None
            try:
                fx_book_partner = eval(
                    f"fx_partner.fx_{campaign_currency_fixed_income_str}_{partner_currency_str}"
                ) * fx_partner_percentage
            except:
                logger_task.critical(
                    f"Fx conversion from {campaign_currency_fixed_income_str} to {partner_currency_str} undefined on DB"
                )
        else:
            logger_task.critical("Currency not defined on field of Model Withdrawal")
    return fx_book_partner


def _calc_fx_no_percentage(
    fx_partner,
    currency_from,
    currency_to,
    currency_from_str,
    currency_to_str,
):
    if(currency_from == currency_to):
        fx_book_partner = 1
    else:
        if(currency_from in CurrencyWithdrawalToUSD.values):
            fx_book_partner = None
            try:
                fx_book_partner = eval(
                    f"fx_partner.fx_{currency_from_str}_{currency_to_str}"
                )
            except:
                logger_task.critical(
                    f"Fx conversion from {currency_from_str} to {currency_to_str} undefined on DB"
                )
        else:
            logger_task.critical(
                f"Currency {currency_from} not defined on field of Model Withdrawal for to USD columns"
            )
    return fx_book_partner
    # For accumulated months must be run another command
