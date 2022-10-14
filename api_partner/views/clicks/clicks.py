import logging

from api_partner.helpers import get_client_ip
from api_partner.models import (
    Campaign,
    Link,
)
from api_partner.tasks import click_count as click_count_task
from cerberus import Validator
from core.helpers import get_client_ip as core_client_ip
from core.helpers import (
    to_campaign_redirect,
    to_lower,
)
from core.tasks import chat_logger as chat_logger_task
from cryptography.fernet import (
    Fernet,
    InvalidToken,
)
from django.conf import settings
from django.db.models import (
    Q,
    Value,
)
from django.db.models.functions import Concat
from django.http.response import HttpResponseRedirect
from django.utils.translation import gettext as _
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class ClickNothingParamsAPI(APIView):
    def get(self, request):
        return HttpResponseRedirect(redirect_to=settings.URL_REDIRECT_LANDING)


class ClickReportTwoParamsAPI(APIView):
    """ Resource to add click """

    def _normal_case(self, filters, validator):
        '''
            Function that validate campaign with filter param and get a object to after check who link
            has is this campaign and validator.prom_code

            return link and campaign object
        '''
        campaign = Campaign.objects.annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            )
        ).filter(
            *filters
        ).first()

        if campaign is None:
            logger.warning(
                f"not found campaign with campaign \"{validator.document.get('campaign')}\" and prom_code "
                f"\"{validator.document.get('prom_code')}\""
            )
            return None, None

        # TEMP Galera.bet BR promcode samirk
        if (campaign.pk == 43 and validator.document.get("prom_code", "") == "147393"):
            # TEMP Get galera bet BR 2 promcode samirk
            link = Link.objects.filter(
                Q(
                    #
                    campaign_id=88,
                ),
                Q(
                    prom_code__iexact="155207",
                )
            ).first()
        else:
            # Normal case
            link = Link.objects.filter(
                Q(
                    campaign=campaign,
                ),
                Q(
                    prom_code__iexact=validator.document.get("prom_code"),
                )
            ).first()

        if link is None:
            logger.warning(
                f"not found link with campaign {validator.document.get('campaign')} and prom_code "
                f"\"{validator.document.get('prom_code')}"
            )
            return None, None
        return link, campaign

    def _multiple_case(self, filters, validator):
        '''
            Function that validate campaign with filter param and get a QUERYSET to after check who link
            has is these campaigns and prom_code

            return link and link.campaign
        '''
        campaign = Campaign.objects.annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            )
        ).filter(
            *filters
        )

        if not campaign:
            logger.warning(
                f"not found campaign with campaign \"{validator.document.get('campaign')}\" and prom_code "
                f"\"{validator.document.get('prom_code')}\""
            )
            return None, None

        link = Link.objects.filter(
            Q(
                campaign__in=campaign
            ),
            Q(
                prom_code__iexact=validator.document.get("prom_code"),
            )
        ).first()

        if link is None:
            logger.warning(
                f"not found link with campaign {validator.document.get('campaign')} and prom_code "
                f"\"{validator.document.get('prom_code')}"
            )
            return None, None
        return link, link.campaign

    def get(self, request, **url_kwargs):
        validator = Validator(
            schema={
                "campaign": {
                    "required": True,
                    "type": "string",
                    "coerce": to_campaign_redirect,
                },
                "prom_code": {
                    "required": True,
                    "type": "string",
                    "coerce": to_campaign_redirect,
                },
            },
        )

        if not validator.validate(url_kwargs):
            return HttpResponseRedirect(redirect_to=settings.URL_REDIRECT_CAMPAIGN_ERROR + request.path)

        filters = []
        #  Check if campaign param is some betfair col campaign
        if (validator.document.get("campaign") == "betfair col"):
            filters.append(Q(campaign_title__istartswith=validator.document.get("campaign")))
            link, campaign = self._multiple_case(filters, validator,)
        else:
            filters.append(Q(campaign_title__iexact=validator.document.get("campaign")))
            link, campaign = self._normal_case(filters, validator,)

        if not link or not campaign:
            return HttpResponseRedirect(redirect_to=settings.URL_REDIRECT_CAMPAIGN_ERROR + request.path)

        ip_client = get_client_ip(request)
        click_count_task.apply_async(
            (
                link.pk,
                campaign.currency_condition,
                campaign.currency_fixed_income,
                ip_client
            ),
            ignore_result=True
        )
        return HttpResponseRedirect(redirect_to=link.url)


class ClickReportThreeParamsAPI(APIView):
    """ Resource to add click """

    def get(self, request, **url_kwargs):
        validator = Validator(
            schema={
                "langague": {
                    "required": True,
                    "type": "string",
                    "coerce": to_lower,
                    "regex": "(?i)(es)",
                },
                "campaign": {
                    "required": True,
                    "type": "string",
                    "coerce": to_campaign_redirect,
                },
                "prom_code": {
                    "required": True,
                    "type": "string",
                    "coerce": to_campaign_redirect,
                },
            },
        )

        if not validator.validate(url_kwargs):
            return HttpResponseRedirect(
                redirect_to=(
                    settings.URL_REDIRECT_CAMPAIGN_ERROR +
                    request.path
                )
            )

        campaign = Campaign.objects.annotate(
            campaign_title=Concat(
                "bookmaker__name",
                Value(" "),
                "title",
            )
        ).filter(
            Q(campaign_title__icontains=validator.document.get("campaign"))
        ).first()

        if campaign is None:
            logger.warning(
                f"not found campaign with campaign \"{validator.document.get('campaign')}\" and prom_code "
                f"\"{validator.document.get('prom_code')}\""
            )
            return HttpResponseRedirect(
                redirect_to=(
                    settings.URL_REDIRECT_CAMPAIGN_ERROR +
                    request.path
                )
            )

        link = Link.objects.filter(
            Q(
                campaign=campaign
            ),
            Q(
                prom_code__iexact=validator.document.get("prom_code"),
            )
        ).first()

        if link is None:
            logger.warning(
                f"not found link with campaign {validator.document.get('campaign')} and prom_code "
                f"\"{validator.document.get('prom_code')}"
            )
            return HttpResponseRedirect(
                redirect_to=(
                    settings.URL_REDIRECT_CAMPAIGN_ERROR +
                    request.path
                )
            )
        return HttpResponseRedirect(redirect_to=settings.URL_REDIRECT_CAMPAIGN_ES + request.path)


class AdsAntiBotAPI(APIView):

    def get(self, request, encrypt_link):
        ip_client = core_client_ip(request)

        fernet = Fernet(settings.REDIRECT_FERNET_KEY)

        try:
            msg_income = fernet.decrypt(encrypt_link).decode()
        except InvalidToken:
            user_agent = request.META.get('HTTP_USER_AGENT')
            msg = (
                f"Invalid Token -> {encrypt_link}\n"
                f"User Agent -> {user_agent}\n"
                f"IP User ->{ip_client}"
            )
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.WEBHOOK_REDIRECT_BOT,
                },
            )
            return HttpResponseRedirect(redirect_to=settings.URL_REDIRECT_CAMPAIGN_ERROR + request.path)

        msg_income = msg_income.split("-")

        if 'HTTP_USER_AGENT' in request.META:
            userAgent = request.META.get('HTTP_USER_AGENT')
            matches = settings.REG_BOT.search(userAgent)
            if matches:
                msg = (
                    "I'm a Bot or a Spider:\n"
                    f"Invalid Token -> {encrypt_link}\n"
                    f"User Agent -> {userAgent}\n"
                    f"IP User ->{ip_client}"
                )
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": msg,
                        "msg_url": settings.WEBHOOK_REDIRECT_BOT,
                    },
                )
                return HttpResponseRedirect(settings.COMPANY_URL)

        if (len(msg_income) == 2):
            query = Q(id=int(msg_income[1]))
            link = Link.objects.filter(
                query
            ).first()
            if link is not None and link.status == Link.Status.GROWTH:
                userAgent = request.META.get('HTTP_USER_AGENT')
                msg = (
                    f"Pass to Page {msg_income[0]}:\n"
                    f"Token -> {encrypt_link}\n"
                    f"User Agent -> {userAgent}\n"
                    f"IP User ->{ip_client}"
                )
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": msg,
                        "msg_url": settings.WEBHOOK_REDIRECT_BOT,
                    },
                )
            return HttpResponseRedirect(link.url)

        msg = (
            "Invalid Link To Redirect\n"
            f"Invalid Token -> {encrypt_link}\n"
            f"IP User ->{ip_client}"
        )
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg,
                "msg_url": settings.WEBHOOK_REDIRECT_BOT,
            },
        )

        return HttpResponseRedirect(redirect_to=settings.URL_REDIRECT_CAMPAIGN_ERROR + request.path)
