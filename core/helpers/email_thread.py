import logging
import threading

from api_partner.helpers import (
    PartnerLevelCHO,
    PartnerStatusCHO,
)
from django.conf import settings
from django.core.mail import EmailMessage
from django.template.loader import get_template
from django.utils import translation
from django.utils.translation import gettext as _

logger = logging.getLogger(__name__)


class EmailThread(threading.Thread):

    def __init__(self, html, subject, email, data, from_email=None, connection=None):
        self.html = html
        self.email = email
        self.subject = subject
        self.data = data
        self.from_email = from_email
        self.connection = connection
        threading.Thread.__init__(self)

    def run(self):
        message = get_template(self.html).render(self.data)
        mail = EmailMessage(
            to=[self.email],
            subject=self.subject,
            body=message,
            from_email=self.from_email,
            connection=self.connection,
        )

        mail.content_subtype = "html"
        mail.send()


def send_email(user, subject, data, request, html="email_message.html"):
    language = user.language or request.LANGUAGE_CODE
    translation.activate(language)
    EmailThread(
        email=user.email,
        subject=subject,
        html=html,
        data={
            "LANGUAGE": language,
            "TITLE": subject,
            "GREETING": _("Hi"),
            "TEMPLATE_IMAGE_INLAZZ": settings.TEMPLATE_IMAGE_INLAZZ,
            "USER": user.get_full_name(),
            "BUTTON_MSG": _("Login to your account"),
            "LOGIN_LINK": settings.INLAZE_LOGIN,
            "CONTACT": _("If you have any questions, please "),
            "CONTACT_US": _("contact us"),
            "CUSTOMER_SERVICE_CHAT": settings.CUSTOMER_SERVICE_CHAT,
            "FOOTER_MESSAGE_PART_1": _("Best regards,"),
            "FOOTER_MESSAGE_PART_2": _("inlaze team"),
            "FOOTER_MESSAGE_PART_3": _("All rights reserved."),
        } | data,
    ).start()


def send_validation_response_email(user, new_status, request_type, message, request):
    translation.activate(user.language)
    actions = {
        "level": _("Your change request to Prime account"),
        "bank": _("Your financial information"),
    }
    if new_status == PartnerStatusCHO.ACCEPTED:
        result = _("approved")
    elif new_status == PartnerStatusCHO.REJECTED:
        result = _("rejected")
    else:
        result = ""
        logger.error(f"'{new_status}' is not a valid partner status")

    if request_type == "basic":
        if new_status == PartnerStatusCHO.ACCEPTED:
            subject = _("Welcome to inlaze!")
        elif new_status == PartnerStatusCHO.REJECTED:
            subject = _("The submitted data has been rejected")
        else:
            subject = ""
    else:
        subject = _("{} has been {}")
        subject = subject.format(actions.get(request_type), result)

    send_email(
        user=user,
        subject=subject,
        html="send_validation_request_response.html",
        request=request,
        data={
            "SUBJECT": subject,
            "MESSAGE": message,
            "STATUS": new_status,
        },
    )


def send_change_level_response_email(user, new_level, message, request):
    translation.activate(user.language)
    subject = _("Your account level has changed to {}")
    subject = subject.format(PartnerLevelCHO(new_level).label)
    send_email(
        user=user,
        subject=subject,
        html="send_level_change_response.html",
        request=request,
        data={
            "SUBJECT": subject,
            "MESSAGE": message,
        },
    )


def send_ban_unban_email(user, message, request):
    translation.activate(user.language)
    action = _("banned") if user.is_banned else _("unbanned")
    subject = _("Your account has been {}").format(action)
    send_email(
        user=user,
        subject=subject,
        request=request,
        data={
            "SUBJECT": subject,
            "MESSAGE": message,
            "CONTACT": None,
        },
    )
