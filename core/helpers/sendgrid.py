import logging
import sys
import traceback

from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.response import Response
from twilio.base.exceptions import TwilioException
from twilio.rest import Client

logger = logging.getLogger(__name__)


def send_phone_message(
    phone,
    valid_phone_by,
    validation_code,
):
    if not settings.TWILIO_ACCOUNT_SID:
        logger.warning("var TWILIO_ACCOUNT_SID is empty, prevent send SMS/WhatsApp")
        return

    from api_partner.models import Partner
    msg = _("Your code is {}. Never share this code with anyone, only use it at inlaze.com")
    msg = msg.format(validation_code)
    client = Client(
        settings.TWILIO_ACCOUNT_SID,
        settings.TWILIO_AUTH_TOKEN
    )
    if valid_phone_by == Partner.ValidPhoneBy.WPP:
        sent_using = Partner.ValidPhoneBy.WPP.label
        from_send = "whatsapp:"+settings.TWILIO_BASE_NUMBER_WHATSAPP
        to_send = "whatsapp:"+phone.replace(" ", "")
    elif valid_phone_by == Partner.ValidPhoneBy.SMS:
        sent_using = Partner.ValidPhoneBy.SMS.label
        from_send = settings.TWILIO_BASE_NUMBER
        to_send = phone.replace(" ", "")
    else:
        logger.critical(f"'{valid_phone_by}' is not a valid phone by enum value")
        return Response(
            data={
                "error": settings.INTERNAL_SERVER_ERROR,
                "detail": {
                    "non_field_errors": [
                        _("Error while sending the message"),
                    ],
                },
            },
            status=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    try:
        message = client.messages.create(
            body=msg,
            from_=from_send,
            to=to_send
        )
    except TwilioException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.critical("".join(e))
        if (
            hasattr(exc_value, "code") and
            exc_value.code == settings.TWILIO_ERROR_CODE_INVALID_TO_PHONE
        ):
            msg = _("The number {} is not a valid phone number")
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": [
                            _(msg.format(phone)),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        elif (exc_value == settings.TWILIO_CREDENTIALS_MSG):
            return Response(
                data={
                    "error": settings.INTERNAL_SERVER_ERROR,
                    "detail": {
                        "non_field_errors": [
                            _("Error while sending the message"),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        else:
            return Response(
                data={
                    "error": settings.INTERNAL_SERVER_ERROR,
                    "detail": {
                        "non_field_errors": [
                            _("Error while sending the message")
                        ],
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.critical("".join(e))
        error_msg = "is not a valid phone number"
        if error_msg in exc_value.msg:
            return Response(
                data={
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "non_field_errors": _(exc_value.msg)
                    }
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        else:
            return Response(
                data={
                    "error": settings.ERROR_SENDING_EMAIL,
                    "detail": {
                        "non_field_errors": [
                            _("Error while sending the message")
                        ],
                    }
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

    logger.warning(
        f"{sent_using} sent to phone: {phone} with SID: {message.sid}"
    )
