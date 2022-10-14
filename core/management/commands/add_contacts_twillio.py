import logging
import sys
import traceback

from core.models import User
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Q
from sendgrid import SendGridAPIClient

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        """
        Arguments that have the custom command runserver
        """
        parser.add_argument(
            "-uid", "--userid",
            help='Determine user to add or update'
        )

    def handle(self, *args, **options):
        if not settings.SENDGRID_API_KEY:
            logger.error(f"SENDGRID_API_KEY is none")
            return None

        filters = []
        if options.get("userid"):
            filters.append(Q(id=int(options.get("userid"))))

        users = User.objects.filter(*filters)
        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        for user in users:
            is_notify_campaign = int(user.partner.is_notify_campaign)
            is_notify_notice = int(user.partner.is_notify_notice)
            data = {
                "contacts": [
                    {
                        "first_name": user.first_name,
                        "last_name": user.last_name,
                        "email": user.email,
                        "city":  user.partner.additionalinfo.city,
                        "country": user.partner.additionalinfo.country,
                        "phone_number": user.phone,
                        "custom_fields": {
                            settings.SENDGRID_CUSTOM_FIELD_CAMPAIGN: is_notify_campaign,
                            settings.SENDGRID_CUSTOM_FIELD_NOTICE: is_notify_notice,
                        },
                    },
                ],
            }

            try:
                response = sg.client.marketing.contacts.put(
                    request_body=data
                )
            except Exception as e:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                e = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.critical("".join(e))
                return e
