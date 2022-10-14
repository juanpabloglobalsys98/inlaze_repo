import logging
import sys
import traceback

from django.utils import timezone
import pytz

logger = logging.getLogger(__name__)


class NotionIPLogHandler(logging.Handler):
    """
    A handler class which writes logging records, appropriately formatted,
    to a stream. Note that this class does not close the stream, as
    sys.stdout or sys.stderr may be used.
    """
    PAGES_URL = "https://api.notion.com/v1/pages"

    def __init__(self, notion_secret_key, notion_db_id, time_zone_settings):
        """
        Initialize the handler.

        If stream is not specified, sys.stderr is used.
        """
        self.notion_secret_key = notion_secret_key
        self.notion_db_id = notion_db_id
        self.time_zone_settings = time_zone_settings

        super().__init__()

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline.  If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.  If the stream
        has an 'encoding' attribute, it is used to determine how to do the
        output to the stream.
        """
        from core.tasks import notion_ips_logger as notion_ips_logger_task

        try:
            msg = self.format(record)

            endpoint, method_ip, ip = msg.split(",")

            time_now = timezone.now().astimezone(pytz.timezone(self.time_zone_settings))
            date = time_now.strftime("%Y-%m-%dT%H:%M%z")

            time_str = time_now.strftime("%Y-%m-%d %H:%M %z")

            notion_ips_logger_task.apply_async((self.notion_secret_key, self.notion_db_id, time_str,
                                                endpoint, method_ip, ip, date), ignore_result=True)

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical(f"Failed to write log on Notion logger, \nRecord: {record}\n\nTraceback: {''.join(e)}")

        self.flush()

    def __repr__(self):
        level = logging.getLevelName(self.level)
        name = getattr(self.stream, 'name', '')
        #  bpo-36015: name can be an int
        name = str(name)
        if name:
            name += ' '
        return '<%s %s(%s)>' % (self.__class__.__name__, name, level)
