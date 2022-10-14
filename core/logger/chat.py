import logging
import sys
import traceback

logger = logging.getLogger(__name__)


class ChatLogHandler(logging.Handler):
    """
    A handler class which writes logging records, appropriately formatted,
    to a stream. Note that this class does not close the stream, as
    sys.stdout or sys.stderr may be used.
    """

    def __init__(self, webhook_url):
        """
        Initialize the handler.

        If stream is not specified, sys.stderr is used.
        """
        self.webhook_url = webhook_url
        self.levels_for_alert = ["ERROR", "CRITICAL"]

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
        from core.tasks import chat_logger as chat_logger_task

        try:
            msg = self.format(record)
            if (record.levelname in self.levels_for_alert):
                msg += "\n\n<users/all>"
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": self.webhook_url,
                },
            )

        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical(f"Failed to write log on Chat logger, \nRecord: {record}\n\nTraceback: {''.join(e)}")

        self.flush()

    def __repr__(self):
        level = logging.getLevelName(self.level)
        name = getattr(self.stream, 'name', '')
        #  bpo-36015: name can be an int
        name = str(name)
        if name:
            name += ' '
        return '<%s %s(%s)>' % (self.__class__.__name__, name, level)
