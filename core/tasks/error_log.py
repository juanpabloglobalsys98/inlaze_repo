from celery.signals import task_failure
from celery.utils.log import get_task_logger
from core.tasks import chat_logger as chat_logger_task
from django.conf import settings

logger_task = get_task_logger(__name__)


@task_failure.connect()
def celery_task_failure_email(**kwargs):
    # Based on
    # https://medium.com/@rosdyanakusuma/django-celery-send-email-to-admin-when-error-occurs-e1e53643549d
    """ celery 4.0 onward has no method to send emails on failed tasks
    so this event handler is intended to replace it
    """
    import socket
    from django.core.mail import mail_admins
    subject = "*[Django][{queue_name}@{host}] Error: Task `{sender.name}` `({task_id}`): `{exception}`*".format(
        queue_name="celery",  # `sender.queue` doesn't exist in 4.1?
        host=socket.gethostname(),
        **kwargs
    )
    content = (
        "Task `{sender.name}` with id `{task_id}` raised exception:\n".format(**kwargs) +
        "`{exception!r}`\n".format(**kwargs) +
        "Task was called with args: `{args}` kwargs: `{kwargs}`.\n".format(**kwargs) +
        "The contents of the full traceback was:\n" +
        "```{einfo}```".format(**kwargs)
    )
    msg = subject+"\n\n"+content

    if not settings.DEBUG:
        if (kwargs.get("sender").name == "core.tasks.chat_logger.chat_logger"):
            logger_task.critical(msg)
            mail_admins(subject, content)
        else:
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )
