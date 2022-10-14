import ast
import os
import subprocess

import pytz
from betenlace.celery import app
from celery.utils.log import get_task_logger
from core.helpers import (
    compress_file,
    upload_to_s3,
)
from core.tasks import chat_logger as chat_logger_task
from django.conf import settings
from django.utils import timezone

logger_task = get_task_logger(__name__)


@app.task(
    ignore_result=True,
)
def backup_upload(databases_list):
    msg = (
        "Starting backup upload\n"
    )
    logger_task.info(msg)
    msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"

    chat_logger_task.apply_async(
        kwargs={
            "msg": msg,
            "msg_url": settings.CHAT_WEBHOOK_CELERY,
        },
    )

    databases_list = ast.literal_eval(databases_list)
    databases = settings.DATABASES
    if not all(database_i in databases_list for database_i in databases.keys()):
        msg = f" {databases_list} database doesn't exist in {databases.keys()}"
        logger_task.error(msg)
        msg = f"*LEVEL:* `ERROR` \n*message:* `{msg}`\n\n"
        chat_logger_task.apply_async(
            kwargs={
                "msg": msg,
                "msg_url": settings.CHAT_WEBHOOK_CELERY,
            },
        )

    for database_i in databases_list:
        if database_i in settings.DATABASES.keys():
            msg = f"starting backup database: `{database_i}`"
            logger_task.info(msg)
            msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )

            postgres_db = databases.get(database_i).get("NAME")

            today = timezone.now()
            today_str = today.astimezone(pytz.timezone(settings.TIME_ZONE)).strftime("%Y_%m_%d-%H_%M_%S")

            filename = "{}-{}.dump".format(postgres_db, today_str)
            filename_compressed = "{}.gz".format(filename)
            local_file_path = os.path.join(settings.BACKUP_PATH, filename)

            logger_task.info("creating pg_dumb file")
            msg = (
                "making pg_dumb file\n"
                f"date -> {today_str}\n"
                f"file -> {filename}"
            )
            logger_task.info(msg)
            msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )

            postgres_db_user = databases.get(database_i).get("USER")
            postgres_db_pass = databases.get(database_i).get("PASSWORD")
            postgres_db_host = databases.get(database_i).get("HOST")
            postgres_db_port = databases.get(database_i).get("PORT")
            postgres_db_name = databases.get(database_i).get("NAME")

            process = subprocess.Popen(
                [
                    "pg_dump",
                    "--dbname=postgresql://{}:{}@{}:{}/{}".format(
                        postgres_db_user,
                        postgres_db_pass,
                        postgres_db_host,
                        postgres_db_port,
                        postgres_db_name,
                    ),
                    "-Fc",
                    "-f",
                    local_file_path,
                ],
                stdout=subprocess.PIPE,
            )
            output = process.communicate()[0]

            msg = (
                "finish pg_dumb file\n"
                f"date -> {today_str}\n"
                f"file -> {filename}"
            )
            logger_task.info(msg)
            msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )

            if int(process.returncode) != 0:
                msg = (
                    "Command failed. Return code : {}".format(process.returncode)
                )
                logger_task.error(msg)
                msg = f"*LEVEL:* `ERROR` \n*message:* `{msg}`\n\n"
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": msg,
                        "msg_url": settings.CHAT_WEBHOOK_CELERY,
                    },
                )
                return

            msg = (
                "starting compress the pg_dumb file\n"
                f"date -> {today_str}\n"
                f"file -> {filename}"
            )
            logger_task.info(msg)
            msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )

            logger_task.info("compressing dumb file")
            comp_file = compress_file(local_file_path)

            msg = (
                "compress file\n"
                f"date -> {today_str}\n"
                f"file -> {filename_compressed}"
            )
            logger_task.info(msg)
            msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )

            aws_key = settings.AWS_BACKUP_KEY_ID
            aws_secret_key = settings.AWS_SECRET_BACKUP_KEY
            aws_bucket_name = settings.AWS_BACKUP_BUCKET_NAME
            aws_bucket_path = os.path.join(settings.AWS_BACKUP_BUCKET_PATH, postgres_db)

            storage_class = "STANDARD_IA"

            today = timezone.now()
            today_str = today.astimezone(pytz.timezone(settings.TIME_ZONE)).strftime("%Y_%m_%d-%H_%M_%S")
            msg = (
                "Uploading file to Amazon S3\n"
                f"date -> {today_str}\n"
                f"file -> {filename_compressed}"
            )
            logger_task.info(msg)
            msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )

            is_uploaded = upload_to_s3(
                key=aws_key,
                secret_key=aws_secret_key,
                bucket_name=aws_bucket_name,
                bucket_path=aws_bucket_path,
                file_full_path=comp_file,
                dest_file=filename_compressed,
                storage=storage_class,
                logger_task=logger_task,
            )

            os.remove(comp_file)
            os.remove(local_file_path)

            if not is_uploaded:
                msg = (
                    "Command failed. Return code : {}".format(process.returncode)
                )
                logger_task.error(msg)
                msg = f"*LEVEL:* `ERROR` \n*message:* `{msg}`\n\n"
                chat_logger_task.apply_async(
                    kwargs={
                        "msg": msg,
                        "msg_url": settings.CHAT_WEBHOOK_CELERY,
                    },
                )
                return

            msg = (
                "Backup uploaded in your bucket\n"
                f"date -> {today_str}"
            )
            logger_task.info(msg)
            msg = f"*LEVEL:* `INFO` \n*message:* `{msg}`\n\n"

            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )
    return output
