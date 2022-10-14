import logging
import os
import subprocess

import pytz
from core.helpers import (
    compress_file,
    upload_to_s3,
)
from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    def add_arguments(self, parser):
        """
        Arguments that have the custom command runserver
        """
        parser.add_argument(
            "-db",
            "--database",
            default="default",
            choices=("default", "admin",),
            help=(
                "name of the database you will backup on S3"
            ),
        )

    def handle(self, *args, **options):
        logger.info(
            "Making call to backup upload\n"
            f"database-> {options.get('database')}\n"
        )

        logger.debug("Starting backup upload")
        databases = settings.DATABASES
        postgres_db = databases.get(options.get("database")).get("NAME")

        today = timezone.now()
        today_str = today.astimezone(pytz.timezone(settings.TIME_ZONE)).strftime("%Y_%m_%d-%H_%M_%S")

        filename = "{}-{}.dump".format(postgres_db, today_str)
        filename_compressed = "{}.gz".format(filename)
        local_file_path = os.path.join(settings.BACKUP_PATH, filename)

        logger.info("creating pg_dumb file")

        postgres_db_user = databases.get(options.get("database")).get("USER")
        postgres_db_pass = databases.get(options.get("database")).get("PASSWORD")
        postgres_db_host = databases.get(options.get("database")).get("HOST")
        postgres_db_port = databases.get(options.get("database")).get("PORT")
        postgres_db_name = databases.get(options.get("database")).get("NAME")
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

        if int(process.returncode) != 0:
            logger.error("Command failed. Return code : {}".format(process.returncode))
            return

        logger.info("compressing dumb file")
        comp_file = compress_file(local_file_path)

        logger.info("Uploading {} to Amazon S3...".format(comp_file))

        aws_key = settings.AWS_BACKUP_KEY_ID
        aws_secret_key = settings.AWS_SECRET_BACKUP_KEY
        aws_bucket_name = settings.AWS_BACKUP_BUCKET_NAME
        aws_bucket_path = os.path.join(settings.AWS_BACKUP_BUCKET_PATH, postgres_db)
        storage_class = "STANDARD_IA"

        is_uploaded = upload_to_s3(
            key=aws_key,
            secret_key=aws_secret_key,
            bucket_name=aws_bucket_name,
            bucket_path=aws_bucket_path,
            file_full_path=comp_file,
            dest_file=filename_compressed,
            storage=storage_class,
        )

        os.remove(comp_file)
        os.remove(local_file_path)

        if not is_uploaded:
            logger.error("Command failed. Return code : {}".format(process.returncode))
            return

        logger.info("Backup uploaded in your bucket".format(comp_file))

        return output
