import copy
import gzip
import logging
import os
import sys
import traceback

import boto3
from core.tasks import chat_logger as chat_logger_task
from django.conf import settings
from storages.backends.s3boto3 import S3Boto3Storage

logger = logging.getLogger(__name__)


class S3StandardIA(S3Boto3Storage):
    """
    Standard Infrecuent Access
    """
    object_parameters = {"StorageClass": "STANDARD_IA"}


class S3DeepArchive(S3Boto3Storage):
    object_parameters = {"StorageClass": "DEEP_ARCHIVE"}


def copy_s3_file(source_file, to_path, bucket_name="betenlacetest"):
    """
    Copies a file to a given path in the same bucket.
    """
    if not source_file:
        return source_file

    destination_file = copy.deepcopy(source_file)
    bucket_name = settings.AWS_STORAGE_BUCKET_NAME
    copy_source = {"Bucket": bucket_name, "Key": source_file.name}
    new_path = os.path.join(to_path, source_file.name.split("/")[-1])
    source_file.storage.bucket.meta.client.copy(copy_source, bucket_name, new_path)
    destination_file.name = new_path
    return destination_file


def upload_to_s3(
    key,
    secret_key,
    bucket_name,
    bucket_path,
    file_full_path,
    dest_file,
    storage,
    logger_task=None,
):
    """
    Upload a file to an AWS S3 bucket.
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret_key,
    )

    try:
        s3_client.upload_file(
            file_full_path,
            bucket_name,
            os.path.join(bucket_path, dest_file),
            ExtraArgs={
                'StorageClass': str(storage)
            },
        )

    except boto3.exceptions.S3UploadFailedError:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        if logger_task is not None:
            msg = (
                f"S3 uploaded failed :\n\n{''.join(e)}"
            )
            logger_task.error(msg)
            msg = f"*LEVEL:* `ERROR` \n*message:* `{msg}`\n\n"
            chat_logger_task.apply_async(
                kwargs={
                    "msg": msg,
                    "msg_url": settings.CHAT_WEBHOOK_CELERY,
                },
            )
        else:
            msg = (
                f"S3 uploaded failed :\n\n{''.join(e)}"
            )
            logger.error(msg)

        return False

    return True


def compress_file(local_file_path):
    compressed_file = "{}.gz".format(
        str(local_file_path)
    )
    with open(
        local_file_path,
        "rb",
    ) as f_in:
        with gzip.open(
            compressed_file,
            "wb",
        ) as f_out:
            for line in f_in:
                f_out.write(line)
    return compressed_file
