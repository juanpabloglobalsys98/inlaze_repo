from cerberus import (
    TypeDefinition,
    Validator,
)
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.core.files.uploadedfile import InMemoryUploadedFile
from rest_framework import status
from rest_framework.response import Response

file_type = TypeDefinition('file', (InMemoryUploadedFile,), ())


class ValidatorFile(Validator):
    types_mapping = Validator.types_mapping.copy()
    types_mapping["file"] = file_type


def create_validator(schema):
    return Validator(
        schema=schema,
        error_handler=StandardErrorHandler,
    )


def validate_validator(validator, data):
    if not validator.validate(document=data):
        return Response(
            data={
                "error": settings.CERBERUS_ERROR_CODE,
                "detail": validator.errors,
            },
            status=status.HTTP_400_BAD_REQUEST,
        )
