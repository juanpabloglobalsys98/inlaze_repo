from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.response import Response


def obj_not_found_response(obj, field="pk"):
    msg = "{} not found".format(obj.__name__)
    return Response(
        data={
            "error": settings.NOT_FOUND_CODE,
            "detail": {
                field: [
                    _(msg),
                ],
            },
        },
        status=status.HTTP_404_NOT_FOUND,
    )


def bad_request_response(detail, error=settings.BAD_REQUEST_CODE):
    return Response(
        data={
            "error": error,
            "detail": detail,
        },
        status=status.HTTP_400_BAD_REQUEST,
    )
