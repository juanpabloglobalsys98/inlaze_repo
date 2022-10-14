from rest_framework.permissions import BasePermission
from django.utils.translation import gettext as _
from django.conf import settings


class CheckEmailOrPhone(BasePermission):
    """  
    """
    message = {
        "error": settings.FORBIDDEN_NOT_ALLOWED,
        "details": {
            "non_field_errors": [
                _("Email or phone already validated"),
            ],
        },
    }

    def has_permission(self, request):
        pass
