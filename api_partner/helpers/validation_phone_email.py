from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework.permissions import BasePermission


class ValidationPhoneEmail(BasePermission):
    """
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN,
            "details": {
                "non_field_errors": [
                    _("Phone or email not validated yet"),
                ],
            },
        }
        user = request.user
        if user.partner.is_email_valid and user.partner.is_phone_valid:
            return True
        return False
