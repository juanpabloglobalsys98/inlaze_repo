from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework.permissions import BasePermission


class NoLevel(BasePermission):
    """
    Verifies if user has not level yet.
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN,
            "detail": {
                "non_field_errors": [
                    _("Your account already has a level"),
                ],
            },
        }
        return request.user.partner.level is None


class HasLevel(BasePermission):
    """
    Verifies if user has level.
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN,
            "detail": {
                "non_field_errors": [
                    _("Your account has not a level defined"),
                ],
            },
        }
        return request.user.partner.level is not None
