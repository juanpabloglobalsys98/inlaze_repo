from rest_framework.permissions import BasePermission
from django.utils.translation import gettext as _
from rest_framework.exceptions import PermissionDenied


class CanGoIn(BasePermission):

    def has_permission(self, request, view):
        return True


class IsAllowedToExecuteFunctionalities(BasePermission):
    """
    The request is authenticated as a user
    """
    message = _(
        'Your are not allowed to execute functionalities your account is not active')

    def has_permission(self, request, view):
        """
        """
        if not request.user.is_active:
            raise PermissionDenied(self.message)

        return True
