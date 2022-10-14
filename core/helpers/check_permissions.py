from rest_framework.permissions import BasePermission
from django.utils.translation import gettext as _
from django.conf import settings


class HavePermissionBasedView(BasePermission):
    """
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("You don't have permission"),
                ],
            },
        }
        name_view = view.get_view_name().lower()
        name_method = request.method.lower()
        name_code = f"{name_view}-{name_method}"
        permission = request.user.has_perm(name_code)
        if permission:
            return True
        return None
