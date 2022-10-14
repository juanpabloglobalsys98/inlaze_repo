from rest_framework.permissions import BasePermission
from django.utils.translation import gettext as _
# from rest_framework.exceptions import PermissionDenied
# from django.contrib.auth.models import Permission

#     """
#     print(dir(view))
#     print(view.get_view_name())
#     print(view.get_permissions())
#     print(request.method)
#     "Partner Log In - POST"
#     if request.user.has_perm(f"{view.get_view_name()} - {request.method}")
#     """

#     content_type = ContentType.objects.get_for_model(
#     BasicInfo, for_concrete_model=False)
#     student_permissions = Permission.objects.filter(
#     content_type=content_type)
#     print([p.codename for p in student_permissions])
#     print("user permission: ", admin.user_permissions.all())
#     # print(view.get_view_name().replace(
#     #     " ", "_").lower()+"_"+request.method)
#     # return bool(request.user and request.user.is_authenticated)
#     return False


class CanAddPermission(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        return request.user.has_perm("auth.add_permission")


class CanGetPermission(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        return request.user.has_perm("api_admin.view_permission")


class CanViewAdmins(BasePermission):
    """

    """

    def has_permission(self, request, view):
        return request.user.has_perm("api_admin.view_admin")


class CanUpdateOrCreateAdmins(BasePermission):
    """

    """

    def has_permission(self, request, view):
        user = request.user
        return user.has_perm("api_admin.add_admin") or user.has_perm(
            "api_admin.change_admin")



class CanUpdateOrCreateRoles(BasePermission):
    """

    """

    def has_permission(self, request, view):
        user = request.user
        return user.has_perm("api_admin.add_role") or user.has_perm(
            "api_admin.change_role")

class CanViewRoles(BasePermission):
    """

    """

    def has_permission(self, request, view):
        return request.user.has_perm("api_admin.view_role")