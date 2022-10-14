from django.conf import settings
from django.utils.translation import gettext as _
from api_partner.helpers.choices.partner_status import PartnerStatusCHO
from rest_framework.permissions import BasePermission


class IsNotBanned(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("You are banned, contact your adviser"),
                ],
            },
        }
        return not request.user.is_banned


class IsTerms(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN_NOT_TERMS,
            "detail": {
                "non_field_errors": [
                    _("You dont have accepted terms"),
                ],
            },
        }
        return request.user.partner.is_terms


class IsNotTerms(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "detail": {
                "non_field_errors": [
                    _("You have accepted terms"),
                ],
            },
        }
        return not request.user.partner.is_terms


class IsActive(BasePermission):
    """
    User is active
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("Your account is deactivated, contact your adviser"),
                ],
            },
        }
        return request.user.is_active


class IsNotOnLogUpPhase2A(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("You are not full registered, please continue with you registration"),
                ],
            },
        }
        return not request.user.partner.status == Partner.Status.ON_PHASE2A


class IsNotOnLogUpPhase2B(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("You are not full registered, please continue with you registration"),
                ],
            },
        }
        return not request.user.partner.status == Partner.Status.ON_PHASE2B


class IsNotOnLogUpPhase2C(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("You are not full registered, please continue with you registration"),
                ],
            },
        }
        return not request.user.partner.status == Partner.Status.ON_PHASE2C


class IsNotToBeVerified(BasePermission):
    """
    The request is authenticated as a user
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("You are being verified yet, please contact your adviser"),
                ],
            },
        }
        return not request.user.partner.status == Partner.Status.REGISTERED


class IsUploadedAll(BasePermission):
    """
    Verify is user has uploaded all pending data
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("You are not uploaded all pending data yet, verify data"),
                ],
            },
        }
        return request.user.partner.status in (
            Partner.Status.VALIDATED,
        )


class IsNotFullRegister(BasePermission):
    """
    Allow permission if user is not full registered yet
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("Already Full register, you cannot follow regisration process"),
                ],
            },
        }
        return not request.user.partner.status in (
            Partner.Status.VALIDATED,
        )


class IsFullRegister(BasePermission):
    """
    Allow permission if user is full registered
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("Your account are not completed the registration process yet"),
                ],
            },
        }
        return request.user.partner.status in (
            Partner.Status.VALIDATED,
        )


class IsFullRegisterSkipData(BasePermission):
    """
    Allow permission if user is full registered and can edit skipped
    data
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("Your account can't edit data"),
                ],
            },
        }
        return request.user.partner.status in (
            Partner.Status.FULL_REGISTERED_SKIPPED,
            Partner.Status.FULL_REGISTERED_SKIPPED_REJECT,
            Partner.Status.FULL_REGISTERED_SKIPPED_UPLOADED_ALL,
        )


class IsFullRegisterAllData(BasePermission):
    """
    Allow permission if user is full registered and already accepted the
    skipped data
    """

    def has_permission(self, request, view):
        from api_partner.models import Partner
        self.message = {
            "error": settings.FORBIDDEN_NOT_ALLOWED,
            "details": {
                "non_field_errors": [
                    _("Your account are not completed the skipped data yet"),
                ],
            },
        }
        return request.user.partner.status in (
            Partner.Status.VALIDATED,
        )


class IsEmailValid(BasePermission):
    """
    Verifies if partner's email is verified.
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN,
            "detail": {
                "non_field_errors": [
                    _("Your email is not verified"),
                ],
            },
        }
        return request.user.partner.is_email_valid


class IsBasicInfoValid(BasePermission):
    """
    Verifies if partner's basic info is validated.
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN,
            "detail": {
                "non_field_errors": [
                    _("Your basic info is not validated yet"),
                ],
            },
        }
        return request.user.partner.basic_info_status == PartnerStatusCHO.ACCEPTED


class IsBankInfoValid(BasePermission):
    """
    Verifies if partner's basic info is validated.
    """

    def has_permission(self, request, view):
        self.message = {
            "error": settings.FORBIDDEN,
            "detail": {
                "non_field_errors": [
                    _("Your bank info is not validated yet"),
                ],
            },
        }
        return request.user.partner.bank_status == PartnerStatusCHO.ACCEPTED
