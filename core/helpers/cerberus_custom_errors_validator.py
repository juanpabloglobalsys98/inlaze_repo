from cerberus import errors
from django.utils.translation import gettext as _


class StandardErrorHandler(errors.BasicErrorHandler):
    messages = errors.BasicErrorHandler.messages.copy()
    messages[errors.REQUIRED_FIELD.code] = _("Required field")


class FilesNamesErrorHandler(errors.BasicErrorHandler):
    messages = errors.BasicErrorHandler.messages.copy()
    messages[errors.REGEX_MISMATCH.code] = _("Invalid filename, allowed extensions .png, .jpg, .jpeg, .webp")


class PartnerFilesNamesErrorHandler(errors.BasicErrorHandler):
    messages = errors.BasicErrorHandler.messages.copy()
    messages[errors.REGEX_MISMATCH.code] = _("Invalid filename, allowed extensions .pdf, .png, .jpg, .jpeg")


class AdminFilenameErrorHandler(errors.BasicErrorHandler):
    messages = errors.BasicErrorHandler.messages.copy()
    messages[errors.REGEX_MISMATCH.code] = _("Invalid filename, allowed extensions .csv")
