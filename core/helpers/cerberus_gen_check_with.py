from django.utils.translation import gettext as _


def check_positive_float(field, value, error):
    if isinstance(value, float) and value < 0:
        error(field, _('Must be a positive Float'))


def check_positive_int(field, value, error):
    if isinstance(value, int) and value < 0:
        if value < 0:
            error(field, _('Must be a positive Int'))
