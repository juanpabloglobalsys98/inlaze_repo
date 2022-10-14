import re

from django.utils.timezone import (
    datetime,
    make_aware,
    timedelta,
)
from django.utils.translation import gettext as _


def to_datetime_from(s):
    """
    Convert string with expected format YYYY-mm-dd (default angular format) to
    Datetime aware time and convert to naive time
    """
    return make_aware(datetime.strptime(s, '%Y-%m-%d'))


def to_datetime_to(s):
    """
    Convert string with expected format YYYY-mm-dd (default angular format) to
    Datetime aware and add 1 day to time data. SQL queries for range dates based
    on hour to get to day is at 23:59 of the current day or same 00:00 of the
    next day time. The final result is converted to naive time
    """
    return make_aware(datetime.strptime(s, '%Y-%m-%d')+timedelta(days=1))


def to_date(s):
    """
    Convert string with expected format YYYY-mm-dd (default angular format) to
    datetime object and get date object
    "regex": "^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$",
    """
    return datetime.strptime(s, '%Y-%m-%d').date()


def to_lower(value):
    """
    Remove multiple spaces on string and space at start and end of string.
    Then executes lower function
    """
    if isinstance(value, str):
        # Replace extra spaces to single space
        value = re.sub(' +', ' ', value)

        # Remove space at end and at start of string, then return
        value = value.strip()
        return value.lower()

    # Other case
    if (value is None):
        return None

    # Undefined case
    raise ValueError(_("Unexpected value"))


def normalize_capitalize(value):
    if isinstance(value, str):
        # Replace extra spaces to single space
        value = re.sub(' +', ' ', value)

        # Remove space at end and at start of string, then return
        value = value.strip()
        return value.title()

    # Other case
    if (value is None):
        return None

    # Undefined case
    raise ValueError(_("Unexpected value"))


def normalize(s):
    return re.sub(' +', ' ', s)


def str_split_to_list(s):
    return s.split(",")


def to_int(value):
    """
    Check data type of value according to case:
    - if str Cast value to integer, if bad format raise value error
    - if int no changes
    - if None prevent cast and return None
    - Another case raise error
    """
    # Case string object
    if isinstance(value, str):
        try:
            return int(value)
        except:
            raise ValueError(_("Bad format"))

    # Case integer object
    if isinstance(value, int):
        return value

    # Other case
    if (value is None):
        return None

    # Undefined case
    raise ValueError(_("Unexpected value"))


def to_float(value):
    """
    Check data type of value according to case:
    - if str Cast value to integer, if bad format raise value error
    - if float no changes
    - if None prevent cast and return None
    - Another case raise error
    """
    # Case string object
    if isinstance(value, str):
        try:
            return float(value)
        except:
            raise ValueError(_("Bad format"))

    # Case integer object
    if isinstance(value, float):
        return value

    # Other case
    if (value is None):
        return None

    # Undefined case
    raise ValueError(_("Unexpected value"))


def to_float_0_null(value):
    """
    Check data type of value according to case:
    - if str Cast value to integer, if bad format raise value error
    - if float no changes
    - if None prevent cast and return None
    - Another case raise error
    """
    # Case string object
    if isinstance(value, str):
        try:
            value = float(value)
        except:
            raise ValueError(_("Bad format"))

    # Case integer object
    if isinstance(value, float):
        if (value == 0):
            return None
        return value
    if isinstance(value, int):
        if (value == 0):
            return None
        return float(value)

    # Other case
    if (value is None):
        return None

    # Undefined case
    raise ValueError(_("Unspected value"))


def str_extra_space_remove(value):
    """
    Remove multiple spaces on string and space at start and end of string
    """
    if isinstance(value, str):
        # Replace extra spaces to single space
        value = re.sub(' +', ' ', value)

        # Remove space at end and at start of string, then return
        return value.strip()

    # Other case
    if (value is None):
        return None

    # Undefined case
    raise ValueError(_("Unexpected value"))


def to_bool(value):
    # Another case
    if isinstance(value, str):
        value_title = value.title()
        if (
            value_title == "True" or
            value == "1"
        ):
            return True
        elif (
            value_title == "False" or
            value == "0"
        ):
            return False

        raise ValueError(_("Bad format"))
    # Case boolean object
    if isinstance(value, bool):
        return value

    # Other case
    if (value is None):
        return None

    # Undefined case
    raise ValueError(_("Unexpected value"))


def to_campaign_redirect(value):
    """
    Remove multiple spaces on string and space at start and end of string.
    Then executes lower function
    """
    if isinstance(value, str):
        # Replace extra spaces to single space
        value = re.sub('_+', ' ', value)
        value = re.sub(' +', ' ', value)
        value = re.sub('[^a-zA-Z0-9. ]+', '', value)

        # Remove space at end and at start of string, then return
        value = value.strip()
        return value.lower()

    # Other case
    if (value is None):
        return None

    # Undefined case
    raise ValueError(_("Unexpected value"))
