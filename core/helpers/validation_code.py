from math import floor
from random import random

from django.conf import settings
from django.utils import timezone


def generate_validation_code():
    """
    Return a random code and its expiration time.
    """
    code = str(
        int(settings.MIN_DIGITS) +
        floor((int(settings.MAX_DIGITS) - int(settings.MIN_DIGITS)) * random())
    )
    default_minutes = int(settings.EXPIRATION_ADDER_MINUTES)
    expiration = timezone.now() + timezone.timedelta(minutes=default_minutes)
    return code, expiration
