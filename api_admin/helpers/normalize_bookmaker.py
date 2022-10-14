import re


def normalize_bookmaker_name(name):
    return re.sub(' +', ' ', name).strip()
