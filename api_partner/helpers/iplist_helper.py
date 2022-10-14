import json
import logging
import sys
import traceback

import requests
from django.conf import settings

logger = logging.getLogger(__name__)


def make_iplist_call(ip):
    try:
        response = requests.get(settings.IP_LIST_CALL+ip)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        logger.error(
            f"make iplist call exception with ip {ip}\n\n"
            f"traceboack:\n\n{''.join(e)}"
        )
        return None
    if response.status_code != 200:
        return None

    try:
        return json.loads(response.text)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(
            etype=exc_type,
            value=exc_value,
            tb=exc_traceback,
        )
        logger.debug(
            f"make iplist call exception with ip {ip}\n\n"
            f"traceboack:\n\n{''.join(e)}"
        )
