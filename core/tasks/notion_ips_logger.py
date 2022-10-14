import sys
import traceback

import requests
from betenlace.celery import app
from celery.utils.log import get_task_logger
from django.conf import settings
import json

logger_task = get_task_logger(__name__)


@app.task(ignore_result=True)
def notion_ips_logger(notion_secret_key, notion_db_id, time_str, endpoint, method_ip, ip, date):
    """
    https://developers.notion.com/reference/post-page
    """
    # Get ip details
    ip_detail = {}
    try:
        if ip:
            response_obj = requests.get(settings.IP_LIST_CALL+ip)
            if response_obj.status_code == 200:
                ip_detail = json.loads(response_obj.text)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger_task.warning(f"Notion IP logger fails to get ip details for ip: {ip}, url: {settings.IP_LIST_CALL+ip}")

    url = f"https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {notion_secret_key}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-02-22",
    }
    # time_str, endpoint, method_ip, ip, date
    body = {
        "parent": {"database_id": notion_db_id},
        "properties": {
            "time_str": {
                "title": [
                    {
                        "text": {
                            "content": time_str
                        }
                    }
                ]
            },
            "endpoint": {
                "select": {
                    "name": str(endpoint).replace(",", ";")
                }
            },
            "method_ip": {
                "select": {
                    "name": str(method_ip).replace(",", ";")
                }
            },
            "ip": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": ip
                        }
                    }
                ]
            },
            "countryname": {
                "select": {
                    "name": str(ip_detail.get("countryname", "Undefined")).replace(",", ";"),
                }
            },
            "city": {
                "select": {
                    "name": str(ip_detail.get("city", "Undefined")).replace(",", ";"),
                }
            },
            "date": {
                "date": {
                    "start": date
                    # "start": "2021-05-11T12:00:00Z",  # "2020-12-08T12:00:00Z"
                }
            },
        },

    }
    try:

        response_obj = requests.post(url=url, headers=headers, json=body)
        if (response_obj.status_code != 200):
            logger_task.error(
                f"Notion IP logger Bad body / token with Response:\n\n"
                f"{response_obj.text}, Status: {response_obj.status_code}"
            )

    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger_task.critical(
            f"Notion IP logger fails at send data to notion Traceback:\n{''.join(e)}")
