import requests
from betenlace.celery import app
from celery.utils.log import get_task_logger

logger_task = get_task_logger(__name__)


@app.task(
    ignore_result=True,
    autoretry_for=(
        ConnectionError,
        OverflowError,
    ),
    retry_kwargs={'max_retries': 5},
    retry_backoff=True,
    retry_backoff_max=30,
    retry_jitter=True,
)
def chat_logger(msg, msg_url):
    msg_chunked = _chunk_string(
        string=msg,
    )
    for msg_i in msg_chunked:
        response = requests.post(
            url=msg_url,
            headers={
                "Content-Type": "application/json; charset=UTF-8",
            },
            json={
                "text": msg_i,
            },
        )

        if(response.status_code == 400):
            # {
            #   "error": {
            #     "code": 400,
            #     "message": "Invalid space resource name in request.",
            #     "status": "INVALID_ARGUMENT"
            #   }
            # }
            logger_task.error(f"Badrequest with webhook, response data\n{response.text}")
        elif(response.status_code == 429):
            # {
            # "error": {
            #     "code": 429,
            #     "message": "Resource has been exhausted (e.g. check quota).",
            #     "status": "RESOURCE_EXHAUSTED"
            # }
            # }
            logger_task.error(
                f"Too Many request in short time, Beware with logger system, response data\n{response.text}"
            )
            raise OverflowError(
                f"Chat logger had too many request, Check logger behaviour, response data\n{response.text}"
            )
        elif(response.status_code != 200):
            logger_task.error(
                f"Chat logger Unspected request status code, execute retry msg_url: {msg_url}\n"
                f"msg: {msg}\n\n Response: {response.text}, Status: {response.status_code}"
            )
            raise ConnectionError(f"Chat logger Unspected request status code {response.status_code}")


def _chunk_string(
    string,
    length=4096,
):
    return (string[0+i:length+i] for i in range(0, len(string), length))
