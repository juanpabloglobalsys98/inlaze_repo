from django.http.response import HttpResponseRedirect
from django.conf import settings


def click_error(request, exception):
    send_path = request.path
    if (request.path):
        if (request.path[-1] != "/"):
            send_path = request.path+"/"
    return HttpResponseRedirect(
        redirect_to=(
            settings.URL_REDIRECT_CAMPAIGN_ERROR +
            send_path +
            "error"
        )
    )
