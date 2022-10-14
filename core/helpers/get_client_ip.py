import logging

logger = logging.getLogger("django.ips")


def get_client_ip(request):
    if x_forwarded_for := request.META.get('HTTP_X_FORWARDED_FOR'):
        return "FORWARDED_FOR", f"{x_forwarded_for}"
    elif x_real_ip := request.META.get('HTTP_X_REAL_IP'):
        return "REAL_IP", f"{x_real_ip}"
    elif(remote_addr := request.META.get('REMOTE_ADDR')):
        return "REMOTE_ADDR", f"{remote_addr}"

    return "Undefined", "Undefined IP"


class GetIpRequestFrom:
    def __init__(self, get_response):
        self.get_response = get_response
        # One-time configuration and initialization.

    def __call__(self, request):
        # Code to be executed for each request before
        # the view (and later middleware) are called.

        ip_method, ip_str = get_client_ip(request)
        logger.debug(f"{request.path},{ip_method},{ip_str}")

        response = self.get_response(request)

        # Code to be executed for each request/response after
        # the view is called.

        return response
