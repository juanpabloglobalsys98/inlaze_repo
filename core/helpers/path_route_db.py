import threading
from django.middleware.csrf import CsrfViewMiddleware

request_cfg = threading.local()


class CsrfViewRouteMiddleware(CsrfViewMiddleware):
    def process_view(self, request, *args, **kwargs):
        """
        Get Url path from view before to response, this will used for
        route db authentication, session
        """
        request_cfg.url_path = request.path
        return super().process_view(request, *args, **kwargs)

    def _set_token(self, request, *args, **kwargs):
        """
        Get Url path from view before to _set_token, this will used for
        route db authentication, session
        """
        request_cfg.url_path = request.path
        return super()._set_token(request, *args, **kwargs)

    def process_response(self, request, *args, **kwargs):
        if hasattr(request_cfg, 'url_path'):
            del request_cfg.url_path
        if hasattr(request_cfg, 'is_partner'):
            del request_cfg.is_partner
        return super().process_response(request, *args, **kwargs)
