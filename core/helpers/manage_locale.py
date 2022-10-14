from django.middleware.locale import LocaleMiddleware
from django.utils import translation


class ManageLocaleMiddleware(LocaleMiddleware):

    def process_request(self, request):
        language = request.META.get('HTTP_APP_LANGUAGE', '')
        if language:
            translation.activate(language)
            request.LANGUAGE_CODE = translation.get_language()
            return

        return super().process_request(request)
