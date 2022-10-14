from api_partner.helpers import DB_USER_PARTNER
from api_partner.serializers.authentication.partner import (
    PartnerStatusSER,
)
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.response import Response
from rest_framework.views import APIView


class TokenUserAPI(APIView):

    def post(self, request):
        validator = Validator(
            {
                "key": {
                    "required": True,
                    "type": "string",
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.data):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        token = Token.objects.using(DB_USER_PARTNER).filter(**validator.document).first()
        if not token:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "key": [
                        _("Not found")
                    ]
                }
            }, status=status.HTTP_404_NOT_FOUND)
        user = token.user
        partner = user.partner
        partner_full_name = user.get_full_name()
        partner_status = PartnerStatusSER(instance=partner)
        try:
            partner_country = partner.additionalinfo.country
        except partner._meta.model.additionalinfo.RelatedObjectDoesNotExist:
            partner_country = None

        return Response({
            "partner_status": partner_status.data,
            "partner_country": partner_country,
            "partner_level": partner.level,
            "partner_full_name": partner_full_name
        }, status=status.HTTP_200_OK)
