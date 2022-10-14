import logging
import sys
import traceback

from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import PartnerLinkAccumulated
from cerberus import Validator
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from django.conf import settings

logger = logging.getLogger(__name__)


class ModifyPorcentAPI(APIView):

    permission_classes = [
        IsAuthenticated,
    ]

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """ 
            Modify percentage cpa 

            #Body
           -  id_partner_link : "int"
                Param to identify link associated to partner
           -  percentage_cpa : "float"
                Param to define the percentage_cpa to update
        """
        validator = Validator(
            schema={
                "id_partner_link": {
                    "required": True,
                    "type": "integer",
                },
                "percentage_cpa": {
                    "required": True,
                    "type": 'float',
                    "min": 0.3,
                    "max": 1.0,
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        partner_link = PartnerLinkAccumulated.objects.filter(
            id=request.data.get("id_partner_link")
        )

        if not partner_link:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "partner_link_accumulated": [
                            "Partner link accumulated not found",
                            request.data.get("id_partner_link"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        try:
            partner_link.update(
                percentage_cpa=request.data.get("percentage_cpa"),
            )
            return Response(
                data={
                    "message": "Porcent updated successfully"
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            transaction.savepoint_rollback(sid, using=DB_USER_PARTNER)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(
                exc_type, exc_value, exc_traceback)
            logger.error((
                "Something is wrong when try update the porcent to the partner"
                f"check traceback:\n\n{''.join(e)}"
            ))
            return Response(
                data={
                    "message": _("Internal Error"),
                    "error": f"Something is wrong when try update the porcent to the partner check traceback:\n\n{''.join(e)}",
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
