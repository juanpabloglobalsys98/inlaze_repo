import logging
import re
import sys
import traceback

from api_admin.helpers import (
    DB_ADMIN,
    DefaultPAG,
)
from api_partner.helpers import IsEmailValid
from api_partner.models import SocialChannel
from api_partner.serializers import (
    SocialChannelSER,
    SocialChannelToPartnerSER,
)
from cerberus import Validator
from core.helpers import (
    request_cfg,
    to_bool,
    to_int,
)
from core.helpers.path_route_db import request_cfg
from core.models import User
from django.conf import settings
from django.db.models import Q
from django.db.models.query_utils import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class SocialChannelAPI(APIView, DefaultPAG):
    permission_classes = [
        IsAuthenticated,
    ]

    def get(self, request):

        validator = Validator(
            schema={
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "partner_id",
                },
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        order_by = validator.document.get("order_by")
        request_cfg.is_partner = True
        query = Q(partner_id=request.user.id, is_active=True)
        partner_channel = SocialChannel.objects.filter(query).order_by(order_by)

        if not partner_channel:
            return Response(

                data={
                    "channels": [],
                },
                status=status.HTTP_200_OK,
            )

        user_paginated = self.paginate_queryset(
            queryset=partner_channel,
            request=request,
            view=self,
        )

        channel_ser = SocialChannelSER(
            instance=user_paginated,
            many=True,
        )

        return Response(
            data={
                "channels": channel_ser.data,
            },
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            }
        )

    def post(self, request):
        request_cfg.is_partner = True
        validator = Validator(
            schema={
                "name": {
                    "required": False,
                    "type": "string",
                },
                "url": {
                    "required": False,
                    "type": "string",
                },
                "type_channel": {
                    "required": False,
                    "type": "integer",
                    "coerce": to_int,
                },
                "is_active": {
                    "required": False,
                    "type": "boolean",
                    "coerce": to_bool,
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        if validator.document is None:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "detail": {
                    "channels": [
                        _("There is not partners  in the system")
                    ],
                },
            },
                status=status.HTTP_404_NOT_FOUND,
            )

        url = validator.document.get("url")
        query = Q(url=url)
        channel_exist = SocialChannel.objects.filter(query).first()
        if channel_exist:
            return Response({
                "error": settings.CONFLICT_CODE,
                "detail": {
                    "channels": [
                        "Link is already exist",
                    ]
                }
            }, status=status.HTTP_400_BAD_REQUEST)

        query = Q(partner_id=request.user.id, is_active=True)
        num_channel = SocialChannel.objects.filter(query).count()

        if num_channel > settings.MAX_SOCIAL_CHANNEL:
            return Response({
                "error": settings.CONFLICT_CODE,
                "detail": {
                    "channels": [
                        "Channel limit already reached",
                    ]
                }
            }, status=status.HTTP_409_CONFLICT)

        channels = SocialChannel.objects.create(partner_id=request.user.id)

        partner_channel_ser = SocialChannelToPartnerSER(
            instance=channels,
            data=validator.document,
        )

        if partner_channel_ser.is_valid():
            partner_channel_ser.save()
            return Response(
                data={},
                status=status.HTTP_204_NO_CONTENT,
            )

        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "detail": partner_channel_ser.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )
