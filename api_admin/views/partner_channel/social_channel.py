import logging

from api_admin.helpers import DefaultPAG
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import SocialChannel
from api_partner.serializers import SocialChannelSER
from cerberus import Validator
from core.helpers import (
    request_cfg,
    to_bool,
    to_int,
)
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class SocialChannelPartnerAPI(APIView, DefaultPAG):
    """
    Here we work on the new social channel for the partner
    """
    permission_classes = [
        IsAuthenticated
    ]

    def post(self, request):
        """
        This is a filter that requires two fields, partner_id and is_active.
        partner_id, return channels for this partner.
        is_active, return channels that are active or not.
        """
        validator = Validator(
            schema={
                "filter": {
                    "required": True,
                    "type": "dict",
                    "schema": {
                        "partner_id": {
                            "required": True,
                            "type": "integer",
                            "coerce": to_int,
                        },
                        "is_active": {
                            "required": True,
                            "type": "boolean",
                            "coerce": to_bool,
                        },
                    },
                },
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

        if validator.document is None:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "partner_id": [
                        _("There is not partners  in the system")
                    ],
                },
            },
                status=status.HTTP_404_NOT_FOUND,
            )

        order_by = validator.document.get("order_by")
        query = Q(**validator.document.get("filter"))
        request_cfg.is_partner = True
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

    def put(self, request):
        """
        this put, adviser can add or update channels for the partner
        """
        validator = Validator(
            {
                'channels': {
                    'required': True,
                    'type': 'list',
                    'schema': {
                        'type': 'dict',
                        'schema': {
                            "partner_id": {
                                "required": True,
                                "type": "integer",
                                "coerce": to_int,
                            },
                            "id": {
                                "required": False,
                                "type": "integer",
                                "coerce": to_int,
                            },
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
                    },
                },
            })

        """
        validator is a list, each element in the list is a channel.
        id only is required if the adviser will update one channel.
        """
        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "detail": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        request_cfg.is_partner = True

        links_to_update = []
        links_to_create = []

        for data in request.data.get("channels"):
            query = Q(id=data.get("id"))
            social_channel = SocialChannel.objects.filter(query).first()

            if social_channel:
                query = Q(url=data.get("url"))
                channel_to_validate = SocialChannel.objects.filter(query).first()
                if channel_to_validate:
                    return Response({
                        "error": settings.CONFLICT_CODE,
                        "details": {
                            "channels": [
                                "Link is already in this channel",
                                data.get("url")
                            ]
                        }
                    }, status=status.HTTP_409_CONFLICT)

                query = Q(id=data.get("id"), partner_id=data.get("partner_id"))
                update_channel = SocialChannel.objects.filter(query).first()
                if update_channel is None:
                    return Response({
                        "error": settings.CONFLICT_CODE,
                        "details": {
                            "channels": [
                                "this channel doesn't exist or partner is wrong",
                            ]
                        }
                    }, status=status.HTTP_409_CONFLICT)

                for data_key, data_value in data.items():
                    setattr(social_channel, data_key, data_value)

                if (update_channel):
                    links_to_update.append(
                        social_channel
                    )
            else:
                name = data.get("name")
                url = data.get("url")
                type_channel = data.get("type_channel")

                if name is None or url is None or type_channel is None:
                    return Response({
                        "error": settings.CONFLICT_CODE,
                        "channels": {
                            "Link": [
                                "You must send all data required",
                            ]
                        }
                    }, status=status.HTTP_409_CONFLICT)
                """
                when adviser create a new social channel must send this information
                """
                query = Q(partner_id=data.get("partner_id"), is_active=True)
                num_channel = SocialChannel.objects.filter(query).count()

                if num_channel > settings.MAX_SOCIAL_CHANNEL:
                    return Response({
                        "error": settings.CONFLICT_CODE,
                        "detail": {
                            "channels": [
                                _("Channel limit already reached"),
                            ]
                        }
                    }, status=status.HTTP_409_CONFLICT)

                query = Q(url=data.get("url"))
                channel_to_validate = SocialChannel.objects.filter(query).first()
                if channel_to_validate:
                    return Response({
                        "error": settings.BAD_REQUEST_CODE,
                        "details": {
                            "channels": [
                                _("Link is already in this channel"),
                                data.get("url")
                            ]
                        }
                    }, status=status.HTTP_409_CONFLICT)
                links_to_create.append(SocialChannel(**data))

        with transaction.atomic(using=DB_USER_PARTNER):

            SocialChannel.objects.bulk_create(
                objs=links_to_create,
            )

            SocialChannel.objects.bulk_update(
                objs=links_to_update,
                fields=[
                    "name",
                    "url",
                    "type_channel",
                    "is_active",
                ],
            )
            return Response(
                data={
                    "msg": "Links were created succesfully",
                },
                status=status.HTTP_201_CREATED,
            )
