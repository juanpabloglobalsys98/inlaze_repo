import logging
import re
import sys
import traceback
from locale import normalize

from api_admin.helpers import normalize_bookmaker_name
from api_admin.paginators import GetAllBookamkers
from api_admin.serializers import BookmakerSerializer
from api_partner.helpers import DB_USER_PARTNER
from api_partner.models import Bookmaker
from core.helpers import HavePermissionBasedView, ValidatorFile
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class BookmakerAPI(APIView, GetAllBookamkers):

    """
        Class View to bookmaker CRUD
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]
    parser_classes = (
        MultiPartParser,
        FormParser,
    )

    def post(self, request):
        """ 
            Method POST to add bookmaker 

            #Body
           -  name : "str"
                Param to create name
           -  id_adviser : "str"
                Image file to create bookmaker

        """
        validator = ValidatorFile({
            "name": {
                "required": True,
                "type": "string",
                "coerce": normalize_bookmaker_name,
            },
            "image": {
                "required": True,
                "type": "file",
            },
        },
        )

        data = request.data.dict()

        if not validator.validate(data):
            return Response(
                data={
                    "message": _("Invalid input"),
                    "error": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        bookmaker_v = Bookmaker.objects.filter(
            name__iexact=data.get("name")
        ).first()
        if bookmaker_v:
            return Response(
                data={
                    "error": settings.CONFLICT_CODE,
                    "details": {
                        "bookmaker": [
                            "bookmaker already exists",
                        ],
                    },
                },
                status=status.HTTP_409_CONFLICT,
            )

        data["name"] = data.get("name").lower()

        bookmaker = Bookmaker.objects.create(**data)

        return Response(
            data={
                "pk": bookmaker.id
            },
            status=status.HTTP_201_CREATED,
        )

    def get(self, request):
        """ 
            Return all Bookmakers 
        """
        bookmakers = Bookmaker.objects.all()
        bookmaker_serializer = BookmakerSerializer(bookmakers, many=True)
        return Response(
            data={
                "bookmakers": bookmaker_serializer.data
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """ 
            Update and specific bookmaker register 

            #Body
           -  name : "str"
                Param to update name
           -  image : "str"
                Image file to update bookmaker
           -  id : "int"
                Identify 

        """
        validator = ValidatorFile(
            schema={
                'name': {
                    'required': True,
                    'type': 'string',
                    "coerce": normalize_bookmaker_name,
                },
                'image': {
                    'required': False,
                    'type': 'file',
                },
                'id': {
                    'required': True,
                    'type': 'string',
                },
            },
        )

        if not validator.validate(request.data):
            return Response({
                "message": _("Invalid input"),
                "error": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        filters = [
            Q(id=validator.document.get("id")),
        ]
        bookmaker = Bookmaker.objects.filter(*filters).first()

        if not bookmaker:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "bookmaker": [
                            "bookmaker not found",
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        filters = [
            Q(name__iexact=validator.document.get("name")),
            ~Q(id=validator.document.get("id"))
        ]
        bookmaker_v = Bookmaker.objects.filter(*filters).first()
        if bookmaker_v:
            return Response({
                "error": settings.CONFLICT_CODE,
                "details": {
                    "Bookmaker": [
                        "Bookmaker already exits",
                    ],
                },
            },
                status=status.HTTP_409_CONFLICT,
            )

        # Rename
        bookmaker.name = validator.document.get("name")
        bookmaker.save()

        if 'image' in validator.document:
            bookmaker.file_delete_image()
            bookmaker.file_update_image(validator.document.get('image'), validator.document.get('name'))

        return Response(
            data={
                "message": _("Bookmaker was updated succesfully"),
            },
            status=status.HTTP_200_OK,
        )
