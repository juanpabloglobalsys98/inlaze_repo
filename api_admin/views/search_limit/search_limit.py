from api_admin.models import SearchPartnerLimit
from api_admin.serializers import SearchSerializer
from cerberus import Validator
from django.conf import settings
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView


class SearchLimitAPI(APIView):

    """
        Class view with resources to search limit API
    """

    def get(self, request):
        """
            Method that return search partner limit records from models

            #Body
           -  rol : "str"
                Param to define since date return membert report records
           -  search_type : "str"
                Param to define until date return membert report records

        """

        validator = Validator(
            schema={
                "rol": {
                    "required": False,
                    "type": "integer",
                    "coerce": int,
                },
                "search_type": {
                    "required": False,
                    "type": "integer",
                    "allowed": SearchPartnerLimit.SearchType.values,
                    "coerce": int,
                },
            },
        )

        if not validator.validate(request.query_params):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = []
        if "rol" in validator.document:
            filters.append(
                Q(rol__id=validator.document.get("rol")),
            )
        if "search_type" in validator.document:
            filters.append(
                Q(search_type=validator.document.get("search_type")),
            )

        searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters)

        if not searchpartnerlimit:
            searchpartner_serializer = []
        else:
            searchpartner_serializer = SearchSerializer(searchpartnerlimit, many=True).data

        return Response(
            data={
                "data": searchpartner_serializer,
            },
            status=status.HTTP_200_OK,
        )

    def put(self, request):
        # account report partners api-get
        # membert report api-get
        # cpa management api-get
        # partners general api-get
        """
            Method to update fields into search partner limit

            #Body
           -  rol : "str"
                Param to define since date return membert report records
           -  search_type : "str"
                Param to define until date return membert report records

        """

        validator = Validator(
            schema={
                "rol": {
                    "required": True,
                    "type": "integer",
                },
                "codename": {
                    "required": True,
                    "type": "string",
                },
                "search_type": {
                    "required": True,
                    "type": "integer",
                    "allowed": SearchPartnerLimit.SearchType.values,
                },
            },
        )

        if not validator.validate(request.data):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        filters = (Q(rol=validator.document.get("rol")), Q(codename=validator.document.get("codename")))
        searchpartnerlimit = SearchPartnerLimit.objects.filter(*filters).first()
        if searchpartnerlimit:
            searchserializer_serializer = SearchSerializer(
                instance=searchpartnerlimit,
                data=validator.document,
            )
            msg = "search limit was updated"
        else:
            searchserializer_serializer = SearchSerializer(
                data=validator.document,
            )
            msg = "search limit created"

        if searchserializer_serializer.is_valid():
            searchserializer_serializer.save()
        else:
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": searchserializer_serializer.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(
            data={
                "msg": _(msg)
            },
            status=status.HTTP_200_OK,
        )
