import logging
import sys
import traceback

from api_partner.helpers.paginators import OwnCompanyPaginator
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.serializers.billing import (OwnCompanySerializer,
                                             OwnCompanyUpdateSerializer)
from cerberus import Validator
from core.helpers import (FilesNamesErrorHandler, HavePermissionBasedView,
                          ValidatorFile)
from django.conf import settings
from django.db import transaction
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


class OwnCompanyAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        """
        Lets an admin gets the current own company information or one specified
        """

        validator = ValidatorFile({
            'id': {
                'required': False,
                'type': 'integer',
                'coerce': int,
            }
        })

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        company_id = validator.document.get("id")
        if company_id:
            own_company = OwnCompanySerializer().get_by_id(company_id, DB_USER_PARTNER)
            seralized_own_company = OwnCompanySerializer(instance=own_company)
            return Response(
                data={"own_company": seralized_own_company.data if own_company else None},
                status=status.HTTP_200_OK
            )

        own_company = OwnCompanySerializer().get_latest(DB_USER_PARTNER)
        if own_company:
            serialized_own_company = OwnCompanySerializer(instance=own_company)
            return Response(
                data={"own_company": serialized_own_company.data},
                status=status.HTTP_200_OK
            )
        else:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {"id": [_("There is not company info yet")]}
                },
                status=status.HTTP_404_NOT_FOUND
            )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def post(self, request):
        """
        Lets an admin creates a new own company information in the database
        """

        validator = ValidatorFile({
            'logo': {
                'required': True,
                'type': 'file',
            },
            'name': {
                'required': True,
                'type': 'string'
            },
            'nit': {
                'required': True,
                'type': 'string'
            },
            'city': {
                'required': True,
                'type': 'string'
            },
            'address': {
                'required': True,
                'type': 'string'
            },
            'phone': {
                'required': True,
                'type': 'string',
                'regex': '\+\d+\s\d+'
            },
        })

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        files_names_validator = Validator(
            {
                "logo": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(png|jpg|jpeg|webp|JPEG|PNG|JPG|WEBP)+",
                },
            }, error_handler=FilesNamesErrorHandler
        )

        logo = validator.document.get("logo")
        if logo and not files_names_validator.validate({"logo": logo.name}):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": files_names_validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        serialized_own_company = OwnCompanySerializer(data=validator.document)

        if serialized_own_company.is_valid():
            own_company = serialized_own_company.create(serialized_own_company.validated_data)
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_own_company.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            if logo:
                OwnCompanySerializer().create_logo(own_company, logo)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                data={
                    "error": settings.ERROR_SAVING_COMPANY_LOGO,
                    "details": {"logo": ["".join(str(e))]}
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an admin updates an own company information in the database
        """

        validator = ValidatorFile({
            'id': {
                'required': False,
                'type': 'integer',
                'coerce': int,
            },
            'logo': {
                'required': False,
                'type': 'file',
            },
            'name': {
                'required': False,
                'type': 'string',
            },
            'nit': {
                'required': False,
                'type': 'string',
            },
            'city': {
                'required': False,
                'type': 'string',
            },
            'address': {
                'required': False,
                'type': 'string',
            },
            'phone': {
                'required': False,
                'type': 'string',
                'regex': '\+\d+\s\d+'
            },
        })

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        files_names_validator = Validator(
            {
                "logo": {
                    "required": False,
                    "type": "string",
                    "regex": ".+\.(png|jpg|jpeg|webp|JPEG|PNG|JPG|WEBP)+",
                },
            }, error_handler=FilesNamesErrorHandler
        )

        logo = validator.document.get("logo")
        if logo and not files_names_validator.validate({"logo": logo.name}):
            return Response(
                {
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": files_names_validator.errors
                }, status=status.HTTP_400_BAD_REQUEST
            )

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        own_company = OwnCompanyUpdateSerializer().get_by_id(validator.document.get("id"), DB_USER_PARTNER)

        if not own_company:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {"id": [_("There is not such company in the system")]}
            }, status=status.HTTP_404_NOT_FOUND)

        try:
            if logo:
                OwnCompanyUpdateSerializer().update_logo(own_company, logo)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                data={
                    "error": settings.ERROR_SAVING_COMPANY_LOGO,
                    "details": {"logo": ["".join(str(e))]}
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        serialized_own_company = OwnCompanyUpdateSerializer(instance=own_company, data=validator.document)
        if serialized_own_company.is_valid():
            serialized_own_company.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_own_company.errors
                },
                status=status.HTTP_400_BAD_REQUEST
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)


class AllOwnCompaniesAPI(APIView, OwnCompanyPaginator):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    def get(self, request):
        """
        Lets an admin gets all own companies in the system
        """

        validator = ValidatorFile({
            'lim': {
                'required': False,
                'type': 'integer',
                'coerce': int,
            },
            'offs': {
                'required': False,
                'type': 'integer',
                'coerce': int,
            }
        })

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        own_companies = OwnCompanySerializer().get_all(DB_USER_PARTNER)
        own_companies = self.paginate_queryset(own_companies, request, view=self)
        own_companies = OwnCompanySerializer(instance=own_companies, many=True)
        return Response(
            data={"own_companies": own_companies.data if own_companies else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if own_companies else None
        )
