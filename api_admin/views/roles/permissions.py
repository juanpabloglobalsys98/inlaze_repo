from api_admin.models import ReportVisualization
from api_admin.serializers import (
    AcountReportAdminSerializers,
    FilterMemeberReportSer,
    MembertReportGroupMultiFxSer,
    MembertReportGroupSer,
    MemeberReportMultiFxSer,
    PermissionSerializer,
    PermissionsSerializer,
    ReportVisualizationSerializer,
)
from api_partner.serializers import GeneralPartnerSER
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    get_view_name,
)
from core.models import Permission
from django.conf import settings
from django.db.models import Q
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class PermissionsManagementAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def get(self, request):
        '''
            Return all permission existing in DB

            #Body
           -  name : "str"
                Param to define the filter by name
           -  section : "str"
                Param to define the filter by section
        '''
        validator = Validator(
            schema={
                "name": {
                    "required": False,
                    "type": "string",
                },
                "section": {
                    "required": False,
                    "type": 'string',
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
        if "name" in validator.document:
            filters.append(
                Q(name__icontains=validator.document.get("name")),
            )

        if "section" in validator.document:
            filters.append(
                Q(section__icontains=validator.document.get("section")),
            )

        permissions = Permission.objects.filter(*filters)
        permission_serializer = PermissionSerializer(permissions, many=True)
        return Response(
            data={
                "data": permission_serializer.data
            },
            status=status.HTTP_200_OK
        )


class AdviserPermissionsAPI(APIView):

    permission_classes = (
        IsAuthenticated,
    )

    def get(self, request):
        from api_admin.views import PartnersGeneralAPI
        """
            Return permissions allowed to loging adviser
        """
        user = request.user
        permissions = Permission.objects.all()
        rol = "superuser"
        if not user.is_superuser:
            if not user.rol:
                return Response(
                    data={
                        "user": user.get_full_name(),
                        "rol": "undefined",
                        "permissions": [],
                        "visualizations": [],
                    },
                    status=status.HTTP_200_OK,
                )
            # Get rol's permissions
            rol = user.rol.rol
            filters = (
                Q(permissions_to_rol=user.rol),
            )
            permissions = permissions.filter(*filters)
            # Get report visualization fields if exits
            filters = (
                Q(rol=user.rol),
            )
            reportvisualization = ReportVisualization.objects.filter(*filters)
            reportvisualizationserializer = ReportVisualizationSerializer(reportvisualization, many=True).data
        else:
            # Get all report visualization fields
            member_group = set(MembertReportGroupSer._declared_fields.keys())
            member_filter = set(FilterMemeberReportSer.Meta.fields)
            member = list(member_filter.union(member_group))
            # Get all fields to member multi fx
            member_multi_fx_group = set(MembertReportGroupMultiFxSer._declared_fields.keys())
            member_multi_fx_non_group = set(MemeberReportMultiFxSer.Meta.fields)
            member_multi_fx = list(member_multi_fx_non_group.union(member_multi_fx_group))
            account = set(AcountReportAdminSerializers.Meta.fields)
            # Get all fields to general partner
            general_partner = set(GeneralPartnerSER.Meta.fields)
            name_view = get_view_name(PartnersGeneralAPI).lower()
            name_method = "get"
            general_partner_codename = f"{name_view}-{name_method}"

            reportvisualizationserializer = [
                {
                    "rol": -1,
                    "permission_codename": "member report api-get",
                    "values_can_view": member,
                },
                {
                    "rol": -1,
                    "permission_codename": "member report multi fx api-get",
                    "values_can_view": member_multi_fx,
                },
                {
                    "rol": -1,
                    "permission_codename": "account report api-get",
                    "values_can_view": account,
                },
                {
                    "rol": -1,
                    "permission_codename": general_partner_codename,
                    "values_can_view": general_partner,
                },
            ]

        # serializers
        permission_serializer = PermissionsSerializer(permissions, many=True)

        return Response(
            data={
                "user": user.get_full_name(),
                "user_pk": user.pk,
                "rol": rol,
                "permissions": permission_serializer.data,
                "visualizations": reportvisualizationserializer,
            },
            status=status.HTTP_200_OK,
        )
