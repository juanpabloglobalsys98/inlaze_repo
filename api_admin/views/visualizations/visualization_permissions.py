from api_admin.helpers import DB_ADMIN
from api_admin.models import ReportVisualization
from api_admin.serializers import (
    AcountReportAdminSerializers,
    FilterMemeberReportSer,
    MembertReportGroupMultiFxSer,
    MembertReportGroupSer,
    MemeberReportMultiFxSer,
    PermissionSerializer,
)
from api_partner.serializers import GeneralPartnerSER
from cerberus import Validator
from core.helpers import (
    HavePermissionBasedView,
    get_view_name,
)
from core.models import (
    Permission,
    Rol,
)
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class VisualizationPermissionsAPI(APIView):

    """
        Class view with resources to visualization permissions
    """

    permission_classes = [
        IsAuthenticated,
        HavePermissionBasedView,
    ]

    def get(self, request):
        """ 
            Show all serializer fields based in MembertReportGroupSer and FilterMemeberReportSer
        """

        # Get all fields to member and account
        from api_admin.views import MemberReportAPI
        member_group = set(MembertReportGroupSer._declared_fields.keys())
        member_filter = set(FilterMemeberReportSer.Meta.fields)
        member = list(member_filter.union(member_group))

        name_view = get_view_name(MemberReportAPI).lower()
        name_method = "get"
        general_partner_codename = f"{name_view}-{name_method}"
        query = Q(codename=general_partner_codename)

        permissions = Permission.objects.filter(query)
        permission_ser = PermissionSerializer(
            instance=permissions,
            many=True,
        )
        membert_report_dict = {
            "visualization_fields": member,
            "permission": permission_ser.data,
        }

        # Get all fields to member multi fx
        from api_admin.views import MemberReportMultiFxAPI
        member_multi_fx_group = set(MembertReportGroupMultiFxSer._declared_fields.keys())
        member_multi_fx_non_group = set(MemeberReportMultiFxSer.Meta.fields)
        member_multi_fx = list(member_multi_fx_non_group.union(member_multi_fx_group))
        name_view = get_view_name(MemberReportMultiFxAPI).lower()
        name_method = "get"
        general_partner_codename = f"{name_view}-{name_method}"
        query = Q(codename=general_partner_codename)

        permissions = Permission.objects.filter(query)
        permission_ser = PermissionSerializer(
            instance=permissions,
            many=True,
        )
        membert_report_multi_fx_dict = {
            "visualization_fields": member_multi_fx,
            "permission": permission_ser.data,
        }

        #
        from api_admin.views import AccountReportAPI
        account = set(AcountReportAdminSerializers.Meta.fields)
        name_view = get_view_name(AccountReportAPI).lower()
        name_method = "get"
        general_partner_codename = f"{name_view}-{name_method}"
        query = Q(codename=general_partner_codename)

        permissions = Permission.objects.filter(query)

        permission_ser = PermissionSerializer(
            instance=permissions,
            many=True,
        )

        account_report_dict = {
            "visualization_fields": account,
            "permission": permission_ser.data,
        }

        # general partner
        from api_admin.views import PartnersGeneralAPI
        general_partner = set(GeneralPartnerSER.Meta.fields)
        name_view = get_view_name(PartnersGeneralAPI).lower()
        name_method = "get"
        general_partner_codename = f"{name_view}-{name_method}"

        query = Q(codename=general_partner_codename)

        permissions = Permission.objects.filter(query)
        permission_ser = PermissionSerializer(
            instance=permissions,
            many=True,
        )

        general_partner_dict = {
            "visualization_fields": general_partner,
            "permission": permission_ser.data,
        }
        return Response(
            data={
                "data": [
                    membert_report_dict,
                    membert_report_multi_fx_dict,
                    account_report_dict,
                    general_partner_dict,
                ],
            },
            status=status.HTTP_200_OK,
        )

    @transaction.atomic(using=DB_ADMIN, savepoint=True)
    def put(self, request):
        """ 
            Update or create visualization record to a rol 

        """
        validator = Validator({
            "rol": {
                "required": True,
                "type": "integer"
            },
            "codename": {
                "required": True,
                "type": "string"
            },
            "values_can_view": {
                "required": True,
                "type": "list",
                "schema": {
                    "type": "string",
                },
            },
        },
        )

        if not validator.validate(request.data):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        filters = (Q(id=validator.document.get("rol")),)
        rol = Rol.objects.filter(*filters).first()
        filters = (Q(codename=validator.document.get("codename")),)
        permmision = Permission.objects.filter(*filters).first()

        if not rol or not permmision:
            return Response({
                "error": settings.NOT_FOUND_CODE,
                "details": {
                    "non_field_errors": [
                        _("Rol or permmission not found")
                    ]
                }
            },
                status=status.HTTP_404_NOT_FOUND,
            )

        visualization, created = ReportVisualization.objects.update_or_create(
            rol__id=validator.document.get("rol"), permission__codename=validator.document.get("codename"),
            defaults={
                "rol": rol,
                "permission": permmision,
                "values_can_view": validator.document.get("values_can_view"),
            }
        )

        if created:
            return Response({
                "msg": _("Visualization permissions was created successfully")
            }, status=status.HTTP_200_OK)

        return Response({
            "msg": _("Visualization permissions was updated successfully")
        }, status=status.HTTP_200_OK)
