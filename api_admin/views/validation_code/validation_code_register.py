from api_admin.helpers import DB_ADMIN
from api_admin.serializers import ValidationCodeRegisterSerializer
from api_partner.models import ValidationCodeRegister
from cerberus import Validator
from core.helpers import HavePermissionBasedView
from core.models import User
from core.paginators import DefaultPAG
from django.conf import settings
from django.db.models import (
    F,
    Q,
)
from django.db.models.functions import Concat
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class ValidationCodeRegisterAPI(APIView, DefaultPAG):
    """
    Allow filtering the ValidationCodeRegisters by email, phone or partner full name.
    """

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    def post(self, request):
        validator = Validator(
            schema={
                "email": {
                    "required": False,
                    "type": "string",
                },
                "phone": {
                    "required": False,
                    "type": "string",
                },
                "full_name": {
                    "required": False,
                    "type": "string",
                },
                "lim": {
                    "required": False,
                    "type": "string",
                },
                "offs": {
                    "required": False,
                    "type": "string",
                },
                "order_by": {
                    "required": False,
                    "type": "string",
                    "default": "-created_at",
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

        validation_code_register = ValidationCodeRegister.objects

        query = Q()
        if "email" in validator.document:
            query &= Q(email__icontains=validator.document.get("email"))
        if "phone" in validator.document:
            query &= Q(phone=validator.document.get("phone"))
        if "full_name" in validator.document:
            validation_code_register = validation_code_register.annotate(
                full_name_search=Concat(
                    F("first_name"),
                    F("second_name"),
                    F("last_name"),
                    F("second_last_name"),
                ),
            )
            query &= Q(full_name_search__icontains=validator.document.get("full_name"))

        order_by = validator.document.get("order_by")
        validation_code_register = validation_code_register.filter(query).order_by(order_by)
        validation_pag = self.paginate_queryset(
            queryset=validation_code_register,
            request=request,
            view=self,
        )
        adviser_ids = {validation_code.adviser_id for validation_code in validation_pag}
        adviser_users = User.objects.using(DB_ADMIN).filter(id__in=adviser_ids)

        validation_ser = ValidationCodeRegisterSerializer(
            instance=validation_pag,
            many=True,
            context={
                "adviser_users": adviser_users,
            },
        )

        return Response(
            data={
                "codes": validation_ser.data,
            },
            headers={
                "count": self.count,
                "next": self.get_next_link(),
                "previous": self.get_previous_link(),
                "access-control-expose-headers": "count,next,previous"
            },
            status=status.HTTP_200_OK,
        )
