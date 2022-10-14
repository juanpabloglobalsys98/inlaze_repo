from api_admin.helpers import PartnerBillingPaginator
from api_partner.helpers import DB_USER_PARTNER
from api_partner.serializers.authentication import (
    PartnerBillingDetailSerializer,
)
from cerberus import Validator
from core.helpers import StandardErrorHandler
from django.conf import settings
from django.db.models.query_utils import Q
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class BilledPartnerAPI(APIView, PartnerBillingPaginator):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets get all partners in the system for an admin
        """

        validator = Validator(
            {
                "email": {
                    "required": False,
                    "nullable": True,
                    "type": "string"
                },
                "identification": {
                    "required": False,
                    "nullable": True,
                    "type": "string"
                },
                "identification_type": {
                    "required": False,
                    "nullable": True,
                    "type": "string"
                },
                "full_name": {
                    "required": False,
                    "nullable": True,
                    "type": "string"
                },
                "user_id": {
                    "required": False,
                    "nullable": True,
                    "type": "integer",
                    "coerce": int
                },
                "lim": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                },
                "offs": {
                    "required": False,
                    "type": "integer",
                    "coerce": int
                }
            }, error_handler=StandardErrorHandler
        )

        if not validator.validate(request.query_params):
            return Response({
                "error": settings.CERBERUS_ERROR_CODE,
                "details": validator.errors
            }, status=status.HTTP_400_BAD_REQUEST)

        validator.normalized(request.data)

        # filters
        user_id = request.query_params.get("user_id")
        email = request.query_params.get("email")
        identification = request.query_params.get("identification")
        identification_type = request.query_params.get("identification_type")
        full_name = request.query_params.get("full_name")

        filters = []
        if email:
            filters.append(Q(user__email__icontains=email))
        if identification:
            filters.append(Q(additionalinfo__identification__icontains=identification))
        if identification_type:
            filters.append(Q(additionalinfo__identification_type=identification_type))
        if full_name:
            filters.append(Q(full_name__icontains=full_name))
        if user_id:
            filters.append(Q(user_id=user_id))

        partners = PartnerBillingDetailSerializer().get_partners(filters=filters, database=DB_USER_PARTNER)

        if partners:
            partners = self.paginate_queryset(partners, request, view=self)
            partners = PartnerBillingDetailSerializer(instance=partners, many=True)

        return Response(
            data={"partner": partners.data if partners else []},
            status=status.HTTP_200_OK,
            headers={
                "access-control-expose-headers": "count, next, previous",
                'count': self.count,
                'next': self.get_next_link(),
                'previous': self.get_previous_link()
            } if partners else None
        )
