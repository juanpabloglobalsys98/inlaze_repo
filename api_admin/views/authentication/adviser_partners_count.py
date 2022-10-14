from api_admin.helpers.routers_db import DB_ADMIN
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.serializers.authentication.partner import (
    PartnersForAdvisersSerializer,
)
from core.helpers.path_route_db import request_cfg
from core.serializers.user import AdvisersWithPartnersSerializer
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class AdviserPartnersCountAPI(APIView):

    permission_classes = (IsAuthenticated, )

    def get(self, request):
        """
        Lets an admin knows details about the partners linking in the system
        """
        advisers = AdvisersWithPartnersSerializer.get_all_advisers(None, DB_ADMIN)
        request_cfg.is_partner = True

        if advisers:
            serialized_advisers = AdvisersWithPartnersSerializer(instance=advisers, many=True)

        total_active_partners = PartnersForAdvisersSerializer().get_all_actives(DB_USER_PARTNER).count()
        total_linked_partners = 0
        for adviser in serialized_advisers.data:
            total_linked_partners += adviser.get("count")

        unassigned_partners = total_active_partners - total_linked_partners
        return Response(
            {
                "adviser_partners_count": serialized_advisers.data if advisers else None,
                "total_active_partners": total_active_partners,
                "total_unassigned": unassigned_partners
            }, status=status.HTTP_200_OK)
