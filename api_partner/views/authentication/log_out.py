from api_partner.helpers import DB_USER_PARTNER
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView


class LogOutAPI(APIView):

    permission_classes = (IsAuthenticated,)

    def post(self, request):
        """
        It let's a user ends their session into Betenlace
        """
        Token.delete(request.auth, using=DB_USER_PARTNER)
        return Response(status=status.HTTP_200_OK)
