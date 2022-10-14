from django.db.models import fields
from rest_framework import serializers
from api_partner.models import Bookmaker


class BookmakerSerializer(serializers.ModelSerializer):
    """ Serializer to data from Bookmaker """
    
    class Meta:
        model = Bookmaker
        fields = (
            "id",
            "name",
            "image"
        )