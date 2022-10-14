import re

from api_partner.models import Partner
from django.utils.translation import gettext as _
from rest_framework import serializers


class ReferredUserSER(serializers.ModelSerializer):

    full_name = serializers.SerializerMethodField("get_full_name")
    email = serializers.CharField()
    referred_by_id = serializers.IntegerField(allow_null=True)

    class Meta:
        model = Partner
        fields = (
            "pk",
            "full_name",
            "email",
            "adviser_id",
            "fixed_income_adviser_percentage",
            "net_revenue_adviser_percentage",
            "referred_by_id",
            "fixed_income_referred_percentage",
            "net_revenue_referred_percentage",

        )

    def get_full_name(self, obj):
        if (obj is not None and obj.full_name is not None):
            full_name = re.sub('\s+', ' ', obj.full_name)
            return full_name.strip()

    def validate_referred_by_id(self, value):
        if (value is not None and self.instance is not None):
            if (self.instance.pk == value):
                raise serializers.ValidationError(_("Cannot be referred himself"))
            referred_by = Partner.objects.filter(pk=value).only("pk").first()
            if (referred_by is None):
                msg = _("referred by with id {} not found")
                msg = msg.format(value)
                raise serializers.ValidationError(msg)
        return value
