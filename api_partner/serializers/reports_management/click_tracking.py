from api_log.helpers import DB_HISTORY
from api_log.models import ClickTracking
from django.db.models import Q
from rest_framework import serializers


class ClickTrackingSerializer(serializers.ModelSerializer):
    """
    Click tracking serializer with specific fields for querying purpose (adviser)
    """

    created_at = serializers.SerializerMethodField(method_name="normalize_created_at")
    partner_full_name = serializers.CharField(allow_null=True, required=False)
    formated_link = serializers.CharField(allow_null=True, required=False)
    countryname = serializers.SerializerMethodField(method_name="normalize_countryname")
    ip = serializers.SerializerMethodField(method_name="normalize_ip")

    def normalize_created_at(self, click):
        return click.created_at.timestamp()

    def normalize_countryname(self, click):
        return click.countryname if click.countryname else "undefined"

    def normalize_ip(self, click):
        return click.ip if click.ip else "undefined"

    class Meta:
        model = ClickTracking
        fields = ("partner_full_name","formated_link", "ip","registry","countrycode","countryname","asn_code",
            "asn_name","asn_route","asn_start","asn_end","asn_count","city","spam","tor","count","created_at",)

    def create(self, database=DB_HISTORY):
        return ClickTracking.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database=DB_HISTORY):
        return ClickTracking.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database=DB_HISTORY):
        return ClickTracking.objects.db_manager(database).filter(id=id).delete()

    def get_by_links_without_partner_link_accumulated(self, links, filters, database=DB_HISTORY):
        if filters:
            filters.append(Q(link_id__in=links))
            filters.append(Q(partner_link_accumulated_id=None))
        else:
            filters = [Q(link_id__in=links), Q(partner_link_accumulated_id=None)]

        return ClickTracking.objects.db_manager(database).filter(*filters).order_by("created_at")

    def get_by_links(self, links, filters, database=DB_HISTORY):
        if filters:
            filters.append(Q(link_id__in=links))
        else:
            filters = [Q(link_id__in=links)]

        return ClickTracking.objects.db_manager(database).filter(*filters).select_related(
            "ClickTracking_to_link").order_by("created_at")


class ClickTrackingBasicSerializer(serializers.ModelSerializer):
    """
    Click tracking serializer with specific fields for querying purpose (partner)
    """

    created_at = serializers.SerializerMethodField(method_name="normalize_created_at")
    countryname = serializers.SerializerMethodField(method_name="normalize_countryname")

    def normalize_created_at(self, click):
        return click.created_at.timestamp()

    def normalize_countryname(self, click):
        return click.countryname if click.countryname else "undefined"

    class Meta:
        model = ClickTracking
        fields = ("count", "countryname", "created_at", )

    def create(self, database=DB_HISTORY):
        return ClickTracking.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database=DB_HISTORY):
        return ClickTracking.objects.db_manager(database).filter(id=id).first()

    def delete(self, id, database=DB_HISTORY):
        return ClickTracking.objects.db_manager(database).filter(id=id).delete()

    def get_by_links_without_partner_link_accumulated(self, links, filters, database=DB_HISTORY):
        if filters:
            filters.append(Q(link_id__in=links))
            filters.append(Q(partner_link_accumulated_id=None))
        else:
            filters = [Q(link_id__in=links), Q(partner_link_accumulated_id=None)]

        return ClickTracking.objects.db_manager(database).filter(*filters).order_by("created_at")

    def get_by_links(self, links, filters, database=DB_HISTORY):
        if filters:
            filters.append(Q(link_id__in=links))
        else:
            filters = [Q(link_id__in=links)]

        return ClickTracking.objects.db_manager(database).filter(*filters).order_by("created_at")
