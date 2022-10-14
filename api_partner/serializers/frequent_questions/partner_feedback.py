from api_partner.models import PartnerFeedback
from django.db.models import Q
from rest_framework import serializers


class PartnerFeedbackSerializer(serializers.ModelSerializer):
    """
    Partner feedback serializer with all fields
    """

    class Meta:
        model = PartnerFeedback
        fields = "__all__"

    def create(self, database="default"):
        """
        """
        return PartnerFeedback.objects.db_manager(database).create(**self.validated_data)

    def exist(self, id, database="default"):
        return PartnerFeedback.objects.db_manager(database).filter(id=id).first()

    def get_all(self, database="default"):
        return PartnerFeedback.objects.db_manager(database).order_by("-id")

    def get_by_question_partner(self, partner, question, database="default"):
        filters = [Q(partner=partner), Q(question=question)]
        return PartnerFeedback.objects.db_manager(database).filter(*filters).order_by("-id")

    def delete(self, id, database="default"):
        return PartnerFeedback.objects.db_manager(database).filter(id=id).delete()
