import os

from api_partner.helpers import PartnerStatusCHO
from core.helpers import S3StandardIA
from django.db import models
from django.utils.translation import gettext as _


class PartnerInfoValidationRequest(models.Model):
    """
    Contains partner info to be validated.
    """
    def file_upload_identification_file(instance, filename):
        return os.path.join(
            "partner",
            "request",
            "basic_info",
            str(instance.partner_id),
            str(instance.id),
            filename,
        )

    partner = models.ForeignKey(
        to="api_partner.Partner",
        related_name="validation_requests",
        on_delete=models.CASCADE,
    )
    adviser_id = models.BigIntegerField(null=True)
    first_name = models.CharField(max_length=150, blank=True)
    second_name = models.CharField(max_length=150, blank=True)
    last_name = models.CharField(max_length=150, blank=True)
    second_last_name = models.CharField(max_length=150, blank=True)
    current_country = models.CharField(max_length=3)

    class IdType(models.IntegerChoices):
        ID = 0
        LICENSE = 1
        PASSPORT = 2
    id_type = models.SmallIntegerField(default=IdType.ID)
    id_number = models.CharField(max_length=60)
    status = models.SmallIntegerField(default=PartnerStatusCHO.REQUESTED)

    document_id_front_file = models.FileField(
        upload_to=file_upload_identification_file,
        storage=S3StandardIA,
        default="",
        blank=True,
    )
    document_id_back_file = models.FileField(
        upload_to=file_upload_identification_file,
        storage=S3StandardIA,
        default="",
        blank=True,
    )
    selfie_file = models.FileField(
        upload_to=file_upload_identification_file,
        storage=S3StandardIA,
        default="",
        blank=True,
    )

    class ErrorField(models.IntegerChoices):
        NAME = 0, _("Name")
        SURNAME = 1, _("Surname")
        COUNTRY = 2, _("Residence country")
        ID_TYPE = 3, _("Document type")
        ID_NUMBER = 4, _("Id number")
        ID_DOCUMENT_FRONT = 5, _("Frontal document identification")
        ID_DOCUMENT_BACK = 6, _("Back document identification")
        ID_SELFIE = 7, _("Selfie file")

    error_fields = models.CharField(max_length=255, default="[]")
    code_id = models.IntegerField(default=0)
    answered_at = models.DateTimeField(null=True, default=None)
    created_at = models.DateTimeField(auto_now_add=True)

    def create_file(self, image, name):
        ext = image.name.split('.')[-1].lower()
        filename = f"{name}.{ext}"
        eval(f"self.{name}.save(name=filename, content=image)")

    def delete_file(self, name):
        eval(f"self.{name}.delete()")

    def delete(self, using=None, keep_parents=False):
        self.document_id_front_file.delete()
        self.save()
        self.document_id_back_file.delete()
        self.save()
        self.selfie_file.delete()
        self.save()
        return super().delete(using, keep_parents)

    def get_destination_path(self):
        return os.path.join(
            "partner",
            "request",
            "basic_info",
            str(self.partner.pk),
            str(self.pk),
        )
