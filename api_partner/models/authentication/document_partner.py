import os
import copy
from core.helpers import (
    S3StandardIA,
    copy_s3_file,
)
from django.db import models
from django.utils.translation import gettext as _


class DocumentPartner(models.Model):
    """
    """
    def file_upload_bank_certification_file(instance, filename):
        ext = filename.split('.')[-1].lower()
        filename = f"bank_certification.{ext}"
        return os.path.join("partner", "documents", str(instance.partner_id), filename)

    def file_upload_identification_file(instance, filename):
        return os.path.join(
            "partner",
            "profile",
            "basic_info",
            str(instance.partner_id),
            filename,
        )

    def file_upload_document_id_front_file(instance, filename):
        return os.path.join(
            "partner",
            "profile",
            "basic_info",
            str(instance.partner_id),
            filename,
        )

    def file_upload_document_id_back_file(instance, filename):
        return os.path.join(
            "partner",
            "profile",
            "basic_info",
            str(instance.partner_id),
            filename,
        )

    def file_upload_selfie_file(instance, filename):
        return os.path.join(
            "partner",
            "profile",
            "basic_info",
            str(instance.partner_id),
            filename,
        )

    partner = models.OneToOneField(
        "api_partner.Partner",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="documents_partner"
    )

    bank_certification_file = models.FileField(
        upload_to=file_upload_bank_certification_file, storage=S3StandardIA, null=True, default=None)
    identification_file = models.FileField(upload_to=file_upload_identification_file,
                                           storage=S3StandardIA, null=True, default=None)

    document_id_front_file = models.FileField(
        upload_to=file_upload_identification_file,
        storage=S3StandardIA,
        null=True,
        default=None,
    )
    document_id_back_file = models.FileField(
        upload_to=file_upload_identification_file,
        storage=S3StandardIA,
        null=True,
        default=None,
    )
    selfie_file = models.FileField(
        upload_to=file_upload_identification_file,
        storage=S3StandardIA,
        null=True,
        default=None,
    )
    updated_at = models.DateTimeField(auto_now=True)

    def create_bank_certification_file(self, image):
        ext = image.name.split('.')[-1].lower()
        filename = f"bank_certification.{ext}"
        self.bank_certification_file.save(name=filename, content=image)

    def create_identification_file(self, image):
        ext = image.name.split('.')[-1].lower()
        filename = f"identification.{ext}"
        self.identification_file.save(name=filename, content=image)

    def create_document_id_front_file(self, image):
        ext = image.name.split('.')[-1].lower()
        filename = f"front.{ext}"
        self.document_id_front_file.save(name=filename, content=image)

    def create_document_id_back_file(self, image):
        ext = image.name.split('.')[-1].lower()
        filename = f"back.{ext}"
        self.document_id_back_file.save(name=filename, content=image)

    def create_selfie_file(self, image):
        ext = image.name.split('.')[-1].lower()
        filename = f"selfie.{ext}"
        self.selfie_file.save(name=filename, content=image)

    def update_bank_certification_file(self, image):
        ext = image.name.split('.')[-1].lower()
        filename = f"bank_certification.{ext}"
        self.bank_certification_file.save(name=filename, content=image)

    def update_identification_file(self, image):
        ext = image.name.split('.')[-1].lower()
        filename = f"identification.{ext}"
        self.identification_file.save(name=filename, content=image)

    def update_document_id_front_file(self, image):
        self.document_id_front_file.delete()
        ext = image.name.split('.')[-1].lower()
        filename = f"front.{ext}"
        self.document_id_front_file.save(name=filename, content=image)

    def update_document_id_back_file(self, image):
        self.document_id_back_file.delete()
        ext = image.name.split('.')[-1].lower()
        filename = f"back.{ext}"
        self.document_id_back_file.save(name=filename, content=image)

    def update_selfie_file(self, image):
        self.selfie_file.delete()
        ext = image.name.split('.')[-1].lower()
        filename = f"selfie.{ext}"
        self.selfie_file.save(name=filename, content=image)

    def delete_identification_file(self):
        self.identification_file.delete()

    def delete_bank_certification_file(self):
        self.bank_certification_file.delete()

    def delete_document_id_front_file(self):
        self.bank_certification_file.delete()

    def delete_document_id_back_file(self):
        self.bank_certification_file.delete()

    def delete_selfie_file(self):
        self.selfie_file.delete()

    def delete(self, using=None, keep_parents=False):
        self.bank_certification_file.delete()
        self.save()
        self.identification_file.delete()
        self.save()
        self.document_id_front_file.delete()
        self.save()
        self.document_id_back_file.delete()
        self.save()
        self.selfie_file.delete()
        self.save()
        return super().delete(using, keep_parents)

    class Meta:
        verbose_name = "Document partner"
        verbose_name_plural = "Document partners"

    def __str__(self):
        return f"{self.partner.user.get_full_name()}'s documents"

    def get_destination_path(self):
        return os.path.join(
            "partner",
            "profile",
            "basic_info",
            str(self.pk),
        )

    def copy_document_files(self, source_obj):
        to_path = self.get_destination_path()
        self.document_id_front_file = copy_s3_file(
            source_file=source_obj.document_id_front_file,
            to_path=to_path,
        )
        self.document_id_back_file = copy_s3_file(
            source_file=source_obj.document_id_back_file,
            to_path=to_path,
        )
        self.selfie_file = copy_s3_file(
            source_file=source_obj.selfie_file,
            to_path=to_path,
        )
