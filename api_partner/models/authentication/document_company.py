import os

from core.helpers import S3StandardIA
from django.db import models
from django.utils.translation import gettext as _


class DocumentCompany(models.Model):
    """
    """
    def file_upload_rut(instance, filename):
        ext = filename.split('.')[-1].lower()
        filename = f"rut.{ext}"
        return os.path.join("partner", "documents", str(instance.company_id), filename)

    def file_upload_exist_legal_repr(instance, filename):
        ext = filename.split('.')[-1].lower()
        filename = f"exist_legal_repr.{ext}"
        return os.path.join("partner", "documents", str(instance.company_id), filename)

    company = models.OneToOneField(
        "api_partner.Company",
        on_delete=models.CASCADE,
        primary_key=True,
        related_name="documents_company"
    )

    rut_file = models.FileField(upload_to=file_upload_rut, storage=S3StandardIA, null=True, default=None)
    exist_legal_repr_file = models.FileField(
        upload_to=file_upload_exist_legal_repr, storage=S3StandardIA, null=True, default=None)

    updated_at = models.DateTimeField(auto_now=True)

    def create_rut_file(self, file):
        ext = file.name.split('.')[-1].lower()
        filename = f"rut.{ext}"
        self.rut_file.save(name=filename, content=file)

    def create_exist_legal_repr_file(self, file):
        ext = file.name.split('.')[-1].lower()
        filename = f"exist_legal_repr_file.{ext}"
        self.exist_legal_repr_file.save(name=filename, content=file)

    def update_rut_file(self, file):
        ext = file.name.split('.')[-1].lower()
        filename = f"rut.{ext}"
        self.rut_file.save(name=filename, content=file)

    def update_exist_legal_repr_file(self, file):
        ext = file.name.split('.')[-1].lower()
        filename = f"exist_legal_repr_file.{ext}"
        self.exist_legal_repr_file.save(name=filename, content=file)

    def delete_rut_file(self):
        self.rut_file.delete()

    def delete_exist_legal_repr_file(self):
        self.exist_legal_repr_file.delete()

    def delete(self, using=None, keep_parents=False):
        """
        """
        self.rut_file.delete()
        self.save()
        self.exist_legal_repr_file.delete()
        self.save()
        return super().delete(using, keep_parents)

    class Meta:
        verbose_name = "Document company"
        verbose_name_plural = "Document companies"

    def __str__(self):
        return f"{self.company}"
