import os

from django.core.files.storage import FileSystemStorage
from django.db import models


class OwnCompany(models.Model):
    """
    """
    def file_upload(instance, filename):
        ext = filename.split('.')[-1].lower()
        filename = f"company-{instance.id}.{ext}"
        return os.path.join("owncompany", filename)

    logo = models.FileField(upload_to=file_upload, storage=FileSystemStorage, null=True, default=None)
    name = models.CharField(max_length=255, null=True, default=None)
    nit = models.CharField(max_length=255, null=True, default=None)
    city = models.CharField(max_length=255, null=True, default=None)
    address = models.CharField(max_length=255, null=True, default=None)
    phone = models.CharField(max_length=50, null=True, default=None)
    created_at = models.DateTimeField(auto_now_add=True)

    def delete_file(self):
        self.logo.delete()

    def save_file(self, image):
        ext = image.name.split('.')[-1].lower()
        filename = f"company-{self.id}.{ext}"
        self.logo.save(name=filename, content=image)

    def delete(self, using=None, keep_parents=False):
        """
        """
        self.logo.delete()
        self.save()
        return super().delete(using, keep_parents)

    class Meta:
        verbose_name = "Own company"
        verbose_name_plural = "Own companies"

    def __str__(self):
        return f"Company name: {self.name} - nit {self.nit}"
