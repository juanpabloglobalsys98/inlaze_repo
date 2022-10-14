import os
from datetime import datetime

from django.db import models
from django.core.files.storage import FileSystemStorage


class Bookmaker(models.Model):
    def file_upload_image(instance, filename):
        ext = filename.split('.')[-1].lower()
        filename = "%s.%s" % (
            f"{instance.name.replace(' ', '_').lower()}-{datetime.now()}",
            ext
        )
        return os.path.join("bookmaker", filename)

    name = models.CharField('Bookmaker name', max_length=100, unique=True)
    image = models.FileField(upload_to=file_upload_image, storage=FileSystemStorage)

    def file_create_image(self, image, bookmaker_name):
        ext = image.name.split('.')[-1].lower()
        filename = "%s.%s" % (
            f"{bookmaker_name.replace(' ', '_').lower()}",
            ext
        )
        self.image.save(name=filename, content=image)

    def file_update_image(self, image, bookmaker_name):
        ext = image.name.split('.')[-1].lower()
        filename = "%s.%s" % (
            f"{bookmaker_name.replace(' ', '_').lower()}",
            ext
        )
        self.image.save(name=filename, content=image)

    def file_delete_image(self):
        self.image.delete()

    def delete(self, using=None, keep_parents=False):
        self.image.delete()
        self.save()
        return super().delete(using, keep_parents)

    class Meta:
        verbose_name = "Bookmaker"
        verbose_name_plural = "Bookmakers"

    def __str__(self):
        return f"Bookmaker name: {self.name}"
