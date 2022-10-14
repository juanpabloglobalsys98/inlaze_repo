from django.db import models


class Rol(models.Model):
    """
    """
    rol = models.CharField(max_length=100, unique=True)
    permissions = models.ManyToManyField("core.Permission", blank=True, related_name="permissions_to_rol")

    def __str__(self):
        return f"{self.rol}"
