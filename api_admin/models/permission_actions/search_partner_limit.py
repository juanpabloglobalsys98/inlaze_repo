from api_admin.helpers import DB_ADMIN
from django.db import models
from django.db.models import Q
from django.utils.translation import gettext as _


class SearchPartnerLimit(models.Model):
    """
    """
    rol = models.ForeignKey("core.Rol", on_delete=models.CASCADE, related_name="search_limit_rol")

    codename = models.CharField(max_length=255)
    """
    Codename same to permission style to define the report limitation
    """

    class SearchType(models.IntegerChoices):
        """
        """
        ONLY_ASSIGNED = 0
        ALL = 1

    search_type = models.SmallIntegerField(choices=SearchType.choices, default=SearchType.ONLY_ASSIGNED)
    """
    List of values with same keys at get request of reports
    """

    class Meta:
        verbose_name = "Search Partner Limit"
        verbose_name_plural = "Search Partner Limits"
        unique_together = ("rol", "codename")

    def __str__(self):
        return f"{self.codename} - {self.search_type}"

    @classmethod
    def has_limit(cls, user, codename):
        query = Q(rol=user.rol) & Q(codename=codename)
        search_partner_limit = cls.objects.using(DB_ADMIN).filter(query).first()
        return (
            (
                not search_partner_limit or
                search_partner_limit.search_type == cls.SearchType.ONLY_ASSIGNED
            ) and
                not user.is_superuser
        )
