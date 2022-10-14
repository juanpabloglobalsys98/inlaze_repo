from django.db import models
from django.utils.translation import gettext as _


class ReportVisualization(models.Model):
    """
    """
    rol = models.ForeignKey("core.Rol", on_delete=models.CASCADE, related_name="report_visualization_rol")

    permission = models.ForeignKey("core.Permission", on_delete=models.CASCADE, null=True, default=None)
    """
    Codename same to permission style to define the report limitation
    """

    values_can_view = models.CharField(max_length=1700, default="[]")
    """
    List of values with same keys at get request of reports
    """

    class Meta:
        verbose_name = "Report Visualization"
        verbose_name_plural = "Report Visualizations"
        unique_together = ("rol", "permission")

    def __str__(self):
        return f"{self.rol.pk} - {self.rol.rol}"
