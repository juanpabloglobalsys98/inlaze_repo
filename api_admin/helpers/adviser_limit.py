from django.db.models import (
    Case,
    F,
    Q,
    Value,
    Sum,
)

import ast


def report_visualization_limit(
    admin,
    permission_codename,
):
    from api_admin.models import (
        ReportVisualization,
    )
    filters = (
        Q(rol_id=admin.rol_id),
        Q(permission__codename=permission_codename),
    )
    report_visualization = ReportVisualization.objects.filter(*filters).first()

    if report_visualization is None:
        return None

    return ast.literal_eval(report_visualization.values_can_view)
