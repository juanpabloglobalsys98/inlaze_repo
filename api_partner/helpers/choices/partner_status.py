from django.db import models
from django.utils.translation import gettext as _


class PartnerStatusCHO(models.IntegerChoices):
    REQUESTED = 0, _("Requested")
    ACCEPTED = 1, _("Accepted")
    REJECTED = 2, _("Rejected")

    @classmethod
    def allowed_status_request(self):
        """
        Get enum values only for create validation
        """
        values = list(self.values)
        values.remove(self.REQUESTED)
        return values
