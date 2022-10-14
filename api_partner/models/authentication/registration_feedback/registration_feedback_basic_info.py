from api_admin.helpers import DB_ADMIN
from django.contrib.auth import get_user_model
from django.db import models
from django.db.models import Q
from django.utils.translation import gettext as _

User = get_user_model()


class RegistrationFeedbackBasicInfo(models.Model):
    """
    Feedback about Reject reason the Bank info of Partner by an adviser
    ### Fields
    - error_fields: `CharField`
        - List of error fields of bank account, about the fields on model 
        `Additional_info`, `Company` and `User`
    """
    partner = models.OneToOneField(to="api_partner.Partner", on_delete=models.CASCADE)
    adviser_id = models.BigIntegerField(null=True)

    @property
    def adviser(self):
        """
        Get user core model on Admin DB, this is a nullable field
        """
        # Only if adviser_id is not null get instance
        if self.adviser_id:
            filters = [Q(id=self.adviser_id)]
            return User.objects.using(DB_ADMIN).filter(*filters).first()

    error_fields = models.CharField(max_length=250)
    description = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Registration Feedback Basic Info"
        verbose_name_plural = "Registrations Feedback Basic Info"

    def __str__(self):
        return f"partner {self.partner.pk}, adviser {self.adviser_id}"
