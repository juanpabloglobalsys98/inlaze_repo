from api_admin.helpers import DB_ADMIN
from api_partner.helpers import DB_USER_PARTNER
from django.contrib.auth import get_user_model
from django.db import (
    models,
    transaction,
)
from django.db.models import Q
from django.utils.translation import gettext as _

User = get_user_model()


class Admin(models.Model):
    """
    """
    user = models.OneToOneField(User, on_delete=models.DO_NOTHING, primary_key=True)

    @property
    def _user(self):
        """
        """
        filters = [Q(id=self.user_id)]
        return User.objects.using(DB_ADMIN).filter(*filters).first()

    def delete(self, using=None, keep_parents=False):
        """
        Delete related admin into DB user (defined by routers)
        """
        from api_partner.models import (
            Partner,
            RegistrationFeedbackBank,
            RegistrationFeedbackBasicInfo,
            RegistrationFeedbackDocuments,
        )

        with transaction.atomic(using=DB_USER_PARTNER):
            filters = [Q(adviser_id=self.user_id)]
            Partner.objects.using(DB_USER_PARTNER).filter(*filters).update(adviser_id=None)

            # Same to previous filters
            RegistrationFeedbackBasicInfo.objects.using(DB_USER_PARTNER).filter(*filters).update(adviser_id=None)
            RegistrationFeedbackBank.objects.using(DB_USER_PARTNER).filter(*filters).update(adviser_id=None)
            RegistrationFeedbackDocuments.objects.using(DB_USER_PARTNER).filter(*filters).update(adviser_id=None)

            with transaction.atomic(using=DB_ADMIN):
                return super().delete(using, keep_parents)

    class Meta:
        verbose_name = "Admin"
        verbose_name_plural = "Admins"

    def __str__(self):
        return f"{self.user.id} - {self.user.get_full_name()} - {self.user.email}"
