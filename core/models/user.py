

import re

from api_admin.helpers import DB_ADMIN
from api_partner.helpers import DB_USER_PARTNER
from core.helpers import LanguagesCHO
from django.contrib.auth.models import (
    AbstractUser,
    BaseUserManager,
)
from django.db import (
    models,
    transaction,
)
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext as _
from django.utils.translation import gettext_lazy as _lazy

MAX_PASSWORD_ATTEMPS = 10


class UserManager(BaseUserManager):
    """
    User account with some personal data, must be follow Personal
    Data Protection Law, this model have some default configuration
    of django user model.
    """
    # DB name defined on DB_ADMIN from api_partner.helpers
    db_admin = "admin"
    use_in_migrations = True

    def _create_user(self, email, password, using="default", **extra_fields):
        """Create and save a User with the given email and password."""
        if not email:
            raise ValueError('The given email must be set')
        email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)
        user.set_password(password)
        user.save(using=using)
        return user

    def _create_user_without_encrypted_password(self, email, using="default", **extra_fields):
        """Create and save a User with the given email and password."""
        if not email:
            raise ValueError('The given email must be set')
        email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)
        user.save(using=using)
        return user

    def create_user_without_encrypted_password(self, email, **extra_fields):
        """
        Create and save a regular User with the given email and 
        password.
        """
        from core.models import User
        extra_fields.setdefault('is_staff', False)
        extra_fields.setdefault('is_superuser', False)
        extra_fields.setdefault('user_type', User.UserType.PARTNER)
        return self._create_user_without_encrypted_password(email, DB_USER_PARTNER, **extra_fields)

    def create_user(self, email, password=None, **extra_fields):
        """
        Create and save a regular User with the given email and 
        password.
        """
        from core.models import User
        extra_fields.setdefault('is_staff', False)
        extra_fields.setdefault('is_superuser', False)
        extra_fields.setdefault('user_type', User.UserType.PARTNER)
        return self._create_user(email, password, DB_USER_PARTNER, **extra_fields)

    def create_staffuser(self, email, password, **extra_fields):
        """
        Create and save a SuperUser with the given email and password.
        """
        from api_admin.models import Admin
        from core.models import User
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', False)
        extra_fields.setdefault('user_type', User.UserType.ADMIN)

        if extra_fields.get('is_superuser') is not False:
            raise ValueError('Staff user must have is_superuser=False.')

        user = self._create_user(email, password, DB_ADMIN, **extra_fields)

        return Admin.objects.db_manager(DB_ADMIN).create(user=user)

    def create_superuser(self, email, password, **extra_fields):
        """
        Create and save a SuperUser with the given email and password.
        """
        from api_admin.models import Admin
        from core.models import User
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        extra_fields.setdefault('user_type', User.UserType.ADMIN)

        if not extra_fields.get('is_staff'):
            raise ValueError('Superuser must have is_staff=True.')
        if not extra_fields.get('is_superuser'):
            raise ValueError('Superuser must have is_superuser=True.')

        user = self._create_user(email, password, DB_ADMIN, **extra_fields)

        return Admin.objects.db_manager(DB_ADMIN).create(user=user)


class User(AbstractUser):
    """
    Base of user models to extent on class model definition

    ### Fields

    ### Django default fields
    - first_name: `CharField`
        - First name of user, name with spaces are allowed, blank data 
        allowed      
    - last_name: `CharField`
        - Last name of user, name with spaces are allowed, blank data 
        allowed
    - password: `CharField`
        - Password saved with encrypt django methods
    - last_login: `DateTimeField`
        - last login date when user logged
    - date_joined: `DateTimeField`
        - date when the user makes a registration
    - is_staff: `BooleanField`
        - Designates whether the user can assign partners (Adviser)
    - is_superuser: `BooleanField`
        - Designates that this user has all permissions without explicitly 
        assigning them. can execute all operations on system
    - is_active: `BooleanField`
        - Designates whether this user should be treated as active. Unselect 
        this instead of deleting accounts.
    - groups: `ManyToManyField` `Group`
        - The groups this user belongs to. But on django permission system
        the group permissions will ignored

        .. groups = models.ManyToManyField(
            Group,
            verbose_name=_('groups'),
            blank=True,
            help_text=_(
                The groups this user belongs to. A user will get all permissions 
                granted to each of their groups.
            ),
            related_name="user_set",
            related_query_name="user",
            )
    - user_permissions: `ManyToManyField` `Permission`
        - Permissions that a certain user have, this is used specially for
        restric the admin users on certain endpoints

        .. user_permissions = models.ManyToManyField(
        Permission,
        verbose_name=_('user permissions'),
        blank=True,
        help_text=_('Specific permissions for this user.'),
        related_name="user_set",
        related_query_name="user",
        )
    """
    first_name = models.CharField(max_length=150, blank=True)
    second_name = models.CharField(max_length=150, blank=True)
    last_name = models.CharField(max_length=150, blank=True)
    second_last_name = models.CharField(max_length=150, blank=True)

    password = models.CharField(max_length=128)

    last_login = models.DateTimeField(blank=True, null=True)

    date_joined = models.DateTimeField(default=timezone.now)

    is_staff = models.BooleanField(default=False)
    """
    is staff status

    Designates whether the user can log into this admin site.
    """
    is_superuser = models.BooleanField(default=False)
    """
    superuser status

    Designates that this user has all permissions without explicitly assigning 
    them.
    """
    is_active = models.BooleanField(default=True)
    """
    Designates whether this user should be treated as active. Unselect this 
    instead of deleting accounts.
    """

    email = models.EmailField(unique=True)
    password_attemps = models.SmallIntegerField(default=0)
    """
    Password attemp times
    """
    is_banned = models.BooleanField(default=False)

    class UserType(models.IntegerChoices):
        """
        It defines the kind of users in the program
        """
        PARTNER = 0
        ADMIN = 1
    user_type = models.SmallIntegerField(default=UserType.PARTNER)
    """
    user type status

    Designates an user like an admin by default.
    """

    rol = models.ForeignKey("core.Rol", on_delete=models.SET_NULL, null=True, default=None, related_name="rol_to_user")
    """
    Rol defined for permission system
    """
    language = models.CharField(default=LanguagesCHO.ENGLISH, max_length=2)

    phone = models.CharField(max_length=50, null=True, default=None, unique=True)
    deactivated_at = models.DateTimeField(null=True, default=None)

    USERNAME_FIELD = 'email'
    # Required when an user is being created
    REQUIRED_FIELDS = ["password"]

    objects = UserManager()

    @transaction.atomic(using=DB_USER_PARTNER)
    def delete_partner(self, filters):
        from api_partner.models import Partner
        Partner.objects.db_manager(DB_USER_PARTNER).filter(*filters).delete()

    @transaction.atomic(using=DB_ADMIN)
    def delete_admin(self, filters):
        from api_admin.models import Admin
        if(self.user_type == self.UserType.ADMIN):
            Admin.objects.db_manager(DB_ADMIN).filter(*filters).delete()

    def delete(self, using=None, keep_parents=False):
        """
        Delete related admin into DB user (defined by routers)
        """
        filters = [Q(user_id=self.id)]
        if(self.user_type == self.UserType.PARTNER):
            self.delete_partner(filters)

        if(self.user_type == self.UserType.ADMIN):
            self.delete_admin(filters)

        return super().delete(using, keep_parents)

    def get_full_name(self):
        """
        Return the first_name plus the last_name, with a space in between.
        """
        full_name = '%s %s %s %s' % (self.first_name, self.second_name, self.last_name, self.second_last_name)
        full_name = re.sub('\s+', ' ', full_name)
        return full_name.strip()

    def get_short_name(self):
        """Return the short name for the user."""
        short_name = '%s %s ' % (self.first_name, self.last_name)
        return short_name

    def is_bloqued_by_attemps(self):
        return self.password_attemps >= MAX_PASSWORD_ATTEMPS

    def has_perm(self, codename):
        """
        User always was supplied by request for this reason was entered how 
        param
        """

        # Super user always have all permissions
        if (self.is_superuser):
            return True

        # Without rol have no permissions
        if (self.rol is None):
            return False

        return bool(self.rol.permissions.filter(Q(codename=codename)).first())
    # Fields that will not be used into current system
    username = None

    def __str__(self):
        return f"{self.id} - {self.get_full_name()} - {self.email}"
