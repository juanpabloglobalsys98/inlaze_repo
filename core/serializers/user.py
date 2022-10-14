from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models.authentication.partner import Partner
from api_partner.serializers.authentication.partner import (
    PartnersForAdvisersSerializer,
)
from core.serializers import DynamicFieldsModelSerializer
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.password_validation import (
    password_changed,
    validate_password,
)
from django.db.models import Q
from django.utils.translation import gettext as _
from rest_framework import serializers

User = get_user_model()


class UserSER(serializers.ModelSerializer):
    class Meta:
        model = User
        exclude = ("groups",)


class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        exclude = ("groups",)

    def create(self, using="default"):
        """
        Create and return a new `User` user, given the validated data.
        """
        return User.objects.db_manager(using).create_user(
            **self.validated_data)

    def create_admin(self, using="default"):
        """
        Create and return a new `User` user, given the validated data.
        """
        return User.objects.db_manager(using).create_staffuser(**self.validated_data)

    def update(self, using="default"):
        """
        Create and return a new `User` user, given the validated data.
        """
        return User.objects.db_manager(using).update(
            **self.validated_data)

    def delete(self, user_id, using="default"):
        return User.objects.db_manager(using).filter(id=user_id).delete()

    def update_or_create(self, using="default"):
        #using = validated_data.pop("using", "default")
        answer, created = User.objects.db_manager(using).update_or_create(
            **self.validated_data)
        return answer

    def get_by_id(self, id, using="default"):
        filters = [Q(id=id)]
        return User.objects.using(using).filter(*filters).first()

    def get_users(self, partners_ids, using="default"):
        return User.objects.using(using).filter(id__in=partners_ids)

    def exist(self, email, using="default"):
        """
        ¿?
        """
        filters = [Q(email=email)]
        return User.objects.using(using).filter(*filters).first()


class UserBasicSerializer(serializers.ModelSerializer):
    adviser_code = serializers.IntegerField(required=False, allow_null=True)
    password = serializers.CharField(required=False)
    email = serializers.EmailField(required=False)
    phone = serializers.CharField(required=False, allow_null=True)
    is_banned = serializers.BooleanField(required=False)
    is_active = serializers.BooleanField(required=False)

    class Meta:
        model = User
        fields = ("email", "password", "phone", "adviser_code", "is_banned", "is_active",)

    def create(self, database="default"):
        """
        Create and return a new `User` user, given the validated data.
        """
        return User.objects.db_manager(database).create_user(
            **self.validated_data)

    def create_without_encrypted_password(self, database):
        return User.objects.db_manager(database).create_user_without_encrypted_password(**self.validated_data)

    def exist(self, id, database="default"):
        return User.objects.using(database).filter(id=id).first()

    def get_by_email(self, email, using="default"):
        """
        ¿?
        """
        filters = [Q(email=email)]
        return User.objects.using(using).filter(*filters).first()

    def set_password(self, user, validated_data):
        user.set_password(validated_data.get('password'))
        validated_data["password"] = user.password

    def validate_old_password(self, request, user):
        if not user.check_password(request.data.get("old_password")):
            raise serializers.ValidationError(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "details": {"old_password": [_("Old password is not correct")]}
                })

    def delete(self, user_id, using="default"):
        return User.objects.db_manager(using).filter(id=user_id).delete()


class AdvisersWithPartnersSerializer(serializers.ModelSerializer):
    count = serializers.SerializerMethodField(method_name="get_partners")
    adviser_full_name = serializers.SerializerMethodField(method_name="get_adviser_full_name")

    class Meta:
        model = User
        fields = ("id", "count", "adviser_full_name",)

    def get_partners(self, adviser):
        partner_actives = PartnersForAdvisersSerializer().by_adviser_actives(adviser.id, DB_USER_PARTNER).count()
        return partner_actives

    def get_adviser_full_name(self, adviser):
        return adviser.first_name + " " + adviser.second_name + " " + adviser.last_name + " " + adviser.second_last_name

    def get_all_advisers(self, using="default"):
        return User.objects.db_manager(using).filter(is_staff=True).all()


class UserBasicForAdminSerializer(serializers.ModelSerializer):

    class Meta:
        model = User
        fields = [
            "first_name",
            "second_name",
            "second_last_name",
            "last_name",
            "email",
            "phone",
            "password",
            "user_type",
            "rol",
            "is_staff",
            "is_active",
        ]

    def create(self, database="defau1lt"):
        """
        Create and return a new `User` user, given the validated data.
        """
        return User.objects.db_manager(database).create_staffuser(
            **self.validated_data)

    def edit(self, database="default"):
        """
        Edit an existing user
        """
        return User.objects.db_manager(database).update(
            **self.validated_data)

    def exist(self, id, database="default"):
        return User.objects.using(database).filter(id=id).first()

    def get_by_email(self, email, using="default"):
        """
        ¿?
        """
        filters = [Q(email=email)]
        return User.objects.using(using).filter(*filters).first()

    def delete(self, user_id, using="default"):
        return User.objects.db_manager(using).filter(id=user_id).delete()


class UserRequiredInfoSerializer(serializers.ModelSerializer):
    email = serializers.CharField(required=False, allow_null=True)
    second_name = serializers.CharField(required=False, allow_blank=True)
    second_last_name = serializers.CharField(required=False, allow_blank=True)
    phone = serializers.CharField(required=False, allow_null=True)
    prefix = serializers.CharField(required=False, allow_null=True)
    is_enterprise = serializers.SerializerMethodField("get_is_enterprise", required=False, allow_null=True)
    last_login = serializers.CharField(required=False, allow_null=True)

    def get_is_enterprise(self, user):
        return Partner.objects.db_manager("default").filter(
            user_id=user.id).first().is_enterprise

    class Meta:
        model = User
        fields = (
            "first_name",
            "second_name",
            "last_name",
            "second_last_name",
            "phone",
            "prefix",
            "is_enterprise",
            "email",
            "is_active",
            "last_login",
        )

    def create(self, database="default"):
        """
        Create and return a new `User` user, given the validated data.
        """
        return User.objects.db_manager(database).create_user(
            **self.validated_data)

    def exist(self, id, database="default"):
        return User.objects.db_manager(database).filter(id=id).first()

    def get_by_email(self, email, database="default"):
        return User.objects.db_manager(database).filter(email=email).first()


class UsersForAdvisersSerializer(serializers.ModelSerializer):
    user_id = serializers.IntegerField()
    is_ban = serializers.CharField()
    was_linked = serializers.BooleanField()
    adviser_full_name = serializers.CharField()
    basic_info_status = serializers.IntegerField()
    bank_status = serializers.IntegerField()
    documents_status = serializers.IntegerField()

    class Meta:
        model = User
        fields = ("user_id", "was_linked", "is_ban",
                  "email", "phone", "adviser_full_name",
                  "basic_info_status", "bank_status", "documents_status"
                  )

    def by_email(self, email, database="default"):
        return User.objects.db_manager(database).filter(email=email).first()

    def get_all(self, order_by, database="default"):
        return User.objects.db_manager(database).all().order_by(order_by)

    def exist(self, id, database="default"):
        return User.objects.db_manager(database).filter(id=id).first()


class UserPasswordSerializer(serializers.ModelSerializer):

    class Meta:
        model = User
        fields = "__all__"

    def validate_old_password(self, user, value):
        if not user.check_password(value):
            raise serializers.ValidationError({
                "error": settings.BAD_REQUEST_CODE,
                "detail": {"old_password": [_("Old password is not correct")]}
            })
        return value

    def password_matches(self, password, user):
        return user.check_password(password)

    def validate_password(self, data, password):

        email = data.get("email")
        if email == password.lower():
            raise serializers.ValidationError({
                "error": settings.BAD_REQUEST_CODE,
                "detail": {"password": [_("The password cannot be equal to email")]}
            })

        try:
            validate_password(password)
        except Exception as e:
            raise serializers.ValidationError({
                "error": settings.BAD_REQUEST_CODE,
                "detail": {"password": [str(e)]}
            })

    def update(self, new_password, user):
        try:
            validate_password(new_password, user=user)
            user.set_password(new_password)
            user.save()
            password_changed(new_password, user=user)
        except Exception as e:
            raise serializers.ValidationError(
                {
                    "error": settings.BAD_REQUEST_CODE,
                    "detail": {
                        "new_password": [
                            "\n".join(e)
                        ],
                    },
                },
            )

    def exist(self, email, using="default"):
        """
        ¿?
        """
        filters = [Q(email=email)]
        return User.objects.using(using).filter(*filters).first()
