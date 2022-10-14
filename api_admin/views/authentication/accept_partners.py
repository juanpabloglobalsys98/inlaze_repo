import logging
import sys
import traceback

from api_admin.helpers.routers_db import DB_ADMIN
from api_partner.helpers.routers_db import DB_USER_PARTNER
from api_partner.models.authentication.partner import Partner
from api_partner.serializers.authentication import PartnerStatusSER
from cerberus import Validator
from core.helpers import (
    EmailThread,
    HavePermissionBasedView,
    request_cfg,
)
from core.models import User
from core.serializers.user import UserBasicSerializer
from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone
from django.utils.translation import gettext as _
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from sendgrid import SendGridAPIClient

logger = logging.getLogger(__name__)


def _email_accept(partner_user, adviser_full_name, adviser_email, adviser_phone, partner_full_name):
    """
    Email when user has changed the state to `FULL_REGISTERED` or `FULL_REGISTERED_SKIPPED_ACCEPTED`
    """
    EmailThread(
        html="full_registered_alert.html",
        email=partner_user.email,
        subject=_("[log_up_completed] Account ready to operate"),
        data={
            "TITLE": _("Welcome to Inlazz"),
            "CUSTOMER_SERVICE_PART_1": _("Welcome to Inlazz"),
            "CUSTOMER_SERVICE_PART_2": _("We welcome you to Inlazz. your partner account it is ready, you can Access to the platform through the following link"),
            "CUSTOMER_SERVICE_PART_3": _("Login"),
            "CUSTOMER_SERVICE_PART_4": _("We are sending this email to keep you informed about your Inlazz account"),
            "DATE": "2022",
        }
    ).start()


def _email_accept_skipped(partner_user, adviser_full_name, adviser_email, adviser_phone, partner_full_name):
    EmailThread(
        html="full_registered_alert.html",
        email=partner_user.email,
        subject=_("[log_up_completed] Account ready to operate"),
        data={
            "TITLE": _("Welcome to Inlazz"),
            "CUSTOMER_SERVICE_PART_1": _("Welcome to Inlazz"),
            "CUSTOMER_SERVICE_PART_2": _("We welcome you to Inlazz. your partner account it is ready, you can Access to the platform through the following link"),
            "CUSTOMER_SERVICE_PART_3": _("Login"),
            "CUSTOMER_SERVICE_PART_4": _("We are sending this email to keep you informed about your Inlazz account"),
            "DATE": "2022",
        }
    ).start()


def _add_contact(user):
    if not settings.SENDGRID_API_KEY:
        logger.error(f"SENDGRID_API_KEY is none")
        return None

    sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
    is_notify_campaign = int(user.partner.is_notify_campaign)
    is_notify_notice = int(user.partner.is_notify_notice)
    data = {
        "contacts": [
            {
                "first_name": user.first_name,
                "last_name": user.last_name,
                "email": user.email,
                "city":  user.partner.additionalinfo.city,
                "country": user.partner.additionalinfo.country,
                "phone_number": user.phone,
                "custom_fields": {
                    settings.SENDGRID_CUSTOM_FIELD_CAMPAIGN: is_notify_campaign,
                    settings.SENDGRID_CUSTOM_FIELD_NOTICE: is_notify_notice,
                },
            },
        ]
    }
    try:
        response = sg.client.marketing.contacts.put(
            request_body=data
        )
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        e = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.critical("".join(e))
        return e


class AcceptPartnerPhase2AAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView,
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an adviser annotate partner basic info as accepted in the system
        """
        request_cfg.is_partner = True
        validator = Validator(
            schema={
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "rename": "partner",
                },
            },
        )

        if not validator.validate(
            document=request.data,
            normalize=False,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.normalized(document=request.data,)
        filters = (
            Q(user=validator.document.get("partner")),
        )
        partner = Partner.objects.db_manager(DB_USER_PARTNER).filter(*filters).first()
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Prevent execution if partner not have status to_be_verified
        if (
            not partner.status in (
                Partner.Status.TO_BE_VERIFIED,
                Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED,
            )
        ):
            Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner not have the expected state TO BE VERIFIED"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Prevent execution if partner already accepted
        if (
            partner.basic_info_status in (
                Partner.ValidationStatus.ACCEPTED,
                Partner.ValidationStatus.SKIPPED_ACCEPTED,
            )
        ):
            Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner basic info is alredy accepted"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        old_partner_status = partner.status

        # Set accept state according to previous state
        # Case first registration process
        if (old_partner_status == Partner.Status.TO_BE_VERIFIED):
            validator.document["basic_info_status"] = Partner.ValidationStatus.ACCEPTED
            # Temp setup document status for re-calculate account state
            partner.basic_info_status = Partner.ValidationStatus.ACCEPTED
        # Case when user has skipped
        elif (old_partner_status == Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED):
            validator.document["basic_info_status"] = Partner.ValidationStatus.SKIPPED_ACCEPTED
            # Temp setup document status for re-calculate account state
            partner.basic_info_status = Partner.ValidationStatus.SKIPPED_ACCEPTED

        # Get new partner account status with incoming status change
        validator.document["status"] = partner.get_status()
        validator.document["full_registered_at"] = None

        # Verify if new account status is full registered with skip or not or
        # full accepted after that reached update full_registered_at
        if (
            validator.document.get("status") in (
                Partner.Status.FULL_REGISTERED,
                Partner.Status.FULL_REGISTERED_SKIPPED_ACCEPTED,
                Partner.Status.FULL_REGISTERED_SKIPPED,
            )
        ):
            validator.document["full_registered_at"] = timezone.now()

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        serialized_partner = PartnerStatusSER(instance=partner, data=validator.document,)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Are not alredy full registered with skip or not prevent continue execution
        if validator.document.get("full_registered_at") is None:
            transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
            return Response(
                status=status.HTTP_200_OK,
            )

        partner_user = partner.user
        # adviser = UserBasicSerializer.exist(None, partner.adviser_id, DB_ADMIN)
        filters = (
            Q(id=partner.adviser_id),
        )
        adviser = User.objects.using(DB_ADMIN).filter(*filters).first()
        partner_full_name = partner_user.get_full_name()
        if adviser:
            adviser_full_name = adviser.get_full_name()
            adviser_email = adviser.email
            adviser_phone = adviser.phone
        else:
            adviser_full_name = ""
            adviser_email = ""
            adviser_phone = ""

        # sending emails
        try:
            # Normal case when user full registered with all information
            if (
                validator.document.get("status") in (
                    Partner.Status.FULL_REGISTERED,
                    Partner.Status.FULL_REGISTERED_SKIPPED_ACCEPTED,
                )
            ):
                _email_accept(
                    partner_user=partner_user,
                    adviser_full_name=adviser_full_name,
                    adviser_email=adviser_email,
                    adviser_phone=adviser_phone,
                    partner_full_name=partner_full_name,
                )
                # Add contact into sengrid
                _add_contact(partner_user)
            # Other case
            else:
                _email_accept_skipped(
                    partner_user=partner_user,
                    adviser_full_name=adviser_full_name,
                    adviser_email=adviser_email,
                    adviser_phone=adviser_phone,
                    partner_full_name=partner_full_name,
                )
                # Add contact into sengrid
                _add_contact(partner_user)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                data={
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {
                        "non_field_errors": [
                            _("Error at send email"),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(
            status=status.HTTP_200_OK,
        )


class AcceptPartnerPhase2BAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an adviser annotate partner bank info as accepted in the system
        """

        request_cfg.is_partner = True
        validator = Validator(
            schema={
                "user_id": {
                    "required": True,
                    "type": "integer",
                    "rename": "partner",
                },
                "is_skip": {
                    "required": False,
                    "type": "boolean",
                    "default": False,
                },
            },
        )

        if not validator.validate(
            document=request.data,
            normalize=False,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.normalized(
            document=request.data,
        )
        filters = (
            Q(user=validator.document.get("partner")),
        )
        partner = Partner.objects.db_manager(DB_USER_PARTNER).filter(*filters).first()

        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Prevent execution if partner not have status to_be_verified
        if (
            not partner.status in (
                Partner.Status.TO_BE_VERIFIED,
                Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED,
            )
        ):
            Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner not have the expected state TO BE VERIFIED"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Prevent execution if partner already accepted
        if (
            partner.bank_status in (
                Partner.ValidationStatus.ACCEPTED,
                Partner.ValidationStatus.SKIPPED_ACCEPTED,
            )
        ):
            Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner bank is alredy accepted"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get partner account status before to change temporaly object status
        old_partner_status = partner.status

        # Case first registration process
        if (old_partner_status == Partner.Status.TO_BE_VERIFIED):
            # Status validation file according to is_skip value
            if (validator.document.get("is_skip")):
                new_validation_status = Partner.ValidationStatus.SKIPPED
            else:
                new_validation_status = Partner.ValidationStatus.ACCEPTED
        # Case when user has skipped
        elif (old_partner_status == Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED):
            new_validation_status = Partner.ValidationStatus.SKIPPED_ACCEPTED

        validator.document["bank_status"] = new_validation_status
        partner.bank_status = new_validation_status
        validator.document["status"] = partner.get_status()
        validator.document["full_registered_at"] = None

        # Verify if new account status is full registered with skip or not or
        # full accepted after that reached update full_registered_at
        if (
            validator.document.get("status") in (
                Partner.Status.FULL_REGISTERED,
                Partner.Status.FULL_REGISTERED_SKIPPED_ACCEPTED,
                Partner.Status.FULL_REGISTERED_SKIPPED,
            )
        ):
            validator.document["full_registered_at"] = timezone.now()

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        serialized_partner = PartnerStatusSER(
            instance=partner,
            data=validator.document,
        )
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Are not alredy full registered with skip or not prevent continue execution
        if validator.document.get("full_registered_at") is None:
            transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
            return Response(
                status=status.HTTP_200_OK,
            )

        partner_user = partner.user
        # adviser = UserBasicSerializer.exist(None, partner.adviser_id, DB_ADMIN)
        filters = (
            Q(id=partner.adviser_id),
        )
        adviser = User.objects.using(DB_ADMIN).filter(*filters).first()
        partner_full_name = partner_user.get_full_name()
        if adviser:
            adviser_full_name = adviser.get_full_name()
            adviser_email = adviser.email
            adviser_phone = adviser.phone
        else:
            adviser_full_name = ""
            adviser_email = ""
            adviser_phone = ""

        # sending emails
        try:
            # Normal case when user full registered with all information
            if (
                validator.document.get("status") in (
                    Partner.Status.FULL_REGISTERED,
                    Partner.Status.FULL_REGISTERED_SKIPPED_ACCEPTED,
                )
            ):
                _email_accept(
                    partner_user=partner_user,
                    adviser_full_name=adviser_full_name,
                    adviser_email=adviser_email,
                    adviser_phone=adviser_phone,
                    partner_full_name=partner_full_name,
                )

                # Add contact to sengrid
                _add_contact(partner_user)

            # Other case
            else:
                _email_accept_skipped(
                    partner_user=partner_user,
                    adviser_full_name=adviser_full_name,
                    adviser_email=adviser_email,
                    adviser_phone=adviser_phone,
                    partner_full_name=partner_full_name,
                )
                # Add contact to sengrid
                _add_contact(partner_user)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                {
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {
                        "non_field_errors": [
                            _("Error at send email"),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
        return Response(
            status=status.HTTP_200_OK,
        )


class AcceptPartnerPhase2CAPI(APIView):

    permission_classes = (
        IsAuthenticated,
        HavePermissionBasedView
    )

    @transaction.atomic(using=DB_USER_PARTNER, savepoint=True)
    def patch(self, request):
        """
        Lets an adviser annotate partner documents as accepted in the system
        """
        request_cfg.is_partner = True
        validator = Validator(
            schema={
                'user_id': {
                    'required': True,
                    'type': 'integer',
                    'rename': 'partner',
                },
                "is_skip": {
                    "required": False,
                    "type": "boolean",
                    "default": False,
                },
            },
        )

        if not validator.validate(
            document=request.data,
            normalize=False,
        ):
            return Response(
                data={
                    "error": settings.CERBERUS_ERROR_CODE,
                    "details": validator.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        validator.normalized(
            document=request.data
        )

        partner = PartnerStatusSER().exist(validator.document.get("partner"), DB_USER_PARTNER)
        if not partner:
            return Response(
                data={
                    "error": settings.NOT_FOUND_CODE,
                    "details": {
                        "user_id": [
                            _("There is not such partner in the system"),
                        ],
                    },
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        # Prevent execution if partner not have status to_be_verified
        if partner.status != Partner.Status.TO_BE_VERIFIED:
            Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner not have the expected state TO BE VERIFIED"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Prevent execution if partner already accepted
        if (
            partner.documents_status in (
                Partner.ValidationStatus.ACCEPTED,
                Partner.ValidationStatus.SKIPPED_ACCEPTED,
            )
        ):
            Response(
                data={
                    "error": settings.ILOGICAL_ACTION,
                    "details": {
                        "user_id": [
                            _("Partner documents is alredy accepted"),
                        ],
                    },
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Get partner account status before to change temporaly object status
        old_partner_status = partner.status

        # Case first registration process
        if (old_partner_status == Partner.Status.TO_BE_VERIFIED):
            # Status validation file according to is_skip value
            if (validator.document.get("is_skip")):
                new_validation_status = Partner.ValidationStatus.SKIPPED
            else:
                new_validation_status = Partner.ValidationStatus.ACCEPTED
        # Case when user has skipped
        elif (old_partner_status == Partner.Status.FULL_REGISTERED_SKIPPED_TO_BE_VERIFIED):
            new_validation_status = Partner.ValidationStatus.SKIPPED_ACCEPTED

        validator.document["documents_status"] = new_validation_status
        partner.documents_status = new_validation_status
        validator.document["status"] = partner.get_status()
        validator.document["full_registered_at"] = None

        # Verify if is fullregistered reached update full_registered_at
        if (
            validator.document.get("status") in (
                Partner.Status.FULL_REGISTERED,
                Partner.Status.FULL_REGISTERED_SKIPPED_ACCEPTED,
                Partner.Status.FULL_REGISTERED_SKIPPED,
            )
        ):
            validator.document["full_registered_at"] = timezone.now()

        sid = transaction.savepoint(using=DB_USER_PARTNER)
        serialized_partner = PartnerStatusSER(instance=partner, data=validator.document)
        if serialized_partner.is_valid():
            serialized_partner.save()
        else:
            transaction.savepoint_rollback(sid=sid, using=DB_USER_PARTNER)
            return Response(
                data={
                    "error": settings.SERIALIZER_ERROR_CODE,
                    "details": serialized_partner.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Are not alredy full registered with skip or not prevent continue execution
        if validator.document.get("full_registered_at") is None:
            transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)
            return Response(
                status=status.HTTP_200_OK,
            )

        partner_user = partner.user
        adviser = UserBasicSerializer.exist(None, partner.adviser_id, DB_ADMIN)
        partner_full_name = partner_user.get_full_name()
        if adviser:
            adviser_full_name = adviser.get_full_name()
            adviser_email = adviser.email
            adviser_phone = adviser.phone
        else:
            adviser_full_name = ""
            adviser_email = ""
            adviser_phone = ""

        # sending emails
        try:
            # Normal case when user full registered with all information
            if (
                validator.document.get("status") in (
                    Partner.Status.FULL_REGISTERED,
                    Partner.Status.FULL_REGISTERED_SKIPPED_ACCEPTED,
                )
            ):
                _email_accept(
                    partner_user=partner_user,
                    adviser_full_name=adviser_full_name,
                    adviser_email=adviser_email,
                    adviser_phone=adviser_phone,
                    partner_full_name=partner_full_name,
                )

                # Add contact to sengrid
                _add_contact(partner_user)
            else:
                _email_accept_skipped(
                    partner_user=partner_user,
                    adviser_full_name=adviser_full_name,
                    adviser_email=adviser_email,
                    adviser_phone=adviser_phone,
                    partner_full_name=partner_full_name,
                )
                # Add contact to sengrid
                _add_contact(partner_user)
        except Exception as e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            e = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.critical("".join(e))
            return Response(
                {
                    "error": settings.ERROR_SENDING_EMAIL,
                    "details": {
                        "non_field_errors": [
                            _("Error at send email"),
                        ],
                    },
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        transaction.savepoint_commit(sid=sid, using=DB_USER_PARTNER)

        return Response(
            status=status.HTTP_200_OK,
        )
