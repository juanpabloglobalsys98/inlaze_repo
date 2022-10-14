from core.helpers import LanguagesCHO
from django.db import models


class CodeReason(models.Model):
    code = models.CharField(max_length=255)
    title = models.CharField(max_length=255)
    code_int = models.SmallIntegerField(default=0)

    class Type(models.TextChoices):
        PARTNER_BAN = "partner_ban",
        PARTNER_UNBAN = "partner_unban",
        ADM_BAN = "adm_ban",
        ADM_UNBAN = "adm_unban",
        PARTNER_ACT = "partner_act",
        PARTNER_DEACT = "partner_deact",
        ADM_ACT = "adm_act",
        ADM_DEACT = "adm_deact",
        PARTNER_LVL_ACCEPT = "partner_lvl_accept",
        PARTNER_LVL_REJECT = "partner_lvl_reject",
        PARTNER_LVL_CHANGE = "partner_lvl_change",
        PARTNER_BASIC_INFO_REQUEST_ACCEPT = "partner_basic_info_accept",
        PARTNER_BASIC_INFO_REQUEST_REJECT = "partner_basic_info_reject",
        PARTNER_BASIC_INFO_CHANGE = "partner_basic_info_change",
        PARTNER_BILLING_INFO_REQUEST_ACCEPT = "partner_billing_info_request_accept",
        PARTNER_BILLING_INFO_REQUEST_REJECT = "partner_billing_info_request_reject",
        PARTNER_BILLING_INFO_ADD = "partner_billing_info_add",
        PARTNER_BILLING_INFO_REMOVE = "partner_billing_info_remove",
        PARTNER_BILLING_INFO_CHANGE = "partner_billing_info_change",

    type_code = models.CharField(max_length=255)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("code_int", "type_code")

    def __str__(self):
        return f"title: {self.title} - code: {self.code}"

    def get_default_message(self):
        return self.translate_messages.filter(
            language=LanguagesCHO.ENGLISH,
        ).first()
