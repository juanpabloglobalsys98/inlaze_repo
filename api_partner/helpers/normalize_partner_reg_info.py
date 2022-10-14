from django.utils.translation import gettext as _
import re


class NormalizePartnerRegInfo():
    """
    """

    def normalize_init_info(self, data):
        email = data.get("email")
        if email:
            data["email"] = email.lower()

    def normalize_additinal_info(self, data):
        complete_name = data.get("first_name")
        if complete_name.count(" ") == 1:
            data["first_name"], data["second_name"] = complete_name.split(" ")
        elif complete_name.count(" ") > 1 or not complete_name.count(" "):
            data["first_name"] = complete_name
            data["second_name"] = ""

        complete_last_name = data.get("last_name")
        if complete_last_name.count(" ") == 1:
            data["last_name"], data["second_last_name"] = complete_last_name.split(" ")
        elif complete_last_name.count(" ") > 1 or not complete_last_name.count(" "):
            data["last_name"] = complete_last_name
            data["second_last_name"] = ""

    def normalize_bank_info(self, data):
        bank_name = data.get("bank_name")
        if bank_name:
            data["bank_name"] = re.sub(' +', ' ', bank_name)
