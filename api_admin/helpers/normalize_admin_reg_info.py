from django.utils.translation import gettext as _
import re


class NormalizeAdminRegInfo():
    """
    """

    def normalize_admin_info(self, data):
        email = data.get("email")
        if email:
            data["email"] = email.lower()

        complete_name = data.get("first_name")
        if complete_name:
            complete_name = re.sub(' +', ' ', complete_name)
            complete_name = complete_name.title()
            if complete_name.count(" ") == 1:
                data["first_name"], data["second_name"] = complete_name.split(" ")
            elif complete_name.count(" ") > 1 or not complete_name.count(" "):
                data["first_name"] = complete_name
                data["second_name"] = ""

        complete_last_name = data.get("last_name")
        if complete_last_name:
            complete_last_name = re.sub(' +', ' ', complete_last_name)
            complete_last_name = complete_last_name.title()
            if complete_last_name.count(" ") == 1:
                data["last_name"], data["second_last_name"] = complete_last_name.split(" ")
            elif complete_last_name.count(" ") > 1 or not complete_last_name.count(" "):
                data["last_name"] = complete_last_name
                data["second_last_name"] = ""

        address = data.get("address")
        if address:
            data["address"] = re.sub(' +', ' ', address)
