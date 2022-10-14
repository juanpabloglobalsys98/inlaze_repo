from django.core.exceptions import ValidationError
from django.utils.translation import gettext as _
import re


class PasswordRequirementsValidator:

    def __init__(self, regular_expression="^(?=.*[a-z])(?=.*[A-Z])(?=.*[@$!%*?&\.#_])[A-Za-z\d@$!%*?&\.#_]{8,}$"):
        self.regular_expression = regular_expression

    def validate(self, password, user=None):
        if not re.search(self.regular_expression, password):
            raise ValidationError(
                _("The password must meet the next requirements:8 characters, at least 1 uppercase letter, at least one special character, at least 1 alphanumeric characters."),
                code='invalid_password',
                params={'regular_expression': self.regular_expression},
            )

    def get_help_text(self):
        return _("The password must meet the next requirements:8 characters, at least 1 uppercase letter, at least one special character, at least 1 alphanumeric characters.")
