from django.db import models


class ValidationCodeType(models.IntegerChoices):
    """
    """
    CODE_REGISTER = 0
    CODE_PASSWORD_RECOVERY = 1
    CODE_EMAIL_CHANGE_REQUEST = 2


class TwillioCodeType(models.IntegerChoices):
    """
        IntegersChoices to choce through which channel to send messages
    """
    EMAIL = 0
    MSM = 1
