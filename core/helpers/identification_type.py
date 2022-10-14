from django.db import models


class IdentificationType(models.IntegerChoices):
    """
    """
    CC = 0
    COMPANY_ID = 1
    DNI = 2
    CCE = 3
