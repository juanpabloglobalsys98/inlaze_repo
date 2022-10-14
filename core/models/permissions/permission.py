from django.db import models


class Permission(models.Model):
    """
    """
    codename = models.CharField(max_length=255, unique=True)
    """
    key to get the permission
    """

    section = models.CharField(max_length=100,blank=True,default="")
    """
    Classification about permission, this makes more easy the management on 
    frontend
    """
    name = models.CharField(max_length=255)

    action = models.CharField(max_length=255,blank=True,default="")
    """
    short description about ermission like a title
    """
    description = models.TextField(blank=True)
    """
    Extended description about permission
    """


    class CategoryChoices(models.IntegerChoices):
        BOOKMAKER = 0
        CHECKING_ACC = 1

    

    def __str__(self):
        return f"{self.codename}"
