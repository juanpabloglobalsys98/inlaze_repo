from django.db import models


class QuestionCategory(models.Model):
    """
    """
    title = models.CharField(max_length=200, unique=True)
    icon = models.SmallIntegerField()
    is_active = models.BooleanField(default=True)

    class Meta:
        verbose_name = "Question answer"
        verbose_name_plural = "Questions answers"

    def __str__(self):
        return f"{self.title} "
