from django.db import models


class Question(models.Model):
    """
    """
    category = models.ForeignKey("api_partner.QuestionCategory", on_delete=models.CASCADE, related_name="question")
    description = models.CharField(max_length=200)
    answer = models.TextField(blank=True)
    is_active = models.BooleanField(default=True)
    is_common = models.BooleanField(default=False)

    class Meta:
        verbose_name = "Question answer"
        verbose_name_plural = "Questions answers"
        unique_together = ("category", "description")

    def __str__(self):
        return f"{self.description} "
