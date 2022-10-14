from django.db import models


class PartnerFeedback(models.Model):
    """
    country required information

    Fields
    ---
    - name : `Char`
        Name of country
    - iso : `Char`
        ISO 3166 Codes for the representation of names of countries and 
        their subdivisions
    """

    question = models.ForeignKey(
        to="api_partner.Question",
        on_delete=models.CASCADE,
    )

    partner = models.ForeignKey(
        to="api_partner.Partner",
        on_delete=models.CASCADE,
    )

    calification = models.FloatField()
    feedback = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Partner feedback"
        verbose_name_plural = "Partner feedbacks"

    def __str__(self):
        return f"{self.feedback}"
