from django.db import models
from django.utils import timezone


class ClickTracking(models.Model):
    """
    ### ASN
    Information about the "Autonomous System Number" (ASN). If information
    about an ASN is available the following three fields will be provided.
    Note that the same ASN can refer to multiple non-continguous IP ranges.
    ASNs can also overlap with other ASNs or be contained within others.
    """
    partner_link_accumulated_id = models.BigIntegerField(null=True, default=None)
    link_id = models.BigIntegerField(null=True)

    ip = models.CharField(max_length=63, null=True, default=None)
    """
    The requested and validated IPv4 or IPv6 address.
    """
    registry = models.CharField(max_length=254, null=True, default=None)
    """
    Regional Internet Registry (RIR) to which this address is assigned. This
    field contains the name of one of the five official RIRs or the value
    "PRIVATE" for lan addresses.
    """
    countrycode = models.CharField(max_length=3, null=True, default=None)
    """
    ISO 3166-2 country code.
    """
    countryname = models.CharField(max_length=127, null=True, default=None)
    """
    Full country name.
    """
    asn_code = models.CharField(max_length=254, null=True, default=None)
    """
    Numeric ASN code.
    """
    asn_name = models.CharField(max_length=254, null=True, default=None)
    """
    The common name for the ASN.
    """
    asn_route = models.CharField(max_length=254, null=True, default=None)
    """
    IP and CIDR prefix of the ASN.
    """
    asn_start = models.CharField(max_length=254, null=True, default=None)
    """
    IP start address of the ASN.
    """
    asn_end = models.CharField(max_length=254, null=True, default=None)
    """
    IP end address of the ASN.
    """
    asn_count = models.CharField(max_length=254, null=True, default=None)
    """
    Number of IP addresses in the ASN, not all addresses are useable.
    """
    city = models.CharField(max_length=254, null=True, default=None)
    """
    Name of the city the IP address has been assigned to.
    """
    spam = models.BooleanField(null=True, default=None)
    """
    Boolean which is true if the provided address occurs on a public spam list, false otherwise.
    """
    tor = models.BooleanField(null=True, default=None)
    """
    Boolean which is true if the provided address is a TOR exit node, false otherwise.
    """
    count = models.IntegerField(default=1)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        verbose_name = "Click tracking"
        verbose_name_plural = "Clicks tracking"

    def __str__(self):
        return f"{self.ip} - {self.created_at}"
