# Generated by Django 3.2.12 on 2022-10-05 20:32

from django.db import migrations
from django.contrib.postgres.operations import UnaccentExtension


class Migration(migrations.Migration):

    dependencies = [
        ('api_partner', '0012_historical_partner_link_accum'),
    ]

    operations = [
        UnaccentExtension()
    ]
