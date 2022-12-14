# Generated by Django 3.2.12 on 2022-10-04 19:12

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('api_partner', '0011_ban_code_gen_withdrawal'),
    ]

    operations = [
        migrations.CreateModel(
            name='HistoricalPartnerLinkAccum',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('prom_code', models.CharField(default=None, max_length=50, null=True)),
                ('is_assigned', models.BooleanField()),
                ('percentage_cpa', models.FloatField()),
                ('is_percentage_custom', models.BooleanField()),
                ('tracker', models.FloatField()),
                ('tracker_deposit', models.FloatField()),
                ('tracker_registered_count', models.FloatField()),
                ('tracker_first_deposit_count', models.FloatField()),
                ('tracker_wagering_count', models.FloatField()),
                ('status', models.IntegerField(default=2)),
                ('partner_level', models.SmallIntegerField(default=0)),
                ('assigned_at', models.DateTimeField(default=None)),
                ('adviser_id', models.SmallIntegerField(null=True)),
                ('update_reason', models.SmallIntegerField()),
                ('link', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='api_partner.link')),
                ('partner_link_accum', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='api_partner.partnerlinkaccumulated')),
            ],
        ),
    ]
