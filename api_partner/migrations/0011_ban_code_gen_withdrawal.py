# Generated by Django 3.2.12 on 2022-10-01 18:01

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('api_partner', '0010_withdrawal_bank'),
    ]

    operations = [
        migrations.AddField(
            model_name='banunbanreason',
            name='code_reason_id',
            field=models.BigIntegerField(null=True),
        ),
        migrations.AddField(
            model_name='withdrawalpartnermoneyaccum',
            name='partner_level',
            field=models.SmallIntegerField(default=None, null=True),
        ),
        migrations.AlterField(
            model_name='banunbanreason',
            name='ban_unban_code_reason',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE, to='api_partner.banunbancodereason'),
        ),
    ]