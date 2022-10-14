# Generated by Django 3.2.12 on 2022-07-12 12:48

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('api_partner', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='partner',
            name='is_email_valid',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='partner',
            name='is_notify_campaign',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='partner',
            name='is_notify_notice',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='partner',
            name='is_phone_valid',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='partner',
            name='is_terms',
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name='partner',
            name='notify_campaign_at',
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
        migrations.AddField(
            model_name='partner',
            name='notify_notice_at',
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
        migrations.AddField(
            model_name='partner',
            name='terms_at',
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name='partner',
            name='valid_phone_by',
            field=models.SmallIntegerField(default=None, null=True),
        ),
        migrations.AddField(
            model_name='validationcode',
            name='phone',
            field=models.CharField(default=None, max_length=50, null=True, unique=True),
        ),
        migrations.AddField(
            model_name='validationcode',
            name='user',
            field=models.OneToOneField(default=1, on_delete=django.db.models.deletion.CASCADE, to='core.user'),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='validationcode',
            name='email',
            field=models.EmailField(default=None, max_length=254, null=True, unique=True),
        ),
        migrations.AlterField(
            model_name='validationcoderegister',
            name='created_at',
            field=models.DateTimeField(default=django.utils.timezone.now),
        ),
        migrations.AlterField(
            model_name='validationcoderegister',
            name='phone',
            field=models.CharField(max_length=50, null=True, unique=True),
        ),
        migrations.AlterUniqueTogether(
            name='validationcode',
            unique_together=set(),
        ),
    ]