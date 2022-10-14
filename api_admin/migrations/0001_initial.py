# Generated by Django 3.2.12 on 2022-06-25 15:19

import api_admin.models.authentication.validation_code
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('core', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Admin',
            fields=[
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.DO_NOTHING, primary_key=True, serialize=False, to='core.user')),
            ],
            options={
                'verbose_name': 'Admin',
                'verbose_name_plural': 'Admins',
            },
        ),
        migrations.CreateModel(
            name='InactiveActiveCodeReason',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.CharField(max_length=255, unique=True)),
                ('reason', models.TextField(unique=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': 'Inactive Active Code Reason',
                'verbose_name_plural': 'Inactive Active Codes Reasons',
            },
        ),
        migrations.CreateModel(
            name='ValidationCode',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('email', models.EmailField(max_length=254, unique=True)),
                ('code', models.CharField(max_length=32)),
                ('expiration', models.DateTimeField(default=api_admin.models.authentication.validation_code.ValidationCode._get_current_expiration)),
                ('attempts', models.SmallIntegerField(default=0)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'unique_together': {('email', 'code')},
            },
        ),
        migrations.CreateModel(
            name='InactiveHistory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('active_inactive_code_reason', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to='api_admin.inactiveactivecodereason')),
                ('adviser_from', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='inactivehistory_from', to='api_admin.admin')),
                ('adviser_to', models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='inactivehistory_to', to='api_admin.admin')),
            ],
            options={
                'verbose_name': 'Inactive History',
                'verbose_name_plural': 'Inactive Histories',
            },
        ),
        migrations.CreateModel(
            name='SearchPartnerLimit',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('codename', models.CharField(max_length=255)),
                ('search_type', models.SmallIntegerField(choices=[(0, 'Only Assigned'), (1, 'All')], default=0)),
                ('rol', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='search_limit_rol', to='core.rol')),
            ],
            options={
                'verbose_name': 'Search Partner Limit',
                'verbose_name_plural': 'Search Partner Limits',
                'unique_together': {('rol', 'codename')},
            },
        ),
        migrations.CreateModel(
            name='ReportVisualization',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('values_can_view', models.CharField(default='[]', max_length=1700)),
                ('permission', models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.CASCADE, to='core.permission')),
                ('rol', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='report_visualization_rol', to='core.rol')),
            ],
            options={
                'verbose_name': 'Report Visualization',
                'verbose_name_plural': 'Report Visualizations',
                'unique_together': {('rol', 'permission')},
            },
        ),
    ]