# Generated by Django 4.2.3 on 2023-10-11 13:05

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('services', '0014_mappedfinancialaccountmodel'),
    ]

    operations = [
        migrations.AlterField(
            model_name='mappedfinancialaccountmodel',
            name='policy_number',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
