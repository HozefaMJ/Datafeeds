# Generated by Django 4.2.3 on 2023-07-12 21:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('services', '0010_financialaccountmodel_lumpsum_contribution'),
    ]

    operations = [
        migrations.AddField(
            model_name='financialaccountmodel',
            name='product',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]