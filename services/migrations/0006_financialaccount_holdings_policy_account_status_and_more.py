# Generated by Django 4.2.3 on 2023-07-12 19:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('services', '0005_rl360records_policy_withdrawals'),
    ]

    operations = [
        migrations.CreateModel(
            name='FinancialAccount',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('provider', models.CharField(blank=True, max_length=255, null=True)),
                ('valuation_date', models.DateField(blank=True, null=True)),
                ('policy_number', models.CharField(blank=True, max_length=255, null=True)),
                ('policy_currency', models.CharField(blank=True, max_length=255, null=True)),
                ('policy_start_date', models.DateField(blank=True, null=True)),
                ('policy_end_date', models.DateField(blank=True, null=True)),
                ('sub_policies', models.TextField(blank=True, null=True)),
                ('sub_product_type', models.TextField(blank=True, null=True)),
                ('policy_basis', models.CharField(blank=True, max_length=255, null=True)),
                ('business_split', models.CharField(blank=True, max_length=255, null=True)),
                ('account_status', models.CharField(blank=True, max_length=255, null=True)),
                ('regular_contribution', models.FloatField(blank=True, null=True)),
                ('policy_term', models.IntegerField(blank=True, null=True)),
                ('contribution_frequency', models.CharField(blank=True, max_length=255, null=True)),
                ('next_contribution_date', models.DateField(blank=True, null=True)),
                ('last_contribution_date', models.DateField(blank=True, null=True)),
                ('number_premiums_missed', models.IntegerField(blank=True, null=True)),
                ('arrear_status', models.CharField(blank=True, max_length=255, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Holdings',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('provider', models.CharField(blank=True, max_length=255, null=True)),
                ('policy_number', models.CharField(blank=True, max_length=255, null=True)),
                ('holding_currency', models.CharField(blank=True, max_length=255, null=True)),
                ('units', models.FloatField(blank=True, null=True)),
                ('unit_price', models.FloatField(blank=True, null=True)),
                ('price_date', models.DateField(blank=True, null=True)),
                ('holding_market_value_holding_currency', models.FloatField(blank=True, null=True)),
                ('book_cost', models.FloatField(blank=True, null=True)),
                ('gain_loss', models.FloatField(blank=True, null=True)),
                ('holding_reference', models.CharField(blank=True, max_length=255, null=True)),
            ],
        ),
        migrations.AddField(
            model_name='policy',
            name='account_status',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='policy',
            name='business_split',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name='policy',
            name='sub_product_type',
            field=models.TextField(blank=True, null=True),
        ),
    ]