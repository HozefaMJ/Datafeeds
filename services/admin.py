from django.contrib import admin
from . import models
# Register your models here.


@admin.register(models.Policy)
class PolicyAdmin(admin.ModelAdmin):
    list_display = ('provider', 'valuation_date', 'broker_id', 'policy_number','holding_name', 'holding_market_value_holding_currency','holding_currency','units','unit_price','total_contribution','book_cost','product','db_entry_date')
    list_filter = ('provider','product')
    search_fields = ['policy_number','provider','product']

@admin.register(models.MappedPolicyModel)
class PolicyAdmin(admin.ModelAdmin):
    list_display = ('provider', 'valuation_date', 'broker_id', 'policy_number','financial_account_name','client_name','holding_name', 'holding_market_value_holding_currency','holding_currency','units','unit_price','total_contribution','book_cost','product','db_entry_date')
    list_filter = ('provider','product')
    search_fields = ['policy_number','provider','product']

@admin.register(models.FinancialAccountModel)
class FinancialAdmin(admin.ModelAdmin):
    list_display = ('provider', 'valuation_date', 'policy_number')
    list_filter = ('provider','product')
    search_fields = ['policy_number','provider','product']

@admin.register(models.MappedFinancialAccountModel)
class FinancialAdmin(admin.ModelAdmin):
    list_display = ('provider', 'valuation_date', 'policy_number','financial_account_name','client_name')
    list_filter = ('provider','product')
    search_fields = ['policy_number','provider','product']

@admin.register(models.Records)
class RecordAdmin(admin.ModelAdmin):
    list_display = ['provider_name','file','date']

@admin.register(models.RL360Records)
class RecordAdmin(admin.ModelAdmin):
    list_display = ['holding','cash_holding','policy','premium_history','cash_transaction','date']

# @admin.register(models.RegularPolicy)
# class RegularPolicyAdmin(admin.ModelAdmin):
#     list_display = ('policy_number', 'db_entry_date')
#     list_filter = ('provider',)