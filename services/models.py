from django.db import models

class Policy(models.Model):
    provider = models.CharField(max_length=255, null=True, blank=True)
    provider_id = models.CharField(max_length=255, null=True, blank=True)
    db_entry_date = models.DateField(null=True, blank=True)
    valuation_date = models.DateField(null=True, blank=True)#FA
    broker_id = models.CharField(max_length=255, null=True, blank=True)
    policy_number = models.CharField(max_length=255, null=True, blank=True)#FA/#HL
    policy_currency = models.CharField(max_length=255, null=True, blank=True)#FA
    policy_start_date = models.DateField(null=True, blank=True)#FA
    policy_end_date = models.DateField(null=True, blank=True)#FA
    policy_status = models.CharField(max_length=255, null=True, blank=True)
    product = models.CharField(max_length=255, null=True, blank=True)
    holding_name = models.CharField(max_length=255, null=True, blank=True)
    holding_currency = models.CharField(max_length=255, null=True, blank=True)#HL
    units = models.FloatField(null=True, blank=True)#HL
    unit_price = models.FloatField(null=True, blank=True)#HL
    price_date = models.DateField(null=True, blank=True)#HL
    total_contribution = models.FloatField(null=True, blank=True)
    holding_market_value_holding_currency = models.FloatField(null=True, blank=True)#HL
    holding_market_value_policy_currency = models.FloatField(null=True, blank=True)
    isin = models.CharField(max_length=255, null=True, blank=True)
    sedol = models.CharField(max_length=255, null=True, blank=True)
    book_cost = models.FloatField(null=True, blank=True)#HL
    gain_loss = models.FloatField(null=True, blank=True)#HL
    holding_reference = models.CharField(max_length=255, null=True, blank=True)#HL
    trust = models.CharField(max_length=255, null=True, blank=True)
    surrender_penalty = models.FloatField(null=True, blank=True)
    max_partial_value = models.FloatField(null=True, blank=True)
    surrender_value = models.FloatField(null=True, blank=True)
    sub_policies = models.TextField(null=True, blank=True)#FA
    sub_product_type = models.TextField(null=True, blank=True)#FA
    policy_basis = models.CharField(max_length=255, null=True, blank=True)#FA
    business_split = models.CharField(max_length=255, null=True, blank=True)#FA
    account_status = models.CharField(max_length=255, null=True, blank=True)#FA
    transaction_date = models.DateField(null=True, blank=True)
    transaction_name = models.CharField(max_length=255, null=True, blank=True)
    transaction_comments = models.TextField(null=True, blank=True)
    transaction_debit_amount = models.FloatField(null=True, blank=True)
    transaction_credit_amount = models.FloatField(null=True, blank=True)
    transaction_amount = models.FloatField(null=True, blank=True)
    transaction_currency = models.CharField(max_length=255, null=True, blank=True)
    #ADDITIONS FROM REGULAR POLICY
    regular_contribution = models.FloatField(null=True, blank=True)#FA
    single_contribution = models.FloatField(null=True, blank=True)
    policy_term = models.IntegerField(null=True, blank=True)#FA
    contribution_frequency = models.CharField(max_length=255, null=True, blank=True)#FA
    contribution_due_date = models.DateField(null=True, blank=True)
    contribution_received_date = models.DateField(null=True, blank=True)
    next_contribution_date = models.DateField(null=True, blank=True)#FA
    last_contribution_date = models.DateField(null=True, blank=True)#FA
    contribution_currency = models.CharField(max_length=255, null=True, blank=True)
    withdrawals = models.CharField(max_length=255, null=True, blank=True)
    arrears = models.FloatField(null=True, blank=True)
    contribution_due = models.FloatField(null=True, blank=True)
    contribution_received = models.FloatField(null=True, blank=True)
    number_premiums_missed = models.IntegerField(null=True, blank=True)#FA
    #SELF CALCULATED FIELD
    arrear_status = models.CharField(max_length=255, null=True, blank=True)#FA

    def __str__(self):
        return self.policy_number

class MappedPolicyModel(models.Model):
    # Fields from Policy
    provider = models.CharField(max_length=255, null=True, blank=True)
    provider_id = models.CharField(max_length=255, null=True, blank=True)
    db_entry_date = models.DateField(null=True, blank=True)
    valuation_date = models.DateField(null=True, blank=True)#FA
    broker_id = models.CharField(max_length=255, null=True, blank=True)
    policy_number = models.CharField(max_length=255, null=True, blank=True)#FA/#HL
    policy_currency = models.CharField(max_length=255, null=True, blank=True)#FA
    policy_start_date = models.DateField(null=True, blank=True)#FA
    policy_end_date = models.DateField(null=True, blank=True)#FA
    policy_status = models.CharField(max_length=255, null=True, blank=True)
    product = models.CharField(max_length=255, null=True, blank=True)
    holding_name = models.CharField(max_length=255, null=True, blank=True)
    holding_currency = models.CharField(max_length=255, null=True, blank=True)#HL
    units = models.FloatField(null=True, blank=True)#HL
    unit_price = models.FloatField(null=True, blank=True)#HL
    price_date = models.DateField(null=True, blank=True)#HL
    total_contribution = models.FloatField(null=True, blank=True)
    holding_market_value_holding_currency = models.FloatField(null=True, blank=True)#HL
    holding_market_value_policy_currency = models.FloatField(null=True, blank=True)
    isin = models.CharField(max_length=255, null=True, blank=True)
    sedol = models.CharField(max_length=255, null=True, blank=True)
    book_cost = models.FloatField(null=True, blank=True)#HL
    gain_loss = models.FloatField(null=True, blank=True)#HL
    holding_reference = models.CharField(max_length=255, null=True, blank=True)#HL
    trust = models.CharField(max_length=255, null=True, blank=True)
    surrender_penalty = models.FloatField(null=True, blank=True)
    max_partial_value = models.FloatField(null=True, blank=True)
    surrender_value = models.FloatField(null=True, blank=True)
    sub_policies = models.TextField(null=True, blank=True)#FA
    sub_product_type = models.TextField(null=True, blank=True)#FA
    policy_basis = models.CharField(max_length=255, null=True, blank=True)#FA
    business_split = models.CharField(max_length=255, null=True, blank=True)#FA
    account_status = models.CharField(max_length=255, null=True, blank=True)#FA
    transaction_date = models.DateField(null=True, blank=True)
    transaction_name = models.CharField(max_length=255, null=True, blank=True)
    transaction_comments = models.TextField(null=True, blank=True)
    transaction_debit_amount = models.FloatField(null=True, blank=True)
    transaction_credit_amount = models.FloatField(null=True, blank=True)
    transaction_amount = models.FloatField(null=True, blank=True)
    transaction_currency = models.CharField(max_length=255, null=True, blank=True)
    #ADDITIONS FROM REGULAR POLICY
    regular_contribution = models.FloatField(null=True, blank=True)#FA
    single_contribution = models.FloatField(null=True, blank=True)
    policy_term = models.IntegerField(null=True, blank=True)#FA
    contribution_frequency = models.CharField(max_length=255, null=True, blank=True)#FA
    contribution_due_date = models.DateField(null=True, blank=True)
    contribution_received_date = models.DateField(null=True, blank=True)
    next_contribution_date = models.DateField(null=True, blank=True)#FA
    last_contribution_date = models.DateField(null=True, blank=True)#FA
    contribution_currency = models.CharField(max_length=255, null=True, blank=True)
    withdrawals = models.CharField(max_length=255, null=True, blank=True)
    arrears = models.FloatField(null=True, blank=True)
    contribution_due = models.FloatField(null=True, blank=True)
    contribution_received = models.FloatField(null=True, blank=True)
    number_premiums_missed = models.IntegerField(null=True, blank=True)#FA
    #SELF CALCULATED FIELD
    arrear_status = models.CharField(max_length=255, null=True, blank=True)#FA

    # Fields from Excel sheet
    client_name = models.CharField(max_length=255, null=True, blank=True)
    financial_account_name = models.CharField(max_length=255, null=True, blank=True)
    account_id = models.CharField(max_length=255, null=True, blank=True)
    financial_account_id = models.CharField(max_length=255, null=True, blank=True)
    consultant_name = models.CharField(max_length=255, null=True, blank=True)


class RegularPolicy(models.Model):
    policy_number = models.CharField(max_length=255, null=True, blank=True)
    regular_contribution = models.FloatField(null=True, blank=True)
    single_contribution = models.FloatField(null=True, blank=True)
    policy_term = models.IntegerField(null=True, blank=True)
    contribution_frequency = models.CharField(max_length=255, null=True, blank=True)
    contribution_due_date = models.DateField(null=True, blank=True)
    contribution_received_date = models.DateField(null=True, blank=True)
    next_contribution_date = models.DateField(null=True, blank=True)
    last_contribution_date = models.DateField(null=True, blank=True)
    contribution_currency = models.CharField(max_length=255, null=True, blank=True)
    arrears = models.FloatField(null=True, blank=True)
    contribution_due = models.FloatField(null=True, blank=True)
    contribution_received = models.FloatField(null=True, blank=True)
    number_premiums_missed = models.IntegerField(null=True, blank=True)

    def __str__(self):
        return self.policy_number


class CashflowHistory(models.Model):
    policy_number = models.CharField(max_length=255, null=True, blank=True)
    amount = models.FloatField(null=True, blank=True)
    cashflow_type = models.CharField(max_length=255, null=True, blank=True)
    cashflow_subtype = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return f"CashflowHistory id: {self.id}"


class FinancialAccountModel(models.Model):
    policy_number = models.CharField(max_length=255, unique=True, null=True, blank=True)
    provider = models.CharField(max_length=255, null=True, blank=True)#RL
    provider_id = models.CharField(max_length=255, null=True, blank=True)
    product = models.CharField(max_length=255, null=True, blank=True)#RL
    valuation_date = models.DateField(null=True, blank=True)
    policy_currency = models.CharField(max_length=255, null=True, blank=True)#RL
    policy_start_date = models.DateField(null=True, blank=True)#RL
    policy_end_date = models.DateField(null=True, blank=True)#RL
    sub_policies = models.TextField(null=True, blank=True)#RL
    sub_product_type = models.TextField(null=True, blank=True)
    policy_basis = models.CharField(max_length=255, null=True, blank=True)#RL
    business_split = models.CharField(max_length=255, null=True, blank=True)
    account_status = models.CharField(max_length=255, null=True, blank=True)#RL (policy status)
    regular_contribution = models.FloatField(null=True, blank=True)#RL
    lumpsum_contribution = models.FloatField(null=True, blank=True)
    policy_term = models.IntegerField(null=True, blank=True)#RL
    contribution_frequency = models.CharField(max_length=255, null=True, blank=True)#RL
    next_contribution_date = models.DateField(null=True, blank=True)#RL
    last_contribution_date = models.DateField(null=True, blank=True)#RL
    number_premiums_missed = models.IntegerField(null=True, blank=True)
    arrear_status = models.CharField(max_length=255, null=True, blank=True)
    premium_holiday_status = models.CharField(max_length=255, null=True, blank=True)
    premium_holiday_commencement_date = models.DateField(null=True, blank=True)
    premium_holiday_end_date = models.DateField(null=True, blank=True)

class HoldingsModel(models.Model):
    policy_number = models.CharField(max_length=255, null=True, blank=True)  # FA/#HL
    holding_currency = models.CharField(max_length=255, null=True, blank=True)  # HL
    units = models.FloatField(null=True, blank=True)  # HL
    unit_price = models.FloatField(null=True, blank=True)  # HL
    price_date = models.DateField(null=True, blank=True)  # HL
    holding_market_value_holding_currency = models.FloatField(null=True, blank=True)  # HL
    book_cost = models.FloatField(null=True, blank=True)  # HL
    gain_loss = models.FloatField(null=True, blank=True)  # HL
    holding_reference = models.CharField(max_length=255, null=True, blank=True)  # HL

    def get_financial_account(self):
        return FinancialAccountModel.objects.get(policy_number=self.policy_number)

class Records(models.Model):
    provider_name = models.CharField(max_length=255)
    file = models.CharField(max_length=255)
    date = models.DateField()

class RL360Records(models.Model):
    holding = models.FileField(upload_to='records/', null=True)
    cash_holding = models.FileField(upload_to='records/', null=True)
    policy = models.FileField(upload_to='records/', null=True)
    premium_history = models.FileField(upload_to='records/', null=True)
    cash_transaction = models.FileField(upload_to='records/', null=True)
    date = models.DateField()

# Assuming you have already defined FinancialAccountModel, create MappedFinancialAccountModel
class MappedFinancialAccountModel(models.Model):
    # Fields from FinancialAccountModel
    policy_number = models.CharField(max_length=255, unique=True, null=True, blank=True)
    provider = models.CharField(max_length=255, null=True, blank=True)
    provider_id = models.CharField(max_length=255, null=True, blank=True)
    product = models.CharField(max_length=255, null=True, blank=True)
    valuation_date = models.DateField(null=True, blank=True)
    policy_currency = models.CharField(max_length=255, null=True, blank=True)
    policy_start_date = models.DateField(null=True, blank=True)
    policy_end_date = models.DateField(null=True, blank=True)
    sub_policies = models.TextField(null=True, blank=True)
    sub_product_type = models.TextField(null=True, blank=True)
    policy_basis = models.CharField(max_length=255, null=True, blank=True)
    business_split = models.CharField(max_length=255, null=True, blank=True)
    account_status = models.CharField(max_length=255, null=True, blank=True)
    regular_contribution = models.FloatField(null=True, blank=True)
    lumpsum_contribution = models.FloatField(null=True, blank=True)
    policy_term = models.IntegerField(null=True, blank=True)
    contribution_frequency = models.CharField(max_length=255, null=True, blank=True)
    next_contribution_date = models.DateField(null=True, blank=True)
    last_contribution_date = models.DateField(null=True, blank=True)
    number_premiums_missed = models.IntegerField(null=True, blank=True)
    arrear_status = models.CharField(max_length=255, null=True, blank=True)
    premium_holiday_status = models.CharField(max_length=255, null=True, blank=True)
    premium_holiday_commencement_date = models.DateField(null=True, blank=True)
    premium_holiday_end_date = models.DateField(null=True, blank=True)
    
    # Fields from Excel sheet
    client_name = models.CharField(max_length=255, null=True, blank=True)
    financial_account_name = models.CharField(max_length=255, null=True, blank=True)
    account_id = models.CharField(max_length=255, null=True, blank=True)
    financial_account_id = models.CharField(max_length=255, null=True, blank=True)
    consultant_name = models.CharField(max_length=255, null=True, blank=True)
