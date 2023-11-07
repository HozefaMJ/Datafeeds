import math
from django.forms import ValidationError
import requests
from django.shortcuts import render
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse
import datetime
from django.utils import timezone
import pandas as pd
from django.utils.dateparse import parse_date
from django.views import View
import pysftp
import json
import time as timeset
from django.core.exceptions import ValidationError as VE
from django.utils.dateparse import parse_date
from .utils import collect_attachments, get_product_id,process_sftp_file,collect_attachments_rl360
from .models import Policy,RegularPolicy,CashflowHistory,FinancialAccountModel,HoldingsModel

########################
client_id = '55b2ac50-5f9d-46cd-94ae-9ae427cd5964'
client_secret = 'iU58Q~tPFvTzCIHPZ5iGXuMTHwvHubPXCJw.scHz'
tenant_id = '4efa3987-7313-4150-8ca4-ea22db3ef98b'
email_address = 'datafeeds@skyboundwealth.com'
# subject_name = 'Quilter'
# collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)    
########################

def quilter(request):
    subject_name = 'Datafeeds_Provider_QUILTER'
    filename = collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
    print(filename)
    # df = pd.read_csv(filename,encoding = "ISO-8859-1")
    # df = pd.read_csv(r"C:\WorkSpace\Datafeeds\Datafeeds\Quilter\Quilter Insite  - Oct 2023\Valuation 000739445_000930564.csv",encoding = "ISO-8859-1")
    df = pd.read_csv(r"C:\WorkSpace\Datafeeds\Datafeeds\Quilter\Valuation 000739445_000930564.csv",encoding = "ISO-8859-1")
    # df = pd.read_excel(r'C:\WorkSpace\Datafeeds\files\DataFeeds\Quilter Reg.xlsx')

    df['ValuationDate'] = df['Valuation Date']
    df['BrokerID'] = df[' Agent Number']
    df['PolicyNumber'] = '000'+df[' Product Number'].astype(str)
    df['Product'] = df[' Product Name']
    df['PolicyCurrency'] = df[' Plan Currency']
    df['HoldingName'] = df[' Fund Name']
    df['HoldingReference'] = df['PolicyNumber']+'-'+df['HoldingName']
    df['HoldingCurrency'] = df[' Fund Currency']
    df['Units'] = df[' Units Allocated']
    df['UnitPrice'] = df[' Bid Price']
    df['HoldingMarketValueHoldingCurrency'] = df[' Fund Value']
    df['HoldingMarketValuePolicyCurrency'] = df[' Plan Value']
    df['SurrenderValue'] = df[' Surrender Value']
    df['TotalContribution'] = df[' Total Premium']

    new_df = df[['ValuationDate','BrokerID','PolicyNumber','Product','PolicyCurrency','HoldingName','HoldingReference','HoldingCurrency','Units','UnitPrice','HoldingMarketValueHoldingCurrency','HoldingMarketValuePolicyCurrency','SurrenderValue','TotalContribution']]
    financial_account = df.drop_duplicates(subset='PolicyNumber')[['ValuationDate','BrokerID','PolicyNumber','Product','PolicyCurrency','HoldingName','HoldingReference','HoldingCurrency','Units','UnitPrice','HoldingMarketValueHoldingCurrency','HoldingMarketValuePolicyCurrency','SurrenderValue','TotalContribution']]

    # def convert_date_format(date_string):
    #     try:
    #         # Attempt to parse the date using the current format
    #         date_object = datetime.datetime.strptime(date_string, "%d/%m/%Y")
    #         # Convert the date to the expected format (YYYY-MM-DD)
    #         converted_date = date_object.strftime("%Y-%m-%d")
    #         return converted_date
    #     except ValueError:
    #         raise ValidationError("Invalid date format. The date must be in DD/MM/YYYY format.")

    def convert_date_format(date_string):
        try:
            if isinstance(date_string, pd.Timestamp):
                date_string = date_string.strftime("%d/%m/%Y")

            # Attempt to parse the date using the current format
            date_object = datetime.datetime.strptime(date_string, "%d/%m/%Y")
            # Convert the date to the expected format (YYYY-MM-DD)
            converted_date = date_object.strftime("%Y-%m-%d")
            return converted_date
        except ValueError:
            raise ValidationError("Invalid date format. The date must be in DD/MM/YYYY format.")


    # new_df.to_excel('Generated_Quilter.xlsx',index=False)

    for index,row in new_df.iterrows():
        data = Policy(
            provider = 'Quilter',
            provider_id = 'a4v3H000000D214',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(row['ValuationDate']),
            broker_id = row['BrokerID'],
            policy_number = row['PolicyNumber'],
            policy_currency = row['PolicyCurrency'],
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = row['Product'],
            holding_name = row['HoldingName'],
            holding_currency = row['HoldingCurrency'],
            units = row['Units'],
            unit_price = row['UnitPrice'],
            price_date = None,
            total_contribution = row['TotalContribution'],
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],
            isin = None,
            sedol = None,
            book_cost = None,
            gain_loss = None,
            holding_reference = row['HoldingReference'],
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = row['SurrenderValue'],
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
        )
        data.save()
    
    for index,row in financial_account.iterrows():
        data = FinancialAccountModel(
            provider = 'Quilter',
            provider_id = 'a4v3H000000D214',
            product = None,
            policy_number = row['PolicyNumber'],
            valuation_date = convert_date_format(row['ValuationDate']),
            policy_currency = row['PolicyCurrency'],
            policy_start_date = None,
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = None,
            regular_contribution = None,
            policy_term = None,
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()

    return HttpResponse('Quilter Data Saved')

def quilterLumpsum(request):
    subject_name = 'Datafeeds_Provider_QUILTER_LUMPSUM'
    filename = collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
    print(filename)
    # df = pd.read_csv(filename,encoding = "ISO-8859-1")
    # df = pd.read_csv(r'C:\WorkSpace\Datafeeds\Datafeeds\Quilter\Lumpsum\Quilter IDD - Oct 2023.csv',encoding = "ISO-8859-1")
    # df = pd.read_csv(r'C:\WorkSpace\Datafeeds\Datafeeds\Quilter\Lumpsum\Quilter Main  - Oct 2023.csv',encoding = "ISO-8859-1")
    df = pd.read_csv(r'C:\WorkSpace\Datafeeds\Datafeeds\Quilter\Lumpsum\Quilter UK - Oct 2023.csv',encoding = "ISO-8859-1")
    # df = pd.read_excel('/Users/dexter/Documents/Workspace/Skybound/Clean_Datafeeds/DataDefaults/Quilter LumpSum.xlsx')

    print('COLUMNS::::',df.columns)


    df['ValuationDate'] = df['ValuationDate']
    df['BrokerID'] = df['BrokerID']
    df['PolicyNumber'] = df['PlanNumber']
    df['PolicyCurrency'] = df['PlanCurrency']
    df['PolicyStartDate'] = df['PlanStartDate']
    df['PolicyStatus'] = df['PlanStatus']
    df['TotalContribution'] = df['Contributions']
    df['Product'] = df['ProductCode']
    df['SEDOL'] = df['SEDOL']
    df['ISIN'] = df['ISIN']
    df['HoldingName'] = df['AssetType']+'-'+df['AssetDetailsSecurity']
    df['HoldingReference'] = df['PolicyNumber'].astype(str)+'-'+df['HoldingName']
    df['Units'] = df['Units']
    df['HoldingMarketValueHoldingCurrency'] = df['ValueAssetCurrency']
    df['HoldingMarketValuePolicyCurrency'] = df['ValuePlanCurrency']
    df['UnitPrice'] = df['AssetPrice']
    df['HoldingCurrency'] = df['AssetCurrency']
    df['PriceDate'] = df['AssetPriceDate']
    df['BookCost'] = pd.to_numeric(df['BookCostValuationCurrency'], errors='coerce')
    df['GainLoss'] = df['BookCost']-pd.to_numeric(df['ValuePlanCurrency'], errors='coerce')/df['BookCost']

    new_df = df[['ValuationDate',
                'BrokerID',
                'PolicyNumber',
                'PolicyCurrency',
                'PolicyStartDate',
                'PolicyStatus',
                'TotalContribution',
                'Product',
                'ISIN',
                'SEDOL',
                'HoldingName',
                'HoldingReference',
                'Units',
                'HoldingMarketValueHoldingCurrency',
                'HoldingMarketValuePolicyCurrency',
                'UnitPrice',
                'HoldingCurrency',
                'PriceDate',
                'BookCost',
                'GainLoss'
                ]]
    
    financialAccount = df.drop_duplicates(subset='PolicyNumber')[[
                'ValuationDate',
                'BrokerID',
                'PolicyNumber',
                'PolicyCurrency',
                'PolicyStartDate',
                'PolicyStatus',
                'TotalContribution',
                'Product',
                'ISIN',
                'SEDOL',
                'HoldingName',
                'HoldingReference',
                'Units',
                'HoldingMarketValueHoldingCurrency',
                'HoldingMarketValuePolicyCurrency',
                'UnitPrice',
                'HoldingCurrency',
                'PriceDate',
                'BookCost',
                'GainLoss'
                ]]

    # def convert_date_format(date_string):
    #     try:
    #         # Attempt to parse the date using the current format
    #         date_object = datetime.datetime.strptime(date_string, "%d/%m/%Y")
    #         # Convert the date to the expected format (YYYY-MM-DD)
    #         converted_date = date_object.strftime("%Y-%m-%d")
    #         return converted_date
    #     except ValueError:
    #         raise ValidationError("Invalid date format. The date must be in DD/MM/YYYY format.")


    def convert_date_format(date_str):
        if isinstance(date_str, float) and math.isnan(date_str):
            # Replace 'nan' with today's date
            date_str = datetime.date.today().strftime('%Y-%m-%d')
        else:
            # Convert input string to datetime object
            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            
            # Extract the date component
            date_str = date_obj.date().strftime('%Y-%m-%d')
        
        return date_str



    # new_df.to_excel('Generated_Quilter_Lumpsum.xlsx',index=False)

    for index,row in new_df.iterrows():
        data = Policy(
            provider = 'Quilter Lumpsum',
            provider_id = 'a4v3H000000D214',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(row['ValuationDate']),
            broker_id = row['BrokerID'],
            policy_number = row['PolicyNumber'],
            policy_currency = row['PolicyCurrency'],
            policy_start_date = convert_date_format(row['PolicyStartDate']),
            policy_end_date = None,
            policy_status = row['PolicyStatus'],
            product = row['Product'],
            holding_name = row['HoldingName'],
            holding_currency = row['HoldingCurrency'],
            units = row['Units'],
            unit_price = row['UnitPrice'],
            price_date = convert_date_format(row['PriceDate']),
            total_contribution = row['TotalContribution'],
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],
            isin = row['ISIN'],
            sedol = row['SEDOL'],
            book_cost = row['BookCost'],
            gain_loss = row['GainLoss'],
            holding_reference = row['HoldingReference'],
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
        )
        data.save()

    
    for index,row in financialAccount.iterrows():
        data = FinancialAccountModel(
            provider = 'Quilter Lumpsum',
            provider_id = 'a4v3H000000D214',
            product = row['Product'],
            policy_number = row['PolicyNumber'],
            valuation_date = convert_date_format(row['ValuationDate']),
            policy_currency = row['PolicyCurrency'],
            policy_start_date = convert_date_format(row['PolicyStartDate']),
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = None,
            regular_contribution = None,
            lumpsum_contribution = None,
            policy_term = None,
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None,
            premium_holiday_status = None,
            premium_holiday_commencement_date = None,
            premium_holiday_end_date = None
        )
        data.save()

    return HttpResponse('Data Saved')

def utmost(request):
    subject_name = 'Datafeeds_Provider_UTMOST'
    filename = collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
    print(filename)
    # df = pd.read_csv(filename,encoding = "ISO-8859-1")
    df = pd.read_csv(r'C:\WorkSpace\Datafeeds\Datafeeds\Utmost\Valuation 40401163_97000257.csv',encoding = "ISO-8859-1")
    # df = pd.read_csv(r'C:\WorkSpace\Datafeeds\files\DataFeeds\Valuation 40401163_97000257.csv',encoding = "ISO-8859-1")
    # df = pd.read_excel('https://skybound-client-app.s3.eu-north-1.amazonaws.com/Utmost+Regulars.xlsx')
    
    df['ValuationDate'] = df['Valuation Date']
    df['BrokerID'] = df[' Agent Number']
    df['PolicyNumber'] = df[' Product Number'].astype(str)
    df['Product'] = df[' Product Name']
    df['PolicyCurrency'] = df[' Plan Currency']
    df['HoldingName'] = df[' Fund Name']
    df['HoldingReference'] = df['PolicyNumber']+'-'+df['HoldingName']
    df['HoldingCurrency'] = df[' Fund Currency']
    df['Units'] = df[' Units Allocated']
    df['UnitPrice'] = df[' Bid Price']
    df['HoldingMarketValueHoldingCurrency'] = df[' Fund Value']
    df['HoldingMarketValuePolicyCurrency'] = df[' Plan Value']
    df['SurrenderValue'] = df[' Surrender Value']
    df['TotalContribution'] = df[' Total Premium']

    new_df = df[['ValuationDate','BrokerID','PolicyNumber','Product','PolicyCurrency','HoldingName','HoldingReference','HoldingCurrency','Units','UnitPrice','HoldingMarketValueHoldingCurrency','HoldingMarketValuePolicyCurrency','SurrenderValue','TotalContribution']]
    financial_account = df.drop_duplicates(subset='PolicyNumber')[['ValuationDate','BrokerID','PolicyNumber','Product','PolicyCurrency','HoldingName','HoldingReference','HoldingCurrency','Units','UnitPrice','HoldingMarketValueHoldingCurrency','HoldingMarketValuePolicyCurrency','SurrenderValue','TotalContribution']]

    def convert_date_format(date_string):
        try:
            # Attempt to parse the date using the current format
            date_object = datetime.datetime.strptime(date_string, "%d/%m/%Y")
            # Convert the date to the expected format (YYYY-MM-DD)
            converted_date = date_object.strftime("%Y-%m-%d")
            return converted_date
        except ValueError:
            raise ValidationError("Invalid date format. The date must be in DD/MM/YYYY format.")

    # new_df.to_excel('Generated_Utmost.xlsx',index=False)
    for index,row in new_df.iterrows():
        data = Policy(
            provider = 'Utmost Regulars',
            provider_id = 'a4v3H000000D22R',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(row['ValuationDate']),
            broker_id = row['BrokerID'],
            policy_number = row['PolicyNumber'],
            policy_currency = row['PolicyCurrency'],
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = row['Product'],
            holding_name = row['HoldingName'],
            holding_currency = row['HoldingCurrency'],
            units = row['Units'],
            unit_price = row['UnitPrice'],
            price_date = None,
            total_contribution = row['TotalContribution'],
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],
            isin = None,
            sedol = None,
            book_cost = None,
            gain_loss = None,
            holding_reference = row['HoldingReference'],
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = row['SurrenderValue'],
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
        )
        data.save()

    for index,row in financial_account.iterrows():
        data = FinancialAccountModel(
            provider = 'Utmost',
            provider_id = 'a4v3H000000D22R',
            product = row['Product'],
            policy_number = row['PolicyNumber'],
            valuation_date = convert_date_format(row['ValuationDate']),
            policy_currency = row['PolicyCurrency'],
            policy_start_date = None,
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = None,
            regular_contribution = None,
            lumpsum_contribution = row['TotalContribution'],
            policy_term = None,
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()

    return HttpResponse('Utmost Data Saved')

def utmostLumpsum(request):
    subject_name = 'Datafeeds_Provider_UTMOST_LUMPSUM'
    filename = collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
    print(filename)
    # df = pd.read_csv(filename,encoding = "ISO-8859-1")
    df = pd.read_csv(r"C:\WorkSpace\Datafeeds\Datafeeds\Utmost\Valuation GP52024_PF911076.csv",encoding = "ISO-8859-1")
    # df = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\DataFeeds\Valuation GP52024_PF911076.csv",encoding = "ISO-8859-1")
    # df = pd.read_excel('https://skybound-client-app.s3.eu-north-1.amazonaws.com/Utmost+LumpSum.xlsx')

    df['ValuationDate'] = df['Valuation Date']
    df['BrokerID'] = df['Agent Number']
    df['PolicyNumber'] = df['Plan Number']
    df['PolicyStartDate'] = df['Plan Commencement Date']
    df['PolicyCurrency'] = df['Plan Currency']
    df['HoldingName'] = df['Security Issuer']+' '+df['Security Name']
    df['HoldingReference'] = df['PolicyNumber']+'-'+df['HoldingName']
    df['HoldingCurrency'] = df['Asset Currency']
    df['SEDOL'] = df['SEDOL']
    df['ISIN'] = df['ISIN']
    df['Units'] = df['Asset Holding']
    df['UnitPrice'] = df['Asset Currency Market Bid Price']
    df['HoldingMarketValueHoldingCurrency'] = df['Asset Currency Market Value']
    df['HoldingMarketValuePolicyCurrency'] = df['Plan Currency Market Value']
    df['BookCost'] = df['Plan Currency Book Cost']
    df['GainLoss'] = df['Plan Currency % Growth']
    df['SurrenderValue'] = df['Plan Currency Surrender Value']

    new_df = df[['ValuationDate',
                'BrokerID',
                'PolicyNumber',
                'PolicyStartDate',
                'PolicyCurrency',
                'HoldingName',
                'HoldingReference',
                'HoldingCurrency',
                'SEDOL',
                'ISIN',
                'Units',
                'UnitPrice',
                'HoldingMarketValueHoldingCurrency',
                'HoldingMarketValuePolicyCurrency',
                'SurrenderValue',
                'BookCost',
                'GainLoss']]
    
    financial_account = df.drop_duplicates(subset='PolicyNumber')[['ValuationDate',
                'BrokerID',
                'PolicyNumber',
                'PolicyStartDate',
                'PolicyCurrency',
                'HoldingName',
                'HoldingReference',
                'HoldingCurrency',
                'SEDOL',
                'ISIN',
                'Units',
                'UnitPrice',
                'HoldingMarketValueHoldingCurrency',
                'HoldingMarketValuePolicyCurrency',
                'SurrenderValue',
                'BookCost',
                'GainLoss']]


    # new_df.to_excel('Generated_Utmost_Lumpsum.xlsx',index=False)

    def convert_date_format(date_str):
        if isinstance(date_str, float) and math.isnan(date_str):
            # Replace 'nan' with today's date
            date_str = datetime.date.today().strftime('%Y-%m-%d')
        else:
            # Convert input string to datetime object
            date_obj = datetime.datetime.strptime(date_str, "%d/%m/%Y")
            
            # Extract the date component
            date_str = date_obj.date().strftime('%Y-%m-%d')
        
        return date_str

    for index,row in new_df.iterrows():
        data = Policy(
            provider = 'Utmost Lumpsum',
            provider_id = 'a4v3H000000D22R',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(row['ValuationDate']),
            broker_id = row['BrokerID'],
            policy_number = row['PolicyNumber'],
            policy_currency = row['PolicyCurrency'],
            policy_start_date = convert_date_format(row['PolicyStartDate']),
            policy_end_date = None,
            policy_status = None,
            product = None,
            holding_name = row['HoldingName'],
            holding_currency = row['HoldingCurrency'],
            units = row['Units'],
            unit_price = row['UnitPrice'],
            price_date = None,
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],
            isin = row['ISIN'],
            sedol = row['SEDOL'],
            book_cost = row['BookCost'],
            gain_loss = row['GainLoss'],
            holding_reference = row['HoldingReference'],
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = row['SurrenderValue'],
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
        )
        data.save()

    for index,row in financial_account.iterrows():
        data = FinancialAccountModel(
            provider = 'Utmost Lumpsum',
            provider_id = 'a4v3H000000D22R',
            product = None,
            policy_number = row['PolicyNumber'],
            valuation_date = convert_date_format(row['ValuationDate']),
            policy_currency = row['PolicyCurrency'],
            policy_start_date = convert_date_format(row['PolicyStartDate']),
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = None,
            regular_contribution = None,
            lumpsum_contribution = None,
            policy_term = None,
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()

    return HttpResponse('Data Saved')

def rl360(request):
    # subject_name = 'Datafeeds_Provider_RL360_TEST'
    # files = collect_attachments_rl360(email_address, subject_name, client_id, client_secret, tenant_id)
    

    # df_Holdings = pd.read_csv("https://skybound-client-app.s3.eu-north-1.amazonaws.com/DF_420_Holdings.csv",encoding = "ISO-8859-1")
    df_Holdings = pd.read_csv(r"C:\WorkSpace\Datafeeds\Datafeeds\RL360\DF_497_Holdings.csv",encoding = "ISO-8859-1")
    df_CashHoldings = pd.read_csv(r"C:\WorkSpace\Datafeeds\Datafeeds\RL360\DF_497_CashHoldings.csv",encoding = "ISO-8859-1")
    # df_Policy = pd.read_csv("https://skybound-client-app.s3.eu-north-1.amazonaws.com/DF_420_Policy.csv",encoding = "ISO-8859-1")
    df_Policy = pd.read_csv(r"C:\WorkSpace\Datafeeds\Datafeeds\RL360\DF_497_Policy.csv",encoding = "ISO-8859-1")
    df_PremiumHistory = pd.read_csv(r"C:\WorkSpace\Datafeeds\Datafeeds\RL360\DF_497_PremHist.csv",encoding = "ISO-8859-1")
    df_CashTransaction = pd.read_csv(r"C:\WorkSpace\Datafeeds\Datafeeds\RL360\DF_497_CashTranHist.csv",encoding = "ISO-8859-1")


    df_Holdings['ValuationDate'] = df_Holdings['System_Dt']
    df_Holdings['PolicyNumber'] = df_Holdings['Policy_No']
    df_Holdings['ISIN'] = df_Holdings['ISIN']
    df_Holdings['SEDOL'] = df_Holdings['SEDOL']
    df_Holdings['HoldingName'] = df_Holdings['Fund_Name']
    df_Holdings['HoldingReference'] = df_Holdings['PolicyNumber'].astype('str')+'-'+df_Holdings['HoldingName']
    df_Holdings['HoldingCurrency'] = df_Holdings['Fund_Curr']
    df_Holdings['HoldingMarketValueHoldingCurrency'] = df_Holdings['Investment_Currency_Val']
    df_Holdings['Units'] = df_Holdings['Fund_Unit_Holding']
    df_Holdings['UnitPrice'] = df_Holdings['Nominal_Price']
    df_Holdings['HoldingMarketValuePolicyCurrency'] = df_Holdings['Policy_Currency_Val']
    df_Holdings['BookCost'] = df_Holdings['Policy_Currency_Book_Cost']
    df_Holdings['GainLoss'] = (df_Holdings['Policy_Currency_Val']/df_Holdings['Policy_Currency_Book_Cost'])-1

    holdings = df_Holdings[[
        'ValuationDate',
        'PolicyNumber',
        'ISIN',
        'SEDOL',
        'HoldingName',
        'HoldingReference',
        'HoldingCurrency',
        'HoldingMarketValueHoldingCurrency',
        'Units',
        'UnitPrice',
        'HoldingMarketValuePolicyCurrency',
        'BookCost',
        'GainLoss']]
    
    df_CashHoldings['PolicyNumber'] = df_CashHoldings['Policy_No']
    df_CashHoldings['HoldingCurrency'] = df_CashHoldings['Cash_Account_Curr']
    df_CashHoldings['HoldingMarketValueHoldingCurrency'] = df_CashHoldings['Cash_Account_Balance']
    df_CashHoldings['HoldingMarketValuePolicyCurrency'] = df_CashHoldings['Policy_Currency_Cash_Val']

    cashHoldings = df_CashHoldings[[
        'PolicyNumber',
        'HoldingCurrency',
        'HoldingMarketValueHoldingCurrency',
        'HoldingMarketValuePolicyCurrency'
    ]]

    df_Policy['PolicyNumber'] = df_Policy['Policy_No']
    df_Policy['BrokerID'] = df_Policy['Agency_No']
    df_Policy['Product'] = df_Policy['Product']
    df_Policy['PolicyCurrency'] = df_Policy['Policy_Curr']
    df_Policy['PolicyStatus'] = df_Policy['Status']
    df_Policy['PolicyStartDate'] = df_Policy['Start_Dt']
    df_Policy['SurrenderPenalty'] = df_Policy['Surrender_Pen']
    df_Policy['SurrenderValue'] = df_Policy['Surrender_Val']
    df_Policy['RegularContribution'] = df_Policy['Regular_Premium_Amt']
    df_Policy['ContributionFrequency'] = df_Policy['Frequency']
    df_Policy['PolicyTerm'] = df_Policy['Premium_Term']
    df_Policy['PolicyEndDate'] = df_Policy['Premium_End_DT']
    df_Policy['TotalContribution'] = df_Policy['Total_Contribution']
    df_Policy['Withdrawals'] = df_Policy['Total_Withdrawal']
    df_Policy['LastContributionDate'] = df_Policy['Last_Premium_Paid_Dt']
    df_Policy['NextContributionDate'] = df_Policy['Next_Premium_Due_Dt']
    df_Policy['PolicyBasis'] = df_Policy['Policy_Basis']
    df_Policy['SubPolicies'] = df_Policy['SubPolicies']

    policy = df_Policy[[
        'PolicyNumber',
        'BrokerID',
        'Product',
        'PolicyCurrency',
        'PolicyStatus',
        'PolicyStartDate',
        'SurrenderPenalty',
        'SurrenderValue',
        'RegularContribution',
        'ContributionFrequency',
        'PolicyTerm',
        'PolicyEndDate',
        'TotalContribution',
        'Withdrawals',
        'LastContributionDate',
        'NextContributionDate',
        'PolicyBasis',
        'SubPolicies'
    ]]

    df_PremiumHistory['PolicyNumber'] = df_PremiumHistory['Policy_No']
    df_PremiumHistory['ContributionDueDate'] = df_PremiumHistory['Due_Dt']
    df_PremiumHistory['PolicyCurrency'] = df_PremiumHistory['Currency']
    df_PremiumHistory['ContributionDue'] = df_PremiumHistory['Amount_Due']
    df_PremiumHistory['ContributionReceivedDate'] = df_PremiumHistory['Received_Dt']
    df_PremiumHistory['ContributionReceived'] = df_PremiumHistory['Amount_Received']

    premHistory = df_PremiumHistory[[
        'PolicyNumber',
        'ContributionDueDate',
        'PolicyCurrency',
        'ContributionDue',
        'ContributionReceivedDate',
        'ContributionReceived'
    ]]

    df_CashTransaction['PolicyNumber'] = df_CashTransaction['Policy_No']
    df_CashTransaction['TransactionDate'] = df_CashTransaction['Cash_Tran_Date']
    df_CashTransaction['TransactionName'] = df_CashTransaction['Cash_Tran_Description']
    df_CashTransaction['TransactionComments'] = df_CashTransaction['Cash_Tran_Narrative']
    df_CashTransaction['TransactionDebitAmount'] = df_CashTransaction['Cash_Tran_Debit_Amt']
    df_CashTransaction['TransactionCreditAmount'] = df_CashTransaction['Cash_Tran_Credit_Amt']
    df_CashTransaction['TransactionCurrency'] = df_CashTransaction['Cash_Tran_Curr']

    cashTransaction = df_CashTransaction[[
        'PolicyNumber',
        'TransactionDate',
        'TransactionName',
        'TransactionComments',
        'TransactionDebitAmount',
        'TransactionCreditAmount',
        'TransactionCurrency'
    ]]

    # Merge DataFrames if they are not empty
    # merged_df = pd.DataFrame()  # Final merged DataFrame

    # if not holdings.empty:
    #     merged_df = holdings

    # if not cashHoldings.empty:
    #     if merged_df.empty:
    #         None
    #     else:
    #         merged_df = merged_df.merge(
    #             cashHoldings,
    #             on=['PolicyNumber'],
    #             how='inner'
    #         )

    financial_account = policy.drop_duplicates(subset='PolicyNumber')
    # financial_account.to_csv('RL3602Unique.csv', index=False)

    def convert_date_format(date_str):
        if isinstance(date_str, float) and math.isnan(date_str):
            # Replace 'nan' with today's date
            date_str = None
        elif isinstance(date_str, pd.Timestamp):
            # Convert Pandas Timestamp to datetime object
            date_obj = date_str.to_pydatetime()

            # Extract the date component
            date_str = date_obj.date().strftime('%Y-%m-%d')
        elif isinstance(date_str, str) and len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            # Check if the date is already in the format '%Y-%m-%d'
            return date_str
        else:
            # Convert input string to datetime object
            date_obj = datetime.datetime.strptime(date_str, "%d/%m/%Y")

            # Extract the date component
            date_str = date_obj.date().strftime('%Y-%m-%d')

        return date_str
    
    def removeNAN(value):
        if isinstance(value, (int, float)) and math.isnan(value):
            return None
        return value
    
    for index,row in holdings.iterrows():
        data = Policy(
            provider = 'RL360',
            provider_id = 'a4v3H000000D22L',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = row['ValuationDate'],#
            broker_id = None,
            policy_number = row['PolicyNumber'],#
            policy_currency = None,#
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = None,
            holding_name = row['HoldingName'],#
            holding_currency = row['HoldingCurrency'],#
            units = row['Units'],#
            unit_price = row['UnitPrice'],#
            price_date = None,#
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],#
            isin = row['ISIN'],
            sedol = row['SEDOL'],#
            book_cost = row['BookCost'],#
            gain_loss = row['GainLoss'],#
            holding_reference = row['HoldingReference'],#
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
            #Added
            regular_contribution = None,#FA
            single_contribution = None,
            # policy_term = None,#FA
            contribution_frequency = None,#FA
            # contribution_due_date = row['ContributionDueDate'],
            # contribution_received_date = row['ContributionReceivedDate'],
            next_contribution_date = None,#FA
            last_contribution_date = None,#FA
            contribution_currency = None,
            withdrawals = None,
            arrears = None,
            # contribution_due = row['ContributionDue'],
            # contribution_received = row['ContributionReceived'],
            number_premiums_missed = None,#FA
            #SELF CALCULATED FIELD
            arrear_status = None,#FA
        )
        data.save()
    
    for index,row in financial_account.iterrows():
        data = FinancialAccountModel(
            provider = 'RL360',
            provider_id = 'a4v3H000000D22L',
            product = row['Product'],
            policy_number = row['PolicyNumber'],
            valuation_date = None,
            policy_currency = row['PolicyCurrency'],
            policy_start_date = row['PolicyStartDate'],
            policy_end_date = convert_date_format(row['PolicyEndDate']),
            sub_policies = row['SubPolicies'],
            sub_product_type = None,
            policy_basis = row['PolicyBasis'],
            business_split = None,
            account_status = row['PolicyStatus'],
            regular_contribution = row['RegularContribution'],
            lumpsum_contribution = row['TotalContribution'],
            policy_term = removeNAN(row['PolicyTerm']),
            contribution_frequency = row['ContributionFrequency'],
            next_contribution_date = convert_date_format(row['NextContributionDate']),
            last_contribution_date = row['LastContributionDate'],
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()

    return HttpResponse("Data Saved")

def zurich(request):
    # subject_name = 'Datafeeds_Provider_ZURICH'
    # files = collect_attachments_rl360(email_address, subject_name, client_id, client_secret, tenant_id)
    
    df_Holdings = pd.read_csv(r'C:\WorkSpace\Datafeeds\Datafeeds\Zurich Oct 2023\PolicyFunds.csv',encoding = "ISO-8859-1")
    df_Policy = pd.read_csv(r'C:\WorkSpace\Datafeeds\Datafeeds\Zurich Oct 2023\Policy.csv',encoding = "ISO-8859-1")
    df_PolicyPremium = pd.read_csv(r'C:\WorkSpace\Datafeeds\Datafeeds\Zurich Oct 2023\PolicyPremium.csv',encoding = "ISO-8859-1")
    df_TransactionHistory = pd.read_csv(r'C:\WorkSpace\Datafeeds\Datafeeds\Zurich Oct 2023\TransactionHistory.csv',encoding = "ISO-8859-1")



    df_Holdings['PolicyNumber'] = df_Holdings['Policy Number']
    df_Holdings['HoldingName'] = df_Holdings['Fund Name']
    df_Holdings['HoldingReference'] = df_Holdings['PolicyNumber'].astype('str')+'-'+df_Holdings['HoldingName']
    df_Holdings['SEDOL'] = df_Holdings['Fund Code']
    df_Holdings['HoldingCurrency'] = df_Holdings['Fund Currency']
    df_Holdings['PolicyCurrency'] = df_Holdings['Plan Currency']
    df_Holdings['UnitPrice'] = df_Holdings['Fund Price']
    df_Holdings['Units'] = df_Holdings['Fund Number Of Units Held']
    df_Holdings['HoldingMarketValueHoldingCurrency'] = df_Holdings['Investment Value Fund Currency']
    df_Holdings['HoldingMarketValuePolicyCurrency'] = df_Holdings['Investment Value Plan Currency']
    df_Holdings['ValuationDate'] = df_Holdings['Valuation Date']

    holdings = df_Holdings[[
            'PolicyNumber',
            'HoldingName',
            'HoldingReference',
            'SEDOL',
            'HoldingCurrency',
            'PolicyCurrency',
            'UnitPrice',
            'Units',
            'HoldingMarketValueHoldingCurrency',
            'HoldingMarketValuePolicyCurrency',
            'ValuationDate'
        ]]


    df_Policy['PolicyNumber'] = df_Policy['Policy Number']
    df_Policy['Product'] = df_Policy['Product Description']
    df_Policy['PolicyStatus'] = df_Policy['Policy Status Description']
    df_Policy['PolicyStartDate'] = df_Policy['Commencement Date']
    df_Policy['PolicyTerm'] = df_Policy['Policy Term']
    df_Policy['PolicyCurrency'] = df_Policy['Plan Currency']
    df_Policy['SingleContribution'] = df_Policy['Total Single Premium Paid']
    df_Policy['RegularContribution'] = df_Policy['Total Regular Premium Paid']
    df_Policy['TotalContribution'] = df_Policy['Total Contributions']
    df_Policy['Withdrawals'] = df_Policy['Total Withdrawals']
    df_Policy['SurrenderValue'] = df_Policy['Surrender Value']
    # df_Policy['BrokerID'] = df_Policy['Agent Number']
    df_Policy['MaxPartialValue'] = df_Policy['Max Partial Surrender']


    policy = df_Policy[[
        'PolicyNumber',
        'Product',
        'PolicyStatus',
        'PolicyStartDate',
        'PolicyTerm',
        'PolicyCurrency',
        'SingleContribution',
        'RegularContribution',
        'TotalContribution',
        'Withdrawals',
        'SurrenderValue',
        # 'BrokerID',
        'MaxPartialValue',
    ]]

    df_PolicyPremium['PolicyNumber'] = df_PolicyPremium['Policy Number']
    df_PolicyPremium['ContributionCurrency'] = df_PolicyPremium['Regular Premium Currency']
    df_PolicyPremium['ContributionFrequency'] = df_PolicyPremium['Regular Premium Frequency Desc']
    df_PolicyPremium['RegularContribution'] = df_PolicyPremium['Regular Premium Amount']
    df_PolicyPremium['LastContributionDate'] = df_PolicyPremium['Last Premium Paid Date']
    df_PolicyPremium['NumberOfPremiumsMissed'] = df_PolicyPremium['Count Of Outstanding Premiums']

    policyPremium = df_PolicyPremium[[
        'PolicyNumber',
        'ContributionCurrency',
        'ContributionFrequency',
        'RegularContribution',
        'LastContributionDate',
        'NumberOfPremiumsMissed',
    ]]


    df_TransactionHistory['PolicyNumber'] = df_TransactionHistory['Policy Number']
    df_TransactionHistory['TransactionDate'] = df_TransactionHistory['Allocation Date']
    df_TransactionHistory['TransactionName'] = df_TransactionHistory['Transaction Type Description']
    df_TransactionHistory['TransactionAmount'] = df_TransactionHistory['Transaction Amount']
    df_TransactionHistory['TransactionCurrency'] = df_TransactionHistory['Transaction Currency']

    transactionHistory = df_TransactionHistory[[
        'PolicyNumber',
        'TransactionDate',
        'TransactionName',
        'TransactionAmount',
        'TransactionCurrency'
    ]]

    financial_account = policy.drop_duplicates(subset='PolicyNumber')

    def convert_date_format(date_str):
        date_str = str(date_str)
        if isinstance(date_str, float) and math.isnan(date_str):
            # Replace 'nan' with today's date
            date_str = datetime.date.today().strftime('%Y-%m-%d')
        elif isinstance(date_str, pd.Timestamp):
            # Convert Pandas Timestamp to datetime object
            date_obj = date_str.to_pydatetime()

            # Extract the date component
            date_str = date_obj.date().strftime('%Y-%m-%d')
        else:
            try:
                # Try converting input string to datetime object using the format '%d/%m/%Y'
                date_obj = datetime.datetime.strptime(str(date_str), "%d/%m/%Y")

                # Extract the date component
                date_str = date_obj.date().strftime('%Y-%m-%d')
            except ValueError:
                # Handle the case when the input string does not match the format '%d/%m/%Y'
                # Convert input string to datetime object using the format '%Y%m%d'
                # date_obj = datetime.datetime.strptime(str(date_str), "%Y%m%d")

                # # Extract the date component
                # date_str = date_obj.date().strftime('%Y-%m-%d')
                return None

        return date_str
    
    def removeNAN(value):
        try:
            return int(value)
        except ValueError:
            return None
    
    for index,row in holdings.iterrows():
        data = Policy(
            provider = 'Zurich',
            provider_id = 'a4v3H000000D225',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(row['ValuationDate']),#@
            # broker_id = row['BrokerID'],
            policy_number = row['PolicyNumber'],#@
            policy_currency = row['PolicyCurrency'],#@
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = None,
            holding_name = row['HoldingName'],#@
            holding_currency = row['HoldingCurrency'],#@
            units = row['Units'],#@
            unit_price = row['UnitPrice'],#@
            price_date = None,#
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#@
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],#@
            isin = None,
            sedol = row['SEDOL'],#@
            book_cost = None,
            gain_loss = None,
            holding_reference = row['HoldingReference'],#@
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
            #Added
            regular_contribution = None,#FA
            single_contribution = None,
            policy_term = None,#FA
            contribution_frequency = None,#FA
            # contribution_due_date = 'ContributionDueDate'],
            # contribution_received_date = 'ContributionReceivedDate'],
            next_contribution_date = None,
            last_contribution_date = None,#FA
            contribution_currency = None,
            withdrawals = None,
            arrears = None,
            # contribution_due = 'ContributionDue'],
            # contribution_received = 'ContributionReceived'],
            number_premiums_missed = None,#FA
            #SELF CALCULATED FIELD
            arrear_status = None,#FA
        )
        data.save()
    
    for index,row in financial_account.iterrows():
        data = FinancialAccountModel(
            provider = 'Zurich',
            provider_id = 'a4v3H000000D225',
            product = row['Product'],#
            policy_number = row['PolicyNumber'],#
            valuation_date = None,
            policy_currency = row['PolicyCurrency'],#
            policy_start_date = convert_date_format(row['PolicyStartDate']),#
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = row['PolicyStatus'],#
            regular_contribution = row['RegularContribution'],#
            lumpsum_contribution = row['TotalContribution'],#
            policy_term = removeNAN(row['PolicyTerm']),#
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()

    return HttpResponse('Data Saved')

def fpiLumpsum(request):
    df_LumpsumHoldings = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractBondHolding.CSV",encoding = "ISO-8859-1")
    df_RegularHoldings = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractLifePolicyHolding.CSV",encoding = "ISO-8859-1")
    df_PlanDetails = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractPlanDetail.CSV",encoding = "ISO-8859-1")
    df_RegularPlanDetails = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractLifePolicyDetail.CSV",encoding = "ISO-8859-1")
    df_PremiumHistory = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractLifePremiumTotal.CSV",encoding = "ISO-8859-1")
    df_CashHoldings = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractBondAccount.CSV",encoding = "ISO-8859-1")
    df_TransactionHistory = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractLifeTransactionHistory.CSV",encoding = "ISO-8859-1")

    df_LumpsumHoldings['ValuationDate'] = df_LumpsumHoldings['DatabaseDate']
    df_LumpsumHoldings['PolicyNumber'] = df_LumpsumHoldings['PlanNumber']
    df_LumpsumHoldings['HoldingName'] = df_LumpsumHoldings['InvestmentDescription']
    df_LumpsumHoldings['HoldingReference'] = df_LumpsumHoldings['PlanNumber'].astype('str')+'-'+df_LumpsumHoldings['InvestmentDescription']
    df_LumpsumHoldings['SEDOL'] = df_LumpsumHoldings['FundCode']
    df_LumpsumHoldings['ISIN'] = df_LumpsumHoldings['FundISINCode']
    df_LumpsumHoldings['Units'] = df_LumpsumHoldings['UnitsHeld']
    df_LumpsumHoldings['UnitPrice'] = df_LumpsumHoldings['UnitPrice']
    df_LumpsumHoldings['PriceDate'] = df_LumpsumHoldings['PriceDate']
    df_LumpsumHoldings['HoldingCurrency'] = df_LumpsumHoldings['InvestmentCurrencyISO']
    df_LumpsumHoldings['HoldingMarketValueHoldingCurrency'] = df_LumpsumHoldings['InvestmentCurrencyValue']
    df_LumpsumHoldings['PolicyCurrency'] = df_LumpsumHoldings['ValuationCurrencyISO']
    df_LumpsumHoldings['HoldingMarketValuePolicyCurrency'] = df_LumpsumHoldings['ValuationCurrencyValue']
    df_LumpsumHoldings['BookCost'] = df_LumpsumHoldings['BookCost']
    df_LumpsumHoldings['GainLoss'] = (df_LumpsumHoldings['ValuationCurrencyValue']/df_LumpsumHoldings['BookCost'])-1

    lumpsumHoldings = df_LumpsumHoldings[[
        'ValuationDate',
        'PolicyNumber',
        'HoldingName',
        'HoldingReference',
        'SEDOL',
        'ISIN',
        'Units',
        'UnitPrice',
        'PriceDate',
        'HoldingCurrency',
        'HoldingMarketValueHoldingCurrency',
        'PolicyCurrency',
        'HoldingMarketValuePolicyCurrency',
        'BookCost',
        'GainLoss'
    ]]

    df_RegularHoldings['ValuationDate'] = df_RegularHoldings['DatabaseDate']
    df_RegularHoldings['PolicyNumber'] = df_RegularHoldings['PlanNumber']
    df_RegularHoldings['ISIN'] = df_RegularHoldings['FundISINCode']
    df_RegularHoldings['HoldingName'] = df_RegularHoldings['FundDescription']
    df_RegularHoldings['HoldingReference'] = df_RegularHoldings['PlanNumber'].astype('str')+'-'+df_RegularHoldings['FundDescription']
    df_RegularHoldings['Units'] = df_RegularHoldings['UnitsHeld']
    df_RegularHoldings['UnitPrice'] = df_RegularHoldings['BidPrice']
    df_RegularHoldings['PriceDate'] = df_RegularHoldings['PriceDate']
    df_RegularHoldings['HoldingCurrency'] = df_RegularHoldings['FundCurrencyISO']
    df_RegularHoldings['HoldingMarketValueHoldingCurrency'] = df_RegularHoldings['FundCurrencyValue']
    df_RegularHoldings['PolicyCurrency'] = df_RegularHoldings['ValuationCurrencyISO']
    df_RegularHoldings['HoldingMarketValuePolicyCurrency'] = df_RegularHoldings['ValuationCurrencyValue']


    regularHolding = df_RegularHoldings[[
        'ValuationDate',
        'PolicyNumber',
        'ISIN',
        'HoldingName',
        'HoldingReference',
        'Units',
        'UnitPrice',
        'PriceDate',
        'HoldingCurrency',
        'HoldingMarketValueHoldingCurrency',
        'PolicyCurrency',
        'HoldingMarketValuePolicyCurrency',
    ]]


    df_PlanDetails['ValuationDate'] = df_PlanDetails['DatabaseDate']
    df_PlanDetails['PolicyNumber'] = df_PlanDetails['PlanNumber']
    df_PlanDetails['PolicyBasis'] = df_PlanDetails['PlanType']
    df_PlanDetails['Product'] = df_PlanDetails['ProductName']
    df_PlanDetails['SubPolicies'] = df_PlanDetails['PolicyCount']
    df_PlanDetails['PolicyStatus'] = df_PlanDetails['PlanStatus']
    df_PlanDetails['PolicyCurrency'] = df_PlanDetails['PlanCurrencyISO']
    df_PlanDetails['HoldingCurrency'] = df_PlanDetails['ValuationCurrencyISO']
    df_PlanDetails['PolicyStartDate'] = df_PlanDetails['CommencementDate']
    df_PlanDetails['RegularContribution'] = df_PlanDetails['Premium']
    df_PlanDetails['ContributionFrequency'] = df_PlanDetails['PremiumFrequency']
    df_PlanDetails['BrokerID'] = df_PlanDetails['IntermediaryID']
    df_PlanDetails['PolicyTerm'] = df_PlanDetails['TermInMonths']/12

    planDetails = df_PlanDetails[[
        'ValuationDate',
        'PolicyNumber',
        'PolicyBasis',
        'Product',
        'SubPolicies',
        'PolicyStatus',
        'PolicyCurrency',
        'HoldingCurrency',
        'PolicyStartDate',
        'RegularContribution',
        'ContributionFrequency',
        'BrokerID',
        'PolicyTerm',
    ]]

    df_RegularPlanDetails['ValuationDate'] = df_RegularPlanDetails['DatabaseDate']
    df_RegularPlanDetails['PolicyNumber'] = df_RegularPlanDetails['PlanNumber']
    df_RegularPlanDetails['PolicyStatus'] = df_RegularPlanDetails['PremiumStatus']
    df_RegularPlanDetails['PolicyEndDate'] = df_RegularPlanDetails['BenefitPremiumCessationDate']

    regularPlanDetails = df_RegularPlanDetails[[
        'ValuationDate',
        'PolicyNumber',
        'PolicyStatus',
        'PolicyEndDate',
    ]]

    # df_PremiumHistory['ValuationDate'] = df_PremiumHistory['DatabaseDate']
    # df_PremiumHistory['PolicyNumber'] = df_PremiumHistory['PlanNumber']
    # df_PremiumHistory['ContributionDueDate'] = df_PremiumHistory['DateExpected']
    # df_PremiumHistory['ContributionReceivedDate'] = df_PremiumHistory['DateReceived']
    # df_PremiumHistory['ContributionReceived'] = df_PremiumHistory['PremiumAmount']
    # df_PremiumHistory['ContributionCurrency'] = df_PremiumHistory['CurrencyISO']


    # premiumHistory = df_PremiumHistory[[
    #     'ValuationDate',
    #     'PolicyNumber',
    #     'ContributionDueDate',
    #     'ContributionReceivedDate',
    #     'ContributionReceived',
    #     'ContributionCurrency'
    # ]]


    # df_CashHoldings['ValuationDate'] = df_CashHoldings['DatabaseDate']
    # df_CashHoldings['PolicyNumber'] = df_CashHoldings['PlanNumber']
    # df_CashHoldings['HoldingName'] = df_CashHoldings['AccountCurrencyISO']
    # df_CashHoldings['PolicyCurrency'] = df_CashHoldings['ValuationCurrencyISO']
    # df_CashHoldings['HoldingMarketValuePolicyCurrency'] = df_CashHoldings['ValuationCurrencyValue']


    # cashHoldings = df_CashHoldings[[
    #     'ValuationDate',
    #     'PolicyNumber',
    #     'HoldingName',
    #     'PolicyCurrency',
    #     'HoldingMarketValuePolicyCurrency',
    # ]]


    # df_TransactionHistory['TransactionDate'] = df_TransactionHistory['TransactionDate']
    # df_TransactionHistory['TransactionName'] = df_TransactionHistory['TransactionType']
    # df_TransactionHistory['TransactionCurrency'] = df_TransactionHistory['MovementCurrencyISO']
    # df_TransactionHistory['TransactionValue'] = df_TransactionHistory['MovementValue']

    # transactionHistory = df_TransactionHistory[[
    #     'TransactionDate',
    #     'TransactionName',
    #     'TransactionCurrency',
    #     'TransactionValue'
    # ]]

    financial_account_lumpsum = planDetails.drop_duplicates(subset='PolicyNumber')
    # financial_account_regular = regularPlanDetails.drop_duplicates(subset='PolicyNumber')


    def convert_date_format(date_str):
        if isinstance(date_str, float) and math.isnan(date_str):
            # Replace 'nan' with today's date
            return datetime.datetime.now().strftime('%Y-%m-%d')
        elif isinstance(date_str, pd.Timestamp):
            # Convert Pandas Timestamp to datetime object
            date_obj = date_str.to_pydatetime()
            return date_obj.strftime('%Y-%m-%d')
        elif isinstance(date_str, str):
            try:
                # Try to parse the date string as it is
                date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                # Handle any other date formats here
                return datetime.datetime(1970, 1, 1).strftime('%Y-%m-%d')  # Return None for unparseable dates
        else:
            # Handle other data types
            return datetime.datetime(1970, 1, 1).strftime('%Y-%m-%d')  # Return None for unsupported data types
    
    def removeNAN(value):
        if isinstance(value, (int, float)) and math.isnan(value):
            return None
        return value

    for index,row in lumpsumHoldings.iterrows():
        data = Policy(
            provider = 'FPI Lumpsum',
            provider_id = 'a4v3H000000D22S',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(row['ValuationDate']),#
            broker_id = None,
            policy_number = row['PolicyNumber'],#
            policy_currency = row['PolicyCurrency'],#
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = None,
            holding_name = row['HoldingName'],#
            holding_currency = row['HoldingCurrency'],#
            units = row['Units'],#
            unit_price = row['UnitPrice'],#
            price_date = convert_date_format(row['PriceDate']),#
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],#
            isin = row['ISIN'],
            sedol = row['SEDOL'],#
            book_cost = row['BookCost'],#
            gain_loss = row['GainLoss'],#
            holding_reference = row['HoldingReference'],#
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
            #Added
            regular_contribution = None,#FA
            single_contribution = None,
            # policy_term = None,#FA
            contribution_frequency = None,#FA
            # contribution_due_date = row['ContributionDueDate'],
            # contribution_received_date = row['ContributionReceivedDate'],
            next_contribution_date = None,#FA
            last_contribution_date = None,#FA
            contribution_currency = None,
            withdrawals = None,
            arrears = None,
            # contribution_due = row['ContributionDue'],
            # contribution_received = row['ContributionReceived'],
            number_premiums_missed = None,#FA
            #SELF CALCULATED FIELD
            arrear_status = None,#FA
        )
        data.save()

    # for index,row in regularHolding.iterrows():
    #     data = Policy(
    #         provider = 'FPI Regular',
    #         provider_id = 'a4v3H000000D22S',
    #         db_entry_date = datetime.date.today().isoformat(),
    #         valuation_date = convert_date_format(row['ValuationDate']),#@
    #         broker_id = None,
    #         policy_number = row['PolicyNumber'],#@
    #         policy_currency = row['PolicyCurrency'],#@
    #         policy_start_date = None,
    #         policy_end_date = None,
    #         policy_status = None,
    #         product = None,
    #         holding_name = row['HoldingName'],#@
    #         holding_currency = row['HoldingCurrency'],#@
    #         units = row['Units'],#@
    #         unit_price = row['UnitPrice'],#@
    #         price_date = convert_date_format(row['PriceDate']),#@
    #         total_contribution = None,
    #         holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#@
    #         holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],#@
    #         isin = row['ISIN'],#@
    #         sedol = None,#
    #         book_cost = None,#
    #         gain_loss = None,#
    #         holding_reference = row['HoldingReference'],#@
    #         trust = None,
    #         surrender_penalty = None,
    #         max_partial_value = None,
    #         surrender_value = None,
    #         sub_policies = None,
    #         policy_basis = None,
    #         transaction_date = None,
    #         transaction_name = None,
    #         transaction_comments = None,
    #         transaction_debit_amount = None,
    #         transaction_credit_amount = None,
    #         transaction_amount = None,
    #         transaction_currency = None,
    #         #Added
    #         regular_contribution = None,#FA
    #         single_contribution = None,
    #         # policy_term = None,#FA
    #         contribution_frequency = None,#FA
    #         # contribution_due_date = row['ContributionDueDate'],
    #         # contribution_received_date = row['ContributionReceivedDate'],
    #         next_contribution_date = None,#FA
    #         last_contribution_date = None,#FA
    #         contribution_currency = None,
    #         withdrawals = None,
    #         arrears = None,
    #         # contribution_due = row['ContributionDue'],
    #         # contribution_received = row['ContributionReceived'],
    #         number_premiums_missed = None,#FA
    #         #SELF CALCULATED FIELD
    #         arrear_status = None,#FA
    #     )
    #     data.save()

    for index,row in financial_account_lumpsum.iterrows():
        data = FinancialAccountModel(
            provider = 'FPI Lumpsum',
            provider_id = 'a4v3H000000D22S',
            product = row['Product'],#
            policy_number = row['PolicyNumber'],#
            valuation_date = convert_date_format(row['ValuationDate']),#
            policy_currency = row['PolicyCurrency'],#
            policy_start_date = convert_date_format(row['PolicyStartDate']),#
            policy_end_date = None,
            sub_policies = row['SubPolicies'],#
            sub_product_type = None,
            policy_basis = row['PolicyBasis'],#
            business_split = None,
            account_status = row['PolicyStatus'],#
            regular_contribution = row['RegularContribution'],#
            lumpsum_contribution = None,
            policy_term = removeNAN(row['PolicyTerm']),#
            contribution_frequency = row['ContributionFrequency'],#
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()
    
    # for index,row in financial_account_regular.iterrows():
    #     data = FinancialAccountModel(
    #         provider = 'FPI Regular',
    #         provider_id = 'a4v3H000000D22S',
    #         product = None,#
    #         policy_number = row['PolicyNumber'],#@
    #         valuation_date = convert_date_format(row['ValuationDate']),#@
    #         policy_currency = None,#
    #         policy_start_date = None,#
    #         policy_end_date =  convert_date_format(row['PolicyEndDate']),#@
    #         sub_policies = None,#
    #         sub_product_type = None,
    #         policy_basis = None,#
    #         business_split = None,
    #         account_status = row['PolicyStatus'],#@
    #         regular_contribution = None,#
    #         lumpsum_contribution = None,
    #         policy_term = None,#
    #         contribution_frequency = None,#
    #         next_contribution_date = None,
    #         last_contribution_date = None,
    #         number_premiums_missed = None,
    #         arrear_status = None
    #     )
    #     data.save()

    return HttpResponse('Data Saved')


def fpiRegular(request):
    df_LumpsumHoldings = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractBondHolding.CSV",encoding = "ISO-8859-1")
    df_RegularHoldings = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractLifePolicyHolding.CSV",encoding = "ISO-8859-1")
    df_PlanDetails = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractPlanDetail.CSV",encoding = "ISO-8859-1")
    df_RegularPlanDetails = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractLifePolicyDetail.CSV",encoding = "ISO-8859-1")
    df_PremiumHistory = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractLifePremiumTotal.CSV",encoding = "ISO-8859-1")
    df_CashHoldings = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractBondAccount.CSV",encoding = "ISO-8859-1")
    df_TransactionHistory = pd.read_csv(r"C:\WorkSpace\Datafeeds\files\fpi\linkplus extractLifeTransactionHistory.CSV",encoding = "ISO-8859-1")

    df_LumpsumHoldings['ValuationDate'] = df_LumpsumHoldings['DatabaseDate']
    df_LumpsumHoldings['PolicyNumber'] = df_LumpsumHoldings['PlanNumber']
    df_LumpsumHoldings['HoldingName'] = df_LumpsumHoldings['InvestmentDescription']
    df_LumpsumHoldings['HoldingReference'] = df_LumpsumHoldings['PlanNumber'].astype('str')+'-'+df_LumpsumHoldings['InvestmentDescription']
    df_LumpsumHoldings['SEDOL'] = df_LumpsumHoldings['FundCode']
    df_LumpsumHoldings['ISIN'] = df_LumpsumHoldings['FundISINCode']
    df_LumpsumHoldings['Units'] = df_LumpsumHoldings['UnitsHeld']
    df_LumpsumHoldings['UnitPrice'] = df_LumpsumHoldings['UnitPrice']
    df_LumpsumHoldings['PriceDate'] = df_LumpsumHoldings['PriceDate']
    df_LumpsumHoldings['HoldingCurrency'] = df_LumpsumHoldings['InvestmentCurrencyISO']
    df_LumpsumHoldings['HoldingMarketValueHoldingCurrency'] = df_LumpsumHoldings['InvestmentCurrencyValue']
    df_LumpsumHoldings['PolicyCurrency'] = df_LumpsumHoldings['ValuationCurrencyISO']
    df_LumpsumHoldings['HoldingMarketValuePolicyCurrency'] = df_LumpsumHoldings['ValuationCurrencyValue']
    df_LumpsumHoldings['BookCost'] = df_LumpsumHoldings['BookCost']
    df_LumpsumHoldings['GainLoss'] = (df_LumpsumHoldings['ValuationCurrencyValue']/df_LumpsumHoldings['BookCost'])-1

    lumpsumHoldings = df_LumpsumHoldings[[
        'ValuationDate',
        'PolicyNumber',
        'HoldingName',
        'HoldingReference',
        'SEDOL',
        'ISIN',
        'Units',
        'UnitPrice',
        'PriceDate',
        'HoldingCurrency',
        'HoldingMarketValueHoldingCurrency',
        'PolicyCurrency',
        'HoldingMarketValuePolicyCurrency',
        'BookCost',
        'GainLoss'
    ]]

    df_RegularHoldings['ValuationDate'] = df_RegularHoldings['DatabaseDate']
    df_RegularHoldings['PolicyNumber'] = df_RegularHoldings['PlanNumber']
    df_RegularHoldings['ISIN'] = df_RegularHoldings['FundISINCode']
    df_RegularHoldings['HoldingName'] = df_RegularHoldings['FundDescription']
    df_RegularHoldings['HoldingReference'] = df_RegularHoldings['PlanNumber'].astype('str')+'-'+df_RegularHoldings['FundDescription']
    df_RegularHoldings['Units'] = df_RegularHoldings['UnitsHeld']
    df_RegularHoldings['UnitPrice'] = df_RegularHoldings['BidPrice']
    df_RegularHoldings['PriceDate'] = df_RegularHoldings['PriceDate']
    df_RegularHoldings['HoldingCurrency'] = df_RegularHoldings['FundCurrencyISO']
    df_RegularHoldings['HoldingMarketValueHoldingCurrency'] = df_RegularHoldings['FundCurrencyValue']
    df_RegularHoldings['PolicyCurrency'] = df_RegularHoldings['ValuationCurrencyISO']
    df_RegularHoldings['HoldingMarketValuePolicyCurrency'] = df_RegularHoldings['ValuationCurrencyValue']


    regularHolding = df_RegularHoldings[[
        'ValuationDate',
        'PolicyNumber',
        'ISIN',
        'HoldingName',
        'HoldingReference',
        'Units',
        'UnitPrice',
        'PriceDate',
        'HoldingCurrency',
        'HoldingMarketValueHoldingCurrency',
        'PolicyCurrency',
        'HoldingMarketValuePolicyCurrency',
    ]]


    df_PlanDetails['ValuationDate'] = df_PlanDetails['DatabaseDate']
    df_PlanDetails['PolicyNumber'] = df_PlanDetails['PlanNumber']
    df_PlanDetails['PolicyBasis'] = df_PlanDetails['PlanType']
    df_PlanDetails['Product'] = df_PlanDetails['ProductName']
    df_PlanDetails['SubPolicies'] = df_PlanDetails['PolicyCount']
    df_PlanDetails['PolicyStatus'] = df_PlanDetails['PlanStatus']
    df_PlanDetails['PolicyCurrency'] = df_PlanDetails['PlanCurrencyISO']
    df_PlanDetails['HoldingCurrency'] = df_PlanDetails['ValuationCurrencyISO']
    df_PlanDetails['PolicyStartDate'] = df_PlanDetails['CommencementDate']
    df_PlanDetails['RegularContribution'] = df_PlanDetails['Premium']
    df_PlanDetails['ContributionFrequency'] = df_PlanDetails['PremiumFrequency']
    df_PlanDetails['BrokerID'] = df_PlanDetails['IntermediaryID']
    df_PlanDetails['PolicyTerm'] = df_PlanDetails['TermInMonths']/12

    planDetails = df_PlanDetails[[
        'ValuationDate',
        'PolicyNumber',
        'PolicyBasis',
        'Product',
        'SubPolicies',
        'PolicyStatus',
        'PolicyCurrency',
        'HoldingCurrency',
        'PolicyStartDate',
        'RegularContribution',
        'ContributionFrequency',
        'BrokerID',
        'PolicyTerm',
    ]]

    df_RegularPlanDetails['ValuationDate'] = df_RegularPlanDetails['DatabaseDate']
    df_RegularPlanDetails['PolicyNumber'] = df_RegularPlanDetails['PlanNumber']
    df_RegularPlanDetails['PolicyStatus'] = df_RegularPlanDetails['PremiumStatus']
    df_RegularPlanDetails['PolicyEndDate'] = df_RegularPlanDetails['BenefitPremiumCessationDate']

    regularPlanDetails = df_RegularPlanDetails[[
        'ValuationDate',
        'PolicyNumber',
        'PolicyStatus',
        'PolicyEndDate',
    ]]

    # df_PremiumHistory['ValuationDate'] = df_PremiumHistory['DatabaseDate']
    # df_PremiumHistory['PolicyNumber'] = df_PremiumHistory['PlanNumber']
    # df_PremiumHistory['ContributionDueDate'] = df_PremiumHistory['DateExpected']
    # df_PremiumHistory['ContributionReceivedDate'] = df_PremiumHistory['DateReceived']
    # df_PremiumHistory['ContributionReceived'] = df_PremiumHistory['PremiumAmount']
    # df_PremiumHistory['ContributionCurrency'] = df_PremiumHistory['CurrencyISO']


    # premiumHistory = df_PremiumHistory[[
    #     'ValuationDate',
    #     'PolicyNumber',
    #     'ContributionDueDate',
    #     'ContributionReceivedDate',
    #     'ContributionReceived',
    #     'ContributionCurrency'
    # ]]


    # df_CashHoldings['ValuationDate'] = df_CashHoldings['DatabaseDate']
    # df_CashHoldings['PolicyNumber'] = df_CashHoldings['PlanNumber']
    # df_CashHoldings['HoldingName'] = df_CashHoldings['AccountCurrencyISO']
    # df_CashHoldings['PolicyCurrency'] = df_CashHoldings['ValuationCurrencyISO']
    # df_CashHoldings['HoldingMarketValuePolicyCurrency'] = df_CashHoldings['ValuationCurrencyValue']


    # cashHoldings = df_CashHoldings[[
    #     'ValuationDate',
    #     'PolicyNumber',
    #     'HoldingName',
    #     'PolicyCurrency',
    #     'HoldingMarketValuePolicyCurrency',
    # ]]


    # df_TransactionHistory['TransactionDate'] = df_TransactionHistory['TransactionDate']
    # df_TransactionHistory['TransactionName'] = df_TransactionHistory['TransactionType']
    # df_TransactionHistory['TransactionCurrency'] = df_TransactionHistory['MovementCurrencyISO']
    # df_TransactionHistory['TransactionValue'] = df_TransactionHistory['MovementValue']

    # transactionHistory = df_TransactionHistory[[
    #     'TransactionDate',
    #     'TransactionName',
    #     'TransactionCurrency',
    #     'TransactionValue'
    # ]]

    financial_account_regular = planDetails.drop_duplicates(subset='PolicyNumber')


    def convert_date_format(date_str):
        if isinstance(date_str, float) and math.isnan(date_str):
            # Replace 'nan' with today's date
            return datetime.datetime.now().strftime('%Y-%m-%d')
        elif isinstance(date_str, pd.Timestamp):
            # Convert Pandas Timestamp to datetime object
            date_obj = date_str.to_pydatetime()
            return date_obj.strftime('%Y-%m-%d')
        elif isinstance(date_str, str):
            try:
                # Try to parse the date string as it is
                date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                # Handle any other date formats here
                return datetime.datetime(1970, 1, 1).strftime('%Y-%m-%d')  # Return None for unparseable dates
        else:
            # Handle other data types
            return datetime.datetime(1970, 1, 1).strftime('%Y-%m-%d')  # Return None for unsupported data types
    
    def removeNAN(value):
        if isinstance(value, (int, float)) and math.isnan(value):
            return None
        return value


    for index,row in regularHolding.iterrows():
        data = Policy(
            provider = 'FPI Regular',
            provider_id = 'a4v3H000000D22S',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(row['ValuationDate']),#@
            broker_id = None,
            policy_number = row['PolicyNumber'],#@
            policy_currency = row['PolicyCurrency'],#@
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = None,
            holding_name = row['HoldingName'],#@
            holding_currency = row['HoldingCurrency'],#@
            units = row['Units'],#@
            unit_price = row['UnitPrice'],#@
            price_date = convert_date_format(row['PriceDate']),#@
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#@
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],#@
            isin = row['ISIN'],#@
            sedol = None,#
            book_cost = None,#
            gain_loss = None,#
            holding_reference = row['HoldingReference'],#@
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
            #Added
            regular_contribution = None,#FA
            single_contribution = None,
            # policy_term = None,#FA
            contribution_frequency = None,#FA
            # contribution_due_date = row['ContributionDueDate'],
            # contribution_received_date = row['ContributionReceivedDate'],
            next_contribution_date = None,#FA
            last_contribution_date = None,#FA
            contribution_currency = None,
            withdrawals = None,
            arrears = None,
            # contribution_due = row['ContributionDue'],
            # contribution_received = row['ContributionReceived'],
            number_premiums_missed = None,#FA
            #SELF CALCULATED FIELD
            arrear_status = None,#FA
        )
        data.save()
    
    for index,row in financial_account_regular.iterrows():
        data = FinancialAccountModel(
            provider = 'FPI Lumpsum',
            provider_id = 'a4v3H000000D22S',
            product = row['Product'],#
            policy_number = row['PolicyNumber'],#
            valuation_date = convert_date_format(row['ValuationDate']),#
            policy_currency = row['PolicyCurrency'],#
            policy_start_date = convert_date_format(row['PolicyStartDate']),#
            policy_end_date = None,
            sub_policies = row['SubPolicies'],#
            sub_product_type = None,
            policy_basis = row['PolicyBasis'],#
            business_split = None,
            account_status = row['PolicyStatus'],#
            regular_contribution = row['RegularContribution'],#
            lumpsum_contribution = None,
            policy_term = removeNAN(row['PolicyTerm']),#
            contribution_frequency = row['ContributionFrequency'],#
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()


    return HttpResponse('Data Saved')


def hansard(request):
    subject_name = 'Datafeeds_Provider_HANSARD'
    filename = collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
    print(filename)
    # df = pd.read_csv(filename,encoding = "ISO-8859-1")
    df = pd.read_csv(r'C:\WorkSpace\Datafeeds\files\DataFeeds\HansardLS17213.csv',encoding = "ISO-8859-1")
    # df = pd.read_excel(r'C:\WorkSpace\Datafeeds\files\DataFeeds\Hansard.xlsx')
    # df = pd.read_excel('https://skybound-client-app.s3.eu-north-1.amazonaws.com/Hansard+Lumpsum.xlsx')

    df['PolicyNumber'] = df['Policy Number']
    df['HoldingName'] = df['Asset Name']
    df['HoldingReference'] = df['PolicyNumber'] +'-'+df['HoldingName']
    df['SEDOL'] = df['Asset Code']
    df['Units'] = df['Units']
    df['HoldingCurrency'] = df['Price Currency']
    df['UnitPrice'] = df['Price']
    df['PriceDate'] = df['Price Date']
    df['HoldingMarketValueHoldingCurrency'] = df['Value in Price Currency']
    df['BookCost'] = df['Book Cost']
    df['GainLoss'] = df['Percent']
    df['PolicyCurrency'] = df['Policy Currency']
    df['HoldingMarketValuePolicyCurrency'] = df['Value In Policy Currency']
    df['ValuationDate'] = df['Valuation Date']

    new_df = df[['PolicyNumber',
                'HoldingName',
                'HoldingReference',
                'SEDOL',
                'Units',
                'HoldingCurrency',
                'UnitPrice',
                'PriceDate',
                'HoldingMarketValueHoldingCurrency',
                'BookCost',
                'GainLoss',
                'PolicyCurrency',
                'HoldingMarketValuePolicyCurrency',
                'ValuationDate'
                ]]
    financial_account = df.drop_duplicates(subset='PolicyNumber')[[
                'PolicyNumber',
                'HoldingName',
                'HoldingReference',
                'SEDOL',
                'Units',
                'HoldingCurrency',
                'UnitPrice',
                'PriceDate',
                'HoldingMarketValueHoldingCurrency',
                'BookCost',
                'GainLoss',
                'PolicyCurrency',
                'HoldingMarketValuePolicyCurrency',
                'ValuationDate'
                ]]

    def convert_date_format(date_str):
        if isinstance(date_str, float) and math.isnan(date_str):
            # Replace 'nan' with today's date
            date_str = datetime.date.today().strftime('%Y-%m-%d')
        elif isinstance(date_str, pd.Timestamp):
            # Convert Pandas Timestamp to datetime object
            date_obj = date_str.to_pydatetime()
            
            # Extract the date component
            date_str = date_obj.date().strftime('%Y-%m-%d')
        else:
            # Convert input string to datetime object
            date_obj = datetime.datetime.strptime(date_str, "%d/%m/%Y")
            
            # Extract the date component
            date_str = date_obj.date().strftime('%Y-%m-%d')

        return date_str

    # new_df.to_excel('Generated_Hansard_Lumpsum.xlsx',index=False)
    for index,row in new_df.iterrows():
        data = Policy(
            provider = 'Hansard',
            provider_id = 'a4v3H000000D227',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = row['ValuationDate'],#
            broker_id = None,
            policy_number = row['PolicyNumber'],#
            policy_currency = row['PolicyCurrency'],#
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = None,
            holding_name = row['HoldingName'],#
            holding_currency = row['HoldingCurrency'],#
            units = row['Units'],#
            unit_price = row['UnitPrice'],#
            price_date = row['PriceDate'],#
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],#
            isin = None,
            sedol = row['SEDOL'],#
            book_cost = row['BookCost'],#
            gain_loss = row['GainLoss'],#
            holding_reference = row['HoldingReference'],#
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
        )
        data.save()
    
    for index,row in financial_account.iterrows():
        data = FinancialAccountModel(
            provider = 'Hansard',
            provider_id = 'a4v3H000000D227',
            product = None,
            policy_number = row['PolicyNumber'],
            valuation_date = row['ValuationDate'],
            policy_currency = row['PolicyCurrency'],
            policy_start_date = None,
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = None,
            regular_contribution = None,
            lumpsum_contribution = None,
            policy_term = None,
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()
    
    return HttpResponse('Hansard Data Saved')

def providence(request):
    # subject_name = 'Datafeeds_Provider_PROVIDENCE'
    # filename = collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
    # print(filename)
    # df = pd.read_excel(filename)
    
    host = 'sftp43.providence.quatrix.it'
    username = 'q9074433'
    password = 'm*deufWGnAh.xn_cCmk6pz_FW'
    remote_directory = '/Skybound UAE/230706##SkyBoundUAE.csv'
    local_directory = '/Users/dexter/Documents/Workspace/Skybound/Datafeeds/services/providence.csv'

    # # Connect to the SFTP server
    # with pysftp.Connection(host, username=username, password=password) as sftp:
    #     # Download the file
    #     sftp.get('/Skybound UAE/230706##SkyBoundUAE.csv', '/Users/dexter/Documents/Workspace/Skybound/Datafeeds/services/providence.csv')
    # print('File downloaded successfully.')

    # with pysftp.Connection(host, username=username, password=password) as sftp:
    #     # Change directory to the desired folder
    #     sftp.cwd('/Skybound UAE')
        
    #     # Get the list of files in the remote folder
    #     file_list = sftp.listdir()
        
    #     # Sort the file list by modified time in descending order
    #     file_list.sort(key=lambda x: sftp.stat(x).st_mtime, reverse=True)
        
    #     if file_list:
    #         latest_file = file_list[0]  # Get the latest file in the folder
            
    #         # Specify the local path to save the downloaded file
    #         local_file_path = '/Users/dexter/Documents/Workspace/Skybound/Datafeeds/services/' + latest_file
            
    #         # Download the latest file
    #         sftp.get(latest_file, local_file_path)
            
    #         print('Latest file downloaded successfully:', latest_file)
    #     else:
    #         print('No files found in the remote folder.')

    # filename = process_sftp_file()
    filename = r'C:\WorkSpace\Datafeeds\services\231026##SkyBoundUAE.csv'
    print(filename)
    df = pd.read_csv(filename,encoding = "ISO-8859-1")
    # df = pd.read_excel('https://skybound-client-app.s3.eu-north-1.amazonaws.com/Providence.xlsx')

    df['PolicyNumber'] = df['Policy ID']
    df['PolicyStatus'] = df[' Status Value']
    df['PolicyCurrency'] = df[' PolicyCurrency']
    df['Product'] = df[' Product']
    df['Trust'] = df[' Trust']
    df['HoldingName'] = df[' Fund Name']
    df['HoldingReference'] = df['PolicyNumber'] +'-'+df['HoldingName']
    df['ISIN'] = df[' ISIN']
    df['HoldingCurrency'] = df[' Fund Currency']
    df['Units'] = df[' Units']
    df['UnitPrice'] = df[' Price']
    df['HoldingMarketValueHoldingCurrency'] = df[' Value in Fund Currency']
    df['HoldingMarketValuePolicyCurrency'] = df[' Value in Policy Currency']
    df['ValuationDate'] = df[' Current Date']
    df['BrokerID'] = df[' Adviser ID']

    new_df = df[['PolicyNumber',
                'PolicyStatus',
                'PolicyCurrency',
                'Product',
                'Trust',
                'HoldingName',
                'HoldingReference',
                'ISIN',
                'HoldingCurrency',
                'Units',
                'UnitPrice',
                'HoldingMarketValueHoldingCurrency',
                'HoldingMarketValuePolicyCurrency',
                'ValuationDate',
                'BrokerID',
                ]]
    financial_account = df.drop_duplicates(subset='PolicyNumber')[['PolicyNumber',
                'PolicyStatus',
                'PolicyCurrency',
                'Product',
                'Trust',
                'HoldingName',
                'HoldingReference',
                'ISIN',
                'HoldingCurrency',
                'Units',
                'UnitPrice',
                'HoldingMarketValueHoldingCurrency',
                'HoldingMarketValuePolicyCurrency',
                'ValuationDate',
                'BrokerID',
                ]]

    def convert_date_format(input_date):
        input_format = "%Y-%m-%d %H:%M:%S"
        output_format = "%Y-%m-%d"
        
        input_datetime = datetime.datetime.strptime(input_date, input_format)
        output_date = input_datetime.strftime(output_format)
        
        return output_date

    new_df.to_excel('Generated_Providence.xlsx',index=False)
    for index,row in new_df.iterrows():
        data = Policy(
            provider = 'Providence',
            provider_id = 'a4v3H000000D228',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(row['ValuationDate']),#14
            broker_id = row['BrokerID'],#15
            policy_number = row['PolicyNumber'],#1
            policy_currency = row['PolicyCurrency'],#3
            policy_start_date = None,
            policy_end_date = None,
            policy_status = row['PolicyStatus'],#2
            product = row['Product'],#4
            holding_name = row['HoldingName'],#6
            holding_currency = row['HoldingCurrency'],#9
            units = row['Units'],#10
            unit_price = row['UnitPrice'],#11
            price_date = None,
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#12
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],#13
            isin = row['ISIN'],#8
            sedol = None,
            book_cost = None,
            gain_loss = None,
            holding_reference = row['HoldingReference'],#7
            trust = row['Trust'],#5
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
        )
        data.save()

    for index,row in financial_account.iterrows():
        data = FinancialAccountModel(
            provider = 'Providence',
            provider_id = 'a4v3H000000D228',
            product = row['Product'],
            policy_number = row['PolicyNumber'],
            valuation_date = convert_date_format(row['ValuationDate']),
            policy_currency = row['PolicyCurrency'],
            policy_start_date = None,
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = row['PolicyStatus'],
            regular_contribution = None,
            lumpsum_contribution = None,
            policy_term = None,
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()


    return HttpResponse('Data Saved')

def praemium(request):
    subject_name = 'Datafeeds_Provider_PRAEMIUM'
    filename = collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
    print(filename)
    df = pd.read_csv(filename,encoding = "ISO-8859-1")
    # df = pd.read_excel('https://skybound-client-app.s3.eu-north-1.amazonaws.com/Praemium.xlsx')

    df['PolicyNumber'] = df['PraemiumPortfolioID']
    df['ISIN'] = df['ISIN']
    df['HoldingName'] = df['Description']
    df['HoldingReference'] = df['PolicyNumber']+'-'+ df['HoldingName']
    df['HoldingCurrency'] = df['asset_currency']
    df['Units'] = df['qty']
    df['UnitPrice'] = df['UnitValue']
    df['PriceDate'] = df['PriceDate']
    df['BookCost'] = df['Cost']
    df['HoldingMarketValueHoldingCurrency'] = df['Value']
    df['GainLoss '] = df['GainLoss']/df['Cost']


    new_df = df[['PolicyNumber',
                'ISIN',
                'HoldingName',
                'HoldingReference',
                'HoldingCurrency',
                'Units',
                'UnitPrice',
                'PriceDate',
                'BookCost',
                'HoldingMarketValueHoldingCurrency',
                'GainLoss',
                ]]
    financial_account = df.drop_duplicates(subset='PolicyNumber')[[
                'PolicyNumber',
                'ISIN',
                'HoldingName',
                'HoldingReference',
                'HoldingCurrency',
                'Units',
                'UnitPrice',
                'PriceDate',
                'BookCost',
                'HoldingMarketValueHoldingCurrency',
                'GainLoss',
                ]]

    def time(index,time):
        if type(time) == float:
            print('nan',index,time,type(time))
            return datetime.date.today().isoformat()
        else:
            print(index,time,type(time))
            return datetime.datetime.strptime(str(time), "%d/%m/%Y").strftime("%Y-%m-%d")

    new_df.to_excel('Generated_Praemium.xlsx',index=False)
    for index,row in new_df.iterrows():
        data = Policy(
            provider = 'Praemium',
            provider_id = 'a4v3H000000D20n',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = None,
            broker_id = None,
            policy_number = row['PolicyNumber'],#1
            policy_currency = None,
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = None,
            holding_name = row['HoldingName'],#3
            holding_currency = row['HoldingCurrency'],#5
            units = row['Units'],#6
            unit_price = row['UnitPrice'],#7
            price_date = time(index,row['PriceDate']),#8
            # price_date = datetime.datetime.strptime(str(row['PriceDate']), "%d/%m/%Y").strftime("%Y-%m-%d"),#8
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#10
            holding_market_value_policy_currency = None,
            isin = row['ISIN'],#2
            sedol = None,
            book_cost = row['BookCost'],#9
            gain_loss = row['GainLoss'],#11
            holding_reference = row['HoldingReference'],#4
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
        )
        data.save()
    for index,row in financial_account.iterrows():
        data = FinancialAccountModel(
            provider = 'Praemium',
            provider_id = 'a4v3H000000D20n',
            product = None,
            policy_number = row['PolicyNumber'],
            valuation_date = None,
            policy_currency = None,
            policy_start_date = None,
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = None,
            regular_contribution = None,
            lumpsum_contribution = None,
            policy_term = None,
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
        )
        data.save()
    return HttpResponse('Praemium Data Saved')

def seb(request):
    subject_name = 'Datafeeds_Provider_SEB'
    filename = collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
    print(filename)
    # df = pd.read_csv(filename,encoding = "ISO-8859-1")
    df = pd.read_excel(r'C:\WorkSpace\Datafeeds\files\DataFeeds\SEB Main Sept 2023.xls')

    df['PolicyNumber'] = df['Policy Number']
    df['HoldingName'] = df['Asset Name']
    df['Units'] = df['Units']
    df['UnitPrice'] = df['Price']
    df['HoldingCurrency'] = df['Asset Currency']
    df['HoldingMarketValueHoldingCurrency'] = df['Value In Asset Currency']
    df['PolicyCurrency'] = df['Policy Currency']
    df['HoldingMarketValuePolicyCurrency'] = df['Value In Policy Currency']
    df['ISIN'] = df['ISIN Code']
    df['PriceDate'] = df['Price Date']
    df['ValuationDate'] = df['Valuation Date']



    new_df = df[['PolicyNumber',
                'HoldingName',
                'Units',
                'UnitPrice',
                'HoldingCurrency',
                'HoldingMarketValueHoldingCurrency',
                'PolicyCurrency',
                'HoldingMarketValuePolicyCurrency',
                'ISIN',
                'PriceDate',
                'ValuationDate',
                ]]
    df.dropna(subset=['PolicyNumber'], inplace=True)
    df.drop_duplicates(subset='PolicyNumber', inplace=True)
    financial_account = df[[
                'PolicyNumber',
                'HoldingName',
                'Units',
                'UnitPrice',
                'HoldingCurrency',
                'HoldingMarketValueHoldingCurrency',
                'PolicyCurrency',
                'HoldingMarketValuePolicyCurrency',
                'ISIN',
                'PriceDate',
                'ValuationDate',
                ]]
    
    # financial_account.to_excel('Generated_SEB_UNIQUE_PR.xlsx',index=False)

    def time(index, time):
        if isinstance(time, float) and math.isnan(time):
            print('nan', index, time, type(time))
            return datetime.date.today().isoformat()
        else:
            print(index, time, type(time))
            try:
                date_object = datetime.datetime.strptime(str(time), "%d/%m/%Y")
                return date_object.strftime("%Y-%m-%d")
            except ValueError:
                # Handle any other invalid date formats here
                return None    
            
    def convert_date_format(index,typeVal,date_string):
        print(index,typeVal,type(date_string),date_string)
        if type(date_string) == float:
            print('nan',index,date_string,type(date_string))
            return datetime.date.today().isoformat()
        else:
            try:
                date_object = datetime.datetime.strptime(date_string, "%d/%m/%y")
                year = date_object.strftime("%Y")
                month = date_object.strftime("%m")
                day = date_object.strftime("%d")
                return timezone.datetime(int(year), int(month), int(day)).date()
            except ValueError:
                return None

    # new_df.to_excel('Generated_SEB.xlsx',index=False)
    for index,row in new_df.iterrows():
        data = Policy(
            provider = 'SEB',
            provider_id = 'a4v3H000000D22B',
            db_entry_date = datetime.date.today().isoformat(),
            valuation_date = convert_date_format(index,'valuation',row['ValuationDate']),#11
            broker_id = None,
            policy_number = row['PolicyNumber'],#1
            policy_currency = row['PolicyCurrency'],#7
            policy_start_date = None,
            policy_end_date = None,
            policy_status = None,
            product = None,
            holding_name = row['HoldingName'],#2
            holding_currency = row['HoldingCurrency'],#5
            units = row['Units'],#3
            unit_price = row['UnitPrice'],#4
            price_date = convert_date_format(index,'price',row['PriceDate']),#10
            total_contribution = None,
            holding_market_value_holding_currency = row['HoldingMarketValueHoldingCurrency'],#6
            holding_market_value_policy_currency = row['HoldingMarketValuePolicyCurrency'],#8
            isin = row['ISIN'],#9
            sedol = None,
            book_cost = None,
            gain_loss = None,
            holding_reference = None,
            trust = None,
            surrender_penalty = None,
            max_partial_value = None,
            surrender_value = None,
            sub_policies = None,
            policy_basis = None,
            transaction_date = None,
            transaction_name = None,
            transaction_comments = None,
            transaction_debit_amount = None,
            transaction_credit_amount = None,
            transaction_amount = None,
            transaction_currency = None,
        )
        data.save()

    for index,row in financial_account.iterrows():
        print(row['PolicyNumber'])
        data = FinancialAccountModel(
            provider = 'SEB',
            provider_id = 'a4v3H000000D22B',
            product = None,
            policy_number = row['PolicyNumber'],
            valuation_date = convert_date_format(index,'valuation',row['ValuationDate']),
            policy_currency = row['PolicyCurrency'],
            policy_start_date = None,
            policy_end_date = None,
            sub_policies = None,
            sub_product_type = None,
            policy_basis = None,
            business_split = None,
            account_status = None,
            regular_contribution = None,
            lumpsum_contribution = None,
            policy_term = None,
            contribution_frequency = None,
            next_contribution_date = None,
            last_contribution_date = None,
            number_premiums_missed = None,
            arrear_status = None
            )
        data.save()

    return HttpResponse('SEB Data Saved')

class FinancialAccount(View):
    def get(self, request):
        # Authentication API
        auth_url = "https://test.salesforce.com/services/oauth2/token?grant_type=password&client_id=3MVG9LlLrkcRhGHZ75_1zzxBX1noA2B0HBuVoIwSwoNrX__w00o1E_3n53HcLWWdLv._kdf9MAtZ0NBYuAHMb&client_secret=AF350B7DB79F3912680AE6621854216E82C39720DE8A906EE6D9B9D5AC092315&username=marwan.elsadat@skybound.staging&password=Coberg@123"
        auth_headers = {
            # 'Authorization': 'Bearer 00D3H0000000NiN!ARcAQHBhtgrTFpfzY_2E71gCc1sOoARYkVxC1TznMjJnAaUBZF3PUO84_RFCiVMTkpf13qn8hs25RCEbH0akQKwehgqy6By8'
        }

        auth_response = requests.post(auth_url, headers=auth_headers)
        access_token = auth_response.json().get('access_token')
        # print(access_token)
        # return HttpResponse(access_token)

        def validString(value):
            if value is None:
                return ""
            return str(value)
        def validDate(value):
            if value is None:
                return "1971-01-01"
            return str(value)
        def validNumber(value):
            if value is None:
                    return 0
            elif isinstance(value, str):
                try:
                    return float(value)
                except ValueError:
                    return None
                else:
                    return value


        if access_token:
            # Financial Account API
            financial_account_url = "https://swm3--staging.sandbox.my.salesforce.com/services/apexrest/DataFeeds/FinancialAccount/*"
            financial_account_headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + access_token
            }

            policies = FinancialAccountModel.objects.all()  # Retrieve all policies from the database
            batch_size = 1000
            total_policies = policies.count()
            num_batches = (total_policies + batch_size - 1) // batch_size  # Calculate number of batches

            for i in range(num_batches):
                start_index = i * batch_size
                end_index = (i + 1) * batch_size
                batch_policies = policies[start_index:end_index]

                # Prepare the payload for the current batch
                payload = {
                    "FinancialAccounts": []
                }

                for policy in batch_policies:
                    # Customize the payload creation according to your model fields
                    print(policy.product)
                    financial_account = {
                        "id": policy.policy_number,
                        "PolicyDetails": {
                            "PolicyNumber": policy.policy_number,
                            "Provider": "",#policy.provider_id,
                            "Product": "",#get_product_id(policy.product),
                            "SubProductType":validString(policy.sub_product_type),#"testHJ24",
                            "Basis":validString(policy.policy_basis),#"testHJ241"
                            "SubPolicies":validString(policy.sub_policies),#"testHJ"
                            "UsedCurrency":validString(policy.policy_currency),#"USD"
                            "BusinessSplit":validString(policy.business_split),#"Regular Savings Plan",#
                            "AccountStatus":validString(policy.account_status),#"Open"
                            "CommencmentDate":validDate(policy.policy_start_date),#"2018-12-12",#
                            "MaturityDate":validDate(policy.policy_end_date),#"2023-07-12",#
                            # Add more fields from the Policy model as needed
                        },
                        "HoldingsDetails": {
                            "LastValuationDate": validDate(policy.valuation_date),#"2023-07-12",#
                        },
                        "ContributionInformation": {

                            "LumpSumContribution": policy.lumpsum_contribution,#"9090",#

                            "CommencementDate": validDate(policy.policy_start_date),

                            "MaturityDate": validDate(policy.policy_end_date),

                            "RegularContribution": policy.regular_contribution,#"2770",#

                            "ContributionFrequency": policy.contribution_frequency,#"4",#

                            "PolicyTerm": policy.policy_term,#"testHJ",#

                            "PremiumHoliday Status": "Active",

                            "PremiumHoliday Commencement Date": validDate(policy.premium_holiday_commencement_date),

                            "PremiumHolidayEndDate": validDate(policy.premium_holiday_end_date),

                            "LastContributionDate": validDate(policy.last_contribution_date),

                            "NextContributionDate": validDate(policy.next_contribution_date),

                            "NumberofPremiumsMissed": policy.number_premiums_missed,

                            "ArrearsStatus": policy.arrear_status

                        }
                        # Add more sections (HoldingsDetails, ContributionInformation) and fields as needed
                    }
                    print(financial_account)
                    payload["FinancialAccounts"].append(financial_account)

                response = requests.post(financial_account_url, headers=financial_account_headers, json=payload)
                # print(payload)
                # Handle the response as needed (e.g., log or check for errors)
                if response.status_code == 200:
                    print("Batch", i+1, "successfully processed.")
                else:
                    print("Error:", response.text)

            return HttpResponse(response)
        
        return HttpResponse("Authentication failed!")

class Holdings(View):
    def get(self, request):
        # Authentication API
        auth_url = "https://test.salesforce.com/services/oauth2/token?grant_type=password&client_id=3MVG9LlLrkcRhGHZ75_1zzxBX1noA2B0HBuVoIwSwoNrX__w00o1E_3n53HcLWWdLv._kdf9MAtZ0NBYuAHMb&client_secret=AF350B7DB79F3912680AE6621854216E82C39720DE8A906EE6D9B9D5AC092315&username=marwan.elsadat@skybound.staging&password=Coberg@123"
        auth_headers = {}

        auth_response = requests.post(auth_url, headers=auth_headers)
        access_token = auth_response.json().get('access_token')

        def validString(value):
            if value is None or (isinstance(value, float) and math.isnan(value)):
                return ""
            elif value == 'nan':
                return ""
            else:
                return str(value)
        def validDate(value):
            if value is None:
                return "1971-01-01"
            return str(value)
        def validNumber(value):
            if value is None:
                    return ""
            elif isinstance(value, str):
                try:
                    return float(value)
                except ValueError:
                    return None
            else:
                return float(value)
            
        def removeNAN(value):
            if isinstance(value, (int, float)) and math.isnan(value):
                return ''
            return value

        if access_token:
            # Holdings API
            holdings_url = "https://swm3--staging.sandbox.my.salesforce.com/services/apexrest/DataFeeds/Holdings/*"
            holdings_headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer ' + access_token
            }

            financial_accounts = FinancialAccountModel.objects.all()

            for financial_account in financial_accounts:
                policy_number = financial_account.policy_number

                if policy_number:
                    holdings = Policy.objects.filter(policy_number=policy_number)

                    batch_size = 1000
                    total_holdings = holdings.count()
                    num_batches = (total_holdings + batch_size - 1) // batch_size  # Calculate number of batches

                    for i in range(num_batches):
                        start_index = i * batch_size
                        end_index = (i + 1) * batch_size
                        batch_holdings = holdings[start_index:end_index]

                        # Prepare the payload for the current batch
                        payload = {
                            "FinancialAccount": financial_account.policy_number,
                            "Holdings": []
                        }

                        for index, holding in enumerate(batch_holdings):
                            # Customize the payload creation according to your model fields
                            
                            holding_data = {
                                "id": f"{holding.policy_number}-{index}",
                                "HoldingReference": holding.holding_reference,
                                "Quantity": removeNAN(holding.units),
                                "UsedCurrency": validString(holding.holding_currency),
                                "UnitPrice": holding.unit_price,
                                "PriceDate": validDate(holding.price_date),
                                "MarketValue": holding.holding_market_value_holding_currency,
                                "Cost": removeNAN(holding.book_cost),
                                "Gain/Loss": validString(holding.gain_loss)
                            }

                            payload["Holdings"].append(holding_data)
                            # print(holding_data)
                        
                        if Policy.objects.filter(policy_number=policy_number).exists():
                            response = requests.post(holdings_url, headers=holdings_headers, json=payload)
                            # Handle the response as needed (e.g., log or check for errors)
                            if response.status_code == 201:
                                print("Batch", i+1, "successfully processed.",financial_account.policy_number)
                            else:
                                print("Error:",response.status_code, response.text)
                        
                        # timeset.sleep(2)  # Add a 2-second delay between requests

            return HttpResponse("Data sent successfully!")

        return HttpResponse("Authentication failed!")
