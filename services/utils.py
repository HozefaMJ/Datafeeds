# import requests
# import datetime

# def collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id):
#     # Get today's date
#     today = datetime.date.today()
#     today_iso_format = today.isoformat()

#     # Authenticate and obtain an access token
#     token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
#     headers = {'Content-Type': 'application/x-www-form-urlencoded'}

#     data = {
#         'grant_type': 'client_credentials',
#         'client_id': client_id,
#         'client_secret': client_secret,
#         'scope': 'https://graph.microsoft.com/.default'
#     }

#     response = requests.post(token_url, headers=headers, data=data)
#     access_token = response.json()['access_token']

#     # Fetch emails with the specific subject received today
#     api_url = f'https://graph.microsoft.com/v1.0/users/{email_address}/messages'
#     headers = {
#         'Authorization': f'Bearer {access_token}',
#         'Content-Type': 'application/json'
#     }

#     params = {
#         '$filter': f"subject eq '{subject_name}' and receivedDateTime ge {today_iso_format}",
#         '$select': 'id'
#     }

#     response = requests.get(api_url, headers=headers, params=params)
#     emails = response.json()['value']

#     attachments_return = []

#     # Iterate over emails and retrieve attachments
#     for email in emails:
#         email_id = email['id']
#         attachment_url = f'{api_url}/{email_id}/attachments'

#         response = requests.get(attachment_url, headers=headers)
#         attachments = response.json()['value']

#         # Download attachments with specific file extensions
#         for attachment in attachments:
#             attachment_id = attachment['id']
#             attachment_name = attachment['name']
#             attachment_url = f'{attachment_url}/{attachment_id}/$value'

#             # Check if the attachment has a supported file extension
#             if attachment_name.endswith(('.xlsx', '.xls', '.csv')):
#                 response = requests.get(attachment_url, headers=headers)
#                 with open(attachment_name, 'wb') as f:
#                     f.write(response.content)
#                     attachments_return.append(attachment_name)
#                     print(f'Saved attachment: {attachment_name}')
#     return attachments_return

# Usage
# email_address = 'datafeeds@skyboundwealth.com'
# subject_name = 'Quilter'
# client_id = '55b2ac50-5f9d-46cd-94ae-9ae427cd5964'
# client_secret = 'iU58Q~tPFvTzCIHPZ5iGXuMTHwvHubPXCJw.scHz'
# tenant_id = '4efa3987-7313-4150-8ca4-ea22db3ef98b'

# collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id)
import os
import base64
import pandas as pd
import requests
import datetime
from django.core.files.base import ContentFile
from storages.backends.s3boto3 import S3Boto3Storage
from django.conf import settings
from .models import Records,RL360Records
import pysftp
from urllib.parse import urlparse


# def collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id):
#     # Get today's date
#     today = datetime.date.today()
#     today_iso_format = today.isoformat()

#     # Authenticate and obtain an access token
#     token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
#     headers = {'Content-Type': 'application/x-www-form-urlencoded'}

#     data = {
#         'grant_type': 'client_credentials',
#         'client_id': client_id,
#         'client_secret': client_secret,
#         'scope': 'https://graph.microsoft.com/.default'
#     }

#     response = requests.post(token_url, headers=headers, data=data)
#     access_token = response.json()['access_token']

#     # Fetch emails with the specific subject received today
#     api_url = f'https://graph.microsoft.com/v1.0/users/{email_address}/messages'
#     headers = {
#         'Authorization': f'Bearer {access_token}',
#         'Content-Type': 'application/json'
#     }

#     params = {
#         '$filter': f"subject eq '{subject_name}' and receivedDateTime ge {today_iso_format}",
#         '$select': 'id'
#     }

#     response = requests.get(api_url, headers=headers, params=params)
#     emails = response.json()['value']

#     # Create S3 storage instance
#     s3_storage = S3Boto3Storage()

#     # Iterate over emails and retrieve attachments
#     for email in emails:
#         email_id = email['id']
#         attachment_url = f'{api_url}/{email_id}/attachments'

#         response = requests.get(attachment_url, headers=headers)
#         attachments = response.json()['value']

#         # Download attachments with specific file extensions
#         for attachment in attachments:
#             attachment_id = attachment['id']
#             attachment_name = attachment['name']
#             attachment_url = f'{attachment_url}/{attachment_id}/$value'

#             # Check if the attachment has a supported file extension
#             if attachment_name.endswith(('.xlsx', '.xls', '.csv')):
#                 response = requests.get(attachment_url, headers=headers)

#                 # Save attachment to S3 bucket
#                 file_content = ContentFile(response.content)
#                 s3_file_path = f'records/{attachment_name}'  # S3 file path
#                 s3_storage.save(s3_file_path, file_content)

#                 # Create a new record in the Django model
#                 record = Records.objects.create(
#                     provider_name=subject_name,
#                     file=s3_storage.url(s3_file_path),
#                     date=today
#                 )
#                 record.save()

#                 # Get the S3 file URL
#                 s3_file_url = s3_storage.url(s3_file_path)
#                 df =  pd.read_csv(s3_file_url,encoding = "ISO-8859-1")
#                 print('COLUMNS::::',df.columns)

#                 print(f'Saved attachment: {attachment_name}')
#                 print(f'S3 file URL: {s3_file_url}')

#                 # Return the S3 file URL
#                 return s3_file_url

import datetime
import requests
from django.core.files.base import ContentFile
from services.models import Records  # Assuming 'records' is the Django app name

def collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id):
    # Get today's date
    today = datetime.date.today()
    today_iso_format = today.isoformat()

    # Authenticate and obtain an access token
    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default'
    }

    response = requests.post(token_url, headers=headers, data=data)
    access_token = response.json()['access_token']

    # Fetch emails with the specific subject received today
    api_url = f'https://graph.microsoft.com/v1.0/users/{email_address}/messages'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    params = {
        '$filter': f"subject eq '{subject_name}' and receivedDateTime ge {today_iso_format}",
        '$select': 'id'
    }

    response = requests.get(api_url, headers=headers, params=params)
    emails = response.json()['value']

    # Create S3 storage instance
    s3_storage = S3Boto3Storage()

    # Iterate over emails and retrieve attachments
    for email in emails:
        email_id = email['id']
        attachment_url = f'{api_url}/{email_id}/attachments'

        response = requests.get(attachment_url, headers=headers)
        attachments = response.json()['value']

        # Download attachments with specific file extensions
        for attachment in attachments:
            attachment_id = attachment['id']
            attachment_name = attachment['name']
            attachment_url = f'{attachment_url}/{attachment_id}/$value'

            # Check if the attachment has a supported file extension
            if attachment_name.endswith(('.xlsx', '.xls', '.csv')):
                response = requests.get(attachment_url, headers=headers)

                # Get the attachment content
                attachment_content = response.content

                # Create a new record in the Django model (optional)
                record = Records.objects.create(
                    provider_name=subject_name,
                    file=attachment_name,  # Change this to save the file name
                    date=today
                )
                record.save()

                # print(f'Saved attachment: {attachment_name}')
                # print(f'Attachment content length: {len(attachment_content)}')

                # Return the attachment content
                return attachment_content


import os

def collect_attachments_rl360(email_address, subject_name, client_id, client_secret, tenant_id):
    # Get today's date
    today = datetime.date.today()
    today_iso_format = today.isoformat()

    # Authenticate and obtain an access token
    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default'
    }

    response = requests.post(token_url, headers=headers, data=data)
    access_token = response.json()['access_token']

    # Fetch emails with the specific subject received today
    api_url = f'https://graph.microsoft.com/v1.0/users/{email_address}/messages'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    params = {
        '$filter': f"subject eq '{subject_name}' and receivedDateTime ge {today_iso_format}",
        '$select': 'id'
    }

    response = requests.get(api_url, headers=headers, params=params)
    emails = response.json()['value']

    # Create a directory to store downloaded CSV files
    directory = 'attachments'
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Iterate over emails and retrieve attachments
    for email in emails:
        email_id = email['id']
        attachment_url = f'{api_url}/{email_id}/attachments'

        response = requests.get(attachment_url, headers=headers)
        attachments = response.json()['value']

        # Download attachments with specific file extensions
        for attachment in attachments:
            attachment_id = attachment['id']
            attachment_name = attachment['name']
            attachment_url = f'{attachment_url}/{attachment_id}'

            # Check if the attachment has a supported file extension
            if attachment_name.endswith('.csv'):
                response = requests.get(attachment_url, headers=headers)

                # Save attachment in the 'attachments' directory
                file_path = os.path.join(directory, attachment_name)
                with open(file_path, 'wb') as file:
                    file.write(response.content)

                print(f'Saved attachment: {attachment_name}')

    return "saved"


# Set up the connection parameters
# host = 'sftp22.providence.quatrix.it'
# username = 'q9074433'
# password = 'm*deufWGnAh.xn_cCmk6pz_FW'
# remote_directory = 'Skybound UAE'
# local_directory = '/path/to/save/'

def download_latest_file_from_sftp(host, username, password, remote_directory, local_directory):
    # Connect to the SFTP server
    with pysftp.Connection(host, port=43, username=username, password=password) as sftp:
        # Change to the remote directory
        sftp.cd(remote_directory)
        
        # Get the list of files in the directory
        file_list = sftp.listdir()
        
        # Sort the file list by modification time in descending order
        sorted_files = sorted(file_list, key=lambda x: sftp.stat(x).st_mtime, reverse=True)
        
        # Get the latest file from the sorted list
        latest_file = sorted_files[0]
        
        # Construct the local file path
        local_file_path = local_directory + latest_file
        
        # Download the latest file
        sftp.get(latest_file, local_file_path)
    
    print('Latest file downloaded successfully.')



def process_sftp_file():
    # Set up the connection parameters for SFTP
    sftp_host = 'sftp43.providence.quatrix.it'
    sftp_username = 'q9074433'
    sftp_password = 'm*deufWGnAh.xn_cCmk6pz_FW'
    remote_folder_path = '/Skybound UAE'

    # Connect to the SFTP server
    with pysftp.Connection(sftp_host, username=sftp_username, password=sftp_password) as sftp:
        # Change directory to the desired folder
        sftp.cwd(remote_folder_path)

        # Get the list of files in the remote folder
        file_list = sftp.listdir()

        # Sort the file list by modified time in descending order
        file_list.sort(key=lambda x: sftp.stat(x).st_mtime, reverse=True)

        if file_list:
            latest_file = file_list[0]  # Get the latest file in the folder

            # Specify the local path to save the downloaded file
            local_file_path = '/Users/dexter/Documents/Workspace/Skybound/Datafeeds/services/' + latest_file

            # Download the latest file
            sftp.get(latest_file, local_file_path)

            print('Latest file downloaded successfully:', latest_file)

            # Create S3 storage instance
            s3_storage = S3Boto3Storage()

            # Read the file content as bytes
            with open(local_file_path, 'rb') as file:
                file_content = file.read()

            # Save attachment to S3 bucket
            s3_file_path = f'records/{latest_file}'  # S3 file path
            s3_storage.save(s3_file_path, ContentFile(file_content))

            # Create a new record in the Django model
            record = Records.objects.create(
                provider_name='Datafeeds_Provider_PROVIDENCE',
                file=s3_storage.url(s3_file_path),
                date=datetime.datetime.now().date()
            )
            record.save()

            # Get the S3 file URL
            s3_file_url = s3_storage.url(s3_file_path)
            print(f'Saved attachment: {latest_file}')
            print(f'S3 file URL: {s3_file_url}')

            # Return the S3 file URL
            return s3_file_url
        else:
            print('No files found in the remote folder.')
            return None



def get_product_id(product_name):
    switch_case = {
        "Test": "a4w3H000000D8Ot",
        "Regular Savings Plan": "a4w3H000000D8Q1",
        "GIA SIPP": "a4w3H000000D8Q6",
        "PIMS":  "a4w3H000000D8h3",#["a4w3H000000D8Pw",
        "PIMS Flexible": "a4w3H000000D8h4",
        "Oracle": "a4w3H000000D8h5",
        "PIMS Focused": "a4w3H000000D8h6",
        "LifePlan": "a4w3H000000D8h7",
        "LifePlan 2019": "a4w3H000000D8h8",
        "Quantum": "a4w3H000000D8h9",
        "Vista": "a4w3H000000D8hD",
        "Futura": "a4w3H000000D8hE",
        "International Term Assurance": "a4w3H000000D8hG",
        "IWA Flexible contribution": "a4w3H000000D8hH",
        "Vista 3": "a4w3H000000D8hI",
        "Horizon Portfolio Bond": "a4w3H000000D8hJ",
        "Compass Regular Saving Plan": "a4w3H000000D8hK",
        "Vision_inactive": "a4w3H000000D8hL",
        "Flex Global": "a4w3H000000D8hM",
        "Ardan GIA": "a4w3H000000D8hN",
        "Morningstar GIA": "a4w3H000000D8hO",
        "Morningstar UK GIA": "a4w3H000000D8hP",
        "Morningstar International SIPP": "a4w3H000000D8hQ",
        "Managed Savings Account": "a4w3H000000D8hR",
        "Managed Capital Account": "a4w3H000000D8hS",
        "ERBA": "a4w3H000000D8hT",
        "Executive Redemption Bond": "a4w3H000000D8hU",
        "CRBSA": "a4w3H000000D8hV",
        "ERBSA": "a4w3H000000D8hW",
        "ERBAA": "a4w3H000000D8hX",
        "Executive Investment Bond": "a4w3H000000D8hY",
        "IEPRA": "a4w3H000000D8hZ",
        "Executive Redemption Bond -SIME": "a4w3H000000D8ha",
        "Collective Redemption Bond": "a4w3H000000D8hb",
        "International Executive Portfolio": "a4w3H000000D8hc",
        "Portfolio": "a4w3H000000D8hd",
        "Focus": "a4w3H000000D8he",
        "Vision": "a4w3H000000D8hf",
        "Professional Portfolio Bond": "a4w3H000000D8hg"
    }

    return switch_case.get(product_name, "")