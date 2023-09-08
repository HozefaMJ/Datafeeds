import requests
import datetime

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


import requests
datetime
from django.core.files.base import ContentFile
from storages.backends.s3boto3 import S3Boto3Storage
from django.conf import settings
from 

def collect_attachments(email_address, subject_name, client_id, client_secret, tenant_id):
    # Get today's date
    today = datetime.date.today()
    today_iso_format = today.isoformat()

    # Authenticate and obtain an access token
    # (same code as before...)

    # Fetch emails with the specific subject received today
    # (same code as before...)

    # Create S3 storage instance
    s3_storage = S3Boto3Storage()

    # Iterate over emails and retrieve attachments
    for email in emails:
        # (same code as before...)

        # Download attachments with specific file extensions
        for attachment in attachments:
            # (same code as before...)

            # Check if the attachment has a supported file extension
            if attachment_name.endswith(('.xlsx', '.xls', '.csv')):
                response = requests.get(attachment_url, headers=headers)

                # Save attachment to S3 bucket
                file_content = ContentFile(response.content)
                s3_file_path = f'records/{attachment_name}'  # S3 file path
                s3_storage.save(s3_file_path, file_content)

                # Create a new record in the Django model
                record = Records.objects.create(
                    provider_name=subject_name,
                    file=s3_file_path,
                    date=today
                )
                record.save()

                # Get the S3 file URL
                s3_file_url = s3_storage.url(s3_file_path)

                print(f'Saved attachment: {attachment_name}')
                print(f'S3 file URL: {s3_file_url}')

                # Return the S3 file URL
                return s3_file_url
