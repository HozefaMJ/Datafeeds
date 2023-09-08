import requests

# Replace these placeholders with your actual values
client_id = '55b2ac50-5f9d-46cd-94ae-9ae427cd5964'
client_secret = 'iU58Q~tPFvTzCIHPZ5iGXuMTHwvHubPXCJw.scHz'
tenant_id = '4efa3987-7313-4150-8ca4-ea22db3ef98b'
scope = 'https://graph.microsoft.com/.default'

token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
headers = {'Content-Type': 'application/x-www-form-urlencoded'}

data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': scope
}

response = requests.post(token_url, headers=headers, data=data)
access_token = response.json()['access_token']


# Replace with your email address and subject name
email_address = 'datafeeds@skyboundwealth.com'
subject_name = 'Hansard Lumpsum'

api_url = f'https://graph.microsoft.com/v1.0/users/{email_address}/messages'
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

# Fetch emails with the specific subject
params = {
    '$filter': f"subject eq '{subject_name}'",
    '$select': 'id'
}

response = requests.get(api_url, headers=headers, params=params)
emails = response.json()['value']

# Iterate over emails and retrieve attachments
for email in emails:
    email_id = email['id']
    attachment_url = f'{api_url}/{email_id}/attachments'

    response = requests.get(attachment_url, headers=headers)
    attachments = response.json()['value']

    # Download attachments
    for attachment in attachments:
        attachment_id = attachment['id']
        attachment_name = attachment['name']
        attachment_url = f'{attachment_url}/{attachment_id}/$value'

        response = requests.get(attachment_url, headers=headers)
        with open(attachment_name, 'wb') as f:
            f.write(response.content)
            print(f'Saved attachment: {attachment_name}')

# import requests
# import json

# def get_access_token(client_id, client_secret, tenant_id):
#     url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
#     data = {
#         "grant_type": "client_credentials",
#         "client_id": client_id,
#         "client_secret": client_secret,
#         "scope": "https://graph.microsoft.com/.default"
#     }
#     response = requests.post(url, data=data)
#     access_token = json.loads(response.text)["access_token"]
#     return access_token

# def get_attachments(access_token, email_address):
#     url = f"https://graph.microsoft.com/v1.0/users/{email_address}/messages"
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Content-Type": "application/json"
#     }
#     params = {
#         "$filter": "hasAttachments eq true",
#         "$select": "id,attachments",
#         "$top": 10
#     }
#     response = requests.get(url, headers=headers, params=params)
#     messages = json.loads(response.text)["value"]
    
#     for message in messages:
#         message_id = message["id"]
        
#         if "attachments" in message:
#             attachments = message["attachments"]
#             print(message["subject"])
#             for attachment in attachments:
#                 attachment_id = attachment["id"]
#                 attachment_name = attachment["name"]
#                 attachment_url = f"{url}/{message_id}/attachments/{attachment_id}"
#                 attachment_response = requests.get(attachment_url, headers=headers)
#                 with open(attachment_name, "wb") as file:
#                     file.write(attachment_response.content)
#                     print(f"Downloaded attachment: {attachment_name}")
#         else:
#             print(f"No attachments found for message ID: {message_id}")

# # Replace with your actual values
# CLIENT_ID = "55b2ac50-5f9d-46cd-94ae-9ae427cd5964"
# CLIENT_SECRET = "iU58Q~tPFvTzCIHPZ5iGXuMTHwvHubPXCJw.scHz"
# # CLIENT_ = "3358fb64-726e-40b1-bae5-4e4645e18cf6"
# TENANT_ID = "4efa3987-7313-4150-8ca4-ea22db3ef98b"
# EMAIL_ADDRESS = "datafeeds@skyboundwealth.com"

# access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, TENANT_ID)
# get_attachments(access_token, EMAIL_ADDRESS)


# import requests

# Replace these placeholders with your actual values
# client_id = '55b2ac50-5f9d-46cd-94ae-9ae427cd5964'
# client_secret = 'iU58Q~tPFvTzCIHPZ5iGXuMTHwvHubPXCJw.scHz'
# tenant_id = '4efa3987-7313-4150-8ca4-ea22db3ef98b'
# scope = 'https://graph.microsoft.com/.default'

# token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
# headers = {'Content-Type': 'application/x-www-form-urlencoded'}

# data = {
#     'grant_type': 'client_credentials',
#     'client_id': client_id,
#     'client_secret': client_secret,
#     'scope': scope
# }

# response = requests.post(token_url, headers=headers, data=data)
# access_token = response.json()['access_token']


# # Replace with your email address and subject name
# email_address = 'datafeeds@skyboundwealth.com'
# subject_name = 'Quilter'
# # subject_name = 'Hansard Lumpsum'

# api_url = f'https://graph.microsoft.com/v1.0/users/{email_address}/messages'
# headers = {
#     'Authorization': f'Bearer {access_token}',
#     'Content-Type': 'application/json'
# }

# # Fetch emails with the specific subject
# params = {
#     '$filter': f"subject eq '{subject_name}'",
#     '$select': 'id'
# }

# response = requests.get(api_url, headers=headers, params=params)
# emails = response.json()['value']

# # Iterate over emails and retrieve attachments
# for email in emails:
#     email_id = email['id']
#     attachment_url = f'{api_url}/{email_id}/attachments'

#     response = requests.get(attachment_url, headers=headers)
#     attachments = response.json()['value']

#     # Download attachments
#     for attachment in attachments:
#         attachment_id = attachment['id']
#         attachment_name = attachment['name']
#         attachment_url = f'{attachment_url}/{attachment_id}/$value'

#         response = requests.get(attachment_url, headers=headers)
#         with open(attachment_name, 'wb') as f:
#             f.write(response.content)
#             print(f'Saved attachment: {attachment_name}')






