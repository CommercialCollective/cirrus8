from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import openpyxl
import os
import warnings
warnings.simplefilter("ignore")

 
##   Copyright [2024] [Commercial Collective]
##
##   Licensed under the Apache License, Version 2.0 (the "License");
##   you may not use this file except in compliance with the License.
##   You may obtain a copy of the License at
##
##       http://www.apache.org/licenses/LICENSE-2.0
##
##   Unless required by applicable law or agreed to in writing, software
##   distributed under the License is distributed on an "AS IS" BASIS,
##   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##   See the License for the specific language governing permissions and
##   limitations under the License.

load_dotenv()

# This parameter accepts the filename of an xlsx file to be transformed and saved as a csv file
excel_file_path = 'Cirrus8 Tenancy Schedule (Compact) - January 2024.xlsx'

#enter credentials
account_name = os.environ["AZURE_STORAGE_ACCOUNT"]
account_key = os.environ["STORAGE_ACCESS_KEY"]
container_name = os.environ["CONTAINER_NAME"]

folder_name = 'Cirrus 8 Reports/Buildings/Tenancy Schedules'
blob_name = f'{folder_name}/{excel_file_path}'


# Create BlobServiceClient
blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)

# Get the blob client
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# Define the permissions and expiry time for the SAS token
sas_permissions = BlobSasPermissions(read=True, write=True, delete=True)  # Adjust permissions as needed
expiry_time = datetime.utcnow() + timedelta(hours=1)  # Adjust the expiry time as needed

# Generate SAS token
sas_token = generate_blob_sas(account_name=account_name, 
                               account_key=account_key,
                               container_name=container_name,
                               blob_name=blob_name,
                               permission=sas_permissions,
                               expiry=expiry_time)

# Construct the SAS URL
sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"

# Encode url spaces as %20 so as to make a valid url
sas_url = sas_url.replace(" ", "%20")

# print("Generated SAS URL:", sas_url)

xls = pd.ExcelFile(sas_url)

count = 0
limit = 100
for sheet_name in xls.sheet_names:
    try:
        df = pd.read_excel(sas_url, skiprows=5, header=None, sheet_name=sheet_name, index_col=None, dtype={13: object, 14: object, 15: object, 16: object, 17: object, 18: object})
        print(f'Worksheet: {sheet_name}')
        print(df.iloc[0:7,21:])
        count += 1
    except Exception as e:
        print(f'Worksheet: {sheet_name} - Error: Failed to load')
        continue
    if count >= limit:
        break

# Cirrus8 Tenancy Schedule (Compact) - January 2024.xlsx

# Extract the Month and Year from the file name
file_parts = ''
month = ''
year = ''

file_parts = excel_file_path.split(sep=' ')
month = file_parts[5]
year = file_parts[6].split(sep='.')[0]

print(f'Year: {year}, Month: {month}')

