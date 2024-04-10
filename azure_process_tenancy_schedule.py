from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta
from dotenv import load_dotenv
import numpy as np
import pandas as pd
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

def to_float_or_string(element: any) -> any:
    #If you expect None to be passed:
    if element is None: 
        return ''
    try:
        value = float(element)
        return value
    except ValueError:
        return str(element)
    
def get_row_property_value(df: pd.DataFrame, prop_name: str) -> str:
    
    property_value = ""

    if df.isin([prop_name]).any().any():
        row, column = np.where(df == prop_name)
        row_index = df.index[row][0]
        col_index = df.columns[column][0]
        label_value = df.iloc[row_index][col_index]
        idx = 1
        property_value = df.iloc[row_index][col_index + idx]
        while pd.isnull(property_value) :
            idx += 1
            property_value = df.iloc[row_index][col_index + idx]

    return property_value


load_dotenv()

# This parameter accepts the filename of an xlsx file to be transformed and saved as a csv file
excel_file_path = 'Cirrus8 Tenancy Schedule (Compact) - April 2024.xlsx'

#enter credentials
account_name = os.environ["AZURE_STORAGE_ACCOUNT"]
account_key = os.environ["STORAGE_ACCESS_KEY"]
container_name = os.environ["CONTAINER_NAME"]
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

folder_name = 'Cirrus 8 Reports/Buildings/Tenancy Schedules'
blob_name = f'{folder_name}/{excel_file_path}'

# Create BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

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

# Load the workbook
xls = pd.ExcelFile(sas_url)

# Get the names of all sheets in the workbook
sheet_names = xls.sheet_names

# Create a dictionary of dataframes
print(f'Loading {len(sheet_names)} worksheets into a dictionary ...')
dfs = {sheet: xls.parse(sheet, header=None, index_col=None, skiprows=5) for sheet in sheet_names}

good_sheets = 0
bad_sheets = []

count = 0
limit = len(dfs)
print(f'Processing {limit} dataframes:')
for sheet_name in sheet_names:
    try:
        df = dfs[sheet_name]
        
        prop_name = 'Property ID:'
        prop_id = get_row_property_value(df, prop_name)
        
        df = df.iloc[4:]
        df = df.dropna(axis=1, how='all')

        good_sheets += 1
        count += 1
    except Exception as e:
        # print(f'Worksheet: {sheet_name} - Error: Failed to load')
        bad_sheets.append(f'Worksheet: {sheet_name} - {e.args[0]}')
        continue
    if count >= limit:
        break
    
print("")
if (good_sheets > 0):
    print(f'There were ({good_sheets}) good sheets.')
if (len(bad_sheets) > 0):
    print(f'The following ({len(bad_sheets)}) sheets failed when loading:')
    for bad_sheet in bad_sheets:
        print(bad_sheet)
    
    
# Cirrus8 Tenancy Schedule (Compact) - January 2024.xlsx
# Extract the Month and Year from the file name
file_parts = ''
month = ''
year = ''

file_parts = excel_file_path.split(sep=' ')
month = file_parts[5]
year = file_parts[6].split(sep='.')[0]

print(f'Year: {year}, Month: {month}')

