from azure.storage.blob import generate_blob_sas, BlobSasPermissions, BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime, timedelta
from dotenv import load_dotenv
import numpy as np
import pandas as pd
import os
import warnings
from urllib.parse import quote
import re
import urllib.parse
import urllib.error


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

def save_df_as_csv_blob(worksheet_df: pd.DataFrame, account_name: str, account_key: str, container_name: str, folder_name: str, worksheet_name: str, year_month_str: str):
    # Define your Azure Storage account details

    csv_file = year_month_str + '_' + os.path.splitext(worksheet_name)[0].replace(" ", "") + '.csv'

    foldername = f"{folder_name}/Inbox"  # If you want to save in a specific folder

    # Initialize the BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)

    # Initialize the ContainerClient
    container_client = blob_service_client.get_container_client(container_name)

    # Define the path where you want to save the file in Azure Blob Storage
    report_blob_name = f'{foldername}/{csv_file}'

    # Convert DataFrame to CSV and save to Azure Blob Storage
    report_csv_bytes = worksheet_df.to_csv(index=False).encode()
    blob_client = container_client.get_blob_client(report_blob_name)
    blob_client.upload_blob(report_csv_bytes, overwrite=True)

    print(f"{report_blob_name} - successfully saved to Azure Blob Storage.")


def load_excel_data(file_path, skip_rows_dict=None):
    # Load the Excel file
    xls = pd.ExcelFile(file_path)
    
    # Dictionary to store dataframes
    dataframes_dict = {}
    
    # Iterate through each worksheet
    for sheet_name in xls.sheet_names:
        # Load the sheet
        df = xls.parse(sheet_name, header=None)
        
        # Skip variable number of rows if specified
        skip_rows = skip_rows_dict.get(sheet_name, 0) if skip_rows_dict else 0
        df = df.iloc[skip_rows:]
                
        # Assuming the first row contains headers
        headers = df.iloc[0].tolist()
        df.columns = headers
        
        # Append dataframe to dictionary using worksheet name as key
        dataframes_dict[sheet_name] = df
        
    return dataframes_dict


def generate_secure_url(blob_name, sas_token, account_name, container_name):
    # Replace spaces with %20
    encoded_blob_name = urllib.parse.quote(blob_name)
    
    # Construct the URL
    url = f"https://{account_name}.blob.core.windows.net/{container_name}/{encoded_blob_name}?{sas_token}"
    
    return url


def find_nth_value_in_dataframe(df: pd.DataFrame, search_value: str, nth: int = 1):
    """
    Find the nth occurrence of the search_value in the DataFrame and return its row index and column index.

    Parameters:
    df (pandas.DataFrame): The DataFrame to search.
    search_value (str): The search_value to find.
    nth (int): Specifies which occurrence to return, default = 1.

    Returns:
    tuple: Row index and column index of the nth occurrence of the search_value.
    """
    hit_count = 0
    # Iterate through each column in the DataFrame
    for col_idx, col in enumerate(df.columns):
        # Check if the search_value exists in any of the column values
        if df[col].astype(str).str.contains(search_value).any():
            # Get the row indices of rows containing the search_value
            row_indices = df.index[df[col].astype(str).str.contains(search_value)].tolist()
            if len(row_indices) >= nth:
                # Return the column index and row index of the nth occurrence
                return row_indices[nth - 1], col_idx
            else:
                hit_count += len(row_indices)
                if hit_count >= nth:
                    # Return the column index and row index of the nth occurrence
                    return row_indices[nth - hit_count - 1], col_idx
    # If search_value is not found, return None
    return None


def get_row_property_value(df: pd.DataFrame, prop_name: str) -> str:
    """
    Get the value of the property in the same row of the DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame to search.
    prop_name (str): The property name to find.

    Returns:
    str: The value of the property in the same row.
    """
    if prop_name in df.values:
        row, column = np.where(df == prop_name)
        row_index = row[0]
        col_index = column[0] + 1
        while col_index < df.shape[1]:
            property_value = df.iloc[row_index, col_index]
            if pd.notnull(property_value):
                return property_value
            col_index += 1
    return ""


def get_effective_year_month(file_path: str) -> tuple[int, int]:
    """
    Extract the Year and Month from the file_path.

    Parameters:
        file_path (str): The file path to extract the Year and Month from.

    Returns:
        tuple: A tuple containing the Year and Month as integers.
    """
    try:
        # Extract the Month and Year from the file_path using regex
        match = re.search(r'(\w+)\s(\d{4})', file_path)
        if match:
            month_name, year_str = match.groups()
            month_name_parts = month_name.split(sep='_')
            month_name = month_name_parts[2]
            # Convert the month name to an integer
            month_int = datetime.strptime(month_name, "%b").month
            # Convert the year string to an integer
            year_int = int(year_str)
            return year_int, month_int
        else:
            return -1, -1
    except Exception as e:
        print(e)
        return -1, -1


def get_sas_url(blob_path: str) -> str:
    """
    Generate a SAS URL for a blob in Azure Storage.

    Parameters:
        blob_path (str): The path of the blob in Azure Storage.

    Returns:
        str: The SAS URL for the blob.
    """
    # Enter credentials
    account_name = os.environ["AZURE_STORAGE_ACCOUNT"]
    account_key = os.environ["STORAGE_ACCESS_KEY"]
    container_name = os.environ["CONTAINER_NAME"]

    # Define the permissions and expiry time for the SAS token
    sas_permissions = BlobSasPermissions(read=True, write=True, delete=True)  # Adjust permissions as needed
    expiry_time = datetime.utcnow() + timedelta(hours=1)  # Adjust the expiry time as needed

    # Generate SAS token
    sas_token = generate_blob_sas(account_name=account_name, 
                                account_key=account_key,
                                container_name=container_name,
                                blob_name=blob_path,
                                permission=sas_permissions,
                                expiry=expiry_time)

    # Construct the SAS URL
    sas_url = generate_secure_url(blob_name, sas_token, account_name, container_name)
    print(f"Secure url: {sas_url}")
    
    return sas_url


def find_non_empty_rows(df: pd.DataFrame, column_index: int) -> list[int]:
    """
    Find non-empty rows in a DataFrame column and return their row indexes.

    Parameters:
        df (pandas.DataFrame): The DataFrame to search.
        column_index (int): The index of the column to search.

    Returns:
        list: A list of row indexes where a non-empty value was found.
    """
    non_empty_rows = df[df.iloc[:, column_index].notnull()].index.tolist()
    return non_empty_rows


def extract_indexes(dictionary: dict, key: str) -> tuple[int, int]:
    """
    Extract row and column indexes from a tuple in a dictionary.

    Parameters:
        dictionary (dict): The dictionary containing key-value pairs.
        key (str): The key to access the dictionary.

    Returns:
        tuple: A tuple containing the row and column indexes.
    """
    try:
        if key in dictionary:
            row_index, col_index = dictionary[key]
            return row_index, col_index
        else:
            return -1, -1
    except Exception as e:
        print(e)
        raise
 
load_dotenv()

# This parameter accepts the filename of an xlsx file to be transformed and saved as a csv file
excel_file_path = 'Excel_Report_Apr 2024.xlsx'

folder_name = 'Cirrus 8 Reports/Buildings/Excel Reports'
blob_name = f'{folder_name}/{excel_file_path}'

sas_url = get_sas_url(blob_name)

# Specify the number of rows to skip for each sheet
skip_rows_dict = {
    "Supplier Reconciliation": 0, 
    "Tenant Reconciliation": 0,
    "Property List": 0, 
    "Lease Expiry": 0,
    "Budgeted manfees summary": 2, 
    "Budget": 4,
    "Vacant Units": 0, 
    "Vacated Leases": 0,
    "Tenant Uninvoiced Charges": 0, 
    "Tenant Charges": 0,
    "Management Fee per Account": 0, 
    "Current tenants with no rent re": 0,
    "Unallocated Cash": 0, 
    "List of Leases": 0,
    "Man fees charged per PM": 0, 
    "Man fees charged for a period": 0,
    "Man fees paid per PM": 0, 
    "Man fees paid for a period ": 0,
    "AP Journal": 0, 
    "Primary Property Contact": 0,
    "Primary Company Contact": 0, 
    "Primary Lease Contact": 0,
    "List of Leases Notes": 0, 
    "AP Allocations": 0,
    "AR Journal": 0, 
    "Check Next Charge Date": 0,
    "Open Rent Review List": 0, 
    "Created Properties": 0,
    "Inactive Properties": 0, 
    "Occupied Units As Of To Date": 0
    } 

excel_data = load_excel_data(sas_url, skip_rows_dict)

count = 0
limit = len(excel_data)
print(f'Processing {limit} worksheets:')

good_sheets = 0
bad_sheets = []

# Define your Azure Storage account details
account_name = 'collectivestorageaccount'
account_key = 'LfOUl1oTMxfpqD9Ftlvj0Qkko9ewr950TCTa0Mo+4HnbfF1ga2OLR3RXSUnnoZgxvzYqFijmWhE2+AStZ9G6Gw=='
container_name = 'bronze'

# Get effective year and month
effective_year, effective_month = get_effective_year_month(excel_file_path)

year_month_str = str(effective_year) + '_' + str(effective_month)

for sheet_name, df in excel_data.items():
    try:
        save_df_as_csv_blob(df, account_name=account_name, account_key=account_key, container_name=container_name, 
                            folder_name=folder_name, worksheet_name=sheet_name, year_month_str=year_month_str)
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
    print(f'There were ({good_sheets}) good worksheets.')
if (len(bad_sheets) > 0):
    print(f'The following ({len(bad_sheets)}) sheets failed when loading:')
    for bad_sheet in bad_sheets:
        print(bad_sheet)


