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

def find_nth_value_in_dataframe(df: pd.DataFrame, search_value: str, nth: int = 1):
    """
    Find the nth occurrence of the search_value in the 
    DataFrame and return its row index and column index.

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
            hit_count += 1
            if hit_count != nth:
                continue
            # Get the row index of the first row containing the search_value
            row_idx = int(df.index[df[col].astype(str).str.contains(search_value)].tolist()[0])
            # Return the column index and row index
            return row_idx, col_idx

    # If search_value is not found in any column, return None
    return None, None

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
        row_index = int(df.index[row][0])
        col_index = int(df.columns[column][0])
        # label_value = df.iloc[row_index][col_index]
        idx = 1
        property_value = df.iloc[row_index][col_index + idx]
        while pd.isnull(property_value) :
            idx += 1
            property_value = df.iloc[row_index][col_index + idx]

    return property_value

def get_effective_year_month(file_path: str) -> tuple[int, int]:
    
    try:
        # Extract the Month and Year from the file_path
        # E.g. Cirrus8 Tenancy Schedule (Compact) - January 2024.xlsx
        file_parts = ''

        file_parts = file_path.split(sep=' ')
        month_name = file_parts[5]
        # Convert the month name to an integer
        month_int = int(datetime.strptime(month_name, "%B").month)
        year_str = file_parts[6].split(sep='.')[0]
        # Convert the year string to an integer
        year_int = int(year_str)

        return year_int, month_int
    
    except Exception as e:
        return -1, -1

def get_sas_url(blob_path: str) -> str:
    
    #enter credentials
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
    sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"

    # Encode url spaces as %20 so as to make a valid url
    sas_url = sas_url.replace(" ", "%20")
    
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
    
    # Convert each element to an integer using list comprehension
    list_of_int = [int(x) for x in non_empty_rows]
    
    return list_of_int

# Function to extract row and column indexes
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
        print()

load_dotenv()

# This parameter accepts the filename of an xlsx file to be transformed and saved as a csv file
excel_file_path = 'Cirrus8 Tenancy Schedule (Compact) - April 2024.xlsx'

folder_name = 'Cirrus 8 Reports/Buildings/Tenancy Schedules'
blob_name = f'{folder_name}/{excel_file_path}'

sas_url = get_sas_url(blob_name)

# Load the workbook
xls = pd.ExcelFile(sas_url)

# Get effective year and month
effective_year, effective_month = get_effective_year_month(excel_file_path)

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

# Existing DataFrame
tenancy_schedules_df = pd.DataFrame({})
tenancy_current_charges_df = pd.DataFrame({})

for sheet_name in sheet_names:
    try:
        df = dfs[sheet_name]
        
        prop_name = 'Property ID:'
        property_id = get_row_property_value(df, prop_name)
        
        # Remove rows
        df = df.iloc[2:]

        # Create list of table column labels
        header_items = ["Unit", "Lease Code", "Start", "Expiry", "NLA", "Account", "Description", 
                        "Effective", "Per m", "Monthly", "Annual", "Spaces", "Date", "Description", "Type"]

        # Create an empty dictionary
        header_dict = {}
        
        last_item = ""
        # Populate the dictionary by iterating over the list of header_items
        for item in header_items:
            # Get the tuple containing row and column indices for the current item
            if item == "Description" and last_item == "Date":
                row_idx, col_idx = find_nth_value_in_dataframe(df, item, nth=2)
                # Add a new dictionary item with the string as the key and the tuple as the value
                header_dict["Rent Review " + item] = (row_idx, col_idx)
            else:
                row_idx, col_idx = find_nth_value_in_dataframe(df, item)
                # Add a new dictionary item with the string as the key and the tuple as the value
                header_dict[item] = (row_idx, col_idx)
            last_item = item
            
        # Remove rows
        df = df.iloc[2:]
        
        # New tenancy charges
        account_row, account_col = extract_indexes(header_dict, 'Account')
        desc_row, desc_col = extract_indexes(header_dict, 'Description')
        effective_row, effective_col = extract_indexes(header_dict, 'Effective')
        per_row, per_col = extract_indexes(header_dict, 'Per m')
        monthly_row, monthly_col = extract_indexes(header_dict, 'Monthly')
        annual_row, annual_col = extract_indexes(header_dict, 'Annual')
        spaces_row, spaces_col = extract_indexes(header_dict, 'Spaces')
        
        # Get Lease Code Rows for sheet
        lease_row, lease_col = extract_indexes(header_dict, 'Lease Code')
        lease_code_rows = find_non_empty_rows(df, column_index=lease_col)
        for code_row in lease_code_rows:
            unit_row, unit_col = extract_indexes(header_dict, 'Unit')
            unit = df.loc[code_row, unit_col]
            
            lease_code = df.loc[code_row, lease_col].strip()
            
            start_row, start_col = extract_indexes(header_dict, 'Start')
            lease_terms_start = df.loc[code_row, start_col] if lease_code != 'Vacant' else ''
            
            expiry_row, expiry_col = extract_indexes(header_dict, 'Expiry')
            lease_terms_expiry = df.loc[code_row, expiry_col] if lease_code != 'Vacant' else ''
            lease_terms_term = str(df.loc[code_row + 1, expiry_col]).strip() if lease_code != 'Vacant' and pd.notnull(df.loc[code_row + 1, expiry_col]) else ''
            lease_terms_option = str(df.loc[code_row + 2, expiry_col]).strip() if lease_code != 'Vacant' and pd.notnull(df.loc[code_row + 2, expiry_col]) else ''
            
            nla_row, nla_col = extract_indexes(header_dict, 'NLA')
            lease_terms_nla = df.loc[code_row, nla_col] if lease_code != 'Vacant' else ''
            
            date_row, date_col = extract_indexes(header_dict, 'Date')
            rent_review_date = df.loc[code_row, date_col] if lease_code != 'Vacant' else ''
            
            review_desc_row, review_desc_col = extract_indexes(header_dict, 'Rent Review Description')
            rent_review_description = str(df.loc[code_row, review_desc_col]).strip() if lease_code != 'Vacant' and pd.notnull(df.loc[code_row, review_desc_col]) else ''
            
            review_type_row, review_type_col = extract_indexes(header_dict, 'Type')
            rent_review_type = str(df.loc[code_row, review_type_col]).strip() if lease_code != 'Vacant' and pd.notnull(df.loc[code_row, review_type_col]) else ''

            # New tenancy charges
            charges_offset = 0
            try:
                while (pd.notnull(df.loc[code_row + charges_offset, spaces_col]) or lease_code != 'Vacant'):
                    if pd.notnull(df.loc[code_row + charges_offset, account_col]) and df.loc[code_row + charges_offset, desc_col] != 'Total':
                        charge_account = df.loc[code_row + charges_offset, account_col]
                        charge_description = df.loc[code_row + charges_offset, desc_col]
                        charge_effective_date = df.loc[code_row + charges_offset, effective_col]
                        charge_dollars_per_m_sq = round(float(df.loc[code_row + charges_offset, per_col]), 2) if pd.notnull(df.loc[code_row + charges_offset, per_col]) else ''
                        charge_dollars_per_month = round(float(df.loc[code_row + charges_offset, monthly_col]), 2) if pd.notnull(df.loc[code_row + charges_offset, monthly_col]) else ''
                        charge_dollars_per_annum = round(float(df.loc[code_row + charges_offset, annual_col]), 2) if pd.notnull(df.loc[code_row + charges_offset, annual_col]) else ''
                        charge_parking_spaces = df.loc[code_row + charges_offset, spaces_col]
                        
                        # New charge to concatenate
                        new_charge = {
                            'effective_year': effective_year, 
                            'effective_month': effective_month,
                            'property_id': property_id,
                            'lease_code': lease_code,
                            'charge_account': charge_account,
                            'charge_description': charge_description,
                            'charge_effective_date': charge_effective_date,
                            'charge_dollars_per_m_sq': charge_dollars_per_m_sq,
                            'charge_dollars_per_month': charge_dollars_per_month,
                            'charge_dollars_per_annum': charge_dollars_per_annum,
                            'charge_parking_spaces': charge_parking_spaces
                            }

                        # Concatenate the new charge to the DataFrame
                        charge_df = pd.DataFrame(new_charge, index=[0])
                        tenancy_current_charges_df = pd.concat([tenancy_current_charges_df, charge_df])

                    # Move to next charges row                 
                    charges_offset += 1
            except Exception as e:
                print(e)

            # New tenancy to concatenate
            new_tenancy = {
                'effective_year': effective_year, 
                'effective_month': effective_month,
                'property_id': property_id,
                'lease_code': lease_code,
                'lease_unit': unit,
                'lease_terms_start': lease_terms_start,
                'lease_terms_expiry': lease_terms_expiry,
                'lease_terms_term': lease_terms_term,
                'lease_terms_option': lease_terms_option,
                'lease_terms_nla': lease_terms_nla,
                'rent_review_date': rent_review_date,
                'rent_review_description': rent_review_description,
                'rent_review_type': rent_review_type
                }

            # Append the new tenancy to the DataFrame
            tenancy_df = pd.DataFrame(new_tenancy, index=[0])
            tenancy_schedules_df = pd.concat([tenancy_schedules_df, tenancy_df])

           
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

# Fill NaN values with a specific value
tenancy_schedules_df.fillna('', inplace=True)  # Fill with '' 

# Save the DataFrame as a CSV file
tenancy_schedules_df.to_csv('april_2024_tenancy_schedules.csv', index=False)  # Set index=False to exclude row indexes from the CSV
print('Saved tenancy schedule to: april_2024_tenancy_schedules.csv')

tenancy_current_charges_df.to_csv('april_2024_tenancy_charges.csv', index=False)  # Set index=False to exclude row indexes from the CSV
print('Saved tenancy schedule to: april_2024_tenancy_charges.csv')

