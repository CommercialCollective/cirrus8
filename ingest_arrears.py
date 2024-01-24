import numpy as np
import pandas as pd
import openpyxl
import os

pd.options.mode.copy_on_write = True

# Specify the file path
source_folder = 'C:/Users/mikej/Documents/Cirrus8/Arrears'

os.chdir(source_folder)
test_number = 0

files = os.listdir(source_folder)
# Filtering only the files.
files = [f for f in files if os.path.isfile(source_folder+'/'+f)]

data = {}

#load data into a DataFrame object:
full_df = None

for file in files:

    # Extract the Month and Year from the file name
    file_parts = ''
    month = ''
    year = ''
    
    file_parts = file.split(sep=' ')
    month = file_parts[3]
    year = file_parts[4].split(sep='.')[0]
    
    # read the file
    with pd.ExcelFile(file) as xls:
    
        # Get information for first sheet
        df = pd.read_excel(xls, sheet_name='Aged Debtors')

        # filter the rows that contain the substring
        substring = ' total'
        filter = df['Tenant'].str.contains(substring)
        filtered_df = df[~filter]

        xformed_df = filtered_df.copy()

        # Identify Property IDs
        is_property_col = pd.isna(xformed_df['Lease Name'])
        xformed_df.insert(loc = 0, column = 'IsProperty', value = is_property_col)

        # Create Property ID Column
        property_id_col = xformed_df['Tenant'].where(xformed_df['IsProperty'])
        xformed_df.insert(loc = 1, column = 'Property ID', value = property_id_col)

        # Forward Fill Property IDs
        xformed_df['Property ID'] = xformed_df['Property ID'].ffill()

        # Create Tenant ID Column
        tenant_id_col = xformed_df['Tenant'].where(~xformed_df['IsProperty'])
        xformed_df.insert(loc = 2, column = 'Tenant ID', value = tenant_id_col)

        # Create Month Column
        month_col = month
        xformed_df.insert(loc = 3, column = 'Month', value = month_col)

        # Create Year Column
        year_col = year
        xformed_df.insert(loc = 4, column = 'Year', value = year_col)

        # Filter out rows where [Lease Name] is na
        filter = pd.isna(xformed_df['Lease Name'])
        xformed_final_df = xformed_df[~filter]

        # Remove [Tenant] and [IsProperty] columns as they are no longer required
        xformed_final_df = xformed_final_df.drop(columns=['Tenant', 'IsProperty'])
        
        if full_df is None:
            full_df = xformed_final_df
        else:
            full_df = full_df._append(xformed_final_df, ignore_index=False)

full_df.to_csv(f"{source_folder}/csv/cirrus8_arrears_full.csv", index=False)

