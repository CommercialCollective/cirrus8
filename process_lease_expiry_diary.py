import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import BytesIO, StringIO
import re

# Parameters
excel_file_name = 'Lease_Expiry_Diary_2024-09-26 01_09_32.xls'

# Azure Blob Storage details
connection_string = 'DefaultEndpointsProtocol=https;AccountName=collectivestorageaccount;AccountKey=LfOUl1oTMxfpqD9Ftlvj0Qkko9ewr950TCTa0Mo+4HnbfF1ga2OLR3RXSUnnoZgxvzYqFijmWhE2+AStZ9G6Gw==;EndpointSuffix=core.windows.net'  # Replace with your actual connection string
container_name = 'bronze'
folder_path = 'City Scope/Lease Expiry Diary/Inbox'
file_name = excel_file_name  # Replace with the actual file name

# Construct the blob client
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Full path of the file in the container
blob_path = f"{folder_path}/{file_name}"

# Download the .xls (HTML) file from the Azure Blob Storage into memory
blob_client = container_client.get_blob_client(blob_path)
xls_data = blob_client.download_blob().readall()

# Read the HTML content from the in-memory bytes data
html_content = xls_data.decode('utf-8')

# Wrap the HTML content in a StringIO object
html_io = StringIO(html_content)

# Read the HTML content, assuming the first row is the header
tables = pd.read_html(html_io, header=0)

# Assuming the first table is the main data, if multiple tables are present
df = tables[0]

# Function to clean up column names
def clean_column_name(name):
    # Convert to lowercase
    name = name.lower()
    # Replace spaces with underscores
    name = name.replace(" ", "_")
    # Remove any superfluous punctuation
    name = re.sub(r'[^\w\s]', '', name)
    return name

# Clean column names
df.columns = [clean_column_name(col) for col in df.columns]

# Optional: Remove any empty or unwanted rows/columns
df = df.dropna(how='all').reset_index(drop=True)

# Convert to .xlsx in-memory
xlsx_data = BytesIO()
xlsx_file_name = file_name.replace('.xls', '.xlsx')
df.to_excel(xlsx_data, index=False, engine='openpyxl')
xlsx_data.seek(0)  # Rewind the buffer

# Upload the .xlsx file back to Azure Blob Storage
blob_client_xlsx = container_client.get_blob_client(f"{folder_path}/{xlsx_file_name}")
blob_client_xlsx.upload_blob(xlsx_data, overwrite=True)
print(f"The .xlsx file has been saved to the Azure Blob Storage at: {folder_path}/{xlsx_file_name}")

# Delete the original .xls file from Azure Blob Storage
blob_client.delete_blob()
print(f"The original .xls file has been deleted from Azure Blob Storage: {blob_path}")
