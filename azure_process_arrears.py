from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
import os
from dotenv import load_dotenv
import urllib
import requests

def encode():
    obj = input('Enter value: ')  # Assuming you are getting the input from the user
    unencoded = obj
    obj = urllib.parse.quote(unencoded).replace("'", "%27").replace('"', "%22")
    print(obj)

def decode():
    obj = input('Enter value: ')  # Assuming you are getting the input from the user
    encoded = obj
    obj = urllib.parse.unquote(encoded.replace('+', ' '))
    print(obj)


load_dotenv()

# This parameter accepts the filename of an xlsx file to be transformed and saved as a csv file
excel_file_path = 'Cirrus8 Arrears - Dec 2023.xlsx'

#enter credentials
account_name = os.environ["AZURE_STORAGE_ACCOUNT"]
account_key = os.environ["STORAGE_ACCESS_KEY"]
container_name = os.environ["CONTAINER_NAME"]

folder_name = 'Cirrus 8 Reports/Buildings/Arrears'
excel_file = excel_file_path

#create a client to interact with blob storage
connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

#use the client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

#generate a shared access signature for the blob file
sas_i = generate_blob_sas(account_name = account_name,
                          container_name = container_name,
                          blob_name = excel_file,
                          account_key=account_key,
                          permission=BlobSasPermissions(read=True),
                          expiry=datetime.utcnow() + timedelta(hours=1))

sas_url = f'https://{account_name}.blob.core.windows.net/{container_name}/{folder_name}/{excel_file}?{sas_i}'

# sas_url = requests.utils.quote(sas_url)

print(sas_url)

df = pd.read_excel(sas_url)

df.head(10)
