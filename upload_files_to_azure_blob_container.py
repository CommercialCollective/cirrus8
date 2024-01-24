from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import sys


# ****************************************************************************************************************************
# Input Arguments:
#   container_name: This is the container name created in the azure storage account e.g. 'bronze'
#   blob_folder:    This is the full folder path within the container e.g. 'Cirrus 8 Reports/Buildings/Arrears'
#   file_path:      This is the path of the source file e.g. 'C:\Users\MichaelMinto\Downloads'
#   file_name:      This is the name and extension of the source file e.g. 'Cirrus8 Arrears - Jan 2024.xlsx'
#
# ****************************************************************************************************************************

if __name__ == "__main__":
    try:
        # The expected argument count includes the script name, plus the 4 passed in arguments.
        expected_argument_count = 5

        if len(sys.argv) == expected_argument_count:
            load_dotenv()

            # Replace these variables with your Azure Storage account information
            account_name = os.environ["AZURE_STORAGE_ACCOUNT"]
            account_key = os.environ["STORAGE_ACCESS_KEY"]
            container_name = sys.argv[1]
            # Replace with the desired folder path inside the container
            blob_folder = sys.argv[2]
            blob_folder = blob_folder.replace("\\", "/")
            file_path = sys.argv[3]
            file_path = file_path.replace("\\", "/")
            file_name = sys.argv[4]
            # Replace with your local file path
            full_file_path = f"{file_path}/{file_name}"
            # Replace with your desired blob name
            blob_name = f"{blob_folder}/{file_name}"

            # Create a BlobServiceClient
            blob_service_client = BlobServiceClient(
                account_url=f"https://{account_name}.blob.core.windows.net",
                credential=account_key,
            )

            # Create a ContainerClient
            container_client = blob_service_client.get_container_client(container_name)

            # Create a BlobClient
            blob_client = container_client.get_blob_client(blob_name)

            # Upload the local file to Azure Storage
            with open(full_file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)

            print(
                f"File uploaded to Azure Storage [{container_name}] container in [{blob_folder}] folder as [{blob_name}] file."
            )
        else:
            print(
                f"Error: Expected {expected_argument_count - 1} arguments. Received {str(len(sys.argv) - 1)} arguments."
            )
    except Exception as err:
        print(f"Unexpected error: {err=}, Type: {type(err)=}, Arguments: {err.args}")
        raise
