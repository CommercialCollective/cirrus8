from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import otp_generator as og
import sys
import os

""" 
   Copyright [2024] [Commercial Collective]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

if __name__ == "__main__":
    try:
        # The expected argument count includes the script name, plus the 0 passed in arguments.
        expected_argument_count = 1
        
        if len(sys.argv) == expected_argument_count:
            load_dotenv()

            client_id = os.environ["AZURE_CLIENT_ID"]
            tenant_id = os.environ["AZURE_TENANT_ID"]
            client_secret = os.environ["AZURE_CLIENT_SECRET"]
            vault_url = os.environ["AZURE_VAULT_URL"]

            cityscope_password_secret_name = os.environ[
                "CITYSCOPE_PASSWORD_SECRET_NAME"
            ]

            # create a credential
            credentials = ClientSecretCredential(
                client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
            )

            # create a secret client object
            secret_client = SecretClient(vault_url=vault_url, credential=credentials)

            # retrieve the CityScope secret values from the [cc-analytics-kv] key vault
            cityscope_password_secret = secret_client.get_secret(
                cityscope_password_secret_name
            )

            password = cityscope_password_secret.value
            print(password)
        else:
            print(
                f"Error: Expected {expected_argument_count - 1} arguments. Received {str(len(sys.argv) - 1)} arguments."
            )
    except Exception as err:
        print(f"Unexpected error: {err=}, Type: {type(err)=}, Arguments: {err.args}")
        raise
