from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import otp_generator as og
import sys
import os

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

            cirrus8_otp_secret_name = os.environ["REPORTS_OTP_SECRET_NAME"]

            # create a credential
            credentials = ClientSecretCredential(
                client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
            )

            # create a secret client object
            secret_client = SecretClient(vault_url=vault_url, credential=credentials)

            # retrieve the Cirrus8 secret values from the [cc-analytics-kv] key vault
            cirrus8_otp_key = secret_client.get_secret(cirrus8_otp_secret_name)

            totp_code = og.get_otp_for_key(cirrus8_otp_key.value)
            print(totp_code)
        else:
            print(f"Error: Expected 0 arguments. Received {str(len(sys.argv) - 1)}")
    except Exception as err:
        print(f"Unexpected error: {err=}, Type: {type(err)=}, Arguments: {err.args}")
        raise
