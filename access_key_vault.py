from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import otp_generator as og
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

load_dotenv()

client_id = os.environ["AZURE_CLIENT_ID"]
tenant_id = os.environ["AZURE_TENANT_ID"]
client_secret = os.environ["AZURE_CLIENT_SECRET"]
vault_url = os.environ["AZURE_VAULT_URL"]


cirrus8_otp_secret_name = os.environ["REPORTS_OTP_SECRET_NAME"]
cirrus8_reports_user_secret_name = os.environ["REPORTS_USER_SECRET_NAME"]
cirrus8_reports_password_secret_name = os.environ["REPORTS_PASSWORD_SECRET_NAME"]

# create a credential
credentials = ClientSecretCredential(
    client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
)

# create a secret client object
secret_client = SecretClient(vault_url=vault_url, credential=credentials)

# retrieve the Cirrus8 secret values from the [cc-analytics-kv] key vault
cirrus8_otp_secret = secret_client.get_secret(cirrus8_otp_secret_name)
cirrus8_username_secret = secret_client.get_secret(cirrus8_reports_user_secret_name)
cirrus8_password_secret = secret_client.get_secret(cirrus8_reports_password_secret_name)
otp_pass_code = og.get_otp_for_key(cirrus8_otp_secret.value)

# For proof of concept testing purposes confirm key vault values, and one time passcode value are correct. OKAY!!
print("The cirrus8 otp secret value is :" + cirrus8_otp_secret.value)
print("The cirrus8 reporting username is :" + cirrus8_username_secret.value)
print("The cirrus8 reporting password is :" + cirrus8_password_secret.value)
print("The cirrus8 reporting otp passcode is :" + otp_pass_code)
