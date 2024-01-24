from bs4 import BeautifulSoup
import requests
import otp_generator as og

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

# Replace these with your actual credentials
username = "user@commercialcollective.com.au"
password = "password"

# URL for the login page
login_url = "https://login.cirrus8.com.au/?ch=1704857766458&returnTo=cirrus8"
owners_reports_url = "https://client.cirrus8.com.au/framework/index.php?module=managementReports&command=ownerReport"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
}

# Create a session to persist the login state
session = requests.Session()

# Step 1: Open the login page to get necessary cookies and tokens
login_page_response = session.get(login_url)

# Extract any required tokens or cookies from login_page_response.text
# soup = BeautifulSoup(login_page_response.text, 'html.parser')
# pretty_html = soup.prettify()

# Show login response tokens
# print(pretty_html)

# Step 2: Prepare login data (including 2FA code)
# Generate the current TOTP code
totp_code = og.get_otp_for_key("thekey")

login_data = {"username": username, "password": password, "totp": totp_code}

# Step 3: Submit login data
login_response = session.post(login_url, data=login_data)
# Handle the response, check for successful login or any errors

# Now, the 'session' object holds the authentication state and can be used for subsequent requests.
# Example: session.get("https://example.com/dashboard") to access authenticated pages.

try:
    response = session.get(owners_reports_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "lxml")
        # Get the main_container div - PROBLEM, it's html has not been loaded yet
        main_container = soup.select("div#main_container")

        pretty_html = soup.prettify()
        print(pretty_html)
except Exception as ex:
    print(f"Error processing url: {owners_reports_url}")
