import requests
import pandas as pd
import pytz
from datetime import datetime
import os

# Initialize lists to store data
project_dates = []
urls_list = []
test_devices = []
performances = []

# Function to fetch PageSpeed Insights data for a given URL and device type
def fetch_pagespeed_data(url, device_type):
    api_key = 'AIzaSyBF_yqkjPKItyiqnIFH2f0LmqcpZrldVfA'  # Replace 'YOUR_API_KEY' with your actual API key
    endpoint = f'https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url={url}&strategy={device_type}&key={api_key}'
    response = requests.get(endpoint)
    data = response.json()
    return data

# List of URLs to fetch data for
urls = [
#    'https://home.joinelevatenow.co.in/pp/postpartum/postpartum_optimized.html',
       'https://www.joinelevatenow.com/pp/postpartumform/optimized_sunny/',
       'https://www.joinelevatenow.com/pp/easy-weight-loss/men/optimized_sunny/',
       'https://www.joinelevatenow.com/nasik-landing-page',
       'https://www.joinelevatenow.com/eligibility-ab',
       'https://www.joinelevatenow.com/en-eligibility'
    
]

# Load existing DataFrame if it exists
filename = 'pagespeed_data.csv'
if os.path.exists(filename):
    df = pd.read_csv(filename)
else:
    df = pd.DataFrame(columns=['Project Date (IST)', 'URL', 'Test Device', 'Performance'])

# Fetch data for each URL and device type
for url in urls:
    for device_type in ['mobile', 'desktop']:
        try:
            data = fetch_pagespeed_data(url, device_type)
            # Accessing the timestamp if available
            if 'analysisUTCTimestamp' in data:
                utc_timestamp = datetime.fromisoformat(data['analysisUTCTimestamp'].replace('Z', '+00:00'))
                utc_timestamp = utc_timestamp.replace(tzinfo=pytz.utc)
                ist_timestamp = utc_timestamp.astimezone(pytz.timezone('Asia/Kolkata'))
                project_dates.append(ist_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            else:
                project_dates.append("Timestamp not found in data.")
            urls_list.append(url)
            test_devices.append(device_type)
            performances.append(data['lighthouseResult']['categories']['performance']['score']*100)
        except Exception as e:
            print(f"Error fetching data for URL: {url} and Device Type: {device_type}")
            print(e)

# Append new data to existing DataFrame
new_df = pd.DataFrame({
    'Project Date (IST)': project_dates,
    'URL': urls_list,
    'Test Device': test_devices,
    'Performance': performances
})
df = pd.concat([df, new_df], ignore_index=True)

# Save DataFrame to CSV
df.to_csv(filename, index=False)
df
df.to_csv('pagespeed_data.csv',index = False)
df
# Display DataFrame
print(df)

import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials

# Define the OAuth2 scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']


# Load the credentials from the JSON key file
credentials = ServiceAccountCredentials.from_json_keyfile_name('my-project-2024-414004-60efb95f9e7f.json', scope)

# Authorize the client using the credentials
gc = gspread.authorize(credentials)

# Open the spreadsheet by its ID
spreadsheetId = '1mEAwGpVQlJLzzsw6mqwqwTQYMYr2tJm7kSHO6YEMknw'
sh = gc.open_by_key(spreadsheetId)

# Define the sheet name and CSV file path
sheetName = 'Raw'
csvFile = 'pagespeed_data.csv'

# Clear existing values in the specified range
sh.values_clear("'Raw'!A:D")

# Read the CSV file with UTF-8 encoding and update the spreadsheet with its values
with open(csvFile, 'r', encoding='utf-8') as file:
    csv_values = list(csv.reader(file))
    sh.values_update(
        sheetName,
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': csv_values}
    )

# Print the data written
print("Data written to the spreadsheet:")
# for row in csv_values:
#     print(row)

# Print the number of rows and columns written
num_rows = len(csv_values)
num_columns = len(csv_values[0]) if csv_values else 0
print(f"Number of rows written: {num_rows}")
print(f"Number of columns written: {num_columns}")



