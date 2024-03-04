import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import xlsxwriter
import numpy as np
import pandas.io.sql as psql
import mysql.connector


mydb = mysql.connector.connect(
  host="prod-nivaancare-mysql-02.cydlopxelbug.ap-south-1.rds.amazonaws.com",
  user="monodeep.saha",
  password="u5eOX37kNPh13J",
  database="nivaancare_production"
)
mycursor = mydb.cursor()
conn = mycursor.execute

sql_query = """
SELECT 
    url.created_time AS Created_Date,
    url.modified_time AS ModifiedDate,
    url.lead_new_status AS leadStatusnew,
    url.lead_new_sub_status AS LeadSubStatus,
    url.extracted_phone AS Mobile, 
    JSON_UNQUOTE(JSON_EXTRACT(url.owner, '$.name')) AS Owner
FROM 
    nivaancare_production.user_registration_lead url
WHERE 
    DATE_FORMAT(url.modified_time, '%Y-%m-%d') BETWEEN '2024-01-01' AND '2024-02-29';

"""
df_DATA = pd.read_sql_query(sql_query,mydb)
df_DATA
df_DATA['Rank'] = df_DATA[(df_DATA['leadStatusnew'] == "L5 - Pitched") & (df_DATA['LeadSubStatus'] == "Call1")].groupby(['Mobile','leadStatusnew','LeadSubStatus'])['ModifiedDate'].rank("dense", ascending=True)
df_DATA['Rank_Status'] = df_DATA.groupby(['Mobile'])['ModifiedDate'].rank("dense", ascending=False)
df_DATA['Rank_Dead'] = df_DATA[df_DATA['leadStatusnew'] == "Lead Dead - PD"].groupby(['Mobile','leadStatusnew'])['ModifiedDate'].rank("dense", ascending=False)

print(df_DATA)
df_DATA.to_csv('LEAD.csv',index = False)



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
spreadsheetId = '1M3Q29dhk8OmhxOsYem96e-In7EemxN7-3HcPdwchmd4'
sh = gc.open_by_key(spreadsheetId)

# Define the sheet name and CSV file path
sheetName = 'RAW'
csvFile = 'LEAD.csv'

# Clear existing values in the specified range
sh.values_clear("'RAW'!A:J")

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