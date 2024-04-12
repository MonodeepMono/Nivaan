import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
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
    DATE_FORMAT(c.consult_datetime, '%d-%m-%y') as Date,
    ap.full_name as DoctorName,
    l.name as ClinicName,
    SUM(CASE WHEN c.title LIKE '%First Consultation%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as New_Consults,
    SUM(CASE WHEN c.title LIKE '%Follow up%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as Follow_up_Consults,
    SUM(CASE WHEN c.title LIKE '%Physio%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as Physio_Consults,
    SUM(CASE WHEN c.title LIKE '%Nutrition%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as Nutrition_Consults,
    SUM(CASE WHEN c.title LIKE '%Psychology%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as Psychology_Consults,
    SUM(CASE WHEN c.title LIKE '%PsychologyPrescribed%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as Psychology_Prescribed,
    SUM(CASE WHEN c.title LIKE '%NutritionPrescribed%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as Nutrition_Prescribed,
    SUM(CASE WHEN c.title LIKE '%CarePlanPrescribed%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as Care_Plan_Prescribed,
    
    SUM(CASE WHEN c.title LIKE '%CRP%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as CRP_Consults, 
    SUM(CASE WHEN c.title LIKE '%PRC%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as PRC_Consults,
    SUM(CASE WHEN c.title LIKE '%CRPPrescribed%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as CRPPrescribed,
    SUM(CASE WHEN c.title LIKE '%PRCPrescribed%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as PRCPrescribed,
    
    SUM(CASE WHEN c.title LIKE '%SingleSpec%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as SingleSpec_CRP_Booked, 
    SUM(CASE WHEN c.title LIKE '%CRPBooked%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as CRPBooked,
    SUM(CASE WHEN c.title LIKE '%MultiSpecCRPBooked%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as MultiSpecCRPBooked,
    SUM(CASE WHEN c.title LIKE '%PhysioCRP%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as PhysioCRP, 
    SUM(CASE WHEN c.title LIKE '%PRCDone%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as PRCDone,
    SUM(CASE WHEN c.title LIKE '%NutritionBooked%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as NutritionBooked,
    SUM(CASE WHEN c.title LIKE '%PhysioBooked%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as PhysioBooked,
    SUM(CASE WHEN c.title LIKE '%PsychBooked%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as PsychBooked,
    
    
    
    SUM(CASE WHEN s.title LIKE '%consult%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as New_Consults_new,
    SUM(CASE WHEN s.title LIKE '%followup%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as Follow_up_Consults_new,
    SUM(CASE WHEN s.title LIKE '%CRP_sessions%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as CRP_Consults_new, 
    SUM(CASE WHEN s.title LIKE '%procedure%' AND c.patient_attendance_status = 'show' THEN 1 ELSE 0 END) as PRC_Consults_new,
   
    (SELECT SUM(amount) 
     FROM nivaancare_production.consultation 
     WHERE (title LIKE '%First Consultation%' OR title LIKE '%Follow up%')
     AND patient_attendance_status = 'show'
     AND consultant_id = c.consultant_id
     AND DATE_FORMAT(consult_datetime, '%d-%m-%y') = DATE_FORMAT(c.consult_datetime, '%d-%m-%y')
    ) as Consult_Revenue,
    
    (SELECT SUM(amount) 
     FROM nivaancare_production.consultation 
     WHERE (s.title LIKE '%consult%' OR s.title LIKE '%followup%')
     AND patient_attendance_status = 'show'
     AND consultant_id = c.consultant_id
     AND DATE_FORMAT(consult_datetime, '%d-%m-%y') = DATE_FORMAT(c.consult_datetime, '%d-%m-%y')
    ) as Consult_Revenue_New,
    
    
    (SELECT SUM(amount) 
     FROM nivaancare_production.consultation 
     WHERE title LIKE '%CRP%' 
     AND patient_attendance_status = 'show'
     AND consultant_id = c.consultant_id
     AND DATE_FORMAT(consult_datetime, '%d-%m-%y') = DATE_FORMAT(c.consult_datetime, '%d-%m-%y')
    ) as CRP_Revenue,
    
    (SELECT SUM(amount) 
     FROM nivaancare_production.consultation 
     WHERE s.title LIKE '%CRP_sessions%' 
     AND patient_attendance_status = 'show'
     AND consultant_id = c.consultant_id
     AND DATE_FORMAT(consult_datetime, '%d-%m-%y') = DATE_FORMAT(c.consult_datetime, '%d-%m-%y')
    ) as CRP_Revenue_New,
    
    (SELECT SUM(amount) 
     FROM nivaancare_production.consultation 
     WHERE title LIKE '%PRC%' 
     AND patient_attendance_status = 'show'
     AND consultant_id = c.consultant_id
     AND DATE_FORMAT(consult_datetime, '%d-%m-%y') = DATE_FORMAT(c.consult_datetime, '%d-%m-%y')
    ) as PRC_Revenue,
    
     (SELECT SUM(amount) 
     FROM nivaancare_production.consultation 
     WHERE s.title LIKE '%procedure%' 
     AND patient_attendance_status = 'show'
     AND consultant_id = c.consultant_id
     AND DATE_FORMAT(consult_datetime, '%d-%m-%y') = DATE_FORMAT(c.consult_datetime, '%d-%m-%y')
    ) as PRC_Revenue_New
    
FROM 
    nivaancare_production.consultation c
LEFT JOIN 
    nivaancare_production.admin_profile ap 
ON 
    c.consultant_id = ap.id 
LEFT JOIN 
    nivaancare_production.location l 
ON 
    c.location_id = l.id
LEFT JOIN 
        nivaancare_production.service s ON c.service_id = s.id
WHERE 
    DATE(c.consult_datetime) <= CURDATE()
GROUP BY 
    Date, DoctorName, ClinicName
ORDER BY 
    STR_TO_DATE(Date, '%d-%m-%y') DESC;

"""
df_DATA = pd.read_sql_query(sql_query,mydb)
df_DATA
df_DATA['New_Consults'] = np.where(df_DATA['New_Consults_new'].notnull(), df_DATA['New_Consults_new'], df_DATA['New_Consults'])
df_DATA['Follow_up_Consults'] = np.where(df_DATA['Follow_up_Consults_new'].notnull(), df_DATA['Follow_up_Consults_new'], df_DATA['Follow_up_Consults'])
df_DATA['CRP_Consults'] = np.where(df_DATA['CRP_Consults_new'].notnull(), df_DATA['CRP_Consults_new'], df_DATA['CRP_Consults'])
df_DATA['PRC_Consults'] = np.where(df_DATA['PRC_Consults_new'].notnull(), df_DATA['PRC_Consults_new'],df_DATA['PRC_Consults'])
df_DATA['Consult_Revenue'] = np.where(df_DATA['Consult_Revenue_New'].notnull(), df_DATA['Consult_Revenue_New'], df_DATA['Consult_Revenue'])
df_DATA['CRP_Revenue'] = np.where(df_DATA['CRP_Revenue_New'].notnull(), df_DATA['CRP_Revenue_New'], df_DATA['CRP_Revenue'])
df_DATA['PRC_Revenue'] = np.where(df_DATA['PRC_Revenue_New'].notnull(), df_DATA['PRC_Revenue_New'], df_DATA['PRC_Revenue'])

df_Final = df_DATA [["Date", "DoctorName", "ClinicName", "New_Consults", "Follow_up_Consults", "Physio_Consults", "Nutrition_Consults", "Psychology_Consults", "Consult_Revenue", "Psychology_Prescribed", "Nutrition_Prescribed", "Care_Plan_Prescribed" ,"SingleSpec_CRP_Booked","MultiSpecCRPBooked","PhysioBooked","NutritionBooked","PsychBooked","CRP_Revenue","PRCPrescribed","PRCDone","PRC_Revenue"]]

df_Final['Date'] = pd.to_datetime(df_Final['Date'], format='%d-%m-%y').dt.strftime('%Y-%m-%d')

df_Final_filtered = df_Final.loc[(df_Final['New_Consults'] != 0) |
                                 (df_Final['Follow_up_Consults'] != 0) ]
df_Final_filtered
df_Final_filtered_sorted = df_Final_filtered.sort_values(by='Date', ascending=False)
df_Final_filtered_sorted
df_Final_filtered_sorted.to_csv('MIS_Tracker.csv',index = False)
df_Final_filtered_sorted
print(df_Final_filtered_sorted)


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
spreadsheetId = '1ldw_7GtmHxRy_tYTaajyeItqezcLRwWztqxnWHYP528'
sh = gc.open_by_key(spreadsheetId)

# Define the sheet name and CSV file path
sheetName = 'Running_MIS_Tracker'
csvFile = 'MIS_Tracker.csv'

# Clear existing values in the specified range
sh.values_clear("'Running_MIS_Tracker'!A:M")

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
print(f"CHECK_CRON_GIT")