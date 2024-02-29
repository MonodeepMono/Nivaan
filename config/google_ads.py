import datetime
import pandas as pd
from pandas import DataFrame
from pprint import pprint
import json
import xlsxwriter
import numpy as np
import pandas.io.sql as psql
import mysql.connector


from google.ads.googleads.client import GoogleAdsClient
import json
from datetime import datetime
def get_current_Date():
    current_datetime = datetime.now()

    return current_datetime.strftime('%Y-%m-%d %H:%M:%S')

def convert_google_ads_row_to_dict(google_ads_row):
    return {
        
            "account_id": google_ads_row.customer.id,
            "descriptive_name": google_ads_row.customer.descriptive_name,
            "ad_group_id": google_ads_row.ad_group.id,
            "ad_id": google_ads_row.ad_group_ad.ad.id,
            "ad_group_name": google_ads_row.ad_group.name,
            "campaign_resource_name": google_ads_row.campaign.resource_name,
            "campaign_id": google_ads_row.campaign.id,
            "campaign_name": google_ads_row.campaign.name,
            "campaign_start_date": google_ads_row.campaign.start_date,
            "campaign_end_date": google_ads_row.campaign.end_date,
            "clicks": google_ads_row.metrics.clicks,
            "conversions": google_ads_row.metrics.conversions,
            "cost_micros": google_ads_row.metrics.cost_micros,
            "impressions": google_ads_row.metrics.impressions,
            "ctr": google_ads_row.metrics.ctr,
            "cost_per_conversion": google_ads_row.metrics.cost_per_conversion,
            "average_cost": google_ads_row.metrics.average_cost,
            "average_cpc": google_ads_row.metrics.average_cpc,
            "conversions_from_interactions_rate": google_ads_row.metrics.conversions_from_interactions_rate,
            "created_at":get_current_Date(),
            "updated_at":get_current_Date(),
            "created_by" : "GoogleAdsAPI-PythonScript",
            "updated_by" : "GoogleAdsAPI-PythonScript",
            "campaign_date": google_ads_row.segments.date,
            
    }

client = GoogleAdsClient.load_from_storage("google-ads.yaml")
ga_service = client.get_service("GoogleAdsService")

query = """
SELECT

ad_group_ad.ad.id,
ad_group_ad.ad.name,

ad_group.id,
ad_group.name,

campaign.id,
campaign.name,
campaign.primary_status,
campaign.start_date,
campaign.end_date,

metrics.average_cost,
metrics.cost_micros,
metrics.conversions,
metrics.cost_per_conversion,
metrics.conversions_from_interactions_rate,
metrics.average_cpc,
metrics.ctr,
metrics.clicks,
metrics.impressions,

customer.id,
customer.descriptive_name,

segments.date
FROM ad_group_ad
WHERE segments.date BETWEEN '2024-02-28' AND '2024-02-29'
ORDER BY
campaign.id
"""

# df_DATA = pd.read_sql_query(sql_query,client)
# df_DATA
# print(df_DATA)

# Convert and save in batches
batch_size = 1000
converted_data = []

try:
    # Issues a search request using streaming.
    stream = ga_service.search_stream(customer_id="1580734935", query=query)

    for batch in stream:
        for row in batch.results:
            converted_data.append(convert_google_ads_row_to_dict(row))
            # print(converted_data)

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # final cleanup or actions here if needed
    pass

df = pd.DataFrame(converted_data)
print(df)