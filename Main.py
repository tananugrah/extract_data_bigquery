#!/usr/bin/python

import os
import datetime
from google.cloud import bigquery


SERVICE_ACCOUNT_JSON = os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = \
    '/Users/tananugrahr/Documents/key_google/vibrant-fabric-364101-b92cc447257a.json'
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

bucket_name = 'extracted_database'
project =  "bigquery-public-data"
Dataset_id = "hacker_news"
Table_id = "stories"

destination_uri = "gs://{}/{}".format(bucket_name, "extracted_stories_data.csv")
dataset_ref = bigquery.DatasetReference(project,Dataset_id)
table_ref = dataset_ref.table(Table_id)
Extract_job = client.extract_table(
	table_ref,
	destination_uri, 
	location="US"
)
Extract_job.result()

timestamp = datetime.datetime.now()
print(f"{timestamp}: Exported {project}:{Dataset_id}.{Table_id} to {destination_uri}")


#///////////////////////////////////////////////////////////////////////
# client = bigquery.Client()

# def big_query_to_csv():
#     sql_query = """ 
#             SELECT id FROM bigquery-public-data.hacker_news.stories LIMIT 5
#             """
    
#     query_job = client.query(sql_query)
#     # print(query_job)
#     for row in query_job.result():
#             print(row)

#     df = client.query(sql_query).to_dataframe() 
#     df.to_csv('extracted_data_hacker_news2.csv', index=False,header=True)
#     print('csv file generated success')

# big_query_to_csv()
    