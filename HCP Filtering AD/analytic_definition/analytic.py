
### Import Libraries and Connect to Snowflake ###
from komodo.client import Client
from komodo.analytics import get_input, register_output
import logging
from komodo.snowflake import get_snowflake_connection
import os
import pandas as pd
from komodo.extensions.dataset import upload_dataset_to_maplab

client = Client()
account_id = os.getenv("KOMODO_ACCOUNT_ID")

conn = get_snowflake_connection(account_id)
curs = conn.cursor()
curs.execute("USE ROLE CUSTOMER_ROLE")
print("--- Success connecting to Snowflake ---")

### Inputs ###
table_1 = "f96771d3-0300-48e0-a96a-0a97debc33f7"
table_2 = "622d224d-e486-410c-b3ba-5b32a0307f19"
print("--- Success setting inputs ---")

### Get Snowflake Location for Datasets ###
def get_snowflake_location(dataset_id: str) -> str:
    get_dataset_response = client.data_catalog.get_dataset(dataset_id)
    return get_dataset_response.manifestations[0].fully_qualified_name

table_1_table = get_snowflake_location(table_1)
table_2_table = get_snowflake_location(table_2)

print("--- Success getting Snowflake location ---")

### Datasets to Pandas DF ###
table_1_sf = f"""
SELECT * 
FROM {table_1_table}
"""

table_1_df = pd.read_sql(table_1_sf, conn)
print("--- Success querying data from table 1 ---")

table_2_sf = f"""
SELECT * 
FROM {table_2_table}
"""

table_2_df = pd.read_sql(table_2_sf, conn)
print("--- Success querying data from table 2 ---")

### Filtering ### 
unique_query = f"""
SELECT PRESCRIBER_NPI
FROM {table_1_df}
EXCEPT
SELECT PRESCRIBER_NPI
FROM {table_2_df};
"""

print("--- Success filtering data ---")

uniques = pd.read_sql(unique_query, conn)

### Upload to MapLab ###
dataset_name = "UNIQUE_HCPS_FROM_ANALYTIC"
table_name, output_env_var_name, error = register_output(dataset_name, uniques, conn)
print("--- Analytic Success ---")
