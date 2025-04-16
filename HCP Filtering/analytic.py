### ---- Import Libraries & Connect to Snowflake --- ### 
import os
from komodo.client import Client
from komodo.definitions.models.cohorts.cohort_create import CohortCreate
from komodo.snowflake import get_snowflake_connection
import pandas as pd
from datetime import datetime
from komodo.dataset import upload_dataset_to_maplab
import pprint
from komodo.analytics import AnalyticDefinitionVersion, AnalyticDefinition, AnalyticDialect, InputType, create_analytic_definition, create_dataflow_from_analytic_definition_version, run_dataflow, check_dataflow_run_status
from komodo.analytics import get_input
from komodo.analytics import register_output

now = datetime.now()
os.chdir("/home/dragon/workspaces/current/workspace/src/cookbook/")
client = Client()

print("--- Connecting to Snowflake ---")
account_id = os.getenv("KOMODO_ACCOUNT_ID")
connection = get_snowflake_connection(client.auth.account_id(), client.auth.access_token)
curs = connection.cursor()
curs.execute("USE ROLE CUSTOMER_ROLE")
print("--- Success connecting to Snowflake ---")

### --- Inputs --- ### 
cohort_id = "fltr_def_BMEBEGSBOFPMQLJT"  # This is the Abecma Cohort ID which you can get from either the UI or the Komodo Extensions tab in a Workspace
medical_events_dataset = "7322e8b3-6af8-4c5d-a92f-f114be9b3210" # This is the Medical Events table from the Cohort above
medical_events_table = get_input("medical_events_dataset")["table_location"]


### --- Analysis --- ###
final_dataset = pd.read_sql(f""" SELECT * FROM {medical_events_table} WHERE UPPER(MODIFIERS) IS NOT NULL""", connection)


### --- Outputs --- ###
table_name, output_env_var_name, error = register_output("Abecma_Dataset_From_AD", final_dataset, connection)
