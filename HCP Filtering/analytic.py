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
import streamlit as st
import polars as pl
import plotly.express as px

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
medical_events_id = "4aa719bf-ede6-471a-aef5-73c5c43f8aea"
patient_demographics_id = "fc518f94-c2ed-4038-ada9-3f0873577acf"
providers_id = "413ee509-d67a-45c1-bd1e-83c00839f62f"
patient_geographic_id = "a04b9d6c-abdc-4c0c-bb78-82d17db39672"
plans_id = "04fb0851-4b0a-4948-bbc3-c880c24b5ef0"


### --- Analysis --- ###

st.set_page_config(
    page_title="Procedure Trends | YOUR COMPANY HERE",
    page_icon="📈",
    layout="wide"
)

st.markdown("""
    <style>
        .main {
            background-color: #ffffff;
            font-family: 'Helvetica Neue', sans-serif;
        }
        .css-18e3th9 {
            padding-top: 1rem;
        }
        .stButton>button {
            background-color: #0057C0;
            color: white;
            border-radius: 4px;
            padding: 0.4rem 1.2rem;
        }
        .stButton>button:hover {
            background-color: #003f95;
        }
    </style>
""", unsafe_allow_html=True)

col1, col2 = st.columns([1, 5])
with col1:
    logo_path = "Komodo-light.png"
    if os.path.exists(logo_path):
        st.image(logo_path, width=120)
with col2:
    st.markdown("### **Procedure Trends Explorer**")
    st.caption("by YOUR COMPANY HERE – Apps can be customized according to your needs")

KOMODO_PATH = "komodo_procedure_adoption.csv"
komodo = pl.read_csv(KOMODO_PATH).with_columns(pl.lit("KOMODO").alias("TYPE"))
komodo = komodo.rename({col: col.lower() for col in komodo.columns})

df = komodo.with_columns([
    pl.col("service_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("service_date_parsed"),
    pl.col("service_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).dt.strftime("%Y-%m").alias("procedure_month")
])

code_labels = {
    "22612": "22612 – Lumbar spine fusion, single level (posterior technique)",
    "22630": "22630 – Arthrodesis, lumbar, interbody technique, including laminectomy/discectomy",
    "22633": "22633 – Combined posterior and interbody lumbar fusion, single segment"
}
target_codes = list(code_labels.keys())

df = df.filter(pl.col("procedure_code").is_in(target_codes))

selected_label = st.sidebar.selectbox("Select Procedure Code", list(code_labels.values()))
selected_code = [code for code, label in code_labels.items() if label == selected_label][0]
final_dataset = df
st.sidebar.header("Filter Panel")

group_by_options = ["provider_state", "payer_name", "patient_gender"]
group_by = st.sidebar.selectbox("Group By", group_by_options)

if selected_code is not None:
    filtered = df.filter(pl.col("procedure_code") == selected_code)

    procedure_months = (
        filtered.select("procedure_month")
        .unique()
        .to_series()
        .to_list()
    )
    procedure_months.sort()

    default_start = next((m for m in procedure_months if m >= "2020-01"), procedure_months[0])
    default_end = max(procedure_months)

    min_month, max_month = st.sidebar.select_slider(
        "Select Procedure Month Range",
        options=procedure_months,
        value=(default_start, default_end)
    )

    filtered = filtered.filter(
        (pl.col("procedure_month") >= min_month) &
        (pl.col("procedure_month") <= max_month)
    )

    grouped = (
        filtered
        .group_by(["procedure_month", group_by])
        .agg(pl.count("medical_event_id").alias("procedure_count"))
        .sort("procedure_month")
    )

    df_pd = grouped.to_pandas()

    fig = px.line(
        df_pd,
        x="procedure_month",
        y="procedure_count",
        color=group_by,
        labels={
            "procedure_month": "Month",
            "procedure_count": "Procedure Volume",
            group_by: group_by.replace("_", " ").title()
        },
        title=f"Monthly ASC Volume for {selected_code} by {group_by.replace('_', ' ').title()}"
    )

    fig.update_layout(width=1400, height=700)
    st.plotly_chart(fig)
else:
    st.info("Please select a procedure code from the sidebar.")


### --- Outputs --- ###
table_name, output_env_var_name, error = register_output("df", final_dataset, connection)
