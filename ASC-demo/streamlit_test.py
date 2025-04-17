import streamlit as st
import polars as pl
import plotly.express as px
import os
from datetime import datetime

st.set_page_config(
    page_title="Procedure Trends | YOUR COMPANY HERE",
    page_icon="📈",
    layout="wide"
)

### STYLING ###
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

### CHRISTIANO RONALDO HEADER ###
col1, col2 = st.columns([1, 5])
with col1:
    logo_path = "Komodo-light.png"
    if os.path.exists(logo_path):
        st.image(logo_path, width=120)
with col2:
    st.markdown("### **Procedure Trends Explorer**")
    st.caption("by YOUR COMPANY HERE – Apps can be customized according to your needs")

### Loading the Data ###
KOMODO_PATH = "komodo_procedure_adoption.csv"

komodo = pl.read_csv(KOMODO_PATH).with_columns(pl.lit("KOMODO").alias("TYPE"))
komodo = komodo.rename({col: col.lower() for col in komodo.columns})

current_year = datetime.now().year

df = komodo.with_columns([
    pl.col("service_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("service_date_parsed"),
    pl.col("service_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).dt.strftime("%Y-%m").alias("procedure_month"),
    (pl.lit(current_year) - pl.col("patient_yob").str.strptime(pl.Date, "%Y-%m-%d", strict=False).dt.year()).alias("patient_age")
])

code_labels = {
    "22612": "22612 – Lumbar spine fusion (posterior)",
    "22630": "22630 – Interbody technique (laminectomy/discectomy)",
    "22633": "22633 – Combined lumbar fusion",
    "S2900": "S2900 – Robotic surgical system (add-on)"
}
target_codes = list(code_labels.keys())
df = df.filter(pl.col("procedure_code").is_in(target_codes))
st.sidebar.header("Filter Panel")

selected_label = st.sidebar.selectbox("Select Procedure Code", list(code_labels.values()))
selected_code = [code for code, label in code_labels.items() if label == selected_label][0]

robotic_only = st.sidebar.checkbox("Filter for Robotic-Assisted Surgeries (CPT S2900 required)", value=False)

group_by_options = ["provider_state", "payer_name", "patient_gender"]
group_by = st.sidebar.selectbox("Group By", group_by_options)

### Procedure Month Filter Setup ###
procedure_months = (
    df.select("procedure_month")
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

if robotic_only:
    robotic_patients = (
        df.filter(pl.col("procedure_code") == "S2900")
        .select(["patient_id", "service_date"])
        .unique()
    )

    filtered = df.join(robotic_patients, on=["patient_id", "service_date"], how="inner")
    filtered = filtered.filter(pl.col("procedure_code") == selected_code)
else:
    filtered = df.filter(pl.col("procedure_code") == selected_code)

filtered = filtered.filter(
    (pl.col("procedure_month") >= min_month) &
    (pl.col("procedure_month") <= max_month)
)

### Preprocessing ###
monthly_grouped = (
    filtered
    .group_by(["procedure_month", group_by])
    .agg(pl.count("medical_event_id").alias("procedure_count"))
    .sort("procedure_month")
)

territory_summary = (
    filtered
    .group_by("provider_state")
    .agg([
        pl.count("medical_event_id").alias("procedure_volume"),
        pl.count("rendering_npi").alias("hcp_count")
    ])
    .with_columns((pl.col("procedure_volume") / pl.col("hcp_count")).alias("avg_procedure_per_hcp"))
    .sort("procedure_volume", descending=True)
)

filtered_with_plans = filtered 

### TABALAB ###
tab1, tab2, tab3 = st.tabs([
    "📈 Procedure Trends",
    "🗺️ Top Diagnoses Codes",
    "🧾 Plans Breakdown"
])

### Procedure Trends ###
with tab1:
    df_pd = monthly_grouped.to_pandas()
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

### Diagnosis Pathways ###
with tab2:
    st.subheader("Top Diagnoses Leading to Selected Procedure")

    diag_counts = (
        filtered
        .group_by("diagnosis_codes")
        .agg(pl.count("medical_event_id").alias("procedure_volume"))
        .sort("procedure_volume", descending=True)
        .filter(pl.col("diagnosis_codes").is_not_null())
        .limit(20)
    )

    diag_df = diag_counts.to_pandas()

    fig2 = px.bar(
        diag_df,
        x="procedure_volume",
        y="diagnosis_codes",
        orientation="h",
        title=f"Top Diagnosis Codes Leading to {selected_code}",
        labels={"procedure_volume": "Procedure Volume", "diagnosis_codes": "Diagnosis Codes"}
    )
    fig2.update_layout(yaxis=dict(autorange="reversed"), height=600)
    st.plotly_chart(fig2)

### Plans Breakdown ###
with tab3:
    st.subheader("Top Payers for Selected Procedure")
    payer_counts = (
        filtered_with_plans.group_by(["payer_name", "parent_name"])
        .agg(pl.count("medical_event_id").alias("procedure_volume"))
        .sort("procedure_volume", descending=True)
        .limit(20)
    )

    payer_df = payer_counts.to_pandas()

    fig4 = px.bar(
        payer_df,
        x="payer_name",
        y="procedure_volume",
        color="parent_name",
        title="Top 20 Payers by Procedure Volume",
        labels={"payer_name": "Payer", "procedure_volume": "Procedure Volume", "parent_name": "Parent Org"}
    )
    fig4.update_layout(xaxis_tickangle=-45, width=1200, height=600)
    st.plotly_chart(fig4)

    st.subheader("Payer Mix by State")
    mix_by_state = (
        filtered_with_plans.group_by(["provider_state", "payer_name"])
        .agg(pl.count("medical_event_id").alias("procedure_volume"))
        .sort("procedure_volume", descending=True)
    )

    mix_df = mix_by_state.to_pandas()

    fig5 = px.bar(
        mix_df,
        x="provider_state",
        y="procedure_volume",
        color="payer_name",
        title="Payer Mix by State",
        labels={"provider_state": "State", "procedure_volume": "Procedure Volume", "payer_name": "Payer"}
    )
    fig5.update_layout(barmode="stack", width=1200, height=600)
    st.plotly_chart(fig5)
