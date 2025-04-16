import streamlit as st
import polars as pl
import plotly.express as px
import os

# --- Config ---
st.set_page_config(
    page_title="Procedure Trends | YOUR COMPANY HERE",
    page_icon="📈",
    layout="wide"
)

# --- Custom Header ---
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

# --- Logo and Title ---
col1, col2 = st.columns([1, 5])
with col1:
    logo_path = "Komodo-light.png"
    if os.path.exists(logo_path):
        st.image(logo_path, width=120)
with col2:
    st.markdown("### **Procedure Trends Explorer**")
    st.caption("by YOUR COMPANY HERE – Apps can be customized according to your needs")

# --- Load Data ---
KOMODO_PATH = "komodo_procedure_adoption.csv"
komodo = pl.read_csv(KOMODO_PATH).with_columns(pl.lit("KOMODO").alias("TYPE"))
komodo = komodo.rename({col: col.lower() for col in komodo.columns})

# --- Parse Dates ---
df = komodo.with_columns([
    pl.col("service_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("service_date_parsed"),
    pl.col("service_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).dt.strftime("%Y-%m").alias("procedure_month")
])

# Limit to specific procedure codes
code_labels = {
    "22612": "22612 – Lumbar spine fusion, single level (posterior technique)",
    "22630": "22630 – Arthrodesis, lumbar, interbody technique, including laminectomy/discectomy",
    "22633": "22633 – Combined posterior and interbody lumbar fusion, single segment"
}
target_codes = list(code_labels.keys())

# Filter the full dataset to only those codes
df = df.filter(pl.col("procedure_code").is_in(target_codes))

# Sidebar procedure code selector with labels
selected_label = st.sidebar.selectbox("Select Procedure Code", list(code_labels.values()))
selected_code = [code for code, label in code_labels.items() if label == selected_label][0]

# --- Sidebar Filters ---
st.sidebar.header("Filter Panel")

group_by_options = ["provider_state", "payer_name", "patient_gender"]
group_by = st.sidebar.selectbox("Group By", group_by_options)

# --- Main Logic ---
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
