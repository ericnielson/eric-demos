import streamlit as st
import polars as pl
import plotly.express as px

KOMODO_PATH = "komodo_procedure_adoption.csv"
st.set_page_config(layout="wide")

komodo = pl.read_csv(KOMODO_PATH).with_columns(pl.lit("KOMODO").alias("TYPE"))

komodo = komodo.rename({col: col.lower() for col in komodo.columns})

df = komodo.with_columns([
    pl.col("service_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("service_date_parsed"),
    pl.col("service_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).dt.strftime("%Y-%m").alias("procedure_month")
])

procedure_codes = df.select("procedure_code").unique().sort("procedure_code").to_series().to_list()
selected_code = st.selectbox("Select Spinal Procedure Code", procedure_codes)

group_by_options = ["provider_state", "payer_name", "patient_gender"]
group_by = st.selectbox("Group By", group_by_options)

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

    min_month, max_month = st.select_slider(
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



