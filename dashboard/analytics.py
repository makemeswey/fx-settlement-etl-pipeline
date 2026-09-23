import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
import pandas as pd
import plotly.express as px

from paths import CURRENCY_SUMMARY, STATUS_SUMMARY, DAILY_SUMMARY

currency_df = pd.read_parquet(CURRENCY_SUMMARY)
status_df = pd.read_parquet(STATUS_SUMMARY)
daily_df = pd.read_parquet(DAILY_SUMMARY)
daily_df["date"] = pd.to_datetime(daily_df["date"])

def display_kpi():
    total_transactions = int(daily_df["transaction_count"].sum())
    total_volume = daily_df["total_myr_volume"].sum()
    settled_transactions = int(status_df.loc[status_df["status"] == "SETTLED","count"].sum())
    failed_transactions = int(status_df.loc[status_df["status"] == "FAILED","count"].sum())

    st.title("FX Settlement Analytics Dashboard")
    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Total Transactions", f"{round(total_transactions,0)}")
    col2.metric("Total Volume (MYR)", f"RM {round(total_volume,2)}")
    col3.metric("Settled", f"{settled_transactions}")
    col4.metric("Failed", f"{failed_transactions}")

def display_transaction_trend():
    st.subheader("Daily Transaction Volume")

    fig_daily = px.line(
        daily_df,
        x="date",
        y="transaction_count",
        markers=True,
        title="Transactions Over Time"
    )

    st.plotly_chart(fig_daily, use_container_width=True)

def display_currency_volume_status():
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Volume by Currency")

        fig_currency = px.bar(
            currency_df,
            x="target_currency",
            y="total_myr_volume",
            title="Total MYR Volume by Currency"
        )

        st.plotly_chart(fig_currency, use_container_width=True)

    with col2:
        st.subheader("Daily MYR Volume")

        fig_volume = px.bar(
            daily_df,
            x="date",
            y="total_myr_volume",
            title="Daily MYR Transaction Volume"
        )

        st.plotly_chart(fig_volume, use_container_width=True)

    with col3:
            st.subheader("Settlement Status")
    
            fig_status = px.pie(
                status_df, 
                names="status",
                values="count",
                hole=0.4,
                title="Settlement Status Breakdown"
            )
    
            st.plotly_chart(fig_status, use_container_width=True)

if __name__ == "__main__":
    st.set_page_config(page_title="FX Settlement Dashboard", layout="wide")
    display_kpi()
    display_transaction_trend()
    display_currency_volume_status()
