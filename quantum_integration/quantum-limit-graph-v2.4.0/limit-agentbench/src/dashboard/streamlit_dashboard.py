import streamlit as st
import pandas as pd
import json
import os

st.title("Green Agent Sustainability Dashboard")

TELEMETRY_FILE = "telemetry_stream.json"

if os.path.exists(TELEMETRY_FILE):

    data = []

    with open(TELEMETRY_FILE, "r") as f:
        for line in f:
            data.append(json.loads(line))

    df = pd.DataFrame(data)

    st.subheader("Raw Telemetry")
    st.dataframe(df)

    st.subheader("Pareto Frontier")

    st.scatter_chart(
        df,
        x="energy_kwh",
        y="latency"
    )

else:
    st.warning("No telemetry data yet.")
