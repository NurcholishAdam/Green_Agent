"""
Streamlit Dashboard for Green Agent.
Run with: streamlit run dashboard/app.py
"""

import streamlit as st
from telemetry_loader import TelemetryLoader
from pareto_visualizer import ParetoVisualizer

st.set_page_config(layout="wide")
st.title("🌱 Green Agent Dashboard")

uploaded = st.file_uploader("Upload green_agent_report.json")

if uploaded:
    report = TelemetryLoader.load(uploaded)
    df = TelemetryLoader.to_dataframe(report)

    st.subheader("📊 Metrics Overview")
    st.dataframe(df)

    st.subheader("📈 Pareto Visualization")
    fig = ParetoVisualizer.plot(df)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("🧠 Reflection Summary")
    st.write(report.get("reflection"))
