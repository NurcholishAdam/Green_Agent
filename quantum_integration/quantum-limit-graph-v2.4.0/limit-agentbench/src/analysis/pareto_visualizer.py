"""
Interactive Pareto visualization using Plotly.
"""

import plotly.express as px
import pandas as pd


class ParetoVisualizer:
    """
    Creates interactive Pareto frontier plots.
    """

    @staticmethod
    def plot(df: pd.DataFrame):
        fig = px.scatter(
            df,
            x="latency",
            y="energy_kwh",
            size="carbon_kg",
            hover_data=df.columns
        )

        fig.update_layout(
            title="Green Agent Pareto Frontier",
            xaxis_title="Latency (seconds)",
            yaxis_title="Energy (kWh)"
        )

        return fig
