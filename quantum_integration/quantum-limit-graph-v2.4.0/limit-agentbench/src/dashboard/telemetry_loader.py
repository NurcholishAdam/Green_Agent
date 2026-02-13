"""
Loads Green Agent telemetry JSON reports.
"""

import json
import pandas as pd
from typing import Dict


class TelemetryLoader:
    """
    Loads green_agent_report.json files and converts to dataframe.
    """

    @staticmethod
    def load(path: str) -> Dict:
        with open(path, "r") as f:
            return json.load(f)

    @staticmethod
    def to_dataframe(report: Dict) -> pd.DataFrame:
        metrics = report.get("metrics", {})
        return pd.DataFrame([metrics])
