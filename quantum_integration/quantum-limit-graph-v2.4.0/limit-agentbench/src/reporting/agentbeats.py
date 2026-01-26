"""
AgentBeats-compatible reporting utilities.
"""

from typing import List, Dict
import json


def build_agentbeats_submission(
    image: str,
    queries: List[Dict],
    output_path: str = "agentbeats_submission.json",
):
    submission = {
        "image": image,
        "queries": queries,
    }

    with open(output_path, "w") as f:
        json.dump(submission, f, indent=2)

    return output_path
