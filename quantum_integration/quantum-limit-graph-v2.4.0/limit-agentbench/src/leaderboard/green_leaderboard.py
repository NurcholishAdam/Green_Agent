from typing import List, Dict


class GreenLeaderboard:
    """
    Sustainability ranking system.
    """

    def __init__(self):
        self.records: List[Dict] = []

    def add(self, agent: str, accuracy: float, energy: float, negawatt: float):
        self.records.append({
            "agent": agent,
            "accuracy": accuracy,
            "energy": energy,
            "accuracy_per_watt": accuracy / energy if energy > 0 else 0,
            "negawatt_score": negawatt,
        })

    def rank(self) -> List[Dict]:
        return sorted(self.records,
                      key=lambda x: x["negawatt_score"],
                      reverse=True)
