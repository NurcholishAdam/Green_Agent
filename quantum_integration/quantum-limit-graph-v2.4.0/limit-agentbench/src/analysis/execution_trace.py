# src/analysis/execution_trace.py

from dataclasses import dataclass, field
from typing import List, Dict


@dataclass
class ExecutionTrace:
    steps: List[Dict] = field(default_factory=list)

    def record(self, event: Dict):
        self.steps.append(event)
