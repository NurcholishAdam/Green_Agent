# src/analysis/execution_trace.py

from dataclasses import dataclass, field
from typing import List, Dict, Any


@dataclass
class ExecutionTrace:
    """
    Records step-level execution events for post-hoc analysis.
    """
    steps: List[Dict[str, Any]] = field(default_factory=list)

    def record(self, event: Dict[str, Any]) -> None:
        self.steps.append(event)

    def to_dict(self) -> Dict[str, Any]:
        return {"steps": self.steps}
