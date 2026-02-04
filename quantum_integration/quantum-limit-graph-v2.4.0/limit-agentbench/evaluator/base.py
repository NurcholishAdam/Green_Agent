from abc import ABC, abstractmethod

class AbstractEvaluator(ABC):
    """Stable evaluator interface for Green Agent."""

    @abstractmethod
    def evaluate(self, metrics: dict) -> dict:
        """
        Evaluate a purple agent run.
        Returns normalized scores + violations.
        """
        pass
