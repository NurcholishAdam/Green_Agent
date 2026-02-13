from abc import ABC, abstractmethod
from typing import Dict

class BaseEnergyMeter(ABC):
    """
    Abstract base class for energy measurement backends.
    """

    @abstractmethod
    def start(self):
        """Start measurement."""
        pass

    @abstractmethod
    def stop(self) -> Dict:
        """
        Stop measurement and return metrics:
        {
            "energy_joules": float,
            "power_watts": float,
            "provenance": "measured|estimated"
        }
        """
        pass
