from codecarbon import EmissionsTracker
from .base_meter import BaseEnergyMeter

class CodeCarbonMeter(BaseEnergyMeter):
    """
    Measures carbon and energy using CodeCarbon.
    """

    def __init__(self):
        self.tracker = EmissionsTracker(measure_power_secs=1)

    def start(self):
        self.tracker.start()

    def stop(self):
        emissions = self.tracker.stop()
        return {
            "energy_joules": self.tracker._total_energy.kWh * 3.6e6,
            "carbon_kg": emissions,
            "provenance": "estimated"
        }
