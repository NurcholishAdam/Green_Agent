import pyRAPL
from .base_meter import BaseEnergyMeter

class RAPLCPUMeter(BaseEnergyMeter):
    """
    Real CPU energy via Intel RAPL.
    """

    def __init__(self):
        pyRAPL.setup()
        self.meter = pyRAPL.Measurement('green_agent')

    def start(self):
        self.meter.begin()

    def stop(self):
        self.meter.end()
        energy = sum(self.meter.result.pkg)
        return {
            "energy_joules": energy / 1e6,
            "provenance": "measured"
        }
