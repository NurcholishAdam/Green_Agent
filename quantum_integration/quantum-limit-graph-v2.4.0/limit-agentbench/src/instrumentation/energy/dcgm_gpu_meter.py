import subprocess
from .base_meter import BaseEnergyMeter

class DCGMMeter(BaseEnergyMeter):
    """
    GPU power draw via NVIDIA DCGM.
    """

    def start(self):
        pass  # DCGM is polled

    def stop(self):
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=power.draw",
             "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True
        )
        watts = float(result.stdout.strip())
        return {
            "power_watts": watts,
            "provenance": "measured"
        }
