import time

class EnergyMeter:
    """
    Real energy measurement with graceful degradation.
    """

    def __init__(self):
        self._tracker = None
        self._start_time = None
        self._energy_joules = 0.0

        try:
            from codecarbon import EmissionsTracker
            self._tracker = EmissionsTracker(
                measure_power_secs=1,
                log_level="error"
            )
        except Exception:
            self._tracker = None

    def start(self):
        self._start_time = time.time()
        if self._tracker:
            self._tracker.start()

    def stop(self):
        if self._tracker:
            emissions = self._tracker.stop()
            # convert kWh → Joules
            self._energy_joules = emissions.energy_consumed * 3.6e6
        else:
            elapsed = time.time() - self._start_time
            # conservative fallback
            self._energy_joules = elapsed * 5.0  # 5W baseline

    def joules(self):
        return self._energy_joules
