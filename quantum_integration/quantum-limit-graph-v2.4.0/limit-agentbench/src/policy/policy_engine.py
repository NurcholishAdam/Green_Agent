# new import
from rewards.negawatt_reward import NegawattReward

class PolicyEngine:

    def __init__(self, energy_budget, baseline_energy=None):
        self.energy_budget = energy_budget
        self.baseline_energy = baseline_energy
        self.negawatt_module = None

        if baseline_energy:
            self.negawatt_module = NegawattReward(baseline_energy)

    # keep all your existing methods here

    def compute_sustainability_reward(self, accuracy, energy):
        if not self.negawatt_module:
            return 0.0

        return self.negawatt_module.combined_reward(
            accuracy=accuracy,
            energy=energy
        )

    def suggest_temporal_shift(self, shifter, forecast_engine, energy_kwh):

        current = forecast_engine.current_intensity()
        forecast = forecast_engine.forecast_next_hours(4)

        return shifter.suggest(
            current,
            forecast,
            energy_kwh
        )
