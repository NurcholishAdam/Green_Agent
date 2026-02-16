from green_agent.rewards.negawatt_reward import NegawattReward


class PolicyEngine:

    def __init__(self, energy_budget, baseline_energy=None):
        self.energy_budget = energy_budget
        self.baseline_energy = baseline_energy

        if baseline_energy is not None:
            self.negawatt_module = NegawattReward(baseline_energy)
        else:
            self.negawatt_module = None

    # --- Sustainability Reward ---

    def compute_sustainability_reward(self, accuracy, energy):

        if self.negawatt_module is None:
            return 0.0

        reward = self.negawatt_module.combined_reward(
            accuracy=accuracy,
            energy=energy
        )

        # Budget enforcement penalty
        if energy > self.energy_budget:
            reward -= 1.0

        return reward

    # --- Temporal Carbon Shift ---

    def suggest_temporal_shift(
        self,
        shifter,
        forecast_engine,
        energy_kwh,
        shiftable=True,
        urgency="low"
    ):

        if not shiftable or urgency == "high":
            return 0, 0.0

        current = forecast_engine.current_intensity()
        forecast = forecast_engine.forecast_next_hours(4)

        return shifter.suggest(
            current,
            forecast,
            energy_kwh
        )
