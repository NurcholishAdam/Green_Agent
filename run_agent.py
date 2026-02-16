from rewards.negawatt_reward import NegawattReward
from leaderboard.green_leaderboard import GreenLeaderboard
from carbon.carbon_forecast import CarbonForecast
from carbon.temporal_shifter import TemporalShifter
from analysis.pareto_analyzer import ParetoAnalyzer
from policy.policy_engine import PolicyEngine
from core.meta_cognition import MetaCognitiveLayer


def run():

    # Simulated metrics
    accuracy = 0.92
    energy = 95.0
    latency = 1.1

    # Policy
    policy = PolicyEngine(energy_budget=100)
    mode = policy.adaptive_mode(energy)

    # Negawatt reward
    negawatt = NegawattReward(baseline_energy=150)
    negawatt_score = negawatt.negawatt_score(accuracy, energy)
    reward = negawatt.combined_reward(accuracy, energy)

    # Leaderboard
    leaderboard = GreenLeaderboard()
    leaderboard.add("PurpleAgent", accuracy, energy, negawatt_score)

    # Carbon shifting
    forecast_engine = CarbonForecast()
    shifter = TemporalShifter()

    current_intensity = forecast_engine.current_intensity()
    forecast = forecast_engine.forecast_next_hours(4)

    best_hour, saving = shifter.suggest(
        current_intensity,
        forecast,
        energy_kwh=0.5
    )

    # Pareto
    analyzer = ParetoAnalyzer()
    frontier = analyzer.compute_frontier([
        {"accuracy": accuracy, "energy": energy}
    ])

    # Reflection
    meta = MetaCognitiveLayer()
    explanation = meta.reflect(accuracy, energy)

    print("Mode:", mode)
    print("Reward:", reward)
    print("Negawatt:", negawatt_score)
    print("Delay Suggestion:", best_hour, "hours")
    print("Carbon Saving:", saving)
    print("Pareto Frontier:", frontier)
    print("Reflection:", explanation)


if __name__ == "__main__":
    run()
