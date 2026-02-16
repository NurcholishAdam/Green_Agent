def create_episode_record(
    episode,
    accuracy,
    energy,
    carbon,
    reward,
    provenance_energy,
    provenance_carbon
):
    return {
        "episode": episode,
        "accuracy": float(accuracy),
        "energy_joules": float(energy),
        "carbon_grams": float(carbon),
        "reward": float(reward),
        "energy_provenance": provenance_energy,
        "carbon_provenance": provenance_carbon
    }
