class SustainabilityFitnessScorer:
    def __init__(self, alpha=0.5, beta=0.3):
        self.alpha = alpha  # weight for accuracy
        self.beta = beta    # weight for energy efficiency

    def compute(self, profile: ExpertProfile):
        """
        Computes a score used by the router and optimizer.
        Higher is better.
        """
        # Use compressed metrics if available, else full
        acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
        energy = profile.energy_per_inference_compressed if profile.compressed_flag else profile.energy_per_inference_full
        
        # Normalize energy (assuming a baseline max energy of 10J)
        normalized_energy = max(0, 1 - (energy / 10.0)) 
        
        # Fitness = Accuracy - Penalty for high energy + Reward for compression
        base_fitness = acc 
        energy_penalty = self.beta * (1 - normalized_energy) 
        compression_bonus = 0.05 if profile.compressed_flag else 0.0
        
        profile.sustainability_fitness_score = base_fitness - energy_penalty + compression_bonus
        return profile.sustainability_fitness_score
