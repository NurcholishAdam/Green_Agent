SUSTAINABILITY_CONFIG = {
    # Triggers compression if full inference exceeds this (Joules)
    "energy_threshold": 5.0,  
    
    # Max allowable accuracy drop (absolute difference, e.g., 0.02 = 2%)
    "accuracy_drop_tolerance": 0.02,  
    
    # Energy estimation coefficient (pJ per MAC)
    "energy_per_mac": 0.5e-12,  
    
    # Fitness weighting
    "fitness_accuracy_weight": 0.6,
    "fitness_energy_weight": 0.4,
}
