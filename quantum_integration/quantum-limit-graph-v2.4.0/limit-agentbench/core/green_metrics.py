# Update src/core/green_metrics.py

from analysis.complexity_analyzer import ComplexityAnalyzer
from metrics.efficiency_calculator import NormalizedEfficiencyCalculator

# Optional imports for enhancements
try:
    import numpy as np
    from collections import deque
    import random
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False


class GreenMetricsTracker:
    """
    Enhanced green metrics tracker with optional integration of LIMIT Graph,
    MODP, RLHF, Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation,
    and MoE expert gating.

    The enhancements are enabled via ``config`` dictionary passed to the
    constructor. When disabled, the class behaves as the original.
    """

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.use_enhancements = self.config.get('use_enhancements', False) and ENHANCEMENTS_AVAILABLE

        self.complexity_analyzer = ComplexityAnalyzer()
        self.efficiency_calc = NormalizedEfficiencyCalculator()

        # Additional attributes (populated during tracking)
        self.energy_kwh = 0.0
        self.carbon_co2e_kg = 0.0
        self.latency_ms = 0.0
        self.accuracy = 0.0
        self.trace = {}

        # Enhanced attributes
        if self.use_enhancements:
            self.modp_weights = self.config.get('modp_weights', [0.3, 0.3, 0.2, 0.2])  # energy, carbon, latency, accuracy
            if not isinstance(self.modp_weights, list) or len(self.modp_weights) != 4:
                self.modp_weights = [0.3, 0.3, 0.2, 0.2]
            total = sum(self.modp_weights)
            self.modp_weights = [w / total for w in self.modp_weights]
            self.human_feedback_score = self.config.get('human_feedback_score', 0.5)
            self.graph_metrics = self.config.get('graph_metrics', {'centrality': 0.5, 'connectivity': 0.5})
            # Distillation + MoE
            self.distillation_optimizer = self._create_distillation_optimizer()
            # Evolutionary
            self.evolutionary_weights = None
            if self.config.get('use_evolutionary', False):
                self.evolutionary_weights = self._create_evolutionary_optimizer()
        else:
            self.distillation_optimizer = None

    def get_normalized_metrics(self, trace: Dict) -> Dict:
        """Get complexity-normalized metrics, optionally enhanced."""
        complexity = self.complexity_analyzer.analyze_from_trace(trace)
        base_metrics = {
            'task_complexity': complexity.compute_composite_score(),
            'complexity_tier': self.complexity_analyzer.categorize_complexity(complexity),
            'energy_efficiency': self.efficiency_calc.calculate_energy_efficiency(
                self.energy_kwh, trace
            ),
            'accuracy_per_watt': self.efficiency_calc.calculate_accuracy_per_watt(
                self.accuracy, self.energy_kwh
            )
        }

        if not self.use_enhancements:
            return base_metrics

        # Enhanced MODP + RLHF adjustments
        # Compute a composite sustainability score using MODP weights
        # Normalize inputs
        energy_norm = 1.0 - min(self.energy_kwh / 10.0, 1.0)  # assume 10 kWh max
        carbon_norm = 1.0 - min(self.carbon_co2e_kg / 1.0, 1.0)
        latency_norm = 1.0 - min(self.latency_ms / 10000.0, 1.0)
        accuracy_norm = min(self.accuracy, 1.0)

        # Apply RLHF: human feedback adjusts weights slightly
        h = self.human_feedback_score
        # Higher feedback increases weight on accuracy, decreases energy weight
        adjusted_weights = [
            self.modp_weights[0] * (1 - 0.2 * h),
            self.modp_weights[1] * (1 - 0.1 * h),
            self.modp_weights[2] * (1 - 0.1 * h),
            self.modp_weights[3] * (1 + 0.3 * h)
        ]
        adjusted_weights = [w / sum(adjusted_weights) for w in adjusted_weights]

        composite_score = (
            adjusted_weights[0] * energy_norm +
            adjusted_weights[1] * carbon_norm +
            adjusted_weights[2] * latency_norm +
            adjusted_weights[3] * accuracy_norm
        )

        # Graph metrics influence complexity and efficiency slightly
        centrality = self.graph_metrics.get('centrality', 0.5)
        connectivity = self.graph_metrics.get('connectivity', 0.5)
        graph_factor = 0.8 + 0.2 * centrality * connectivity

        base_metrics['modp_composite_score'] = composite_score
        base_metrics['graph_adjusted_complexity'] = base_metrics['task_complexity'] * graph_factor

        # Distillation: use learned weights to adjust final score
        if self.distillation_optimizer:
            state_vec = np.array([
                energy_norm,
                carbon_norm,
                latency_norm,
                accuracy_norm,
                centrality,
                connectivity,
                h,
                base_metrics['task_complexity']
            ], dtype=np.float32)
            learned_score = self.distillation_optimizer.predict(state_vec)
            # Blend with MODP score
            final_score = 0.7 * composite_score + 0.3 * learned_score
            base_metrics['distillation_score'] = float(learned_score)
            base_metrics['final_sustainability_score'] = float(final_score)
            # Update distillation with reward (assume composite_score is reward)
            self.distillation_optimizer.update(state_vec, composite_score)
        else:
            base_metrics['final_sustainability_score'] = composite_score

        # Evolutionary weights if enabled
        if self.evolutionary_weights:
            self.evolutionary_weights.update_fitness(composite_score)
            best_weights = self.evolutionary_weights.get_best_weights()
            base_metrics['evolutionary_weights'] = best_weights.tolist()

        return base_metrics

    # ------------------------------------------------------------------
    # Helper methods for enhanced components
    # ------------------------------------------------------------------

    def _create_distillation_optimizer(self):
        class DistillationOptimizer:
            def __init__(self, feature_dim=8):
                self.weights = np.zeros(feature_dim)
                self.bias = 0.0
                self.lr = 0.01
                self.counter = 0

            def predict(self, state_vec):
                return float(np.dot(state_vec, self.weights) + self.bias)

            def update(self, state_vec, target):
                # Simple regression update
                pred = self.predict(state_vec)
                grad = (pred - target) * state_vec
                self.weights -= self.lr * grad
                self.bias -= self.lr * (pred - target)
                self.counter += 1

        return DistillationOptimizer()

    def _create_evolutionary_optimizer(self):
        class EvolutionaryOptimizer:
            def __init__(self, n_weights=4, population_size=10):
                self.population = [np.random.dirichlet(np.ones(n_weights)) for _ in range(population_size)]
                self.fitness = np.zeros(population_size)
                self.best_weights = self.population[0]
                self.best_fitness = 0.0
                self.counter = 0

            def update_fitness(self, reward, index=0):
                self.fitness[index] = reward
                best_idx = int(np.argmax(self.fitness))
                self.best_weights = self.population[best_idx]
                self.best_fitness = self.fitness[best_idx]
                # Simple evolution
                new_pop = [self.best_weights]
                while len(new_pop) < len(self.population):
                    parent = self.population[random.randint(0, len(self.population)-1)]
                    child = parent + np.random.dirichlet(np.ones(len(parent))) * 0.1
                    child = child / child.sum()
                    new_pop.append(child)
                self.population = new_pop
                self.fitness = np.zeros(len(self.population))
                self.counter += 1

            def get_best_weights(self):
                return self.best_weights

        return EvolutionaryOptimizer()
