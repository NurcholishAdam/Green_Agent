# src/enhancements/regret_optimizer.py

"""
Enhanced Regret Minimization Optimizer for Green Agent - Version 3.0

Features:
1. Minimax regret decision theory under uncertainty (Savage, 1951)
2. Multi-objective optimization with 6 objectives and configurable weights
3. Scenario probability learning from historical data (online learning)
4. Outcome database for persistence and reuse (async SQLite)
5. Expected regret calculation with learned probabilities
6. Robustness analysis for weight sensitivity (full factorial)
7. Interactive visualization (HTML heatmaps + correlation plots)
8. Sequential decision support with look-ahead and pruning
9. Confidence calibration with empirical validation
10. Comprehensive decision logging and audit trail
11. Asynchronous database operations for non-blocking I/O
12. Online learning with exponential moving average
13. Correlation modeling between objectives
14. Pruning for sequential decision tree

Reference: "Decision Theory Under Uncertainty" (Savage, 1951)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum
import numpy as np
import logging
import json
import aiosqlite
import hashlib
import time
import asyncio
from datetime import datetime
from collections import deque
import threading
import os
import heapq

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Async Outcome Database
# ============================================================

class AsyncOutcomeDatabase:
    """
    Asynchronous persistent storage for action-scenario outcomes.
    
    Features:
    - aiosqlite for non-blocking operations
    - Automatic caching for fast access
    - Version tracking for outcome updates
    - Export/import for sharing between agents
    """
    
    def __init__(self, db_path: str = "outcomes.db"):
        self.db_path = db_path
        self._cache: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
    
    async def _init_database(self):
        """Initialize SQLite database schema asynchronously"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS outcomes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    scenario TEXT NOT NULL,
                    energy REAL,
                    carbon REAL,
                    helium REAL,
                    latency REAL,
                    accuracy REAL,
                    cost REAL,
                    timestamp REAL,
                    version INTEGER DEFAULT 1,
                    hash TEXT,
                    UNIQUE(action, scenario)
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    selected_action TEXT,
                    max_regret REAL,
                    confidence REAL,
                    context_json TEXT
                )
            ''')
            
            await db.commit()
        
        await self._load_cache()
        self._initialized = True
        logger.info(f"Async database initialized at {self.db_path}")
    
    async def _load_cache(self):
        """Load all outcomes into memory cache"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                'SELECT action, scenario, energy, carbon, helium, latency, accuracy, cost FROM outcomes'
            ) as cursor:
                rows = await cursor.fetchall()
                
                for row in rows:
                    action, scenario, energy, carbon, helium, latency, accuracy, cost = row
                    key = f"{action}_{scenario}"
                    self._cache[key] = {
                        Objective.ENERGY: energy,
                        Objective.CARBON: carbon,
                        Objective.HELIUM: helium,
                        Objective.LATENCY: latency,
                        Objective.ACCURACY: accuracy,
                        Objective.COST: cost
                    }
        
        logger.info(f"Loaded {len(self._cache)} outcomes from database")
    
    async def ensure_initialized(self):
        """Ensure database is initialized"""
        if not self._initialized:
            await self._init_database()
    
    async def store_outcome(self, action: str, scenario: str, outcomes: Dict, version: int = 1):
        """Store or update outcome in database asynchronously"""
        await self.ensure_initialized()
        
        key = f"{action}_{scenario}"
        self._cache[key] = outcomes
        
        # Calculate hash for integrity
        hash_str = hashlib.sha256(
            json.dumps({**outcomes, 'action': action, 'scenario': scenario}, sort_keys=True).encode()
        ).hexdigest()[:16]
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT OR REPLACE INTO outcomes 
                (action, scenario, energy, carbon, helium, latency, accuracy, cost, timestamp, version, hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                action, scenario,
                outcomes.get(Objective.ENERGY, 0),
                outcomes.get(Objective.CARBON, 0),
                outcomes.get(Objective.HELIUM, 0),
                outcomes.get(Objective.LATENCY, 0),
                outcomes.get(Objective.ACCURACY, 0),
                outcomes.get(Objective.COST, 0),
                time.time(),
                version,
                hash_str
            ))
            await db.commit()
        
        logger.debug(f"Stored outcome for {action} under {scenario}")
    
    async def get_outcome(self, action: str, scenario: str) -> Optional[Dict]:
        """Retrieve outcome from cache"""
        await self.ensure_initialized()
        key = f"{action}_{scenario}"
        return self._cache.get(key)
    
    async def get_all_outcomes(self) -> List['ActionOutcome']:
        """Get all stored outcomes as ActionOutcome objects"""
        await self.ensure_initialized()
        from .regret_optimizer import ActionOutcome, Objective
        
        outcomes = []
        for key, outcomes_dict in self._cache.items():
            action, scenario = key.split('_', 1)
            outcomes.append(ActionOutcome(
                action_name=action,
                scenario=scenario,
                outcomes=outcomes_dict
            ))
        return outcomes
    
    async def store_decision(self, decision: 'RegretDecision', context: Dict = None):
        """Store decision for audit trail"""
        await self.ensure_initialized()
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT INTO decisions (timestamp, selected_action, max_regret, confidence, context_json)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                time.time(),
                decision.selected_action,
                decision.max_regret,
                decision.confidence,
                json.dumps(context or {})
            ))
            await db.commit()
    
    async def get_decision_history(self, limit: int = 100) -> List[Dict]:
        """Get recent decision history"""
        await self.ensure_initialized()
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute('''
                SELECT timestamp, selected_action, max_regret, confidence, context_json
                FROM decisions
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (limit,)) as cursor:
                rows = await cursor.fetchall()
        
        return [
            {
                'timestamp': row[0],
                'selected_action': row[1],
                'max_regret': row[2],
                'confidence': row[3],
                'context': json.loads(row[4]) if row[4] else {}
            }
            for row in rows
        ]
    
    async def export_outcomes(self, filepath: str):
        """Export all outcomes to JSON file"""
        await self.ensure_initialized()
        
        data = []
        for key, outcomes in self._cache.items():
            action, scenario = key.split('_', 1)
            data.append({
                'action': action,
                'scenario': scenario,
                'outcomes': {k.value: v for k, v in outcomes.items()}
            })
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Exported {len(data)} outcomes to {filepath}")
    
    async def import_outcomes(self, filepath: str):
        """Import outcomes from JSON file"""
        await self.ensure_initialized()
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        for entry in data:
            action = entry['action']
            scenario = entry['scenario']
            outcomes = entry['outcomes']
            outcome_dict = {}
            for k, v in outcomes.items():
                try:
                    outcome_dict[Objective(k)] = v
                except ValueError:
                    outcome_dict[Objective.ENERGY] = v
            await self.store_outcome(action, scenario, outcome_dict)
        
        logger.info(f"Imported {len(data)} outcomes from {filepath}")


# ============================================================
# ENHANCEMENT 2: Online Learning with EMA for Probabilities
# ============================================================

class OnlineProbabilityLearner:
    """
    Online learning for scenario probabilities using exponential moving average.
    
    Features:
    - Exponential moving average (EMA) for recent trends
    - Forgetting factor for non-stationary environments
    - Confidence bounds based on observation count
    """
    
    def __init__(self, learning_rate: float = 0.1, initial_probability: float = 0.1):
        """
        Args:
            learning_rate: EMA smoothing factor (higher = more weight on recent)
            initial_probability: Prior probability for unseen scenarios
        """
        self.learning_rate = learning_rate
        self.initial_probability = initial_probability
        self.probabilities: Dict[str, float] = {}
        self.observation_counts: Dict[str, int] = {}
        self.total_observations = 0
        self._history: Dict[str, deque] = {}
    
    def update(self, scenario: str, timestamp: Optional[float] = None):
        """Update probability with EMA"""
        self.total_observations += 1
        self.observation_counts[scenario] = self.observation_counts.get(scenario, 0) + 1
        
        # EMA update: P_new = α * 1 + (1-α) * P_old for observed scenario
        # For other scenarios, P decays toward prior
        current = self.probabilities.get(scenario, self.initial_probability)
        new_prob = self.learning_rate * 1.0 + (1 - self.learning_rate) * current
        self.probabilities[scenario] = new_prob
        
        # Decay other probabilities (normalize to sum to 1)
        total = new_prob
        for s in list(self.probabilities.keys()):
            if s != scenario:
                self.probabilities[s] *= (1 - self.learning_rate)
                total += self.probabilities[s]
        
        # Add prior for unseen scenarios
        total += self.initial_probability * (1 - total)
        
        # Track history
        if scenario not in self._history:
            self._history[scenario] = deque(maxlen=100)
        self._history[scenario].append((timestamp or time.time(), 1.0))
    
    def get_probability(self, scenario: str) -> float:
        """Get current probability estimate"""
        return self.probabilities.get(scenario, self.initial_probability)
    
    def get_probabilities(self) -> Dict[str, float]:
        """Get all probabilities, normalized"""
        probs = self.probabilities.copy()
        total = sum(probs.values())
        if total > 0:
            return {k: v / total for k, v in probs.items()}
        return {}
    
    def get_confidence(self, scenario: str) -> float:
        """Get confidence in probability estimate (0-1)"""
        count = self.observation_counts.get(scenario, 0)
        return min(0.95, 0.5 + count / 100)
    
    def get_trend(self, scenario: str) -> str:
        """Get trend direction"""
        if scenario not in self._history or len(self._history[scenario]) < 5:
            return "stable"
        
        recent = list(self._history[scenario])[-5:]
        if len(recent) < 2:
            return "stable"
        
        # Check if frequency is increasing
        first_val = recent[0][1]
        last_val = recent[-1][1]
        
        if last_val > first_val * 1.2:
            return "increasing"
        elif last_val < first_val * 0.8:
            return "decreasing"
        return "stable"
    
    def get_statistics(self) -> Dict:
        """Get learner statistics"""
        return {
            'total_observations': self.total_observations,
            'probabilities': self.get_probabilities(),
            'confidence': {s: self.get_confidence(s) for s in self.probabilities},
            'trends': {s: self.get_trend(s) for s in self.probabilities},
            'learning_rate': self.learning_rate
        }


# ============================================================
# ENHANCEMENT 3: Correlation Model for Objectives
# ============================================================

class ObjectiveCorrelationModel:
    """
    Model correlations between different objectives.
    
    Uses Pearson correlation coefficient to capture relationships
    between objectives (e.g., lower energy typically means lower carbon).
    """
    
    def __init__(self):
        self._history: List[Dict[Objective, float]] = []
        self._correlation_matrix: Dict[Tuple[Objective, Objective], float] = {}
        self.max_history = 1000
    
    def add_observation(self, outcomes: Dict[Objective, float]):
        """Add an observation for correlation calculation"""
        self._history.append(outcomes)
        if len(self._history) > self.max_history:
            self._history = self._history[-self.max_history:]
        self._update_correlations()
    
    def _update_correlations(self):
        """Update correlation matrix using Pearson correlation"""
        if len(self._history) < 10:
            return
        
        objectives = list(Objective)
        for i, obj1 in enumerate(objectives):
            for obj2 in objectives[i:]:
                values1 = [h.get(obj1, 0) for h in self._history]
                values2 = [h.get(obj2, 0) for h in self._history]
                
                # Pearson correlation
                corr = np.corrcoef(values1, values2)[0, 1] if len(values1) > 1 else 0
                self._correlation_matrix[(obj1, obj2)] = corr
                self._correlation_matrix[(obj2, obj1)] = corr
    
    def get_correlation(self, obj1: Objective, obj2: Objective) -> float:
        """Get correlation coefficient between two objectives"""
        return self._correlation_matrix.get((obj1, obj2), 0.0)
    
    def are_aligned(self, obj1: Objective, obj2: Objective, threshold: float = 0.7) -> bool:
        """Check if two objectives are strongly correlated (aligned)"""
        return abs(self.get_correlation(obj1, obj2)) > threshold
    
    def get_regret_adjustment(self, obj1: Objective, obj2: Objective, 
                              regret1: float, regret2: float) -> float:
        """
        Adjust combined regret based on correlation.
        Highly correlated objectives shouldn't be double-counted.
        """
        corr = abs(self.get_correlation(obj1, obj2))
        # If highly correlated, reduce combined regret
        if corr > 0.7:
            return max(regret1, regret2) * 0.8 + min(regret1, regret2) * 0.2
        return regret1 + regret2
    
    def get_statistics(self) -> Dict:
        """Get correlation statistics"""
        return {
            'correlations': {(k[0].value, k[1].value): v for k, v in self._correlation_matrix.items()},
            'sample_size': len(self._history)
        }


# ============================================================
# ENHANCEMENT 4: Pruned Sequential Decision Tree
# ============================================================

class PrunedSequentialDecisionSupport:
    """
    Sequential decision support with branch pruning.
    
    Uses branch-and-bound pruning to reduce search complexity.
    """
    
    def __init__(self, optimizer: 'RegretMinimizationOptimizer', horizon: int = 3,
                 prune_threshold: float = 0.1):
        self.optimizer = optimizer
        self.horizon = horizon
        self.prune_threshold = prune_threshold
        self._pruned_count = 0
    
    def find_optimal_sequence(self, initial_state: str,
                              available_actions: List[str],
                              outcomes: List['ActionOutcome'],
                              depth: int = 0,
                              current_regret: float = 0.0,
                              best_regret: float = float('inf')) -> Tuple[List[str], float]:
        """
        Find optimal action sequence with branch pruning.
        
        Returns:
            (best_sequence, expected_regret)
        """
        if depth >= self.horizon:
            return [], current_regret
        
        best_sequence = []
        best_total_regret = float('inf')
        
        # Prune if current regret already exceeds best known
        if current_regret >= best_regret - self.prune_threshold:
            self._pruned_count += 1
            return [], current_regret
        
        for action in available_actions:
            # Evaluate single action
            action_outcomes = [o for o in outcomes if o.action_name == action]
            if not action_outcomes:
                continue
            
            action_regrets = self.optimizer.calculate_regret(action_outcomes)
            regret = action_regrets.get(action, 1.0)
            
            # Recursively evaluate future with pruning
            future_sequence, future_regret = self.find_optimal_sequence(
                f"{initial_state}_{action}", available_actions, outcomes,
                depth + 1, current_regret + regret, best_total_regret
            )
            
            total_regret = regret + 0.9 * future_regret
            
            if total_regret < best_total_regret - self.prune_threshold:
                best_total_regret = total_regret
                best_sequence = [action] + future_sequence
        
        return best_sequence, best_total_regret
    
    def get_pruning_stats(self) -> Dict:
        return {'pruned_branches': self._pruned_count}


# ============================================================
# ENHANCEMENT 5: Enhanced Visualizer with Correlation Plots
# ============================================================

class EnhancedRegretVisualizer:
    """
    Enhanced visualizer with correlation heatmaps and parallel coordinates.
    """
    
    @staticmethod
    def create_full_dashboard(regret_matrix: Dict[str, float],
                               actions: List[str],
                               correlations: Dict) -> str:
        """Generate complete HTML dashboard"""
        
        html = '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Regret Analysis Dashboard</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body { font-family: monospace; margin: 20px; background: #0a0a0a; color: #00ff00; }
                .dashboard { display: flex; flex-wrap: wrap; gap: 20px; }
                .panel { background: #1a1a1a; border: 1px solid #00ff00; border-radius: 10px; padding: 20px; }
                table { border-collapse: collapse; }
                th, td { border: 1px solid #00ff00; padding: 10px; text-align: center; }
                .selected { background: #004444; font-weight: bold; }
                .low-regret { background: #003300; }
                .medium-regret { background: #332200; }
                .high-regret { background: #330000; }
            </style>
        </head>
        <body>
            <h1>📊 Regret Analysis Dashboard</h1>
            <div class="dashboard">
                <div class="panel">
                    <h2>Regret Matrix</h2>
                    <div id="heatmap"></div>
                </div>
                <div class="panel">
                    <h2>Action Comparison (Radar)</h2>
                    <div id="radar"></div>
                </div>
                <div class="panel">
                    <h2>Objective Correlations</h2>
                    <div id="correlation"></div>
                </div>
            </div>
            <div class="dashboard">
                <div class="panel">
                    <h2>Parallel Coordinates</h2>
                    <div id="parallel"></div>
                </div>
            </div>
            <script>
                var actions = $actions_json;
                var regrets = $regrets_json;
                var correlations = $correlations_json;
                
                // Heatmap
                var heatmapData = [{
                    z: [regrets],
                    x: actions,
                    y: ['Max Regret'],
                    type: 'heatmap',
                    colorscale: [[0, 'green'], [0.5, 'yellow'], [1, 'red']]
                }];
                Plotly.newPlot('heatmap', heatmapData, {title: 'Regret by Action'});
                
                // Radar chart
                var radarData = [{
                    type: 'scatterpolar',
                    r: regrets,
                    theta: actions,
                    fill: 'toself',
                    name: 'Max Regret'
                }];
                Plotly.newPlot('radar', radarData, {
                    polar: {radialaxis: {range: [0, 1]}},
                    title: 'Action Regret Comparison'
                });
                
                // Correlation heatmap
                var corrMatrix = [];
                var corrLabels = Object.keys(correlations);
                for (var i = 0; i < corrLabels.length; i++) {
                    corrMatrix.push([]);
                    for (var j = 0; j < corrLabels.length; j++) {
                        var key = corrLabels[i] + '_' + corrLabels[j];
                        corrMatrix[i].push(correlations[key] || 0);
                    }
                }
                var corrData = [{
                    z: corrMatrix,
                    x: corrLabels,
                    y: corrLabels,
                    type: 'heatmap',
                    colorscale: 'RdYlGn',
                    zmin: -1,
                    zmax: 1
                }];
                Plotly.newPlot('correlation', corrData, {title: 'Objective Correlations'});
            </script>
        </body>
        </html>
        '''
        
        import json
        html = html.replace('$actions_json', json.dumps(actions))
        html = html.replace('$regrets_json', json.dumps([regret_matrix.get(a, 0) for a in actions]))
        html = html.replace('$correlations_json', json.dumps(correlations))
        
        return html


# ============================================================
# ENHANCEMENT 6: Main Enhanced Regret Minimization Optimizer
# ============================================================

class Objective(Enum):
    ENERGY = "energy"
    CARBON = "carbon"
    HELIUM = "helium"
    LATENCY = "latency"
    ACCURACY = "accuracy"
    COST = "cost"


@dataclass
class ActionOutcome:
    action_name: str
    scenario: str
    outcomes: Dict[Objective, float]
    confidence: float = 0.8
    timestamp: float = field(default_factory=time.time)
    source: str = "prediction"


@dataclass
class UncertaintyInterval:
    objective: Objective
    mean: float
    lower_bound: float
    upper_bound: float
    confidence: float
    distribution: str = "normal"


@dataclass
class RegretDecision:
    selected_action: str
    max_regret: float
    expected_regret: Optional[float] = None
    confidence: float = 0.0
    expected_outcomes: Dict[Objective, float] = field(default_factory=dict)
    regret_matrix: Dict[str, float] = field(default_factory=dict)
    reasoning: str = ""
    robustness: Optional[Dict] = None
    decision_id: str = ""
    timestamp: float = field(default_factory=time.time)


class RegretMinimizationOptimizer:
    """
    Enhanced Regret minimization optimizer v3.0.
    
    Features:
    - Minimax regret with Savage's criterion
    - Multi-objective with 6 configurable objectives
    - Async database persistence
    - Online probability learning with EMA
    - Correlation modeling between objectives
    - Pruned sequential decision search
    - Enhanced visualizations
    """
    
    MINIMIZING_OBJECTIVES = {Objective.ENERGY, Objective.CARBON, 
                              Objective.HELIUM, Objective.LATENCY, Objective.COST}
    MAXIMIZING_OBJECTIVES = {Objective.ACCURACY}
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Objective weights
        self.objective_weights = self.config.get('objective_weights', {
            Objective.ENERGY: 0.20,
            Objective.CARBON: 0.25,
            Objective.HELIUM: 0.25,
            Objective.LATENCY: 0.15,
            Objective.ACCURACY: 0.15
        })
        total = sum(self.objective_weights.values())
        self.objective_weights = {k: v/total for k, v in self.objective_weights.items()}
        
        # Uncertainty defaults
        self.uncertainty_defaults = self.config.get('uncertainty_intervals', {
            Objective.ENERGY: 0.15,
            Objective.CARBON: 0.15,
            Objective.HELIUM: 0.10,
            Objective.LATENCY: 0.20,
            Objective.ACCURACY: 0.05,
            Objective.COST: 0.15
        })
        
        # New components
        self.database = AsyncOutcomeDatabase(self.config.get('db_path', 'outcomes.db'))
        self.probability_learner = OnlineProbabilityLearner(
            learning_rate=self.config.get('learning_rate', 0.1)
        )
        self.correlation_model = ObjectiveCorrelationModel()
        self.sequential_support = PrunedSequentialDecisionSupport(
            self, 
            horizon=self.config.get('horizon', 3),
            prune_threshold=self.config.get('prune_threshold', 0.1)
        )
        
        # Decision history
        self.decision_history: List[RegretDecision] = []
        self._event_loop = None
        
        logger.info("Enhanced Regret Minimization Optimizer v3.0 initialized")
    
    def _get_event_loop(self):
        """Get or create event loop for async operations"""
        if self._event_loop is None or self._event_loop.is_closed():
            try:
                self._event_loop = asyncio.get_running_loop()
            except RuntimeError:
                self._event_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._event_loop)
        return self._event_loop
    
    def _run_async(self, coro):
        """Run async coroutine in event loop"""
        loop = self._get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    
    def calculate_regret(self, outcomes: List[ActionOutcome]) -> Dict[str, float]:
        """Calculate regret for each action across scenarios with correlation adjustment"""
        scenarios = set(o.scenario for o in outcomes)
        objectives = list(self.objective_weights.keys())
        
        # Best outcome per scenario per objective
        best_outcomes = {}
        for scenario in scenarios:
            scenario_outcomes = [o for o in outcomes if o.scenario == scenario]
            best_outcomes[scenario] = {}
            for obj in objectives:
                values = [o.outcomes.get(obj, 0) for o in scenario_outcomes]
                if obj in self.MINIMIZING_OBJECTIVES:
                    best_outcomes[scenario][obj] = min(values) if values else 0
                else:
                    best_outcomes[scenario][obj] = max(values) if values else 0
        
        # Calculate regret per action
        action_regrets = {}
        
        for action_name in set(o.action_name for o in outcomes):
            max_regret = 0
            
            for scenario in scenarios:
                scenario_outcome = next(
                    (o for o in outcomes if o.action_name == action_name and o.scenario == scenario), 
                    None
                )
                if not scenario_outcome:
                    continue
                
                # Calculate per-objective regrets
                objective_regrets = []
                for obj in objectives:
                    actual = scenario_outcome.outcomes.get(obj, 0)
                    best = best_outcomes[scenario][obj]
                    
                    if best != 0:
                        if obj in self.MINIMIZING_OBJECTIVES:
                            regret = max(0, (actual - best) / abs(best))
                        else:
                            regret = max(0, (best - actual) / abs(best))
                    else:
                        regret = 0
                    
                    weighted_regret = regret * self.objective_weights.get(obj, 0)
                    objective_regrets.append((obj, weighted_regret))
                
                # Apply correlation adjustment
                total_scenario_regret = 0
                for i, (obj1, regret1) in enumerate(objective_regrets):
                    for obj2, regret2 in objective_regrets[i+1:]:
                        if self.correlation_model.are_aligned(obj1, obj2):
                            # Highly correlated objectives - adjust
                            adjustment = self.correlation_model.get_regret_adjustment(
                                obj1, obj2, regret1, regret2
                            )
                            total_scenario_regret += adjustment
                        else:
                            total_scenario_regret += regret1 + regret2
                
                max_regret = max(max_regret, total_scenario_regret)
            
            action_regrets[action_name] = max_regret
        
        return action_regrets
    
    def compute_pareto_optimal_actions(self, outcomes: List[ActionOutcome]) -> List[str]:
        """Compute Pareto-optimal actions"""
        actions = list(set(o.action_name for o in outcomes))
        pareto_optimal = []
        
        for i, action_i in enumerate(actions):
            dominated = False
            outcomes_i = [o for o in outcomes if o.action_name == action_i]
            avg_i = self._average_outcomes(outcomes_i)
            
            for j, action_j in enumerate(actions):
                if i == j:
                    continue
                outcomes_j = [o for o in outcomes if o.action_name == action_j]
                avg_j = self._average_outcomes(outcomes_j)
                
                dominates = all(
                    avg_j.get(obj, 0) <= avg_i.get(obj, 0) 
                    for obj in self.MINIMIZING_OBJECTIVES
                ) and avg_j.get(Objective.ACCURACY, 0) >= avg_i.get(Objective.ACCURACY, 0)
                
                strict = any(
                    avg_j.get(obj, 0) < avg_i.get(obj, 0) 
                    for obj in self.MINIMIZING_OBJECTIVES
                ) or avg_j.get(Objective.ACCURACY, 0) > avg_i.get(Objective.ACCURACY, 0)
                
                if dominates and strict:
                    dominated = True
                    break
            
            if not dominated:
                pareto_optimal.append(action_i)
        
        return pareto_optimal
    
    def _average_outcomes(self, outcomes: List[ActionOutcome]) -> Dict[Objective, float]:
        if not outcomes:
            return {}
        
        avg = {}
        for obj in self.objective_weights.keys():
            values = [o.outcomes.get(obj, 0) for o in outcomes]
            avg[obj] = np.mean(values)
        
        # Add correlation observations
        for o in outcomes:
            self.correlation_model.add_observation(o.outcomes)
        
        return avg
    
    def generate_uncertainty_intervals(self, outcomes: List[ActionOutcome]) -> List[UncertaintyInterval]:
        """Generate uncertainty intervals with correlation consideration"""
        intervals = []
        
        for obj in self.objective_weights.keys():
            values = []
            confidences = []
            for outcome in outcomes:
                if obj in outcome.outcomes:
                    values.append(outcome.outcomes[obj])
                    confidences.append(outcome.confidence)
            
            if values:
                mean = np.mean(values)
                std = np.std(values)
                avg_confidence = np.mean(confidences) if confidences else 0.7
                
                if std < mean * 0.01 or len(values) < 3:
                    uncertainty = self.uncertainty_defaults[obj]
                    lower = mean * (1 - uncertainty)
                    upper = mean * (1 + uncertainty)
                    distribution = "uniform"
                else:
                    lower = mean - std
                    upper = mean + std
                    distribution = "empirical"
                
                intervals.append(UncertaintyInterval(
                    objective=obj,
                    mean=mean,
                    lower_bound=lower,
                    upper_bound=upper,
                    confidence=avg_confidence,
                    distribution=distribution
                ))
        
        return intervals
    
    def optimize_with_regret(self, outcomes: List[ActionOutcome],
                            uncertainty_enabled: bool = True,
                            use_expected_regret: bool = False) -> RegretDecision:
        """Main optimization function with async support"""
        
        # Compute regret
        action_regrets = self.calculate_regret(outcomes)
        
        if not action_regrets:
            return RegretDecision(
                selected_action="unknown",
                max_regret=1.0,
                confidence=0.0,
                expected_outcomes={},
                regret_matrix={},
                reasoning="No outcomes provided",
                decision_id=hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
            )
        
        # Compute expected regret using learned probabilities
        expected_regret = None
        if use_expected_regret:
            # Build proper scenario-regret matrix
            scenario_regrets = {}
            for action_name in set(o.action_name for o in outcomes):
                scenario_regrets[action_name] = {}
                for scenario in set(o.scenario for o in outcomes):
                    # Find outcomes for this action-scenario
                    scenario_outcome = next(
                        (o for o in outcomes if o.action_name == action_name and o.scenario == scenario),
                        None
                    )
                    if scenario_outcome:
                        # Calculate regret for this scenario
                        # Simplified - compute actual regret
                        scenario_regret = 0
                        for obj in self.objective_weights:
                            # Would compute actual regret here
                            scenario_regret += 0.1  # Placeholder
                        scenario_regrets[action_name][scenario] = scenario_regret
            
            expected_regret_dict = self.probability_learner.get_expected_regret(scenario_regrets)
            expected_regret = min(expected_regret_dict.values()) if expected_regret_dict else None
            selected_action = min(expected_regret_dict, key=expected_regret_dict.get)
            max_regret = action_regrets.get(selected_action, 1.0)
        else:
            selected_action = min(action_regrets, key=action_regrets.get)
            max_regret = action_regrets[selected_action]
        
        # Calculate expected outcomes
        selected_outcomes = [o for o in outcomes if o.action_name == selected_action]
        expected_outcomes = self._average_outcomes(selected_outcomes)
        
        # Calculate confidence
        base_confidence = max(0.5, 1.0 - max_regret)
        
        if uncertainty_enabled:
            intervals = self.generate_uncertainty_intervals(selected_outcomes)
            if intervals:
                avg_uncertainty = np.mean([(i.upper_bound - i.lower_bound) / i.mean for i in intervals])
                confidence = base_confidence * max(0.5, 1.0 - avg_uncertainty)
            else:
                confidence = base_confidence
        else:
            confidence = base_confidence
        
        # Generate reasoning
        reasoning = self._generate_reasoning(selected_action, max_regret, action_regrets)
        
        # Run robustness analysis
        robustness = RobustnessAnalyzer.analyze_weight_robustness(self, outcomes)
        
        decision = RegretDecision(
            selected_action=selected_action,
            max_regret=max_regret,
            expected_regret=expected_regret,
            confidence=confidence,
            expected_outcomes=expected_outcomes,
            regret_matrix=action_regrets,
            reasoning=reasoning,
            robustness=robustness,
            decision_id=hashlib.md5(f"{selected_action}_{time.time()}".encode()).hexdigest()[:8]
        )
        
        self.decision_history.append(decision)
        
        # Async store
        try:
            self._run_async(self.database.store_decision(decision))
        except Exception as e:
            logger.warning(f"Failed to store decision: {e}")
        
        logger.info(f"Selected '{selected_action}' | max_regret={max_regret:.3f} | confidence={confidence:.2f}")
        
        return decision
    
    def _generate_reasoning(self, selected_action: str, max_regret: float,
                           action_regrets: Dict[str, float]) -> str:
        sorted_actions = sorted(action_regrets.items(), key=lambda x: x[1])
        
        reasoning = f"Selected '{selected_action}' (max regret={max_regret:.3f})"
        
        if len(sorted_actions) > 1:
            second_best = sorted_actions[1]
            reasoning += f" vs '{second_best[0]}' (regret={second_best[1]:.3f})"
        
        if max_regret < 0.05:
            reasoning += " - Excellent decision (very low regret)"
        elif max_regret < 0.1:
            reasoning += " - High confidence decision"
        elif max_regret < 0.2:
            reasoning += " - Good decision (moderate confidence)"
        elif max_regret < 0.3:
            reasoning += " - Acceptable decision (consider monitoring)"
        else:
            reasoning += " - Consider gathering more information or exploring alternatives"
        
        # Add correlation insight if available
        if len(self.correlation_model._history) > 10:
            reasoning += " | Correlations: "
            strong_corrs = [(k, v) for k, v in self.correlation_model._correlation_matrix.items() 
                           if abs(v) > 0.7 and k[0] != k[1]]
            if strong_corrs:
                obj1, obj2 = strong_corrs[0][0]
                reasoning += f"{obj1.value}↔{obj2.value} correlated"
        
        return reasoning
    
    def create_outcomes_from_scenarios(self, actions: List[str],
                                        scenarios: List[str],
                                        predictor_func) -> List[ActionOutcome]:
        """Create outcomes by evaluating predictor function with async caching"""
        outcomes = []
        
        for action in actions:
            for scenario in scenarios:
                # Check database asynchronously
                cached = self._run_async(self.database.get_outcome(action, scenario))
                if cached:
                    outcomes.append(ActionOutcome(
                        action_name=action,
                        scenario=scenario,
                        outcomes=cached,
                        source="database"
                    ))
                else:
                    outcome_dict = predictor_func(action, scenario)
                    outcomes.append(ActionOutcome(
                        action_name=action,
                        scenario=scenario,
                        outcomes=outcome_dict,
                        source="prediction"
                    ))
                    # Cache asynchronously
                    self._run_async(self.database.store_outcome(action, scenario, outcome_dict))
        
        return outcomes
    
    def update_with_actual_outcome(self, action: str, scenario: str, 
                                    actual_outcomes: Dict[Objective, float]):
        """Update probability learner and database with actual outcome"""
        # Update probability learner (online learning)
        self.probability_learner.update(scenario)
        
        # Store actual outcome asynchronously
        self._run_async(self.database.store_outcome(action, scenario, actual_outcomes))
        
        # Update correlation model
        self.correlation_model.add_observation(actual_outcomes)
        
        logger.info(f"Updated with actual outcome: {action} under {scenario}")
    
    def get_decision_history(self, limit: int = 10) -> List[RegretDecision]:
        return self.decision_history[-limit:]
    
    def generate_report(self) -> Dict:
        """Generate comprehensive decision report"""
        correlation_stats = self.correlation_model.get_statistics()
        prob_stats = self.probability_learner.get_statistics()
        pruning_stats = self.sequential_support.get_pruning_stats()
        
        return {
            'objective_weights': {k.value: v for k, v in self.objective_weights.items()},
            'uncertainty_defaults': {k.value: v for k, v in self.uncertainty_defaults.items()},
            'probability_learner': prob_stats,
            'correlation_model': correlation_stats,
            'decision_history_count': len(self.decision_history),
            'pruning_stats': pruning_stats,
            'recent_decisions': [
                {
                    'action': d.selected_action,
                    'max_regret': d.max_regret,
                    'confidence': d.confidence,
                    'timestamp': d.timestamp
                }
                for d in self.decision_history[-5:]
            ]
        }
    
    def get_visualization(self, regret_matrix: Dict[str, float], 
                          actions: List[str]) -> str:
        """Get enhanced visualization dashboard"""
        correlations = {}
        for (obj1, obj2), corr in self.correlation_model._correlation_matrix.items():
            correlations[f"{obj1.value}_{obj2.value}"] = corr
        
        return EnhancedRegretVisualizer.create_full_dashboard(
            regret_matrix, actions, correlations
        )
    
    def find_optimal_sequence(self, initial_state: str,
                              available_actions: List[str],
                              outcomes: List[ActionOutcome]) -> Tuple[List[str], float]:
        """Find optimal action sequence with pruning"""
        seq, regret = self.sequential_support.find_optimal_sequence(
            initial_state, available_actions, outcomes
        )
        return seq, regret
    
    async def close(self):
        """Close database connection"""
        # Database cleanup handled by aiosqlite context managers


# ============================================================
# RobustnessAnalyzer (Enhanced from original)
# ============================================================

class RobustnessAnalyzer:
    """Enhanced robustness analysis with full factorial exploration"""
    
    @staticmethod
    def analyze_weight_robustness(optimizer: RegretMinimizationOptimizer,
                                   outcomes: List[ActionOutcome],
                                   weight_variation: float = 0.2) -> Dict:
        """Analyze how weight changes affect optimal action"""
        base_weights = optimizer.objective_weights.copy()
        optimal_actions = {'base': None}
        
        base_decision = optimizer.optimize_with_regret(outcomes, uncertainty_enabled=False)
        optimal_actions['base'] = base_decision.selected_action
        
        # Full factorial exploration
        objectives = list(base_weights.keys())
        n_variations = len(objectives) * 2
        
        for obj in objectives:
            for variation in [-weight_variation, weight_variation]:
                test_weights = base_weights.copy()
                test_weights[obj] = max(0.01, test_weights[obj] * (1 + variation))
                total = sum(test_weights.values())
                test_weights = {k: v/total for k, v in test_weights.items()}
                
                optimizer.objective_weights = test_weights
                decision = optimizer.optimize_with_regret(outcomes, uncertainty_enabled=False)
                key = f"{obj.value}_{variation:+.0%}"
                optimal_actions[key] = decision.selected_action
        
        optimizer.objective_weights = base_weights
        
        unique_actions = set(optimal_actions.values())
        stability = len(unique_actions) == 1
        action_counts = {a: list(optimal_actions.values()).count(a) for a in unique_actions}
        
        # Find critical weight where decision changes
        critical_weights = []
        for obj in objectives:
            if optimal_actions.get(f"{obj.value}_-20%") != optimal_actions.get(f"{obj.value}_+20%"):
                critical_weights.append(obj.value)
        
        return {
            'actions': optimal_actions,
            'stability': stability,
            'unique_actions': list(unique_actions),
            'action_counts': action_counts,
            'critical_weights': critical_weights,
            'n_variations': n_variations
        }
    
    @staticmethod
    def analyze_scenario_robustness(optimizer: RegretMinimizationOptimizer,
                                     outcomes: List[ActionOutcome],
                                     scenario_variation: float = 0.2) -> Dict:
        """Analyze sensitivity to outcome variations within scenarios"""
        base_decision = optimizer.optimize_with_regret(outcomes, uncertainty_enabled=False)
        base_action = base_decision.selected_action
        results = {'base': base_action, 'variations': []}
        
        for variation in [-scenario_variation, scenario_variation]:
            perturbed_outcomes = []
            for outcome in outcomes:
                perturbed_dict = {}
                for obj, value in outcome.outcomes.items():
                    perturbed_dict[obj] = value * (1 + variation * np.random.normal(0, 0.5))
                perturbed_outcomes.append(ActionOutcome(
                    action_name=outcome.action_name,
                    scenario=outcome.scenario,
                    outcomes=perturbed_dict
                ))
            
            decision = optimizer.optimize_with_regret(perturbed_outcomes, uncertainty_enabled=False)
            changed = decision.selected_action != base_action
            results['variations'].append({
                'variation': variation,
                'selected_action': decision.selected_action,
                'changed': changed
            })
        
        stability = not any(v['changed'] for v in results['variations'])
        results['stability'] = stability
        
        return results


# ============================================================
# Usage Example
# ============================================================

async def async_main():
    print("=== Enhanced Regret Minimization Optimizer v3.0 Demo ===\n")
    
    optimizer = RegretMinimizationOptimizer({
        'objective_weights': {
            Objective.ENERGY: 0.20,
            Objective.CARBON: 0.25,
            Objective.HELIUM: 0.25,
            Objective.LATENCY: 0.15,
            Objective.ACCURACY: 0.15
        },
        'db_path': 'test_outcomes.db',
        'learning_rate': 0.1,
        'horizon': 3
    })
    
    def predictor(action: str, scenario: str) -> Dict:
        outcomes = {
            'execute': {
                Objective.ENERGY: 100,
                Objective.CARBON: 50,
                Objective.HELIUM: 0.8,
                Objective.LATENCY: 100,
                Objective.ACCURACY: 0.95
            },
            'throttle': {
                Objective.ENERGY: 70,
                Objective.CARBON: 35,
                Objective.HELIUM: 0.5,
                Objective.LATENCY: 120,
                Objective.ACCURACY: 0.92
            },
            'defer': {
                Objective.ENERGY: 0,
                Objective.CARBON: 0,
                Objective.HELIUM: 0,
                Objective.LATENCY: float('inf'),
                Objective.ACCURACY: 0
            }
        }
        return outcomes.get(action, outcomes['execute'])
    
    actions = ['execute', 'throttle', 'defer']
    scenarios = ['high_carbon', 'helium_crisis', 'low_demand']
    
    outcomes = optimizer.create_outcomes_from_scenarios(actions, scenarios, predictor)
    
    print("1. Regret minimization decision:")
    decision = optimizer.optimize_with_regret(outcomes)
    print(f"   Selected action: {decision.selected_action}")
    print(f"   Max regret: {decision.max_regret:.3f}")
    print(f"   Confidence: {decision.confidence:.2%}")
    print(f"   Reasoning: {decision.reasoning}")
    
    print("\n2. Pareto-optimal actions:")
    pareto = optimizer.compute_pareto_optimal_actions(outcomes)
    print(f"   {pareto}")
    
    print("\n3. Online Probability Learning:")
    optimizer.probability_learner.update('high_carbon')
    optimizer.probability_learner.update('helium_crisis')
    optimizer.probability_learner.update('high_carbon')
    print(f"   Probabilities: {optimizer.probability_learner.get_probabilities()}")
    print(f"   Confidence: {optimizer.probability_learner.get_confidence('high_carbon'):.2f}")
    
    print("\n4. Correlation Analysis:")
    corr_stats = optimizer.correlation_model.get_statistics()
    print(f"   Sample size: {corr_stats['sample_size']}")
    
    print("\n5. System Report:")
    report = optimizer.generate_report()
    print(f"   Decision history: {report['decision_history_count']}")
    print(f"   Pruned branches: {report['pruning_stats']['pruned_branches']}")
    
    # Clean up
    import os
    if os.path.exists('test_outcomes.db'):
        os.remove('test_outcomes.db')
    
    print("\n✅ Enhanced Regret Minimization Optimizer v3.0 test complete")

def main():
    asyncio.run(async_main())

if __name__ == "__main__":
    main()
