"""
Benchmark Intelligence Layer
=============================

Multi-dimensional benchmarking that tracks accuracy, energy, carbon, cost, and efficiency.

Reveals eco-efficient models through comprehensive performance analysis.

Location: src/benchmarking/benchmark_intelligence.py
"""

from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
import json
from pathlib import Path
import numpy as np
import logging

logger = logging.getLogger(__name__)


class BenchmarkCategory(Enum):
    """Benchmark categories"""
    TEXT_CLASSIFICATION = "text_classification"
    QUESTION_ANSWERING = "question_answering"
    TEXT_GENERATION = "text_generation"
    IMAGE_CLASSIFICATION = "image_classification"
    OBJECT_DETECTION = "object_detection"
    AGENT_SIMULATION = "agent_simulation"


@dataclass
class BenchmarkMetrics:
    """Complete benchmark metrics"""
    # Traditional metrics
    accuracy: float
    f1_score: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    
    # Performance metrics
    latency_ms: float = 0.0  # Per inference
    throughput_samples_per_sec: float = 0.0
    
    # Resource metrics
    energy_kwh: float = 0.0  # Total energy
    carbon_kgco2e: float = 0.0  # Total carbon
    cost_usd: float = 0.0  # Total cost
    memory_gb: float = 0.0  # Peak memory
    
    # Efficiency metrics
    energy_per_sample_wh: float = 0.0
    carbon_per_sample_gco2e: float = 0.0
    performance_per_watt: float = 0.0  # Accuracy / Energy
    carbon_per_accuracy_point: float = 0.0  # Carbon / Accuracy


@dataclass
class BenchmarkResult:
    """Complete benchmark result"""
    benchmark_id: str
    timestamp: datetime
    model_name: str
    model_params: int
    dataset: str
    category: BenchmarkCategory
    metrics: BenchmarkMetrics
    hardware: str
    region: str
    team: str
    
    # Composite scores
    efficiency_score: float  # 0-1 (composite)
    eco_efficiency_rank: Optional[int] = None


@dataclass
class LeaderboardEntry:
    """Entry in efficiency leaderboard"""
    rank: int
    model_name: str
    accuracy: float
    energy_kwh: float
    carbon_kgco2e: float
    cost_usd: float
    efficiency_score: float
    performance_per_watt: float
    carbon_per_accuracy: float


class BenchmarkIntelligence:
    """
    Intelligent benchmarking system
    
    Tracks:
    - Accuracy (traditional)
    - Latency (performance)
    - Energy (sustainability)
    - Carbon (environmental)
    - Cost (economic)
    - Efficiency (composite)
    
    Generates:
    - Multi-dimensional leaderboards
    - Pareto frontiers (accuracy vs carbon)
    - Efficiency trends over time
    - Eco-efficiency champions
    """
    
    def __init__(self, results_path: Optional[Path] = None):
        self.results_path = results_path or Path("data/benchmark_results.json")
        self.results: List[BenchmarkResult] = []
        self._load_results()
        
        logger.info(f"Benchmark Intelligence initialized with {len(self.results)} results")
    
    def _load_results(self):
        """Load benchmark results from disk"""
        if not self.results_path.exists():
            return
        
        try:
            with open(self.results_path, 'r') as f:
                data = json.load(f)
                self.results = [
                    BenchmarkResult(
                        **{**r, 
                           'timestamp': datetime.fromisoformat(r['timestamp']),
                           'category': BenchmarkCategory(r['category']),
                           'metrics': BenchmarkMetrics(**r['metrics'])
                        }
                    )
                    for r in data
                ]
        except Exception as e:
            logger.error(f"Failed to load results: {e}")
    
    def _save_results(self):
        """Save benchmark results to disk"""
        try:
            self.results_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.results_path, 'w') as f:
                data = [
                    {
                        **asdict(r),
                        'timestamp': r.timestamp.isoformat(),
                        'category': r.category.value,
                        'metrics': asdict(r.metrics)
                    }
                    for r in self.results
                ]
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
    
    def record_benchmark(
        self,
        model_name: str,
        model_params: int,
        dataset: str,
        category: BenchmarkCategory,
        accuracy: float,
        energy_kwh: float,
        carbon_kgco2e: float,
        cost_usd: float,
        latency_ms: float,
        num_samples: int,
        hardware: str = "V100",
        region: str = "US-CA",
        team: str = "default",
        additional_metrics: Optional[Dict[str, float]] = None
    ) -> BenchmarkResult:
        """
        Record a benchmark result
        
        Args:
            model_name: Model identifier
            model_params: Number of parameters
            dataset: Dataset name
            category: Benchmark category
            accuracy: Model accuracy (0-1)
            energy_kwh: Total energy consumed
            carbon_kgco2e: Total carbon emitted
            cost_usd: Total cost
            latency_ms: Average latency per sample
            num_samples: Number of samples evaluated
            hardware: Hardware used
            region: Geographic region
            team: Team name
            additional_metrics: Optional additional metrics
        
        Returns:
            Complete BenchmarkResult
        """
        
        # Calculate derived metrics
        energy_per_sample_wh = (energy_kwh * 1000) / num_samples if num_samples > 0 else 0
        carbon_per_sample_gco2e = (carbon_kgco2e * 1000) / num_samples if num_samples > 0 else 0
        performance_per_watt = accuracy / energy_kwh if energy_kwh > 0 else 0
        carbon_per_accuracy = carbon_kgco2e / accuracy if accuracy > 0 else float('inf')
        throughput = (1000 / latency_ms) if latency_ms > 0 else 0
        
        # Create metrics
        metrics = BenchmarkMetrics(
            accuracy=accuracy,
            latency_ms=latency_ms,
            throughput_samples_per_sec=throughput,
            energy_kwh=energy_kwh,
            carbon_kgco2e=carbon_kgco2e,
            cost_usd=cost_usd,
            energy_per_sample_wh=energy_per_sample_wh,
            carbon_per_sample_gco2e=carbon_per_sample_gco2e,
            performance_per_watt=performance_per_watt,
            carbon_per_accuracy_point=carbon_per_accuracy,
            **({k: v for k, v in (additional_metrics or {}).items() 
                if k in ['f1_score', 'precision', 'recall', 'memory_gb']})
        )
        
        # Calculate efficiency score (composite)
        efficiency_score = self._calculate_efficiency_score(metrics)
        
        # Create result
        result = BenchmarkResult(
            benchmark_id=f"bench_{len(self.results)}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            timestamp=datetime.now(),
            model_name=model_name,
            model_params=model_params,
            dataset=dataset,
            category=category,
            metrics=metrics,
            hardware=hardware,
            region=region,
            team=team,
            efficiency_score=efficiency_score
        )
        
        self.results.append(result)
        self._save_results()
        
        logger.info(
            f"Recorded benchmark: {model_name} on {dataset} - "
            f"Accuracy: {accuracy:.2%}, Energy: {energy_kwh:.3f} kWh, "
            f"Efficiency: {efficiency_score:.3f}"
        )
        
        return result
    
    def _calculate_efficiency_score(self, metrics: BenchmarkMetrics) -> float:
        """
        Calculate composite efficiency score (0-1, higher is better)
        
        Balances:
        - Accuracy (40%)
        - Energy efficiency (30%)
        - Carbon efficiency (20%)
        - Cost efficiency (10%)
        """
        
        # Normalize metrics (assuming typical ranges)
        accuracy_norm = metrics.accuracy  # Already 0-1
        
        # Energy efficiency (inverse, normalized to typical range 0-10 kWh)
        energy_efficiency = 1.0 - min(1.0, metrics.energy_kwh / 10.0)
        
        # Carbon efficiency (inverse, normalized to typical range 0-5 kgCO2e)
        carbon_efficiency = 1.0 - min(1.0, metrics.carbon_kgco2e / 5.0)
        
        # Cost efficiency (inverse, normalized to typical range 0-$2)
        cost_efficiency = 1.0 - min(1.0, metrics.cost_usd / 2.0)
        
        # Weighted composite
        efficiency_score = (
            0.40 * accuracy_norm +
            0.30 * energy_efficiency +
            0.20 * carbon_efficiency +
            0.10 * cost_efficiency
        )
        
        return efficiency_score
    
    def get_leaderboard(
        self,
        category: Optional[BenchmarkCategory] = None,
        sort_by: str = "efficiency_score",
        limit: int = 10
    ) -> List[LeaderboardEntry]:
        """
        Get leaderboard
        
        Args:
            category: Filter by category
            sort_by: Sort criterion ("efficiency_score", "performance_per_watt", 
                     "carbon_per_accuracy", "accuracy")
            limit: Max entries to return
        
        Returns:
            Ranked leaderboard entries
        """
        
        # Filter by category if specified
        if category:
            filtered = [r for r in self.results if r.category == category]
        else:
            filtered = self.results
        
        if not filtered:
            return []
        
        # Sort by criterion
        if sort_by == "efficiency_score":
            sorted_results = sorted(filtered, key=lambda r: r.efficiency_score, reverse=True)
        elif sort_by == "performance_per_watt":
            sorted_results = sorted(
                filtered, 
                key=lambda r: r.metrics.performance_per_watt, 
                reverse=True
            )
        elif sort_by == "carbon_per_accuracy":
            sorted_results = sorted(
                filtered,
                key=lambda r: r.metrics.carbon_per_accuracy_point
            )
        elif sort_by == "accuracy":
            sorted_results = sorted(filtered, key=lambda r: r.metrics.accuracy, reverse=True)
        else:
            sorted_results = filtered
        
        # Create leaderboard entries
        leaderboard = []
        for rank, result in enumerate(sorted_results[:limit], 1):
            entry = LeaderboardEntry(
                rank=rank,
                model_name=result.model_name,
                accuracy=result.metrics.accuracy,
                energy_kwh=result.metrics.energy_kwh,
                carbon_kgco2e=result.metrics.carbon_kgco2e,
                cost_usd=result.metrics.cost_usd,
                efficiency_score=result.efficiency_score,
                performance_per_watt=result.metrics.performance_per_watt,
                carbon_per_accuracy=result.metrics.carbon_per_accuracy_point
            )
            leaderboard.append(entry)
        
        return leaderboard
    
    def get_pareto_frontier(
        self,
        category: Optional[BenchmarkCategory] = None,
        x_metric: str = "carbon_kgco2e",
        y_metric: str = "accuracy"
    ) -> List[BenchmarkResult]:
        """
        Get Pareto frontier (non-dominated solutions)
        
        Args:
            category: Filter by category
            x_metric: X-axis metric (minimize)
            y_metric: Y-axis metric (maximize)
        
        Returns:
            Pareto-optimal benchmark results
        """
        
        # Filter by category
        if category:
            filtered = [r for r in self.results if r.category == category]
        else:
            filtered = self.results
        
        if len(filtered) < 2:
            return filtered
        
        # Extract metric values
        def get_metric(result: BenchmarkResult, metric: str) -> float:
            if metric == "accuracy":
                return result.metrics.accuracy
            elif metric == "energy_kwh":
                return result.metrics.energy_kwh
            elif metric == "carbon_kgco2e":
                return result.metrics.carbon_kgco2e
            elif metric == "cost_usd":
                return result.metrics.cost_usd
            else:
                return 0.0
        
        # Find Pareto frontier
        pareto = []
        for candidate in filtered:
            is_dominated = False
            
            x_cand = get_metric(candidate, x_metric)
            y_cand = get_metric(candidate, y_metric)
            
            for other in filtered:
                if other == candidate:
                    continue
                
                x_other = get_metric(other, x_metric)
                y_other = get_metric(other, y_metric)
                
                # Check if other dominates candidate
                # (lower x AND higher y)
                if x_other <= x_cand and y_other >= y_cand:
                    if x_other < x_cand or y_other > y_cand:
                        is_dominated = True
                        break
            
            if not is_dominated:
                pareto.append(candidate)
        
        # Sort by x metric
        pareto.sort(key=lambda r: get_metric(r, x_metric))
        
        return pareto
    
    def get_efficiency_trends(
        self,
        window_days: int = 30
    ) -> Dict[str, List[Tuple[datetime, float]]]:
        """Get efficiency trends over time"""
        
        cutoff = datetime.now() - timedelta(days=window_days)
        recent = [r for r in self.results if r.timestamp >= cutoff]
        
        if not recent:
            return {}
        
        # Sort by timestamp
        recent.sort(key=lambda r: r.timestamp)
        
        trends = {
            "efficiency_score": [(r.timestamp, r.efficiency_score) for r in recent],
            "energy_kwh": [(r.timestamp, r.metrics.energy_kwh) for r in recent],
            "carbon_kgco2e": [(r.timestamp, r.metrics.carbon_kgco2e) for r in recent],
            "performance_per_watt": [(r.timestamp, r.metrics.performance_per_watt) for r in recent]
        }
        
        return trends
    
    def get_eco_champions(
        self,
        category: Optional[BenchmarkCategory] = None,
        min_accuracy: float = 0.80
    ) -> List[BenchmarkResult]:
        """
        Get eco-efficient champions (high accuracy, low carbon)
        
        Args:
            category: Filter by category
            min_accuracy: Minimum accuracy threshold
        
        Returns:
            Top eco-efficient models
        """
        
        # Filter by category and accuracy
        filtered = self.results
        if category:
            filtered = [r for r in filtered if r.category == category]
        filtered = [r for r in filtered if r.metrics.accuracy >= min_accuracy]
        
        if not filtered:
            return []
        
        # Sort by carbon efficiency (low carbon per accuracy point)
        filtered.sort(key=lambda r: r.metrics.carbon_per_accuracy_point)
        
        return filtered[:5]  # Top 5
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get benchmark statistics"""
        
        if not self.results:
            return {"num_benchmarks": 0}
        
        accuracies = [r.metrics.accuracy for r in self.results]
        energies = [r.metrics.energy_kwh for r in self.results]
        carbons = [r.metrics.carbon_kgco2e for r in self.results]
        
        return {
            "num_benchmarks": len(self.results),
            "avg_accuracy": np.mean(accuracies),
            "avg_energy_kwh": np.mean(energies),
            "avg_carbon_kgco2e": np.mean(carbons),
            "total_energy_kwh": np.sum(energies),
            "total_carbon_kgco2e": np.sum(carbons),
            "best_efficiency_score": max(r.efficiency_score for r in self.results),
            "categories": list(set(r.category.value for r in self.results))
        }


if __name__ == "__main__":
    from datetime import timedelta
    
    # Create benchmark intelligence
    intelligence = BenchmarkIntelligence()
    
    # Record some benchmarks
    models = [
        ("bert-base", 110_000_000, 0.92, 0.8, 0.32),
        ("bert-large", 340_000_000, 0.94, 2.5, 1.0),
        ("distilbert", 66_000_000, 0.90, 0.3, 0.12),
        ("roberta-base", 125_000_000, 0.93, 1.0, 0.40),
    ]
    
    for model_name, params, acc, energy, carbon in models:
        intelligence.record_benchmark(
            model_name=model_name,
            model_params=params,
            dataset="sst2",
            category=BenchmarkCategory.TEXT_CLASSIFICATION,
            accuracy=acc,
            energy_kwh=energy,
            carbon_kgco2e=carbon,
            cost_usd=energy * 0.20,
            latency_ms=50,
            num_samples=1000,
            team="nlp_research"
        )
    
    # Get leaderboards
    print(f"\n{'='*80}")
    print(f"EFFICIENCY LEADERBOARD")
    print(f"{'='*80}")
    leaderboard = intelligence.get_leaderboard(sort_by="efficiency_score", limit=10)
    print(f"{'Rank':<6} {'Model':<20} {'Accuracy':<10} {'Energy':<12} {'Carbon':<12} {'Efficiency':<12}")
    print(f"{'-'*80}")
    for entry in leaderboard:
        print(
            f"{entry.rank:<6} {entry.model_name:<20} {entry.accuracy:<10.1%} "
            f"{entry.energy_kwh:<12.2f} {entry.carbon_kgco2e:<12.3f} {entry.efficiency_score:<12.3f}"
        )
    
    # Get Pareto frontier
    print(f"\n{'='*80}")
    print(f"PARETO FRONTIER (Carbon vs Accuracy)")
    print(f"{'='*80}")
    pareto = intelligence.get_pareto_frontier(x_metric="carbon_kgco2e", y_metric="accuracy")
    for result in pareto:
        print(
            f"  • {result.model_name}: {result.metrics.accuracy:.1%} accuracy, "
            f"{result.metrics.carbon_kgco2e:.3f} kgCO2e"
        )
    
    # Eco-champions
    print(f"\n{'='*80}")
    print(f"ECO-EFFICIENCY CHAMPIONS")
    print(f"{'='*80}")
    champions = intelligence.get_eco_champions(min_accuracy=0.90)
    for result in champions:
        print(
            f"  🏆 {result.model_name}: {result.metrics.accuracy:.1%} accuracy, "
            f"{result.metrics.carbon_per_accuracy_point:.3f} kgCO2e per accuracy point"
        )
    
    # Statistics
    stats = intelligence.get_statistics()
    print(f"\n{'='*80}")
    print(f"BENCHMARK STATISTICS")
    print(f"{'='*80}")
    print(f"Total Benchmarks: {stats['num_benchmarks']}")
    print(f"Avg Accuracy: {stats['avg_accuracy']:.1%}")
    print(f"Avg Energy: {stats['avg_energy_kwh']:.2f} kWh")
    print(f"Avg Carbon: {stats['avg_carbon_kgco2e']:.3f} kgCO2e")
    print(f"Best Efficiency Score: {stats['best_efficiency_score']:.3f}")
