# File: enhancements/moe_expert_system/monitoring/expert_metrics.py

import time
from typing import Dict, Any, List
from collections import defaultdict
import threading
import logging

logger = logging.getLogger(__name__)

class ExpertMetricsCollector:
    """
    Collects and exposes expert-specific metrics for monitoring.
    Integrates with Layer 7 (Dual Monitoring) and Layer 9 (3D Pareto).
    """
    
    def __init__(self):
        # Expert usage metrics
        self.expert_usage = defaultdict(int)
        self.expert_success = defaultdict(int)
        self.expert_failures = defaultdict(int)
        self.expert_latency = defaultdict(list)
        
        # Routing metrics
        self.routing_decisions = []
        self.routing_latency = []
        
        # Resource metrics
        self.expert_energy = defaultdict(float)
        self.expert_carbon = defaultdict(float)
        self.expert_helium = defaultdict(float)
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Pareto frontier data
        self.pareto_points = []
        
        logger.info("Expert Metrics Collector initialized")
    
    def record_routing(
        self,
        routing_decisions: List[tuple],
        gating_context,
        execution_time: float,
        success: bool
    ):
        """Record a routing decision"""
        with self._lock:
            # Record per-expert usage
            for expert_idx, weight in routing_decisions:
                self.expert_usage[expert_idx] += 1
                if success:
                    self.expert_success[expert_idx] += 1
                else:
                    self.expert_failures[expert_idx] += 1
            
            # Record routing latency
            self.routing_latency.append(execution_time)
            
            # Keep only last 1000 records
            if len(self.routing_latency) > 1000:
                self.routing_latency = self.routing_latency[-1000:]
            
            # Store routing decision
            self.routing_decisions.append({
                'decisions': routing_decisions,
                'context': str(gating_context),
                'execution_time': execution_time,
                'success': success,
                'timestamp': time.time()
            })
            
            if len(self.routing_decisions) > 1000:
                self.routing_decisions = self.routing_decisions[-1000:]
    
    def record_expert_execution(
        self,
        expert_id: str,
        execution_time: float,
        energy_kwh: float,
        carbon_kg: float,
        helium_units: float,
        success: bool
    ):
        """Record expert execution metrics"""
        with self._lock:
            self.expert_latency[expert_id].append(execution_time)
            self.expert_energy[expert_id] += energy_kwh
            self.expert_carbon[expert_id] += carbon_kg
            self.expert_helium[expert_id] += helium_units
            
            if success:
                self.expert_success[expert_id] += 1
            else:
                self.expert_failures[expert_id] += 1
            
            # Add to Pareto frontier data
            self.pareto_points.append({
                'expert_id': expert_id,
                'energy': energy_kwh,
                'time': execution_time,
                'helium': helium_units
            })
            
            # Keep only last 10000 Pareto points
            if len(self.pareto_points) > 10000:
                self.pareto_points = self.pareto_points[-10000:]
    
    def get_expert_usage(self) -> Dict[int, float]:
        """Get expert usage rates"""
        total_usage = sum(self.expert_usage.values())
        if total_usage == 0:
            return {}
        
        return {
            expert: count / total_usage
            for expert, count in self.expert_usage.items()
        }
    
    def get_expert_success_rate(self) -> Dict[int, float]:
        """Get expert success rates"""
        rates = {}
        for expert_id in self.expert_usage:
            total = self.expert_success[expert_id] + self.expert_failures[expert_id]
            if total > 0:
                rates[expert_id] = self.expert_success[expert_id] / total
        return rates
    
    def get_expert_latency_stats(self) -> Dict[str, Dict[str, float]]:
        """Get latency statistics per expert"""
        stats = {}
        for expert_id, latencies in self.expert_latency.items():
            if latencies:
                import statistics
                stats[expert_id] = {
                    'avg_ms': statistics.mean(latencies),
                    'p50_ms': statistics.median(latencies),
                    'p95_ms': sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 1 else latencies[0],
                    'p99_ms': sorted(latencies)[int(len(latencies) * 0.99)] if len(latencies) > 1 else latencies[0],
                    'min_ms': min(latencies),
                    'max_ms': max(latencies)
                }
        return stats
    
    def get_resource_consumption(self) -> Dict[str, Dict[str, float]]:
        """Get resource consumption per expert"""
        consumption = {}
        for expert_id in self.expert_usage:
            consumption[expert_id] = {
                'total_energy_kwh': self.expert_energy[expert_id],
                'total_carbon_kg': self.expert_carbon[expert_id],
                'total_helium_units': self.expert_helium[expert_id]
            }
        return consumption
    
    def get_pareto_frontier(self) -> List[Dict]:
        """Get Pareto-optimal points from collected data"""
        if not self.pareto_points:
            return []
        
        # Find non-dominated points
        pareto_optimal = []
        
        for i, point in enumerate(self.pareto_points):
            dominated = False
            for j, other in enumerate(self.pareto_points):
                if i != j:
                    # Check if 'other' dominates 'point'
                    if (other['energy'] <= point['energy'] and
                        other['time'] <= point['time'] and
                        other['helium'] <= point['helium'] and
                        (other['energy'] < point['energy'] or
                         other['time'] < point['time'] or
                         other['helium'] < point['helium'])):
                        dominated = True
                        break
            
            if not dominated:
                pareto_optimal.append(point)
        
        return pareto_optimal
    
    def reset_metrics(self):
        """Reset all metrics"""
        with self._lock:
            self.expert_usage.clear()
            self.expert_success.clear()
            self.expert_failures.clear()
            self.expert_latency.clear()
            self.routing_decisions.clear()
            self.routing_latency.clear()
            self.expert_energy.clear()
            self.expert_carbon.clear()
            self.expert_helium.clear()
            self.pareto_points.clear()
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        return {
            'expert_usage': self.get_expert_usage(),
            'success_rates': self.get_expert_success_rate(),
            'latency_stats': self.get_expert_latency_stats(),
            'resource_consumption': self.get_resource_consumption(),
            'pareto_frontier_size': len(self.get_pareto_frontier()),
            'total_routes': len(self.routing_decisions),
            'avg_routing_latency_ms': sum(self.routing_latency) / max(len(self.routing_latency), 1)
        }
    
    def to_prometheus_format(self) -> str:
        """Export metrics in Prometheus-compatible format"""
        lines = []
        
        # Expert usage
        for expert_id, usage in self.get_expert_usage().items():
            lines.append(f'moe_expert_usage{{expert="{expert_id}"}} {usage}')
        
        # Success rates
        for expert_id, rate in self.get_expert_success_rate().items():
            lines.append(f'moe_expert_success_rate{{expert="{expert_id}"}} {rate}')
        
        # Latency stats
        for expert_id, stats in self.get_expert_latency_stats().items():
            lines.append(f'moe_expert_latency_avg{{expert="{expert_id}"}} {stats["avg_ms"]}')
            lines.append(f'moe_expert_latency_p95{{expert="{expert_id}"}} {stats["p95_ms"]}')
        
        # Resource consumption
        for expert_id, resources in self.get_resource_consumption().items():
            lines.append(f'moe_expert_energy_kwh{{expert="{expert_id}"}} {resources["total_energy_kwh"]}')
            lines.append(f'moe_expert_carbon_kg{{expert="{expert_id}"}} {resources["total_carbon_kg"]}')
            lines.append(f'moe_expert_helium_units{{expert="{expert_id}"}} {resources["total_helium_units"]}')
        
        return '\n'.join(lines)
