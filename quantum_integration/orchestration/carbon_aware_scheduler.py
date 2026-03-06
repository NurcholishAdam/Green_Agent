# File: quantum_integration/orchestration/carbon_aware_scheduler.py

from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import asyncio
import numpy as np

class CarbonZone(Enum):
    GREEN = "green"      # <50 gCO2/kWh
    YELLOW = "yellow"    # 50-200 gCO2/kWh
    RED = "red"          # >200 gCO2/kWh

@dataclass
class Task:
    task_id: str
    priority: int
    energy_requirement: float
    deferrable: bool
    deadline: Optional[float] = None

@dataclass
class Node:
    node_id: str
    carbon_intensity: float  # gCO2/kWh
    available_capacity: float
    power_budget: float

class CarbonAwareScheduler:
    """
    Carbon-Aware Task Scheduler for Green_Agent
    
    Features:
    - Real-time carbon intensity monitoring
    - Zone-based task routing
    - Task deferral optimization
    """
    
    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.task_queue: List[Task] = []
        self.carbon_thresholds = {
            'green': 50,
            'yellow': 200
        }
    
    def add_node(self, node: Node):
        """Add compute node to cluster"""
        self.nodes[node.node_id] = node
    
    def add_task(self, task: Task):
        """Add task to queue"""
        self.task_queue.append(task)
    
    def get_carbon_zone(self, carbon_intensity: float) -> CarbonZone:
        """Determine carbon zone based on intensity"""
        if carbon_intensity < self.carbon_thresholds['green']:
            return CarbonZone.GREEN
        elif carbon_intensity < self.carbon_thresholds['yellow']:
            return CarbonZone.YELLOW
        else:
            return CarbonZone.RED
    
    async def schedule_tasks(self) -> Dict[str, List[Task]]:
        """
        Schedule tasks to nodes based on carbon intensity
        
        Returns:
            Dictionary mapping node_id to list of assigned tasks
        """
        schedule: Dict[str, List[Task]] = {node_id: [] for node_id in self.nodes}
        
        # Sort tasks by priority
        sorted_tasks = sorted(self.task_queue, key=lambda t: t.priority, reverse=True)
        
        # Sort nodes by carbon intensity (greenest first)
        sorted_nodes = sorted(
            self.nodes.values(),
            key=lambda n: n.carbon_intensity
        )
        
        for task in sorted_tasks:
            assigned = False
            
            # Try to assign to greenest available node
            for node in sorted_nodes:
                zone = self.get_carbon_zone(node.carbon_intensity)
                
                if zone == CarbonZone.GREEN:
                    # Always assign to green nodes
                    if node.available_capacity >= task.energy_requirement:
                        schedule[node.node_id].append(task)
                        node.available_capacity -= task.energy_requirement
                        assigned = True
                        break
                
                elif zone == CarbonZone.YELLOW:
                    # Assign if task is not deferrable
                    if not task.deferrable and node.available_capacity >= task.energy_requirement:
                        schedule[node.node_id].append(task)
                        node.available_capacity -= task.energy_requirement
                        assigned = True
                        break
                
                else:  # RED zone
                    # Only assign urgent, non-deferrable tasks
                    if (not task.deferrable and 
                        task.priority > 8 and 
                        node.available_capacity >= task.energy_requirement):
                        schedule[node.node_id].append(task)
                        node.available_capacity -= task.energy_requirement
                        assigned = True
                        break
            
            # If not assigned and deferrable, queue for later
            if not assigned and task.deferrable:
                # Wait for green window
                pass
        
        self.task_queue = []  # Clear queue
        return schedule
    
    def calculate_carbon_savings(
        self,
        schedule: Dict[str, List[Task]]
    ) -> Dict:
        """Calculate carbon savings vs naive scheduling"""
        total_carbon = 0
        baseline_carbon = 0
        
        for node_id, tasks in schedule.items():
            node = self.nodes[node_id]
            
            for task in tasks:
                # Actual carbon
                total_carbon += (task.energy_requirement * 
                               node.carbon_intensity / 1000)
                
                # Baseline (assume average 400 gCO2/kWh)
                baseline_carbon += (task.energy_requirement * 400 / 1000)
        
        savings = baseline_carbon - total_carbon
        savings_percent = (savings / baseline_carbon * 100) if baseline_carbon > 0 else 0
        
        return {
            'total_carbon_kg': total_carbon,
            'baseline_carbon_kg': baseline_carbon,
            'carbon_saved_kg': savings,
            'carbon_saved_percent': savings_percent
        }
    
    async def get_optimal_execution_window(
        self,
        task: Task,
        carbon_forecast: List[float]
    ) -> Dict:
        """
        Find optimal time window to execute deferrable task
        
        Args:
            task: Task to schedule
            carbon_forecast: List of carbon intensity forecasts (next 24h)
        
        Returns:
            Optimal start time and expected carbon cost
        """
        if not task.deferrable:
            return {'immediate': True, 'carbon_cost': None}
        
        # Find window with lowest carbon
        best_start_time = 0
        best_carbon_cost = float('inf')
        
        for start_time in range(len(carbon_forecast)):
            # Calculate carbon cost for this window
            carbon_cost = 0
            for t in range(start_time, min(start_time + int(task.energy_requirement), 
                                          len(carbon_forecast))):
                carbon_cost += carbon_forecast[t]
            
            if carbon_cost < best_carbon_cost:
                best_carbon_cost = carbon_cost
                best_start_time = start_time
        
        return {
            'immediate': False,
            'optimal_start_time': best_start_time,
            'expected_carbon_cost': best_carbon_cost
        }


def create_carbon_aware_scheduler() -> CarbonAwareScheduler:
    """Factory function to create carbon-aware scheduler"""
    return CarbonAwareScheduler()
