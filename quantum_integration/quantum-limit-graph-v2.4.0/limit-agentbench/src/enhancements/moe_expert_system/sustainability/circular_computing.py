# File: enhancements/moe_expert_system/sustainability/circular_computing.py

"""
Circular Computing Module for Green Agent
Implements circular economy principles for computational resources
including hardware lifecycle management and waste reduction.
"""

import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class HardwareState(Enum):
    MANUFACTURING = "manufacturing"
    DEPLOYED = "deployed"
    MAINTENANCE = "maintenance"
    DEGRADED = "degraded"
    REPURPOSED = "repurposed"
    RECYCLED = "recycled"
    DECOMMISSIONED = "decommissioned"

class MaterialType(Enum):
    SILICON = "silicon"
    COPPER = "copper"
    GOLD = "gold"
    ALUMINUM = "aluminum"
    PLASTIC = "plastic"
    RARE_EARTH = "rare_earth"
    HELIUM = "helium"

@dataclass
class HardwareComponent:
    """Track individual hardware components for circular economy"""
    component_id: str
    type: str
    materials: Dict[MaterialType, float]  # Material type -> grams
    manufacturing_carbon: float  # kg CO2
    current_state: HardwareState
    deployment_date: datetime
    expected_lifetime_days: int
    utilization_history: List[float] = None
    maintenance_log: List[Dict] = None
    
    def __post_init__(self):
        if self.utilization_history is None:
            self.utilization_history = []
        if self.maintenance_log is None:
            self.maintenance_log = []

class CircularComputingManager:
    """
    Manages circular economy principles for computing resources.
    
    Features:
    - Hardware lifecycle tracking
    - Material recovery optimization
    - Waste reduction strategies
    - Helium recycling from hardware
    - Carbon-aware hardware rotation
    """
    
    def __init__(self):
        self.components: Dict[str, HardwareComponent] = {}
        self.material_inventory: Dict[MaterialType, float] = {}
        self.recycling_history: List[Dict] = []
        
        # Circular economy metrics
        self.circularity_score = 0.0
        self.waste_diversion_rate = 0.0
        self.material_recovery_rate = 0.0
        
        # Initialize material inventory
        self._initialize_inventory()
        
        logger.info("Circular Computing Manager initialized")
    
    def _initialize_inventory(self):
        """Initialize material inventory tracking"""
        for material in MaterialType:
            self.material_inventory[material] = 0.0
    
    def register_component(
        self,
        component_type: str,
        materials: Dict[MaterialType, float],
        manufacturing_carbon: float,
        expected_lifetime_days: int = 1825  # 5 years
    ) -> str:
        """
        Register a new hardware component
        
        Returns:
            component_id
        """
        component_id = f"COMP-{datetime.utcnow().timestamp()}-{component_type}"
        
        component = HardwareComponent(
            component_id=component_id,
            type=component_type,
            materials=materials,
            manufacturing_carbon=manufacturing_carbon,
            current_state=HardwareState.MANUFACTURING,
            deployment_date=datetime.utcnow(),
            expected_lifetime_days=expected_lifetime_days
        )
        
        self.components[component_id] = component
        
        # Update material inventory
        for material, amount in materials.items():
            self.material_inventory[material] += amount
        
        logger.info(f"Registered component {component_id}: {component_type}")
        return component_id
    
    def deploy_component(self, component_id: str):
        """Mark component as deployed"""
        if component_id in self.components:
            self.components[component_id].current_state = HardwareState.DEPLOYED
            logger.info(f"Deployed component {component_id}")
    
    def record_utilization(
        self,
        component_id: str,
        utilization_rate: float
    ):
        """Record component utilization for lifecycle tracking"""
        if component_id in self.components:
            component = self.components[component_id]
            component.utilization_history.append(utilization_rate)
            
            # Check for degradation
            if len(component.utilization_history) > 100:
                recent_util = np.mean(component.utilization_history[-100:])
                if recent_util < 0.3:  # Underutilized
                    self._suggest_repurposing(component)
                elif recent_util > 0.9:  # Overutilized
                    self._suggest_maintenance(component)
    
    def _suggest_repurposing(self, component: HardwareComponent):
        """Suggest repurposing underutilized hardware"""
        logger.info(f"Suggesting repurposing for {component.component_id}: "
                   f"utilization below threshold")
        
        # Calculate carbon savings from repurposing vs new manufacturing
        new_manufacturing_carbon = component.manufacturing_carbon
        repurposing_carbon = component.manufacturing_carbon * 0.1  # 10% of manufacturing
        
        carbon_saved = new_manufacturing_carbon - repurposing_carbon
        
        if carbon_saved > 0:
            logger.info(f"Repurposing would save {carbon_saved:.2f} kg CO2")
    
    def _suggest_maintenance(self, component: HardwareComponent):
        """Suggest maintenance for overutilized hardware"""
        logger.info(f"Suggesting maintenance for {component.component_id}: "
                   f"utilization above threshold")
        
        component.maintenance_log.append({
            'timestamp': datetime.utcnow().isoformat(),
            'type': 'preventive',
            'reason': 'high_utilization'
        })
    
    async def recycle_component(
        self,
        component_id: str
    ) -> Dict[str, Any]:
        """
        Recycle hardware component and recover materials
        
        Returns:
            Recycling results with material recovery
        """
        if component_id not in self.components:
            return {'error': 'Component not found'}
        
        component = self.components[component_id]
        
        # Calculate recoverable materials
        recovered_materials = {}
        total_recovery_rate = 0.0
        
        recovery_rates = {
            MaterialType.SILICON: 0.95,
            MaterialType.COPPER: 0.98,
            MaterialType.GOLD: 0.99,
            MaterialType.ALUMINUM: 0.95,
            MaterialType.PLASTIC: 0.80,
            MaterialType.RARE_EARTH: 0.90,
            MaterialType.HELIUM: 0.85  # Helium recovery from cooling systems
        }
        
        for material, amount in component.materials.items():
            recovery_rate = recovery_rates.get(material, 0.9)
            recovered_amount = amount * recovery_rate
            recovered_materials[material.value] = {
                'original_g': amount,
                'recovered_g': recovered_amount,
                'recovery_rate': recovery_rate
            }
            
            # Update inventory
            self.material_inventory[material] -= amount
            self.material_inventory[material] += recovered_amount
            
            total_recovery_rate += recovery_rate
        
        avg_recovery_rate = total_recovery_rate / len(recovered_materials) if recovered_materials else 0
        
        # Calculate carbon savings
        manufacturing_carbon = component.manufacturing_carbon
        recycling_carbon = manufacturing_carbon * 0.2  # 20% of manufacturing carbon
        carbon_saved = manufacturing_carbon - recycling_carbon
        
        # Update component state
        component.current_state = HardwareState.RECYCLED
        
        # Record recycling
        recycling_record = {
            'component_id': component_id,
            'component_type': component.type,
            'timestamp': datetime.utcnow().isoformat(),
            'materials_recovered': recovered_materials,
            'average_recovery_rate': avg_recovery_rate,
            'carbon_saved_kg': carbon_saved,
            'helium_recovered_g': recovered_materials.get('helium', {}).get('recovered_g', 0)
        }
        
        self.recycling_history.append(recycling_record)
        
        # Update metrics
        self._update_circularity_metrics()
        
        logger.info(f"Recycled component {component_id}: "
                   f"{avg_recovery_rate:.1%} recovery, {carbon_saved:.2f} kg CO2 saved")
        
        return recycling_record
    
    def _update_circularity_metrics(self):
        """Update circular economy metrics"""
        total_components = len(self.components)
        if total_components == 0:
            return
        
        # Calculate circularity score
        recycled = sum(
            1 for c in self.components.values()
            if c.current_state == HardwareState.RECYCLED
        )
        repurposed = sum(
            1 for c in self.components.values()
            if c.current_state == HardwareState.REPURPOSED
        )
        
        self.circularity_score = (recycled + repurposed) / total_components
        
        # Calculate material recovery rate
        total_recovered = sum(
            r['average_recovery_rate'] for r in self.recycling_history
        )
        self.material_recovery_rate = total_recovered / max(len(self.recycling_history), 1)
        
        # Calculate waste diversion
        total_waste = total_components - recycled - repurposed
        self.waste_diversion_rate = (recycled + repurposed) / max(total_components, 1)
    
    def optimize_expert_hardware_allocation(
        self,
        expert_requirements: Dict[str, Any],
        carbon_budget: float,
        helium_budget: float
    ) -> Dict[str, Any]:
        """
        Optimize hardware allocation for expert execution
        
        Considers:
        - Hardware lifecycle stage
        - Carbon footprint
        - Helium availability
        - Circular economy principles
        """
        available_components = [
            c for c in self.components.values()
            if c.current_state in [HardwareState.DEPLOYED, HardwareState.MAINTENANCE]
        ]
        
        if not available_components:
            return {'error': 'No available hardware', 'suggestion': 'deploy_new'}
        
        # Score each component
        scored_components = []
        for component in available_components:
            # Lifecycle score: prefer older but functional hardware
            age_days = (datetime.utcnow() - component.deployment_date).days
            lifecycle_score = 1.0 - (age_days / component.expected_lifetime_days)
            lifecycle_score = max(lifecycle_score, 0.1)  # Minimum score
            
            # Carbon score: prefer low manufacturing carbon
            carbon_score = 1.0 / (1.0 + component.manufacturing_carbon)
            
            # Helium score: prefer components with more helium for recycling
            helium_content = component.materials.get(MaterialType.HELIUM, 0)
            helium_score = helium_content / 100.0  # Normalize
            
            # Utilization score: prefer underutilized hardware
            if component.utilization_history:
                avg_util = np.mean(component.utilization_history[-50:])
                utilization_score = 1.0 - avg_util  # Lower utilization = higher score
            else:
                utilization_score = 0.5
            
            # Combined score
            if carbon_budget < 0.01:
                score = 0.4 * carbon_score + 0.3 * lifecycle_score + 0.3 * utilization_score
            elif helium_budget < 0.01:
                score = 0.4 * helium_score + 0.3 * carbon_score + 0.3 * lifecycle_score
            else:
                score = 0.25 * carbon_score + 0.25 * lifecycle_score + 0.25 * utilization_score + 0.25 * helium_score
            
            scored_components.append((component, score))
        
        # Select best component
        scored_components.sort(key=lambda x: x[1], reverse=True)
        best_component, best_score = scored_components[0]
        
        return {
            'selected_component': best_component.component_id,
            'score': best_score,
            'component_type': best_component.type,
            'age_days': (datetime.utcnow() - best_component.deployment_date).days,
            'manufacturing_carbon': best_component.manufacturing_carbon,
            'recommendation': 'use_existing' if best_score > 0.5 else 'consider_repurposing'
        }
    
    def get_circularity_report(self) -> Dict[str, Any]:
        """Generate circular economy report"""
        # Calculate material flows
        material_flows = {}
        for material in MaterialType:
            total_in_use = sum(
                c.materials.get(material, 0)
                for c in self.components.values()
                if c.current_state != HardwareState.RECYCLED
            )
            total_recovered = sum(
                r['materials_recovered'].get(material.value, {}).get('recovered_g', 0)
                for r in self.recycling_history
            )
            material_flows[material.value] = {
                'in_use_g': total_in_use,
                'recovered_g': total_recovered,
                'inventory_g': self.material_inventory[material]
            }
        
        return {
            'circularity_score': self.circularity_score,
            'waste_diversion_rate': self.waste_diversion_rate,
            'material_recovery_rate': self.material_recovery_rate,
            'total_components': len(self.components),
            'components_by_state': {
                state.value: sum(
                    1 for c in self.components.values()
                    if c.current_state == state
                )
                for state in HardwareState
            },
            'material_flows': material_flows,
            'total_carbon_saved_kg': sum(
                r['carbon_saved_kg'] for r in self.recycling_history
            ),
            'helium_recovered_g': sum(
                r['helium_recovered_g'] for r in self.recycling_history
            )
        }
