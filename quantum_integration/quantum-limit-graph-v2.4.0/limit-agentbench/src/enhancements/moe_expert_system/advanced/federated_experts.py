# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/federated_experts.py
# Enhanced section for gating network integration

"""
Enhanced Federated Experts with Gating Network Integration
Version: 2.0.0

Now integrates with gating_network.py for:
- Federated weight updates for routing
- Global knowledge distillation into routing preferences
- Federated performance feedback for gate evolution
"""

# Add these methods to the EnhancedFederatedOrchestrator class

class GatingNetworkBridge:
    """
    Bridge between federated learning and gating network.
    
    Enables federated knowledge to improve routing decisions.
    """
    
    def __init__(self):
        self.gating_network = None  # Will be injected
        self.federated_weights_history: List[Dict] = []
        self.last_sync_time: Optional[datetime] = None
        self.sync_interval_seconds = 60.0
        
        logger.info("GatingNetworkBridge initialized")
    
    def inject_gating_network(self, gating_network: Any):
        """Inject gating network for weight updates"""
        self.gating_network = gating_network
        logger.info("Gating network injected into federated bridge")
    
    async def sync_federated_weights(
        self,
        global_model: Dict[str, Any],
        participant_contributions: Dict[str, float]
    ):
        """
        Synchronize federated learning results with gating network.
        
        Updates routing preferences based on federated knowledge.
        """
        if not self.gating_network:
            return
        
        try:
            # Extract routing-relevant information from global model
            routing_updates = self._extract_routing_updates(
                global_model, participant_contributions
            )
            
            # Update gating network preferences
            for expert_id, weight_update in routing_updates.items():
                # Convert expert_id to index if needed
                expert_idx = self._get_expert_index(expert_id)
                if expert_idx is not None:
                    # Update routing weight with federated knowledge
                    self.gating_network.update_routing_feedback(
                        expert_id=expert_idx,
                        reward=weight_update.get('performance', 0.5),
                        carbon_kg=weight_update.get('carbon_efficiency', 0.0),
                        helium_units=weight_update.get('helium_efficiency', 0.0)
                    )
            
            # Record sync
            self.last_sync_time = datetime.utcnow()
            self.federated_weights_history.append({
                'timestamp': self.last_sync_time.isoformat(),
                'participants': len(participant_contributions),
                'routing_updates': len(routing_updates)
            })
            
            logger.info(
                f"Federated weights synced: {len(routing_updates)} routing updates"
            )
            
        except Exception as e:
            logger.error(f"Federated weight sync error: {str(e)}")
    
    def _extract_routing_updates(
        self,
        global_model: Dict[str, Any],
        contributions: Dict[str, float]
    ) -> Dict[str, Dict[str, float]]:
        """Extract routing-relevant updates from global model"""
        updates = {}
        
        for participant_id, contribution in contributions.items():
            # Calculate performance score from contribution
            performance = min(contribution, 1.0)
            
            # Calculate efficiency metrics
            carbon_efficiency = 1.0 / (1.0 + contribution * 0.1)
            helium_efficiency = 1.0 / (1.0 + contribution * 0.05)
            
            updates[participant_id] = {
                'performance': performance,
                'carbon_efficiency': carbon_efficiency,
                'helium_efficiency': helium_efficiency,
                'contribution': contribution
            }
        
        return updates
    
    def _get_expert_index(self, expert_id: str) -> Optional[int]:
        """Get expert index from gating network"""
        if not self.gating_network:
            return None
        
        if hasattr(self.gating_network, 'expert_index_map'):
            for idx, eid in self.gating_network.expert_index_map.items():
                if eid == expert_id or f"remote_{expert_id}" in str(eid):
                    return idx
        
        return None
    
    async def distill_federated_knowledge(
        self,
        global_model: Dict[str, Any],
        temperature: float = 3.0
    ):
        """
        Distill federated knowledge into gating network.
        
        Uses knowledge distillation to transfer global model insights.
        """
        if not self.gating_network:
            return
        
        try:
            # Extract soft labels from global model
            soft_labels = self._extract_soft_labels(global_model, temperature)
            
            # Update gating network with soft labels
            if hasattr(self.gating_network, 'sparse_gate'):
                # Use distillation loss to update gate
                for expert_idx, soft_label in soft_labels.items():
                    if hasattr(self.gating_network, 'update_routing_feedback'):
                        self.gating_network.update_routing_feedback(
                            expert_id=expert_idx,
                            reward=soft_label,
                            carbon_kg=0.0,
                            helium_units=0.0
                        )
            
            logger.info(f"Federated knowledge distilled: {len(soft_labels)} experts")
            
        except Exception as e:
            logger.error(f"Knowledge distillation error: {str(e)}")
    
    def _extract_soft_labels(
        self,
        global_model: Dict[str, Any],
        temperature: float
    ) -> Dict[int, float]:
        """Extract soft labels from global model"""
        soft_labels = {}
        
        # Process global model parameters
        for key, value in global_model.items():
            if isinstance(value, (int, float)):
                # Soften with temperature
                softened = np.exp(value / temperature) / (
                    np.exp(value / temperature) + np.exp(-value / temperature)
                )
                
                # Map to expert index (simplified)
                expert_idx = hash(key) % 5  # Assuming 5 experts
                soft_labels[expert_idx] = max(
                    soft_labels.get(expert_idx, 0),
                    float(softened)
                )
        
        return soft_labels
    
    def get_sync_status(self) -> Dict[str, Any]:
        """Get synchronization status"""
        return {
            'last_sync_time': self.last_sync_time.isoformat() if self.last_sync_time else None,
            'total_syncs': len(self.federated_weights_history),
            'sync_interval_seconds': self.sync_interval_seconds,
            'gating_network_connected': self.gating_network is not None
        }


# Add to EnhancedFederatedOrchestrator class:

class EnhancedFederatedOrchestrator:
    """
    Add these methods to the existing EnhancedFederatedOrchestrator class.
    """
    
    def __init__(self, *args, **kwargs):
        # ... existing initialization ...
        
        # NEW: Gating network bridge
        self.gating_bridge = GatingNetworkBridge()
        
        # NEW: Auto-sync configuration
        self.auto_sync_gating = kwargs.get('auto_sync_gating', True)
    
    def inject_gating_network(self, gating_network: Any):
        """Inject gating network for federated weight updates"""
        self.gating_bridge.inject_gating_network(gating_network)
    
    async def federated_round_with_gating_sync(
        self,
        carbon_zone: int,
        helium_scarcity: float,
        sync_gating: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Execute federated round and sync with gating network.
        
        This is the key integration point.
        """
        # Execute standard federated round
        global_model = await self.federated_round(carbon_zone, helium_scarcity)
        
        if global_model and sync_gating and self.auto_sync_gating:
            # Calculate participant contributions
            contributions = {}
            for participant_id in self.participants:
                if participant_id in self.participants:
                    participant = self.participants[participant_id]
                    contributions[participant_id] = participant.calculate_contribution_potential()
            
            # Sync with gating network
            await self.gating_bridge.sync_federated_weights(
                global_model, contributions
            )
            
            # Also distill knowledge
            await self.gating_bridge.distill_federated_knowledge(global_model)
        
        return global_model
    
    def get_federated_gating_status(self) -> Dict[str, Any]:
        """Get federated-gating integration status"""
        return {
            'gating_sync_status': self.gating_bridge.get_sync_status(),
            'auto_sync_enabled': self.auto_sync_gating,
            'federation_status': self.get_federation_status()
        }
