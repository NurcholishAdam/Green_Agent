# dashboard/helium_dashboard.py (Enhanced)
# Integrates advanced enhancements: LIMIT Graph, MODP, RLHF,
# Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, MoE expert gating.

from fastapi import APIRouter, WebSocket, HTTPException
from typing import Dict, List, Optional, Any
import asyncio
import logging

# Optional imports for advanced enhancements (graceful fallback)
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    logger.warning("Enhanced modules not fully available; dashboard will run in legacy mode.")
    NodeDescriptor = None
    WorkloadDescriptor = None
    ZeroTrustArchitecture = None
    FeedbackEvent = None

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/helium", tags=["helium"])


class HeliumDashboard:
    """
    Helium dashboard integration for Layer 11.
    Provides real-time visualizations and alerts, now augmented with
    advanced enhancement metrics (LIMIT Graph, MODP, RLHF, distillation,
    MoE, evolutionary) when available.
    """

    def __init__(self, orchestrator, use_enhancements: bool = False):
        self.orchestrator = orchestrator
        self.websocket_clients = []
        self.use_enhancements = use_enhancements and ENHANCEMENTS_AVAILABLE

        # Optional enhancement components (lazily initialized)
        self.enhancement_components = self._init_enhancement_components()

    def _init_enhancement_components(self) -> Dict[str, Any]:
        """
        Initialize or retrieve advanced components from the orchestrator or
        from the enhancements module. Returns a dict with references.
        """
        components = {}
        if not self.use_enhancements:
            return components

        # Try to get components from orchestrator if it exposes them
        for comp_name in ['node_descriptor', 'workload_descriptor', 'zero_trust',
                          'distillation_optimizer', 'evolutionary_optimizer',
                          'moe_gating', 'graph_registry']:
            comp = getattr(self.orchestrator, comp_name, None)
            if comp is not None:
                components[comp_name] = comp

        # If not present in orchestrator, try to import default instances
        # (This is a fallback; in production, components should be shared.)
        if 'node_descriptor' not in components and NodeDescriptor is not None:
            try:
                # Create a minimal descriptor just for metrics
                components['node_descriptor'] = NodeDescriptor(
                    id="dashboard_node",
                    type=NodeType.EDGE if 'NodeType' in globals() else None,
                    region="unknown",
                    region_carbon_intensity=400.0,
                    energy_per_token=0.00005,
                )
            except Exception:
                pass

        if 'workload_descriptor' not in components and WorkloadDescriptor is not None:
            try:
                components['workload_descriptor'] = WorkloadDescriptor(
                    task_id="dashboard_task",
                    task_type=TaskType.INFERENCE if 'TaskType' in globals() else None,
                    tokens=0,
                    latency_target=0,
                )
            except Exception:
                pass

        return components

    # ------------------------------------------------------------------
    # Helper to gather enhancement metrics from available components
    # ------------------------------------------------------------------
    def get_enhancement_metrics(self) -> Dict[str, Any]:
        """
        Collect metrics from advanced modules for display and monitoring.
        Returns a dict that can be merged into dashboard responses.
        """
        if not self.use_enhancements:
            return {}

        metrics = {}
        comp = self.enhancement_components

        # LIMIT Graph metrics
        node = comp.get('node_descriptor')
        if node and hasattr(node, 'graph_metrics') and node.graph_metrics:
            metrics['graph_metrics'] = {
                'centrality': node.graph_metrics.get('centrality', 0.5),
                'connectivity': node.graph_metrics.get('connectivity', 0.5),
            }
        elif hasattr(self.orchestrator, 'graph_registry'):
            reg = self.orchestrator.graph_registry
            if hasattr(reg, 'compute_graph_metrics'):
                metrics['graph_metrics'] = reg.compute_graph_metrics()

        # MODP weights and score
        if hasattr(self.orchestrator, 'get_modp_score'):
            metrics['modp_score'] = self.orchestrator.get_modp_score()
        elif hasattr(node, 'modp_score'):
            metrics['modp_score'] = node.modp_score
        # Try to get MODP weights from config
        if hasattr(self.orchestrator, 'config'):
            modp_weights = self.orchestrator.config.get('modp_weights')
            if modp_weights is not None:
                metrics['modp_weights'] = modp_weights

        # RLHF feedback
        if hasattr(self.orchestrator, 'human_feedback_score'):
            metrics['rlhf_feedback'] = self.orchestrator.human_feedback_score
        elif hasattr(node, 'human_feedback_score'):
            metrics['rlhf_feedback'] = node.human_feedback_score

        # Distillation stats
        dist_optimizer = comp.get('distillation_optimizer')
        if dist_optimizer and hasattr(dist_optimizer, 'get_stats'):
            metrics['distillation_stats'] = dist_optimizer.get_stats()
        # Or from orchestrator
        if hasattr(self.orchestrator, 'distillation_stats'):
            metrics['distillation_stats'] = self.orchestrator.distillation_stats

        # MoE gating metrics
        moe = comp.get('moe_gating')
        if moe and hasattr(moe, 'get_gate_weights'):
            metrics['moe_gate_weights'] = moe.get_gate_weights()
        elif hasattr(self.orchestrator, 'moe_gate_weights'):
            metrics['moe_gate_weights'] = self.orchestrator.moe_gate_weights

        # Evolutionary / bio-inspired metrics
        evo = comp.get('evolutionary_optimizer')
        if evo and hasattr(evo, 'get_best_weights'):
            metrics['evolutionary_best_weights'] = evo.get_best_weights()
        if hasattr(self.orchestrator, 'evolutionary_best_fitness'):
            metrics['evolutionary_best_fitness'] = self.orchestrator.evolutionary_best_fitness

        return metrics

    # ------------------------------------------------------------------
    # Original endpoints (augmented with enhancement data)
    # ------------------------------------------------------------------
    @router.get("/status")
    async def get_helium_status(self):
        """Get current helium supply status (plus enhancement metrics)."""
        base_status = await self.orchestrator.get_helium_status()
        if self.use_enhancements:
            base_status['enhancements'] = self.get_enhancement_metrics()
        return base_status

    @router.get("/report")
    async def get_helium_report(self):
        """Get comprehensive helium report (plus enhancement summary)."""
        base_report = await self.orchestrator.get_helium_report()
        if self.use_enhancements:
            base_report['enhancements'] = self.get_enhancement_metrics()
        return base_report

    @router.get("/metrics")
    async def get_helium_metrics(self):
        """Get helium metrics for Prometheus (now includes enhanced metrics)."""
        report = await self.orchestrator.get_helium_report()
        metrics = {
            'helium_scarcity_score': report.get('current_supply', {}).get('scarcity_score', 0),
            'helium_spot_price': report.get('current_supply', {}).get('spot_price_usd', 0),
            'helium_efficiency_avg': report.get('efficiency_report', {}).get('helium_per_energy_ratio', 0),
            'helium_fallback_rate': report.get('efficiency_report', {}).get('fallback_rate', 0)
        }

        if self.use_enhancements:
            enh_metrics = self.get_enhancement_metrics()
            # Flatten enhancement metrics for Prometheus (Prometheus expects numeric)
            if 'graph_metrics' in enh_metrics:
                metrics['green_agent_graph_centrality'] = enh_metrics['graph_metrics'].get('centrality', 0.5)
                metrics['green_agent_graph_connectivity'] = enh_metrics['graph_metrics'].get('connectivity', 0.5)
            if 'modp_score' in enh_metrics:
                metrics['green_agent_modp_score'] = enh_metrics['modp_score']
            if 'rlhf_feedback' in enh_metrics:
                metrics['green_agent_rlhf_feedback'] = enh_metrics['rlhf_feedback']
            if 'distillation_stats' in enh_metrics and isinstance(enh_metrics['distillation_stats'], dict):
                # Expose update count if available
                if 'student_counter' in enh_metrics['distillation_stats']:
                    metrics['green_agent_distillation_update_count'] = enh_metrics['distillation_stats']['student_counter']
            if 'evolutionary_best_fitness' in enh_metrics:
                metrics['green_agent_evolutionary_best_fitness'] = enh_metrics['evolutionary_best_fitness']

        return metrics

    # New endpoint for full enhancement status
    @router.get("/enhancements")
    async def get_enhancement_status(self):
        """Endpoint to get all available enhancement metrics and status."""
        if not self.use_enhancements:
            return {"enabled": False, "message": "Enhancements not enabled or modules unavailable"}
        return {
            "enabled": True,
            "metrics": self.get_enhancement_metrics(),
            "components": list(self.enhancement_components.keys())
        }

    @router.websocket("/ws")
    async def websocket_endpoint(self, websocket: WebSocket):
        """WebSocket for real-time helium updates (now includes enhancements)."""
        await websocket.accept()
        self.websocket_clients.append(websocket)
        try:
            while True:
                status = await self.orchestrator.get_helium_status()
                if self.use_enhancements:
                    status['enhancements'] = self.get_enhancement_metrics()
                await websocket.send_json(status)
                await asyncio.sleep(30)
        except Exception:
            self.websocket_clients.remove(websocket)

    # ------------------------------------------------------------------
    # Grafana dashboard configuration (enhanced)
    # ------------------------------------------------------------------
    def get_grafana_dashboard_config(self) -> Dict:
        """Generate Grafana dashboard configuration for helium metrics + enhancements."""

        base_panels = [
            {
                "title": "Helium Supply Scarcity Trend",
                "type": "timeseries",
                "targets": [{"expr": "helium_scarcity_score"}],
                "alert": {
                    "conditions": [
                        {"type": "gt", "value": 0.7, "message": "Helium scarcity critical"}
                    ]
                }
            },
            {
                "title": "Helium Spot Price (USD/Liter)",
                "type": "timeseries",
                "targets": [{"expr": "helium_spot_price"}]
            },
            {
                "title": "Helium Efficiency by Hardware",
                "type": "barchart",
                "targets": [{"expr": "helium_efficiency_per_hardware"}]
            },
            {
                "title": "Fallback Usage Rate",
                "type": "gauge",
                "targets": [{"expr": "helium_fallback_rate"}],
                "thresholds": [{"value": 0.2, "color": "green"}, {"value": 0.5, "color": "orange"}]
            },
            {
                "title": "Carbon-Helium Trade-off",
                "type": "scatter",
                "targets": [
                    {"expr": "carbon_emissions", "name": "Carbon"},
                    {"expr": "helium_usage", "name": "Helium"}
                ]
            },
            {
                "title": "Worker Pool Helium Footprint",
                "type": "piechart",
                "targets": [{"expr": "worker_pool_helium_footprint"}]
            }
        ]

        enhanced_panels = []
        if self.use_enhancements:
            enhanced_panels = [
                {
                    "title": "MODP Composite Score",
                    "type": "gauge",
                    "targets": [{"expr": "green_agent_modp_score"}],
                    "thresholds": [{"value": 0.6, "color": "green"}, {"value": 0.4, "color": "orange"}]
                },
                {
                    "title": "RLHF Feedback Score",
                    "type": "timeseries",
                    "targets": [{"expr": "green_agent_rlhf_feedback"}]
                },
                {
                    "title": "LIMIT Graph Centrality",
                    "type": "timeseries",
                    "targets": [{"expr": "green_agent_graph_centrality"}]
                },
                {
                    "title": "Distillation Update Count",
                    "type": "stat",
                    "targets": [{"expr": "green_agent_distillation_update_count"}]
                },
                {
                    "title": "Evolutionary Best Fitness",
                    "type": "timeseries",
                    "targets": [{"expr": "green_agent_evolutionary_best_fitness"}]
                }
            ]

        return {
            "dashboard": {
                "title": "Helium-Aware AI Orchestration (Enhanced)",
                "panels": base_panels + enhanced_panels
            }
        }
