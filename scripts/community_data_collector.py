#!/usr/bin/env python3
# scripts/community_data_collector.py

"""
Community data collector for Green Agent.
Runs in background to collect and share anonymized data.

Usage:
    python community_data_collector.py --contribute [--enhanced]
    python community_data_collector.py --fetch [--enhanced]

Options:
    --enhanced   Enable integration with advanced Green Agent modules
                 (Zero Trust, distillation, RLHF, MoE, evolutionary, LIMIT Graph).
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.integration.free_apis import CommunityDataHub, FreeAPIManager

# ------------------------------------------------------------------------------
# Optional imports for enhanced functionality
# ------------------------------------------------------------------------------
try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor, NodeType, CoolingType, MaintenanceStatus,
        RoutingStrategy, create_node_descriptor
    )
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor, TaskType, Urgency, Priority, BioMode,
        create_workload_descriptor
    )
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture, ZeroTrustConfig, SecurityContext, SecurityException
    )
    ENHANCED_MODULES_AVAILABLE = True
except ImportError as e:
    ENHANCED_MODULES_AVAILABLE = False
    print(f"⚠️  Enhanced modules not fully available: {e}")
    print("   Running in legacy mode (no distillation/security integrations).")

# ------------------------------------------------------------------------------
# Enhanced Community Data Collector
# ------------------------------------------------------------------------------
class EnhancedCommunityCollector:
    """
    Wraps the original collect/contribute and fetch logic with optional
    advanced Green Agent enhancements.
    """

    def __init__(self, manager: FreeAPIManager = None, use_enhanced: bool = False):
        self.manager = manager or FreeAPIManager()
        self.use_enhanced = use_enhanced and ENHANCED_MODULES_AVAILABLE

        # Enhanced objects (lazily initialized)
        self.zero_trust = None
        self.node_descriptor = None
        self.workload_descriptor = None

    async def initialize_enhanced(self):
        """Initialize Zero Trust architecture and descriptors."""
        if not self.use_enhanced:
            return

        # 1. Zero Trust
        try:
            config = ZeroTrustConfig()
            self.zero_trust = ZeroTrustArchitecture(config)
            print("🔐 Zero Trust security initialized.")
        except Exception as e:
            print(f"⚠️  Could not initialize Zero Trust: {e}")
            self.zero_trust = None

        # 2. NodeDescriptor for region selection
        try:
            # Create a generic node descriptor for the collector
            self.node_descriptor = create_node_descriptor(
                id="community-collector-node",
                node_type=NodeType.EDGE,
                region="us-east",  # default, will be updated dynamically
                region_carbon_intensity=400,  # placeholder
                energy_per_token=0.00005,
                helium_connectivity_score=0.8,
                uptime=0.99,
                renewable_fraction=0.3,
                cooling_type=CoolingType.LIQUID,
                hardware_model="collector",
                metadata={"role": "data_collector"}
            )
            # Set graph metadata if available
            if self.zero_trust:
                self.node_descriptor.graph_id = self.zero_trust.graph_id
                self.node_descriptor.graph_embedding = self.zero_trust.graph_embedding
                self.node_descriptor.graph_metrics = self.zero_trust.graph_metrics
            print("🖥️  Node descriptor ready for routing strategy distillation.")
        except Exception as e:
            print(f"⚠️  Could not initialize NodeDescriptor: {e}")
            self.node_descriptor = None

        # 3. WorkloadDescriptor for contribution task
        try:
            self.workload_descriptor = create_workload_descriptor(
                task_type=TaskType.DATA_PROCESSING,
                tokens=100,  # approx token count for contribution
                latency_target=2000,  # 2 seconds max for contribution
                urgency=Urgency.LOW,
                bio_mode=BioMode.NONE,
                estimated_energy_joules=0.001,
                estimated_carbon_kg=0.0001,
                user_id="community_contributor",
                metadata={"purpose": "data_contribution"}
            )
            # Add human feedback score (RLHF)
            self.workload_descriptor.human_feedback_score = 0.7  # default, can be adjusted
            print("📦 Workload descriptor ready for priority distillation.")
        except Exception as e:
            print(f"⚠️  Could not initialize WorkloadDescriptor: {e}")
            self.workload_descriptor = None

    async def authenticate_if_needed(self):
        """Authenticate using Zero Trust if available."""
        if not self.use_enhanced or not self.zero_trust:
            return None
        try:
            context = await self.zero_trust.authenticate_request(
                {"data_classification": "internal", "task_id": "community_data_collection"},
                {"identity": "community_collector", "authentication_method": "token",
                 "token": "dummy_token"}  # In production, use proper token
            )
            print("✅ Authenticated with Zero Trust.")
            return context
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            return None

    async def select_region_with_distillation(self):
        """
        Use NodeDescriptor's routing strategy to choose the best region
        for carbon observation based on current state and learned policy.
        """
        if not self.use_enhanced or not self.node_descriptor:
            return "us-east"  # default

        # Update node descriptor with current carbon intensity (from manager)
        try:
            intensity, source, conf = await self.manager.get_carbon_intensity("us-east")
            self.node_descriptor.region_carbon_intensity = intensity
        except Exception:
            pass

        # Select routing strategy (carbon_first, latency_first, cost_first, balanced, adaptive)
        strategy = await self.node_descriptor.select_routing_strategy(exploration=True)
        # Map strategy to region choice (simplified: carbon_first -> low-carbon region, etc.)
        # For demo, we return a fixed list; in production you'd map to actual regions.
        if strategy == RoutingStrategy.CARBON_FIRST:
            return "eu-north"  # low carbon
        elif strategy == RoutingStrategy.LATENCY_FIRST:
            return "us-east"   # low latency
        elif strategy == RoutingStrategy.COST_FIRST:
            return "asia-southeast"  # low cost (hypothetical)
        elif strategy == RoutingStrategy.ADAPTIVE:
            # Use graph health if available
            graph_health = self.node_descriptor.graph_metrics.get("centrality", 0.5) if self.node_descriptor.graph_metrics else 0.5
            return "eu-west" if graph_health > 0.6 else "us-west"
        else:  # balanced
            return "us-central"

    async def decide_contribution_with_workload(self, region, intensity, helium):
        """
        Use WorkloadDescriptor's adaptive priority to decide whether to contribute
        and with what priority (accuracy, green, balanced). Returns (contribute: bool, priority: str).
        """
        if not self.use_enhanced or not self.workload_descriptor:
            return True, "balanced"

        # Update workload descriptor with current context
        self.workload_descriptor.estimated_carbon_kg = intensity / 1000  # rough conversion
        self.workload_descriptor.estimated_energy_joules = 0.001
        self.workload_descriptor.helium_units = helium.spot_price_usd_per_liter / 100  # arbitrary

        # Select priority (accuracy, green, balanced) via distillation
        priority = await self.workload_descriptor.select_priority(exploration=True)
        # Based on priority, decide whether to contribute (simplified)
        if priority == Priority.ACCURACY:
            contribute = True
        elif priority == Priority.GREEN:
            contribute = intensity < 500  # only contribute if carbon low
        else:  # balanced
            contribute = True
        return contribute, priority.value

    async def contribute_with_enhancements(self):
        """Enhanced contribution flow."""
        # 1. Authenticate
        auth_context = await self.authenticate_if_needed()
        if auth_context is None and self.zero_trust:
            print("❌ Skipping contribution due to authentication failure.")
            return

        # 2. Select region using NodeDescriptor distillation
        region = await self.select_region_with_distillation()
        print(f"🔍 Selected region for carbon observation: {region}")

        # 3. Observe carbon and helium
        intensity, source, conf = await self.manager.get_carbon_intensity(region)
        helium = await self.manager.get_helium_data()
        print(f"   ✅ Carbon: {intensity:.0f} gCO2/kWh (region: {region}, source: {source})")
        print(f"   ✅ Helium: ${helium.spot_price_usd_per_liter:.2f}/L")

        # 4. Decide if contribution is worthwhile using WorkloadDescriptor
        should_contribute, priority = await self.decide_contribution_with_workload(region, intensity, helium)
        if not should_contribute:
            print("⏭️  Contribution skipped based on adaptive priority (green mode).")
            return

        # 5. Record outcome (for distillation updates) - simulate reward
        carbon_saved = 0.01  # dummy
        sustainability_score = 0.8 if priority == "green" else 0.5
        await self.node_descriptor.record_outcome(
            carbon_saved_kg=carbon_saved,
            latency_ms=100,
            cost_usd=0.001
        )
        await self.workload_descriptor.record_outcome(
            latency_achieved_ms=100,
            carbon_saved_kg=carbon_saved,
            energy_used_joules=0.001
        )

        # 6. Contribute to community
        CommunityDataHub.contribute_carbon_observation(region, intensity, source)
        CommunityDataHub.contribute_helium_observation(helium.spot_price_usd_per_liter,
                                                        helium.inventory_days,
                                                        helium.source)
        print(f"✅ Data contributed with priority '{priority}'.")

        # 7. Optionally log to Zero Trust ledger
        if self.zero_trust and auth_context:
            await self.zero_trust._log_security_event(
                "data_contribution",
                auth_context.request_id,
                {"region": region, "intensity": intensity, "priority": priority}
            )
            print("🔏 Contribution recorded in immutable ledger.")

    async def fetch_with_enhancements(self):
        """Enhanced fetch flow: display additional insights."""
        carbon_avg = CommunityDataHub.get_community_carbon_average('us-east')
        helium_avg = CommunityDataHub.get_community_helium_price()

        print("\n🌍 Green Agent - Community Data (Enhanced)")
        print("=" * 50)

        if carbon_avg:
            print(f"\n📊 Community Carbon Average (us-east): {carbon_avg:.0f} gCO2/kWh")
        else:
            print("\n📊 No community carbon data yet. Be the first to contribute!")

        if helium_avg:
            print(f"🎈 Community Helium Average: ${helium_avg:.2f}/L")
        else:
            print("🎈 No community helium data yet. Be the first to contribute!")

        # Display distillation/security stats if available
        if self.use_enhanced and self.node_descriptor and self.node_descriptor._routing_optimizer:
            stats = self.node_descriptor._routing_optimizer.get_stats()
            print(f"\n🧠 Distillation stats (Node): {stats}")
        if self.use_enhanced and self.workload_descriptor and self.workload_descriptor._priority_optimizer:
            stats = self.workload_descriptor._priority_optimizer.get_stats()
            print(f"🧠 Distillation stats (Workload): {stats}")
        if self.zero_trust:
            posture = self.zero_trust.get_security_posture()
            print(f"🔐 Security posture: active sessions={posture.get('active_sessions', 0)}, "
                  f"violations={posture.get('security_violations', 0)}")

# ------------------------------------------------------------------------------
# Main CLI functions
# ------------------------------------------------------------------------------
async def collect_and_contribute(use_enhanced: bool = False):
    """Collect local observations and contribute to community (legacy or enhanced)."""
    print("🌱 Green Agent - Community Data Collector")
    print("=" * 50)

    manager = FreeAPIManager()

    if use_enhanced and ENHANCED_MODULES_AVAILABLE:
        collector = EnhancedCommunityCollector(manager=manager, use_enhanced=True)
        await collector.initialize_enhanced()
        await collector.contribute_with_enhancements()
    else:
        # Original legacy flow
        print("\n📊 Collecting current observations...")
        intensity, source, conf = await manager.get_carbon_intensity('us-east')
        CommunityDataHub.contribute_carbon_observation('us-east', intensity, source)
        print(f"   ✅ Carbon: {intensity:.0f} gCO2/kWh (source: {source})")

        helium = await manager.get_helium_data()
        CommunityDataHub.contribute_helium_observation(helium.spot_price_usd_per_liter,
                                                        helium.inventory_days,
                                                        helium.source)
        print(f"   ✅ Helium: ${helium.spot_price_usd_per_liter:.2f}/L")

        print("\n✅ Data collection complete")
        print("   Thank you for contributing to the Green Agent community!")

async def fetch_and_display(use_enhanced: bool = False):
    """Fetch and display community data (legacy or enhanced)."""
    if use_enhanced and ENHANCED_MODULES_AVAILABLE:
        collector = EnhancedCommunityCollector(use_enhanced=True)
        await collector.initialize_enhanced()
        await collector.fetch_with_enhancements()
    else:
        print("🌍 Green Agent - Community Data")
        print("=" * 50)

        carbon_avg = CommunityDataHub.get_community_carbon_average('us-east')
        if carbon_avg:
            print(f"\n📊 Community Carbon Average (us-east): {carbon_avg:.0f} gCO2/kWh")
        else:
            print("\n📊 No community carbon data yet. Be the first to contribute!")

        helium_avg = CommunityDataHub.get_community_helium_price()
        if helium_avg:
            print(f"🎈 Community Helium Average: ${helium_avg:.2f}/L")
        else:
            print("🎈 No community helium data yet. Be the first to contribute!")

def main():
    parser = argparse.ArgumentParser(description='Green Agent Community Data Collector')
    parser.add_argument('--contribute', action='store_true',
                        help='Collect and contribute observations')
    parser.add_argument('--fetch', action='store_true',
                        help='Fetch and display community data')
    parser.add_argument('--enhanced', action='store_true',
                        help='Enable enhanced mode (Zero Trust, distillation, RLHF, MoE, etc.)')
    args = parser.parse_args()

    if args.contribute:
        asyncio.run(collect_and_contribute(use_enhanced=args.enhanced))
    elif args.fetch:
        asyncio.run(fetch_and_display(use_enhanced=args.enhanced))
    else:
        print("Please specify --contribute or --fetch")
        parser.print_help()

if __name__ == "__main__":
    main()
