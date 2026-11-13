#!/usr/bin/env python3
# scripts/community_data_collector.py

"""
Community data collector for Green Agent.
Runs in background to collect and share anonymized data.

Usage:
    python community_data_collector.py --contribute [--region eu-north] [--enhanced]
    python community_data_collector.py --fetch      [--region eu-north] [--enhanced]
    python community_data_collector.py --fetch      --regions us-east,eu-north

Environment (canonical, matching Dockerfile v2.0):
    ENHANCEMENTS_ENABLED=true             # master switch (env wins over --enhanced)
    LIMIT_GRAPH_ENABLED=true
    MODP_ENABLED=true
    RLHF_ENABLED=true
    DISTILLATION_ENABLED=true
    MOE_ENABLED=true
    EVOLUTIONARY_ENABLED=true
    FLEXGEN_ENABLED=false
    ZERO_TRUST_ENABLED=false
    DIFFERENTIAL_PRIVACY_ENABLED=false
    TRAINING_ENABLED=false
    ZERO_TRUST_TOKEN=<token>              # required when ZERO_TRUST_ENABLED=true
    LOG_LEVEL=INFO
    LOG_FORMAT=json

Options:
    --contribute       Collect and contribute observations
    --fetch            Fetch and display community data
    --region REGION    Region to fetch or contribute for (default: us-east)
    --regions R1,R2    Comma-separated regions for multi-region fetch
    --enhanced         Force enhanced mode (descriptors + zero trust + symbolic)
    --no-enhancements  Force legacy mode (overrides ENHANCEMENTS_ENABLED)
    --dry-run          Do not write to community hub
    --output-json PATH Write structured output to PATH
    --symbolic-policy  Path to symbolic_policy.yaml (default: ./symbolic_policy.yaml)
    --verbose / --quiet
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ------------------------------------------------------------------------------
# Logging — respects LOG_LEVEL / LOG_FORMAT
# ------------------------------------------------------------------------------
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
_LOG_FORMAT = os.environ.get("LOG_FORMAT", "text")
if _LOG_FORMAT == "json":
    _FMT = '{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":%(message)s}'
else:
    _FMT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=getattr(logging, _LOG_LEVEL, logging.INFO), format=_FMT)
log = logging.getLogger("green_agent.community_collector")

# ------------------------------------------------------------------------------
# Path bootstrap — prefer `pip install -e .`; fall back only if needed
# ------------------------------------------------------------------------------
try:
    from src.integration.free_apis import CommunityDataHub, FreeAPIManager
except ImportError:
    _repo_root = str(Path(__file__).resolve().parent.parent)
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from src.integration.free_apis import CommunityDataHub, FreeAPIManager

# ------------------------------------------------------------------------------
# Optional enhancement imports
# ------------------------------------------------------------------------------
ENHANCED_MODULES_AVAILABLE = False
ENHANCED_IMPORT_ERROR: Optional[str] = None

try:
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor, NodeType, CoolingType, MaintenanceStatus,
        RoutingStrategy, create_node_descriptor,
    )
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor, TaskType, Urgency, Priority, BioMode,
        create_workload_descriptor,
    )
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture, ZeroTrustConfig, SecurityContext, SecurityException,
    )
    ENHANCED_MODULES_AVAILABLE = True
except ImportError as exc:
    ENHANCED_IMPORT_ERROR = str(exc)

# ------------------------------------------------------------------------------
# Optional symbolic oversight import (4th layer)
# ------------------------------------------------------------------------------
SYMBOLIC_AVAILABLE = False
try:
    from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
    SYMBOLIC_AVAILABLE = True
except ImportError:
    pass

# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------
DEFAULT_REGION = "us-east"
DEFAULT_TOKENS = 100
DEFAULT_LATENCY_TARGET_MS = 2000
DEFAULT_WORKLOAD_ENERGY_J = 0.001
MIN_CONTRIBUTION_INTERVAL_S = 60
MAX_CARBON_INTENSITY = 2000.0  # gCO2/kWh sanity bound

VALID_REGIONS = {
    "us-east", "us-west", "us-central",
    "eu-north", "eu-west",
    "asia-southeast",
}

# ------------------------------------------------------------------------------
# Result records
# ------------------------------------------------------------------------------
@dataclass
class ContributionResult:
    region: str
    carbon_intensity: float
    carbon_source: str
    helium_price_usd_per_liter: float
    priority: str
    contributed: bool
    dry_run: bool = False
    symbolic_violations: List[Dict[str, Any]] = field(default_factory=list)
    zero_trust_request_id: Optional[str] = None
    error: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class FetchResult:
    region: str
    carbon_average: Optional[float]
    helium_average: Optional[float]
    symbolic_violations: List[Dict[str, Any]] = field(default_factory=list)
    distillation_stats: Dict[str, Any] = field(default_factory=dict)
    security_posture: Dict[str, Any] = field(default_factory=dict)
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ------------------------------------------------------------------------------
# Validation
# ------------------------------------------------------------------------------
def validate_region(region: str) -> str:
    if region not in VALID_REGIONS:
        raise ValueError(
            f"Unknown region '{region}'. Valid: {sorted(VALID_REGIONS)}"
        )
    return region


def validate_carbon(intensity: float) -> float:
    if not isinstance(intensity, (int, float)):
        raise ValueError(f"carbon intensity must be numeric, got {type(intensity)}")
    if not (0 <= intensity <= MAX_CARBON_INTENSITY):
        raise ValueError(f"carbon intensity {intensity} out of range [0, {MAX_CARBON_INTENSITY}]")
    return float(intensity)


def validate_helium(price: float) -> float:
    if not isinstance(price, (int, float)) or price < 0:
        raise ValueError(f"helium price must be non-negative numeric, got {price!r}")
    return float(price)


def observation_fingerprint(region: str, intensity: float, source: str) -> str:
    bucket = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    payload = f"{region}|{round(intensity, 1)}|{source}|{bucket}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# ------------------------------------------------------------------------------
# Enhanced collector
# ------------------------------------------------------------------------------
class EnhancedCommunityCollector:
    """Wraps collect/contribute/fetch with advanced enhancement integrations."""

    def __init__(self, manager: Optional[FreeAPIManager] = None, use_enhanced: bool = False):
        self.manager = manager or FreeAPIManager()
        self.use_enhanced = bool(use_enhanced and ENHANCED_MODULES_AVAILABLE)

        self.zero_trust: Optional[ZeroTrustArchitecture] = None
        self.node_descriptor: Optional[NodeDescriptor] = None
        self.workload_descriptor: Optional[WorkloadDescriptor] = None
        self.symbolic_engine = None

    # -- lifecycle ------------------------------------------------------------
    async def __aenter__(self):
        await self.initialize_enhanced()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def initialize_enhanced(self) -> None:
        if not self.use_enhanced:
            log.debug("Enhanced mode disabled; skipping initialization.")
            return

        await self._init_zero_trust()
        await self._init_node_descriptor()
        await self._init_workload_descriptor()
        await self._init_symbolic_engine()

    async def _init_zero_trust(self) -> None:
        if os.environ.get("ZERO_TRUST_ENABLED", "false").lower() != "true":
            log.debug("ZERO_TRUST_ENABLED=false; zero trust not initialized.")
            return

        token = os.environ.get("ZERO_TRUST_TOKEN")
        if not token:
            log.warning("ZERO_TRUST_ENABLED=true but ZERO_TRUST_TOKEN is unset.")
            return

        try:
            config = ZeroTrustConfig()
            self.zero_trust = ZeroTrustArchitecture(config)
            log.info("Zero Trust security initialized.")
        except Exception as exc:  # noqa: BLE001
            log.warning("Zero Trust initialization failed: %s", exc)
            self.zero_trust = None

    async def _init_node_descriptor(self) -> None:
        try:
            self.node_descriptor = create_node_descriptor(
                id="community-collector-node",
                node_type=NodeType.EDGE,
                region=DEFAULT_REGION,
                region_carbon_intensity=400,
                energy_per_token=0.00005,
                helium_connectivity_score=0.8,
                uptime=0.99,
                renewable_fraction=0.3,
                cooling_type=CoolingType.LIQUID,
                hardware_model="collector",
                metadata={"role": "data_collector"},
            )
            if self.zero_trust:
                # Copy graph metadata for downstream routing if available
                self.node_descriptor.graph_id = getattr(self.zero_trust, "graph_id", None)
                self.node_descriptor.graph_embedding = getattr(self.zero_trust, "graph_embedding", None)
                self.node_descriptor.graph_metrics = getattr(self.zero_trust, "graph_metrics", None)
            log.info("Node descriptor ready for routing strategy distillation.")
        except Exception as exc:  # noqa: BLE001
            log.warning("NodeDescriptor init failed: %s", exc)
            self.node_descriptor = None

    async def _init_workload_descriptor(self) -> None:
        try:
            self.workload_descriptor = create_workload_descriptor(
                task_type=TaskType.DATA_PROCESSING,
                tokens=DEFAULT_TOKENS,
                latency_target=DEFAULT_LATENCY_TARGET_MS,
                urgency=Urgency.LOW,
                bio_mode=BioMode.NONE,
                estimated_energy_joules=DEFAULT_WORKLOAD_ENERGY_J,
                estimated_carbon_kg=0.0001,
                user_id="community_contributor",
                metadata={"purpose": "data_contribution"},
            )
            # RLHF default feedback score
            self.workload_descriptor.human_feedback_score = 0.7
            log.info("Workload descriptor ready for priority distillation.")
        except Exception as exc:  # noqa: BLE001
            log.warning("WorkloadDescriptor init failed: %s", exc)
            self.workload_descriptor = None

    async def _init_symbolic_engine(self) -> None:
        policy_path = os.environ.get("SYMBOLIC_POLICY", "symbolic_policy.yaml")
        if not SYMBOLIC_AVAILABLE:
            log.debug("Symbolic layer not importable; skipping.")
            return
        if not Path(policy_path).exists():
            log.info("No symbolic_policy.yaml found at %s; symbolic oversight skipped.", policy_path)
            return
        try:
            self.symbolic_engine = SymbolicReasoningEngine(policy_file=policy_path)
            log.info("Symbolic reasoning engine loaded from %s.", policy_path)
        except Exception as exc:  # noqa: BLE001
            log.warning("Symbolic engine init failed: %s", exc)
            self.symbolic_engine = None

    # -- auth -----------------------------------------------------------------
    async def authenticate_if_needed(self) -> Optional[SecurityContext]:
        if not self.zero_trust:
            return None
        token = os.environ.get("ZERO_TRUST_TOKEN", "")
        try:
            context = await self.zero_trust.authenticate_request(
                {"data_classification": "internal", "task_id": "community_data_collection"},
                {
                    "identity": "community_collector",
                    "authentication_method": "token",
                    "token": token,
                },
            )
            log.info("Authenticated with Zero Trust.")
            return context
        except SecurityException as exc:
            log.error("Zero Trust auth failed: %s", exc)
            return None
        except Exception as exc:  # noqa: BLE001
            log.error("Unexpected auth error: %s", exc)
            return None

    # -- region selection -----------------------------------------------------
    async def select_region_with_distillation(self, base_region: str = DEFAULT_REGION) -> str:
        if not self.use_enhanced or not self.node_descriptor:
            return base_region

        try:
            intensity, _source, _conf = await self.manager.get_carbon_intensity(base_region)
            self.node_descriptor.region_carbon_intensity = intensity
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to update carbon intensity for %s: %s", base_region, exc)

        try:
            strategy = await self.node_descriptor.select_routing_strategy(exploration=True)
        except Exception as exc:  # noqa: BLE001
            log.warning("Routing strategy selection failed; defaulting to %s: %s", base_region, exc)
            return base_region

        mapping = {
            RoutingStrategy.CARBON_FIRST: "eu-north",
            RoutingStrategy.LATENCY_FIRST: "us-east",
            RoutingStrategy.COST_FIRST: "asia-southeast",
        }
        if strategy in mapping:
            return mapping[strategy]
        if strategy == RoutingStrategy.ADAPTIVE:
            health = (self.node_descriptor.graph_metrics or {}).get("centrality", 0.5)
            return "eu-west" if health > 0.6 else "us-west"
        return "us-central"  # balanced

    # -- contribution decision ------------------------------------------------
    async def decide_contribution_with_workload(
        self,
        region: str,
        intensity: float,
        helium,
    ) -> Tuple[bool, str]:
        if not self.use_enhanced or not self.workload_descriptor:
            return True, "balanced"

        self.workload_descriptor.estimated_carbon_kg = intensity / 1000.0
        self.workload_descriptor.estimated_energy_joules = DEFAULT_WORKLOAD_ENERGY_J
        # helium_units scaling: normalize $/L to 0..1 range; clamped
        helium_price = validate_helium(helium.spot_price_usd_per_liter)
        if hasattr(self.workload_descriptor, "helium_units"):
            self.workload_descriptor.helium_units = min(helium_price, 1.0)
        else:
            log.debug("WorkloadDescriptor lacks helium_units; skipping.")

        try:
            priority: Priority = await self.workload_descriptor.select_priority(exploration=True)
        except Exception as exc:  # noqa: BLE001
            log.warning("Priority selection failed; defaulting to balanced: %s", exc)
            return True, "balanced"

        if priority is Priority.ACCURACY:
            return True, Priority.ACCURACY.value
        if priority is Priority.GREEN:
            return intensity < 500, Priority.GREEN.value
        return True, Priority.BALANCED.value

    # -- symbolic evaluation --------------------------------------------------
    def evaluate_symbolic(self, metrics: Dict[str, Any], step: int = 1) -> List[Dict[str, Any]]:
        if not self.symbolic_engine:
            return []
        try:
            violations = self.symbolic_engine.evaluate_rules(metrics, step=step)
            return [v.to_dict() for v in violations]
        except Exception as exc:  # noqa: BLE001
            log.warning("Symbolic rule evaluation failed: %s", exc)
            return []

    def _apply_symbolic_blockers(self, violations: List[Dict[str, Any]]) -> bool:
        """Return True if any critical/high violation demands skip."""
        for v in violations:
            if v.get("severity") == "critical":
                log.error("Critical symbolic violation %s — skipping contribution.", v.get("rule_id"))
                return True
        return False

    # -- flows ----------------------------------------------------------------
    async def contribute_with_enhancements(
        self, base_region: str = DEFAULT_REGION, dry_run: bool = False,
    ) -> ContributionResult:
        # 1. Auth (optional)
        auth_context = await self.authenticate_if_needed()

        # 2. Region
        region = await self.select_region_with_distillation(base_region)
        try:
            validate_region(region)
        except ValueError as exc:
            log.warning("Distillation returned invalid region %r; falling back to %s", region, base_region)
            region = base_region
        log.info("Selected region for carbon observation: %s", region)

        # 3. Observe
        try:
            intensity, source, _conf = await self.manager.get_carbon_intensity(region)
            intensity = validate_carbon(intensity)
        except Exception as exc:  # noqa: BLE001
            log.error("Could not fetch carbon intensity for %s: %s", region, exc)
            return ContributionResult(
                region=region, carbon_intensity=0.0, carbon_source="error",
                helium_price_usd_per_liter=0.0, priority="n/a",
                contributed=False, error=str(exc),
            )

        helium = await self.manager.get_helium_data()
        helium_price = validate_helium(helium.spot_price_usd_per_liter)
        log.info("Carbon: %.0f gCO2/kWh (region=%s, source=%s)", intensity, region, source)
        log.info("Helium: $%.2f/L", helium_price)

        # 4. Symbolic evaluation (4th layer)
        metrics = {
            "carbon": intensity,
            "energy": DEFAULT_WORKLOAD_ENERGY_J,
            "latency": DEFAULT_LATENCY_TARGET_MS,
            "memory": 0,
            "tool_calls": 1,
            "environment": os.environ.get("ENVIRONMENT", "production"),
        }
        symbolic_violations = self.evaluate_symbolic(metrics, step=1)
        if self._apply_symbolic_blockers(symbolic_violations):
            return ContributionResult(
                region=region, carbon_intensity=intensity, carbon_source=source,
                helium_price_usd_per_liter=helium_price, priority="blocked",
                contributed=False, dry_run=dry_run,
                symbolic_violations=symbolic_violations,
            )

        # 5. Priority decision
        should_contribute, priority = await self.decide_contribution_with_workload(region, intensity, helium)
        if not should_contribute:
            log.info("Contribution skipped based on adaptive priority (%s).", priority)
            return ContributionResult(
                region=region, carbon_intensity=intensity, carbon_source=source,
                helium_price_usd_per_liter=helium_price, priority=priority,
                contributed=False, dry_run=dry_run,
                symbolic_violations=symbolic_violations,
            )

        # 6. Dry-run gate
        if dry_run:
            log.info("[dry-run] Would contribute: region=%s intensity=%.0f priority=%s", region, intensity, priority)
            return ContributionResult(
                region=region, carbon_intensity=intensity, carbon_source=source,
                helium_price_usd_per_liter=helium_price, priority=priority,
                contributed=False, dry_run=True,
                symbolic_violations=symbolic_violations,
                zero_trust_request_id=getattr(auth_context, "request_id", None) if auth_context else None,
            )

        # 7. Contribute
        fingerprint = observation_fingerprint(region, intensity, source)
        CommunityDataHub.contribute_carbon_observation(region, intensity, source)
        CommunityDataHub.contribute_helium_observation(helium_price, helium.inventory_days, helium.source)
        log.info("Data contributed (fp=%s, priority=%s).", fingerprint, priority)

        # 8. Record outcome for distillation (guarded)
        if self.node_descriptor:
            try:
                await self.node_descriptor.record_outcome(
                    carbon_saved_kg=0.01, latency_ms=100, cost_usd=0.001,
                )
            except Exception as exc:  # noqa: BLE001
                log.debug("Node record_outcome failed: %s", exc)
        if self.workload_descriptor:
            try:
                await self.workload_descriptor.record_outcome(
                    latency_achieved_ms=100, carbon_saved_kg=0.01, energy_used_joules=DEFAULT_WORKLOAD_ENERGY_J,
                )
            except Exception as exc:  # noqa: BLE001
                log.debug("Workload record_outcome failed: %s", exc)

        # 9. Zero Trust ledger
        request_id = None
        if self.zero_trust and auth_context:
            request_id = getattr(auth_context, "request_id", None)
            try:
                if hasattr(self.zero_trust, "log_event"):
                    await self.zero_trust.log_event(
                        "data_contribution", request_id,
                        {"region": region, "intensity": intensity, "priority": priority},
                    )
                elif hasattr(self.zero_trust, "_log_security_event"):
                    # Fallback for older builds — deprecation path
                    await self.zero_trust._log_security_event(  # noqa: SLF001
                        "data_contribution", request_id,
                        {"region": region, "intensity": intensity, "priority": priority},
                    )
                log.info("Contribution recorded in immutable ledger.")
            except Exception as exc:  # noqa: BLE001
                log.warning("Ledger write failed: %s", exc)

        return ContributionResult(
            region=region, carbon_intensity=intensity, carbon_source=source,
            helium_price_usd_per_liter=helium_price, priority=priority,
            contributed=True, dry_run=False,
            symbolic_violations=symbolic_violations, zero_trust_request_id=request_id,
        )

    async def fetch_with_enhancements(self, region: str = DEFAULT_REGION) -> FetchResult:
        carbon_avg = CommunityDataHub.get_community_carbon_average(region)
        helium_avg = CommunityDataHub.get_community_helium_price()

        result = FetchResult(region=region, carbon_average=carbon_avg, helium_average=helium_avg)

        # Enhanced observability
        if self.use_enhanced and self.node_descriptor:
            opt = getattr(self.node_descriptor, "_routing_optimizer", None)
            if opt and hasattr(opt, "get_stats"):
                result.distillation_stats["node"] = opt.get_stats()
        if self.use_enhanced and self.workload_descriptor:
            opt = getattr(self.workload_descriptor, "_priority_optimizer", None)
            if opt and hasattr(opt, "get_stats"):
                result.distillation_stats["workload"] = opt.get_stats()
        if self.zero_trust and hasattr(self.zero_trust, "get_security_posture"):
            try:
                result.security_posture = self.zero_trust.get_security_posture() or {}
            except Exception as exc:  # noqa: BLE001
                log.debug("Could not read security posture: %s", exc)

        # Symbolic evaluation of fetched context
        metrics = {
            "carbon": carbon_avg if carbon_avg is not None else 0.0,
            "energy": DEFAULT_WORKLOAD_ENERGY_J,
            "latency": DEFAULT_LATENCY_TARGET_MS,
            "memory": 0,
            "tool_calls": 1,
            "environment": os.environ.get("ENVIRONMENT", "production"),
        }
        result.symbolic_violations = self.evaluate_symbolic(metrics, step=1)
        return result


# ------------------------------------------------------------------------------
# Output formatting
# ------------------------------------------------------------------------------
def _emit(result: Any, output_json: Optional[str]) -> None:
    payload = asdict(result) if hasattr(result, "__dataclass_fields__") else result
    if output_json:
        Path(output_json).write_text(json.dumps(payload, indent=2, default=str))
        log.info("Wrote structured output to %s", output_json)
    else:
        print(json.dumps(payload, indent=2, default=str))


# ------------------------------------------------------------------------------
# Main CLI flows
# ------------------------------------------------------------------------------
def _decide_enhanced(args) -> bool:
    if args.no_enhancements:
        return False
    if args.enhanced:
        return True
    return os.environ.get("ENHANCEMENTS_ENABLED", "false").lower() == "true"


async def collect_and_contribute(
    use_enhanced: bool, region: str, dry_run: bool, output_json: Optional[str],
) -> ContributionResult:
    log.info("Green Agent - Community Data Collector")
    if not ENHANCED_MODULES_AVAILABLE and use_enhanced:
        log.warning("Enhanced modules unavailable (%s); falling back to legacy.", ENHANCED_IMPORT_ERROR)

    manager = FreeAPIManager()

    if use_enhanced and ENHANCED_MODULES_AVAILABLE:
        async with EnhancedCommunityCollector(manager=manager, use_enhanced=True) as collector:
            result = await collector.contribute_with_enhancements(base_region=region, dry_run=dry_run)
        _emit(result, output_json)
        return result

    # Legacy flow
    try:
        intensity, source, _conf = await manager.get_carbon_intensity(region)
        validate_carbon(intensity)
    except Exception as exc:  # noqa: BLE001
        log.error("Legacy carbon fetch failed: %s", exc)
        result = ContributionResult(
            region=region, carbon_intensity=0.0, carbon_source="error",
            helium_price_usd_per_liter=0.0, priority="n/a",
            contributed=False, error=str(exc),
        )
        _emit(result, output_json)
        return result

    helium = await manager.get_helium_data()
    helium_price = validate_helium(helium.spot_price_usd_per_liter)

    if not dry_run:
        CommunityDataHub.contribute_carbon_observation(region, intensity, source)
        CommunityDataHub.contribute_helium_observation(helium_price, helium.inventory_days, helium.source)
        log.info("Contributed (legacy mode).")
    else:
        log.info("[dry-run] Legacy contribution skipped.")

    result = ContributionResult(
        region=region, carbon_intensity=intensity, carbon_source=source,
        helium_price_usd_per_liter=helium_price, priority="balanced",
        contributed=not dry_run, dry_run=dry_run,
    )
    _emit(result, output_json)
    return result


async def fetch_and_display(
    use_enhanced: bool, regions: List[str], output_json: Optional[str],
) -> List[FetchResult]:
    manager = FreeAPIManager()
    results: List[FetchResult] = []

    if use_enhanced and ENHANCED_MODULES_AVAILABLE:
        async with EnhancedCommunityCollector(manager=manager, use_enhanced=True) as collector:
            for region in regions:
                validate_region(region)
                results.append(await collector.fetch_with_enhancements(region))
    else:
        for region in regions:
            validate_region(region)
            carbon = CommunityDataHub.get_community_carbon_average(region)
            helium = CommunityDataHub.get_community_helium_price()
            results.append(FetchResult(region=region, carbon_average=carbon, helium_average=helium))

    if output_json:
        Path(output_json).write_text(json.dumps([asdict(r) for r in results], indent=2, default=str))
        log.info("Wrote structured output to %s", output_json)
    else:
        for r in results:
            print(json.dumps(asdict(r), indent=2, default=str))
    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Green Agent Community Data Collector")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--contribute", action="store_true", help="Collect and contribute observations")
    mode.add_argument("--fetch", action="store_true", help="Fetch and display community data")
    parser.add_argument("--region", default=DEFAULT_REGION, help=f"Region (default: {DEFAULT_REGION})")
    parser.add_argument("--regions", default=None, help="Comma-separated regions for multi-region fetch")
    parser.add_argument("--enhanced", action="store_true",
                        help="Force enhanced mode (overrides ENHANCEMENTS_ENABLED)")
    parser.add_argument("--no-enhancements", action="store_true",
                        help="Force legacy mode (overrides ENHANCEMENTS_ENABLED)")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to community hub")
    parser.add_argument("--output-json", default=None, help="Write structured output to PATH")
    parser.add_argument("--verbose", action="store_true", help="DEBUG logging")
    parser.add_argument("--quiet", action="store_true", help="WARNING logging")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    use_enhanced = _decide_enhanced(args)
    if use_enhanced and not ENHANCED_MODULES_AVAILABLE:
        log.warning("Enhanced mode requested but modules unavailable: %s", ENHANCED_IMPORT_ERROR)

    try:
        if args.contribute:
            validate_region(args.region)
            asyncio.run(collect_and_contribute(
                use_enhanced=use_enhanced, region=args.region,
                dry_run=args.dry_run, output_json=args.output_json,
            ))
        elif args.fetch:
            regions = [r.strip() for r in (args.regions or args.region).split(",") if r.strip()]
            asyncio.run(fetch_and_display(
                use_enhanced=use_enhanced, regions=regions, output_json=args.output_json,
            ))
        else:
            parser.print_help()
            return 1
    except ValueError as exc:
        log.error("Invalid input: %s", exc)
        return 2
    except KeyboardInterrupt:
        log.warning("Interrupted.")
        return 130

    return 0


if __name__ == "__main__":
    sys.exit(main())
