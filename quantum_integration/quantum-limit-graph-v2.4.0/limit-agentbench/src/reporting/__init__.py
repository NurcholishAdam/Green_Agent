"""
Reporting package for Green Agent (Enhanced)
=============================================

Auditable presentation and disclosure layer. Consumes validated outputs
from analysis, policy, verification, and runtime components; produces
stakeholder-specific reports, dashboards, benchmark submissions, and
machine-readable evidence.
"""

from .layered_reporter import (
    # Original exports
    Layer1RawMetrics,
    Layer2NormalizedMetrics,
    Layer3ScenarioScore,
    LayeredReporter,
    # Enhanced contracts
    EvidenceBundle,
    BundleBuilder,
    CarbonInstruments,
    MetricProvenance,
    ExplanationCard,
    VerificationEvidence,
    # Enums
    Audience,
    ProvenanceKind,
    TrustLevel,
)

from .report_generator import (
    ReportGenerator,
    JSONRenderer,
    MarkdownRenderer,
    HTMLRenderer,
    CSVRenderer,
)

from .leaderboard import (
    generate_leaderboard,
    Leaderboard,
    LeaderboardEntry,
    LeaderboardResult,
    RankStatus,
)

from .agentbeats import (
    build_agentbeats_submission,
    ReproducibilityBundle,
    verify_submission,
)

__all__ = [
    # Original
    "Layer1RawMetrics",
    "Layer2NormalizedMetrics",
    "Layer3ScenarioScore",
    "LayeredReporter",
    "ReportGenerator",
    "generate_leaderboard",
    "build_agentbeats_submission",
    # Enhanced contracts
    "EvidenceBundle",
    "BundleBuilder",
    "CarbonInstruments",
    "MetricProvenance",
    "ExplanationCard",
    "VerificationEvidence",
    # Renderers
    "JSONRenderer",
    "MarkdownRenderer",
    "HTMLRenderer",
    "CSVRenderer",
    # Leaderboard
    "Leaderboard",
    "LeaderboardEntry",
    "LeaderboardResult",
    "RankStatus",
    # Benchmarking
    "ReproducibilityBundle",
    "verify_submission",
    # Enums
    "Audience",
    "ProvenanceKind",
    "TrustLevel",
]
