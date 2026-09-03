# -*- coding: utf-8 -*-
"""Multi-framework agent adapters with optional enhancements."""

from .base_adapter import BaseAgentAdapter
from .langchain_adapter import LangChainAdapter
from .autogen_adapter import AutoGenAdapter
from .crewai_adapter import CrewAIAdapter
from .limit_graph_adapter import LimitGraphAdapter

# Try to import enhanced schemas from the enhancements folder.
# If not available, these imports are silently ignored.
try:
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    FeedbackEvent = None
    WorkloadDescriptor = None
    NodeDescriptor = None
    ZeroTrustArchitecture = None

__all__ = [
    "BaseAgentAdapter",
    "LangChainAdapter",
    "AutoGenAdapter",
    "CrewAIAdapter",
    "LimitGraphAdapter",
]

# Optionally expose the enhanced components if available
if ENHANCEMENTS_AVAILABLE:
    __all__.extend([
        "FeedbackEvent",
        "WorkloadDescriptor",
        "NodeDescriptor",
        "ZeroTrustArchitecture",
    ])
