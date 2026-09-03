# -*- coding: utf-8 -*-
"""
LIMIT-GRAPH Agent Adapter (Enhanced)
Adapter for native LIMIT-GRAPH quantum agents with optional advanced
decision-making and sustainability tracking.
"""

from typing import Dict, Any, Optional
import logging
from .base_adapter import BaseAgentAdapter

logger = logging.getLogger(__name__)


class LimitGraphAdapter(BaseAgentAdapter):
    """
    Adapter for native LIMIT-GRAPH agents.

    Supports:
    - Quantum-enhanced agents
    - Level 3/4/5 maturity agents
    - NSN-integrated agents
    - Multilingual agents

    When ``use_enhancements`` is enabled in the config, this adapter delegates
    to the enhanced pipeline defined in :class:`BaseAgentAdapter`. That pipeline
    can select an execution strategy (native LIMIT‑GRAPH vs FlexGen low/high
    precision), compute multi‑objective rewards (MODP), incorporate RLHF feedback
    and LIMIT Graph metrics, and emit ``FeedbackEvent`` for cross‑module learning.
    """

    def __init__(self, agent: Any, config: Optional[Dict[str, Any]] = None):
        """
        Initialize LIMIT-GRAPH adapter.

        Args:
            agent: LIMIT-GRAPH agent instance.
            config: Optional configuration dictionary for enhancements.
        """
        super().__init__(agent, "limit_graph", config)

    def _execute(self, task_input: Dict[str, Any], strategy: str = "native") -> Dict[str, Any]:
        """
        Execute the LIMIT-GRAPH agent on the given task input.

        Args:
            task_input: Task input data (dict or string).
            strategy: Execution strategy; ignored by LIMIT-GRAPH adapter but present
                      for interface consistency.

        Returns:
            Dictionary containing the agent's output. The base adapter will merge
            latency and metrics into the final result.
        """
        logger.debug(f"Running LIMIT-GRAPH agent on task (strategy={strategy})")

        # LIMIT-GRAPH agents typically have a run or execute method
        if hasattr(self.agent, 'run'):
            result = self.agent.run(task_input)
        elif hasattr(self.agent, 'execute'):
            result = self.agent.execute(task_input)
        elif hasattr(self.agent, 'process'):
            result = self.agent.process(task_input)
        elif callable(self.agent):
            result = self.agent(task_input)
        else:
            raise ValueError("Unsupported LIMIT-GRAPH agent type")

        # Ensure result includes framework info
        if isinstance(result, dict):
            if 'framework' not in result:
                result['framework'] = 'limit_graph'
            return result
        else:
            return {"answer": result, "framework": "limit_graph"}

    def _get_agent_name(self) -> str:
        """Get agent name from LIMIT-GRAPH agent."""
        if hasattr(self.agent, 'name'):
            return self.agent.name
        elif hasattr(self.agent, 'agent_id'):
            return f"LIMIT-{self.agent.agent_id}"
        else:
            return "LimitGraphAgent"
