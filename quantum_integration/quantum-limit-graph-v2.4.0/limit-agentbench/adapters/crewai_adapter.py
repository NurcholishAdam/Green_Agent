# -*- coding: utf-8 -*-
"""
CrewAI Agent Adapter (Enhanced)
Adapter for CrewAI role-based agents with optional advanced decision-making
and sustainability tracking.
"""

from typing import Dict, Any, Optional
import logging
from .base_adapter import BaseAgentAdapter

logger = logging.getLogger(__name__)


class CrewAIAdapter(BaseAgentAdapter):
    """
    Adapter for CrewAI agents.

    Supports:
    - CrewAI Agent
    - CrewAI Crew (multi-agent)
    - Role-based task execution

    When ``use_enhancements`` is enabled in the config, this adapter delegates
    to the enhanced pipeline defined in :class:`BaseAgentAdapter`. That pipeline
    can select an execution strategy (native CrewAI vs FlexGen low/high precision),
    compute multi‑objective rewards (MODP), incorporate RLHF feedback and LIMIT
    Graph metrics, and emit ``FeedbackEvent`` for cross‑module learning.
    """

    def __init__(self, agent: Any, config: Optional[Dict[str, Any]] = None):
        """
        Initialize CrewAI adapter.

        Args:
            agent: CrewAI agent or crew instance.
            config: Optional configuration dictionary for enhancements.
        """
        super().__init__(agent, "crewai", config)

    def _execute(self, task_input: Dict[str, Any], strategy: str = "native") -> Dict[str, Any]:
        """
        Execute the CrewAI agent on the given task input.

        Args:
            task_input: Task input data (dict or string).
            strategy: Execution strategy; ignored by CrewAI adapter but present
                      for interface consistency.

        Returns:
            Dictionary containing at least 'answer' key; may also include
            other metrics like 'accuracy' if available.
        """
        logger.debug(f"Running CrewAI agent on task (strategy={strategy})")

        # Extract task description
        if isinstance(task_input, dict):
            task_desc = task_input.get('question') or task_input.get('input') or str(task_input)
        else:
            task_desc = str(task_input)

        # Run agent or crew
        if hasattr(self.agent, 'kickoff'):
            result = self.agent.kickoff()
        elif hasattr(self.agent, 'execute_task'):
            result = self.agent.execute_task(task_desc)
        else:
            raise ValueError("Unsupported CrewAI agent type")

        # Return raw result; BaseAgentAdapter will merge metrics and latency.
        return {"answer": result, "framework": "crewai"}

    def _get_agent_name(self) -> str:
        """Get agent name from CrewAI agent."""
        if hasattr(self.agent, 'role'):
            return f"CrewAI-{self.agent.role}"
        elif hasattr(self.agent, 'name'):
            return self.agent.name
        else:
            return "CrewAIAgent"
