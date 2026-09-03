# -*- coding: utf-8 -*-
"""
LangChain Agent Adapter (Enhanced)
Adapter for LangChain/LangGraph agents with optional advanced decision-making
and sustainability tracking.
"""

from typing import Dict, Any, Optional
import logging
from .base_adapter import BaseAgentAdapter

logger = logging.getLogger(__name__)


class LangChainAdapter(BaseAgentAdapter):
    """
    Adapter for LangChain and LangGraph agents.

    Supports:
    - LangChain Agent Executor
    - LangGraph StateGraph agents
    - LCEL chains

    When ``use_enhancements`` is enabled in the config, this adapter delegates
    to the enhanced pipeline defined in :class:`BaseAgentAdapter`. That pipeline
    can select an execution strategy (native LangChain vs FlexGen low/high precision),
    compute multi‑objective rewards (MODP), incorporate RLHF feedback and LIMIT
    Graph metrics, and emit ``FeedbackEvent`` for cross‑module learning.
    """

    def __init__(self, agent: Any, config: Optional[Dict[str, Any]] = None):
        """
        Initialize LangChain adapter.

        Args:
            agent: LangChain agent instance.
            config: Optional configuration dictionary for enhancements.
        """
        super().__init__(agent, "langchain", config)

    def _execute(self, task_input: Dict[str, Any], strategy: str = "native") -> Dict[str, Any]:
        """
        Execute the LangChain agent on the given task input.

        Args:
            task_input: Task input data (dict or string).
            strategy: Execution strategy; ignored by LangChain adapter but present
                      for interface consistency.

        Returns:
            Dictionary containing the agent's output. The base adapter will merge
            latency and metrics into the final result.
        """
        logger.debug(f"Running LangChain agent on task (strategy={strategy})")

        # Handle different LangChain agent types
        if hasattr(self.agent, 'invoke'):
            # LangGraph or LCEL chain
            result = self.agent.invoke(task_input)
        elif hasattr(self.agent, 'run'):
            # Legacy Agent Executor
            if isinstance(task_input, dict) and 'input' in task_input:
                result = self.agent.run(task_input['input'])
            else:
                result = self.agent.run(str(task_input))
        else:
            raise ValueError("Unsupported LangChain agent type")

        # Return result as dictionary; base adapter expects a dict
        if isinstance(result, dict):
            return result
        else:
            return {"answer": result, "framework": "langchain"}

    def _get_agent_name(self) -> str:
        """Get agent name from LangChain agent."""
        if hasattr(self.agent, 'name'):
            return self.agent.name
        elif hasattr(self.agent, '__class__'):
            return self.agent.__class__.__name__
        else:
            return "LangChainAgent"
