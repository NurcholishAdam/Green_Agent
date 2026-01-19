# -*- coding: utf-8 -*-
"""
LIMIT-GRAPH Agent Adapter
Adapter for native LIMIT-GRAPH quantum agents
"""

from typing import Dict, Any
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
    """
    
    def __init__(self, agent: Any):
        """
        Initialize LIMIT-GRAPH adapter.
        
        Args:
            agent: LIMIT-GRAPH agent instance
        """
        super().__init__(agent, "limit_graph")
    
    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: bool = True
    ) -> Dict[str, Any]:
        """
        Run LIMIT-GRAPH agent on task input.
        
        Args:
            task_input: Task input data
            track_green_metrics: Whether to track energy/carbon
            
        Returns:
            Agent output
        """
        logger.debug(f"Running LIMIT-GRAPH agent on task")
        
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
