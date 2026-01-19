# -*- coding: utf-8 -*-
"""
LangChain Agent Adapter
Adapter for LangChain/LangGraph agents
"""

from typing import Dict, Any
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
    """
    
    def __init__(self, agent: Any):
        """
        Initialize LangChain adapter.
        
        Args:
            agent: LangChain agent instance
        """
        super().__init__(agent, "langchain")
    
    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: bool = True
    ) -> Dict[str, Any]:
        """
        Run LangChain agent on task input.
        
        Args:
            task_input: Task input data
            track_green_metrics: Whether to track energy/carbon
            
        Returns:
            Agent output
        """
        logger.debug(f"Running LangChain agent on task")
        
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
        
        return result
    
    def _get_agent_name(self) -> str:
        """Get agent name from LangChain agent."""
        if hasattr(self.agent, 'name'):
            return self.agent.name
        elif hasattr(self.agent, '__class__'):
            return self.agent.__class__.__name__
        else:
            return "LangChainAgent"
