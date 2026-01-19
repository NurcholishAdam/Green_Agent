# -*- coding: utf-8 -*-
"""
AutoGen Agent Adapter
Adapter for Microsoft AutoGen agents
"""

from typing import Dict, Any
import logging
from .base_adapter import BaseAgentAdapter

logger = logging.getLogger(__name__)


class AutoGenAdapter(BaseAgentAdapter):
    """
    Adapter for Microsoft AutoGen agents.
    
    Supports:
    - ConversableAgent
    - AssistantAgent
    - UserProxyAgent
    - Multi-agent conversations
    """
    
    def __init__(self, agent: Any):
        """
        Initialize AutoGen adapter.
        
        Args:
            agent: AutoGen agent instance
        """
        super().__init__(agent, "autogen")
    
    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: bool = True
    ) -> Dict[str, Any]:
        """
        Run AutoGen agent on task input.
        
        Args:
            task_input: Task input data
            track_green_metrics: Whether to track energy/carbon
            
        Returns:
            Agent output
        """
        logger.debug(f"Running AutoGen agent on task")
        
        # Extract message from task input
        if isinstance(task_input, dict):
            message = task_input.get('question') or task_input.get('input') or str(task_input)
        else:
            message = str(task_input)
        
        # Run agent
        if hasattr(self.agent, 'generate_reply'):
            # Single agent
            result = self.agent.generate_reply(messages=[{"role": "user", "content": message}])
        elif hasattr(self.agent, 'initiate_chat'):
            # Multi-agent conversation
            result = self.agent.initiate_chat(message=message)
        else:
            raise ValueError("Unsupported AutoGen agent type")
        
        return {"answer": result, "framework": "autogen"}
    
    def _get_agent_name(self) -> str:
        """Get agent name from AutoGen agent."""
        if hasattr(self.agent, 'name'):
            return self.agent.name
        else:
            return "AutoGenAgent"
