# -*- coding: utf-8 -*-
"""
CrewAI Agent Adapter
Adapter for CrewAI role-based agents
"""

from typing import Dict, Any
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
    """
    
    def __init__(self, agent: Any):
        """
        Initialize CrewAI adapter.
        
        Args:
            agent: CrewAI agent or crew instance
        """
        super().__init__(agent, "crewai")
    
    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: bool = True
    ) -> Dict[str, Any]:
        """
        Run CrewAI agent on task input.
        
        Args:
            task_input: Task input data
            track_green_metrics: Whether to track energy/carbon
            
        Returns:
            Agent output
        """
        logger.debug(f"Running CrewAI agent on task")
        
        # Extract task description
        if isinstance(task_input, dict):
            task_desc = task_input.get('question') or task_input.get('input') or str(task_input)
        else:
            task_desc = str(task_input)
        
        # Run agent or crew
        if hasattr(self.agent, 'kickoff'):
            # Crew execution
            result = self.agent.kickoff()
        elif hasattr(self.agent, 'execute_task'):
            # Single agent task execution
            result = self.agent.execute_task(task_desc)
        else:
            raise ValueError("Unsupported CrewAI agent type")
        
        return {"answer": result, "framework": "crewai"}
    
    def _get_agent_name(self) -> str:
        """Get agent name from CrewAI agent."""
        if hasattr(self.agent, 'role'):
            return f"CrewAI-{self.agent.role}"
        elif hasattr(self.agent, 'name'):
            return self.agent.name
        else:
            return "CrewAIAgent"
