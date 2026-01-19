# -*- coding: utf-8 -*-
"""
Base Agent Adapter
Abstract base class for framework-specific adapters
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class BaseAgentAdapter(ABC):
    """
    Abstract base class for agent adapters.
    
    Provides a unified interface for agents from different frameworks.
    """
    
    def __init__(self, agent: Any, framework_name: str):
        """
        Initialize adapter.
        
        Args:
            agent: Agent instance from specific framework
            framework_name: Name of the framework
        """
        self.agent = agent
        self.framework_name = framework_name
        self.agent_name = self._get_agent_name()
        
        logger.info(f"Initialized {framework_name} adapter for agent {self.agent_name}")
    
    @abstractmethod
    def run(
        self,
        task_input: Dict[str, Any],
        track_green_metrics: bool = True
    ) -> Dict[str, Any]:
        """
        Run agent on task input.
        
        Args:
            task_input: Task input data
            track_green_metrics: Whether to track energy/carbon
            
        Returns:
            Agent output
        """
        pass
    
    @abstractmethod
    def _get_agent_name(self) -> str:
        """Get agent name from framework-specific agent."""
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get agent metadata.
        
        Returns:
            Dictionary with agent metadata
        """
        return {
            "agent_name": self.agent_name,
            "framework": self.framework_name,
            "adapter_version": "2.4.2"
        }
