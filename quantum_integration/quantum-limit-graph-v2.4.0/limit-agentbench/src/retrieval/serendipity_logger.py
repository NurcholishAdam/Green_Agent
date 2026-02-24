# -*- coding: utf-8 -*-
"""
Serendipity Trace Logger

Captures unexpected efficiency gains or retrieval shortcuts discovered by agents.
Integrates with the Negawatt Reward System to reward low-energy retrieval paths.
"""

import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import hashlib


@dataclass
class SerendipityEvent:
    """Record of a serendipitous efficiency discovery."""
    event_id: str
    event_type: str  # "shortcut", "efficiency_gain", "unexpected_path"
    description: str
    baseline_energy_wh: float
    actual_energy_wh: float
    energy_saved_wh: float
    savings_pct: float
    retrieval_path: List[str]
    context: Dict[str, Any]
    timestamp: float
    negawatt_reward: float  # Reward for energy savings
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class NegawattReward:
    """Reward for energy-efficient behavior."""
    reward_id: str
    agent_id: str
    energy_saved_wh: float
    reward_points: float
    reason: str
    timestamp: float
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SerendipityTraceLogger:
    """
    Logger for serendipitous efficiency discoveries.
    
    Responsibilities:
    - Capture unexpected efficiency gains
    - Track retrieval shortcuts discovered by agents
    - Calculate negawatt rewards for energy savings
    - Integrate with meta-cognitive reflection
    - Build knowledge base of efficient patterns
    """
    
    def __init__(
        self,
        negawatt_reward_rate: float = 100.0,  # Points per Wh saved
        min_savings_threshold_pct: float = 10.0  # Minimum savings to log
    ):
        """
        Initialize serendipity trace logger.
        
        Args:
            negawatt_reward_rate: Reward points per Wh saved
            min_savings_threshold_pct: Minimum savings percentage to log
        """
        self.reward_rate = negawatt_reward_rate
        self.min_threshold = min_savings_threshold_pct
        
        # Event storage
        self.serendipity_events: List[SerendipityEvent] = []
        self.negawatt_rewards: List[NegawattReward] = []
        
        # Pattern knowledge base
        self.efficient_patterns: Dict[str, List[str]] = {}
        
        # Statistics
        self.total_energy_saved = 0.0
        self.total_rewards_issued = 0.0
        
    def log_efficiency_gain(
        self,
        description: str,
        baseline_energy_wh: float,
        actual_energy_wh: float,
        retrieval_path: List[str],
        context: Optional[Dict[str, Any]] = None,
        agent_id: str = "default_agent"
    ) -> Optional[SerendipityEvent]:
        """
        Log an efficiency gain discovery.
        
        Args:
            description: Description of the efficiency gain
            baseline_energy_wh: Expected energy consumption
            actual_energy_wh: Actual energy consumption
            retrieval_path: Path taken during retrieval
            context: Additional context information
            agent_id: ID of the agent that discovered the gain
            
        Returns:
            SerendipityEvent if logged, None if below threshold
        """
        # Calculate savings
        energy_saved = baseline_energy_wh - actual_energy_wh
        savings_pct = (energy_saved / baseline_energy_wh * 100) if baseline_energy_wh > 0 else 0
        
        # Check if savings meet threshold
        if savings_pct < self.min_threshold:
            return None
        
        # Calculate negawatt reward
        negawatt_reward = energy_saved * self.reward_rate
        
        # Create event
        event = SerendipityEvent(
            event_id=self._generate_event_id(description),
            event_type="efficiency_gain",
            description=description,
            baseline_energy_wh=baseline_energy_wh,
            actual_energy_wh=actual_energy_wh,
            energy_saved_wh=energy_saved,
            savings_pct=savings_pct,
            retrieval_path=retrieval_path,
            context=context or {},
            timestamp=datetime.now().timestamp(),
            negawatt_reward=negawatt_reward
        )
        
        self.serendipity_events.append(event)
        self.total_energy_saved += energy_saved
        
        # Issue negawatt reward
        self._issue_negawatt_reward(agent_id, energy_saved, description)
        
        # Learn from pattern
        self._learn_efficient_pattern(retrieval_path, energy_saved)
        
        return event
    
    def log_shortcut_discovery(
        self,
        description: str,
        original_path: List[str],
        shortcut_path: List[str],
        energy_saved_wh: float,
        context: Optional[Dict[str, Any]] = None,
        agent_id: str = "default_agent"
    ) -> SerendipityEvent:
        """
        Log discovery of a retrieval shortcut.
        
        Args:
            description: Description of the shortcut
            original_path: Original retrieval path
            shortcut_path: Discovered shortcut path
            energy_saved_wh: Energy saved by using shortcut
            context: Additional context
            agent_id: ID of the discovering agent
            
        Returns:
            SerendipityEvent
        """
        # Estimate baseline energy from original path
        baseline_energy = len(original_path) * 0.001  # Simplified estimate
        actual_energy = baseline_energy - energy_saved_wh
        savings_pct = (energy_saved_wh / baseline_energy * 100) if baseline_energy > 0 else 0
        
        # Calculate negawatt reward
        negawatt_reward = energy_saved_wh * self.reward_rate
        
        # Create event
        event = SerendipityEvent(
            event_id=self._generate_event_id(description),
            event_type="shortcut",
            description=description,
            baseline_energy_wh=baseline_energy,
            actual_energy_wh=actual_energy,
            energy_saved_wh=energy_saved_wh,
            savings_pct=savings_pct,
            retrieval_path=shortcut_path,
            context={
                "original_path": original_path,
                "shortcut_path": shortcut_path,
                **(context or {})
            },
            timestamp=datetime.now().timestamp(),
            negawatt_reward=negawatt_reward
        )
        
        self.serendipity_events.append(event)
        self.total_energy_saved += energy_saved_wh
        
        # Issue negawatt reward
        self._issue_negawatt_reward(agent_id, energy_saved_wh, description)
        
        # Learn from shortcut
        self._learn_efficient_pattern(shortcut_path, energy_saved_wh)
        
        return event
    
    def log_unexpected_path(
        self,
        description: str,
        path: List[str],
        energy_wh: float,
        expected_energy_wh: float,
        context: Optional[Dict[str, Any]] = None,
        agent_id: str = "default_agent"
    ) -> Optional[SerendipityEvent]:
        """
        Log an unexpected but efficient retrieval path.
        
        Args:
            description: Description of the path
            path: Retrieval path taken
            energy_wh: Actual energy consumed
            expected_energy_wh: Expected energy consumption
            context: Additional context
            agent_id: ID of the agent
            
        Returns:
            SerendipityEvent if efficient, None otherwise
        """
        energy_saved = expected_energy_wh - energy_wh
        
        if energy_saved <= 0:
            return None  # Not efficient
        
        savings_pct = (energy_saved / expected_energy_wh * 100) if expected_energy_wh > 0 else 0
        
        if savings_pct < self.min_threshold:
            return None
        
        # Calculate negawatt reward
        negawatt_reward = energy_saved * self.reward_rate
        
        # Create event
        event = SerendipityEvent(
            event_id=self._generate_event_id(description),
            event_type="unexpected_path",
            description=description,
            baseline_energy_wh=expected_energy_wh,
            actual_energy_wh=energy_wh,
            energy_saved_wh=energy_saved,
            savings_pct=savings_pct,
            retrieval_path=path,
            context=context or {},
            timestamp=datetime.now().timestamp(),
            negawatt_reward=negawatt_reward
        )
        
        self.serendipity_events.append(event)
        self.total_energy_saved += energy_saved
        
        # Issue negawatt reward
        self._issue_negawatt_reward(agent_id, energy_saved, description)
        
        # Learn from path
        self._learn_efficient_pattern(path, energy_saved)
        
        return event
    
    def _issue_negawatt_reward(self, agent_id: str, energy_saved_wh: float, reason: str):
        """Issue negawatt reward for energy savings."""
        reward_points = energy_saved_wh * self.reward_rate
        
        reward = NegawattReward(
            reward_id=self._generate_reward_id(agent_id),
            agent_id=agent_id,
            energy_saved_wh=energy_saved_wh,
            reward_points=reward_points,
            reason=reason,
            timestamp=datetime.now().timestamp()
        )
        
        self.negawatt_rewards.append(reward)
        self.total_rewards_issued += reward_points
    
    def _learn_efficient_pattern(self, path: List[str], energy_saved: float):
        """Learn from efficient retrieval pattern."""
        # Create pattern signature
        pattern_sig = "->".join(path[:3])  # Use first 3 nodes as signature
        
        if pattern_sig not in self.efficient_patterns:
            self.efficient_patterns[pattern_sig] = []
        
        self.efficient_patterns[pattern_sig].append({
            "path": path,
            "energy_saved": energy_saved,
            "timestamp": datetime.now().timestamp()
        })
    
    def get_efficient_patterns(self, top_n: int = 10) -> List[Dict[str, Any]]:
        """
        Get most efficient patterns discovered.
        
        Args:
            top_n: Number of top patterns to return
            
        Returns:
            List of efficient patterns
        """
        pattern_scores = []
        
        for pattern_sig, instances in self.efficient_patterns.items():
            total_savings = sum(inst["energy_saved"] for inst in instances)
            avg_savings = total_savings / len(instances)
            
            pattern_scores.append({
                "pattern": pattern_sig,
                "instances": len(instances),
                "total_energy_saved_wh": total_savings,
                "avg_energy_saved_wh": avg_savings,
                "example_path": instances[0]["path"]
            })
        
        # Sort by total savings
        pattern_scores.sort(key=lambda x: x["total_energy_saved_wh"], reverse=True)
        
        return pattern_scores[:top_n]
    
    def get_serendipity_summary(self) -> Dict[str, Any]:
        """Get summary of serendipity discoveries."""
        if not self.serendipity_events:
            return {"status": "no_events"}
        
        avg_savings_pct = sum(e.savings_pct for e in self.serendipity_events) / len(self.serendipity_events)
        
        # Count by type
        type_counts = {}
        for event in self.serendipity_events:
            type_counts[event.event_type] = type_counts.get(event.event_type, 0) + 1
        
        # Find best discovery
        best_event = max(self.serendipity_events, key=lambda e: e.energy_saved_wh)
        
        return {
            "total_events": len(self.serendipity_events),
            "total_energy_saved_wh": self.total_energy_saved,
            "total_rewards_issued": self.total_rewards_issued,
            "avg_savings_pct": avg_savings_pct,
            "events_by_type": type_counts,
            "best_discovery": {
                "description": best_event.description,
                "energy_saved_wh": best_event.energy_saved_wh,
                "savings_pct": best_event.savings_pct,
                "reward": best_event.negawatt_reward
            },
            "efficient_patterns_learned": len(self.efficient_patterns)
        }
    
    def get_agent_rewards(self, agent_id: str) -> Dict[str, Any]:
        """Get rewards for specific agent."""
        agent_rewards = [r for r in self.negawatt_rewards if r.agent_id == agent_id]
        
        if not agent_rewards:
            return {"agent_id": agent_id, "total_rewards": 0}
        
        total_points = sum(r.reward_points for r in agent_rewards)
        total_energy_saved = sum(r.energy_saved_wh for r in agent_rewards)
        
        return {
            "agent_id": agent_id,
            "total_rewards": total_points,
            "total_energy_saved_wh": total_energy_saved,
            "reward_count": len(agent_rewards),
            "recent_rewards": [r.to_dict() for r in agent_rewards[-5:]]
        }
    
    def export_serendipity_log(self, filepath: str):
        """Export serendipity log to JSON file."""
        data = {
            "events": [e.to_dict() for e in self.serendipity_events],
            "rewards": [r.to_dict() for r in self.negawatt_rewards],
            "efficient_patterns": self.get_efficient_patterns(),
            "summary": self.get_serendipity_summary()
        }
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    def _generate_event_id(self, description: str) -> str:
        """Generate unique event ID."""
        hash_input = f"{description}:{datetime.now().timestamp()}"
        return hashlib.md5(hash_input.encode()).hexdigest()[:16]
    
    def _generate_reward_id(self, agent_id: str) -> str:
        """Generate unique reward ID."""
        hash_input = f"{agent_id}:{datetime.now().timestamp()}"
        return hashlib.md5(hash_input.encode()).hexdigest()[:16]
