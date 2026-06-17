# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/knowledge_transfer.py

"""
Intergenerational Knowledge Transfer for Green Agent
Preserves knowledge when experts are deprecated and replaced.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
import numpy as np
from collections import deque
import hashlib
import json

logger = logging.getLogger(__name__)

@dataclass
class KnowledgePackage:
    """Distilled knowledge from a deprecated expert"""
    package_id: str
    source_expert_id: str
    source_generation: int
    created_at: datetime
    task_patterns: Dict[str, Any] = field(default_factory=dict)
    successful_strategies: List[Dict] = field(default_factory=list)
    failure_patterns: List[Dict] = field(default_factory=list)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    optimized_parameters: Dict[str, Any] = field(default_factory=dict)
    lessons_learned: List[str] = field(default_factory=list)
    total_experiences: int = 0
    survival_score: float = 0.0

class KnowledgeTransferManager:
    """
    Manages knowledge transfer between expert generations.
    
    Preserves institutional knowledge when experts evolve.
    """
    
    def __init__(self):
        self.knowledge_bank: Dict[str, KnowledgePackage] = {}
        self.transfer_history: List[Dict] = []
        self.curriculum_templates: Dict[str, List[Dict]] = {}
        
        # Experience replay buffer
        self.experience_buffer: Dict[str, deque] = {}
        self.max_buffer_size = 10000
        
        logger.info("Knowledge Transfer Manager initialized")
    
    def capture_knowledge(self, expert_id: str, expert_instance: Any) -> KnowledgePackage:
        """Capture knowledge from expert before deprecation"""
        package = KnowledgePackage(
            package_id=f"kp_{expert_id}_{datetime.utcnow().timestamp()}",
            source_expert_id=expert_id,
            source_generation=self._get_generation(expert_id),
            created_at=datetime.utcnow(),
            total_experiences=self._get_total_experiences(expert_id)
        )
        
        # Capture task patterns
        if hasattr(expert_instance, 'optimization_history'):
            history = list(expert_instance.optimization_history)
            if history:
                package.task_patterns = self._extract_task_patterns(history)
                package.successful_strategies = self._extract_successful_strategies(history)
                package.failure_patterns = self._extract_failure_patterns(history)
        
        # Capture performance metrics
        if hasattr(expert_instance, 'get_expert_statistics'):
            stats = expert_instance.get_expert_statistics()
            package.performance_metrics = {
                'success_rate': stats.get('success_rate', 0.5),
                'avg_latency': stats.get('avg_latency_ms', 100),
                'carbon_efficiency': stats.get('carbon_efficiency', 0.5),
                'token_efficiency': stats.get('token_efficiency', 0.5)
            }
        
        # Capture optimized parameters
        if hasattr(expert_instance, 'adaptive_thresholds'):
            package.optimized_parameters = expert_instance.adaptive_thresholds.copy()
        
        # Generate lessons learned
        package.lessons_learned = self._generate_lessons(package)
        
        # Calculate survival score (how valuable this knowledge is)
        package.survival_score = self._calculate_survival_score(package)
        
        # Store in knowledge bank
        self.knowledge_bank[package.package_id] = package
        
        logger.info(
            f"Captured knowledge from {expert_id}: "
            f"{package.total_experiences} experiences, "
            f"score={package.survival_score:.2f}"
        )
        
        return package
    
    def transfer_knowledge(self, source_package_id: str, 
                          target_expert: Any) -> Dict[str, Any]:
        """Transfer knowledge to a new expert"""
        if source_package_id not in self.knowledge_bank:
            return {'success': False, 'reason': 'Package not found'}
        
        package = self.knowledge_bank[source_package_id]
        transfer_results = {'transferred_items': [], 'failed_items': []}
        
        # Transfer adaptive thresholds
        if package.optimized_parameters:
            if hasattr(target_expert, 'adaptive_thresholds'):
                for key, value in package.optimized_parameters.items():
                    if key in target_expert.adaptive_thresholds:
                        # Blend with existing (70% inherited, 30% fresh)
                        target_expert.adaptive_thresholds[key] = (
                            value * 0.7 + target_expert.adaptive_thresholds[key] * 0.3
                        )
                        transfer_results['transferred_items'].append(f'threshold:{key}')
        
        # Transfer successful strategies as curriculum
        if package.successful_strategies:
            curriculum = self._create_curriculum(package.successful_strategies)
            if hasattr(target_expert, 'set_curriculum'):
                target_expert.set_curriculum(curriculum)
                transfer_results['transferred_items'].append('curriculum')
        
        # Transfer experience buffer
        if hasattr(target_expert, 'experience_buffer') and package.source_expert_id in self.experience_buffer:
            for exp in list(self.experience_buffer[package.source_expert_id])[-100:]:
                if hasattr(target_expert, 'memory'):
                    target_expert.memory.append(exp)
            transfer_results['transferred_items'].append('experiences')
        
        # Record transfer
        self.transfer_history.append({
            'source_package': source_package_id,
            'target_expert': getattr(target_expert, 'expert_id', 'unknown'),
            'timestamp': datetime.utcnow().isoformat(),
            'items_transferred': len(transfer_results['transferred_items'])
        })
        
        logger.info(
            f"Transferred knowledge to {getattr(target_expert, 'expert_id', 'unknown')}: "
            f"{len(transfer_results['transferred_items'])} items"
        )
        
        return transfer_results
    
    def create_curriculum_for_new_expert(self, expert_type: str) -> List[Dict]:
        """Create a training curriculum based on all accumulated knowledge"""
        curriculum = []
        
        # Collect all packages for this expert type
        relevant_packages = [
            pkg for pkg in self.knowledge_bank.values()
            if expert_type in pkg.source_expert_id
        ]
        
        if not relevant_packages:
            return self._default_curriculum()
        
        # Sort by survival score (best knowledge first)
        relevant_packages.sort(key=lambda p: p.survival_score, reverse=True)
        
        # Phase 1: Basic tasks from best expert
        for pkg in relevant_packages[:1]:
            curriculum.append({
                'phase': 'basic',
                'tasks': pkg.task_patterns.get('simple_tasks', [])[:10],
                'difficulty': 0.3,
                'source': pkg.source_expert_id
            })
        
        # Phase 2: Intermediate tasks
        for pkg in relevant_packages[:2]:
            curriculum.append({
                'phase': 'intermediate',
                'tasks': pkg.task_patterns.get('medium_tasks', [])[:15],
                'difficulty': 0.6,
                'source': pkg.source_expert_id
            })
        
        # Phase 3: Advanced tasks
        for pkg in relevant_packages:
            curriculum.append({
                'phase': 'advanced',
                'tasks': pkg.task_patterns.get('complex_tasks', [])[:20],
                'difficulty': 0.9,
                'source': pkg.source_expert_id
            })
        
        return curriculum
    
    def _extract_task_patterns(self, history: List) -> Dict[str, Any]:
        """Extract task patterns from optimization history"""
        patterns = {'simple_tasks': [], 'medium_tasks': [], 'complex_tasks': []}
        for entry in history[-100:]:
            complexity = entry.get('complexity', 0.5)
            if complexity < 0.4:
                patterns['simple_tasks'].append(entry)
            elif complexity < 0.7:
                patterns['medium_tasks'].append(entry)
            else:
                patterns['complex_tasks'].append(entry)
        return patterns
    
    def _extract_successful_strategies(self, history: List) -> List[Dict]:
        """Extract strategies that led to successful outcomes"""
        return [
            {
                'strategy': h.get('strategy', 'unknown'),
                'conditions': h.get('conditions', {}),
                'reward': h.get('reward', 0)
            }
            for h in history[-200:]
            if h.get('success', False) and h.get('reward', 0) > 0.7
        ]
    
    def _extract_failure_patterns(self, history: List) -> List[Dict]:
        """Extract patterns that led to failures"""
        return [
            {
                'strategy': h.get('strategy', 'unknown'),
                'conditions': h.get('conditions', {}),
                'reason': h.get('error', 'unknown')
            }
            for h in history[-200:]
            if not h.get('success', True)
        ]
    
    def _generate_lessons(self, package: KnowledgePackage) -> List[str]:
        """Generate human-readable lessons from captured knowledge"""
        lessons = []
        
        if package.performance_metrics.get('success_rate', 0) > 0.9:
            lessons.append("High success rate achieved through consistent strategy selection")
        
        if len(package.failure_patterns) > 10:
            common_failure = self._most_common_failure(package.failure_patterns)
            lessons.append(f"Most common failure: {common_failure}")
        
        if package.optimized_parameters:
            lessons.append(f"Optimal parameters discovered: {list(package.optimized_parameters.keys())}")
        
        return lessons
    
    def _most_common_failure(self, failures: List[Dict]) -> str:
        """Find most common failure reason"""
        reasons = {}
        for f in failures:
            reason = f.get('reason', 'unknown')
            reasons[reason] = reasons.get(reason, 0) + 1
        return max(reasons, key=reasons.get) if reasons else 'unknown'
    
    def _calculate_survival_score(self, package: KnowledgePackage) -> float:
        """Calculate how valuable this knowledge is for survival"""
        score = 0.0
        score += package.performance_metrics.get('success_rate', 0.5) * 0.4
        score += package.performance_metrics.get('token_efficiency', 0.5) * 0.3
        score += package.performance_metrics.get('carbon_efficiency', 0.5) * 0.2
        score += min(1.0, package.total_experiences / 1000) * 0.1
        return score
    
    def _create_curriculum(self, strategies: List[Dict]) -> List[Dict]:
        """Create training curriculum from strategies"""
        return [
            {'task_type': s['strategy'], 'conditions': s.get('conditions', {}),
             'expected_reward': s.get('reward', 0.5)}
            for s in strategies[:20]
        ]
    
    def _default_curriculum(self) -> List[Dict]:
        """Default curriculum when no knowledge available"""
        return [
            {'phase': 'basic', 'tasks': [], 'difficulty': 0.3},
            {'phase': 'intermediate', 'tasks': [], 'difficulty': 0.6},
            {'phase': 'advanced', 'tasks': [], 'difficulty': 0.9}
        ]
    
    def _get_generation(self, expert_id: str) -> int:
        """Extract generation number from expert ID"""
        try:
            parts = expert_id.split('_')
            for part in parts:
                if part.startswith('v') or part.startswith('gen'):
                    return int(''.join(filter(str.isdigit, part)) or 1)
        except Exception:
            pass
        return 1
    
    def _get_total_experiences(self, expert_id: str) -> int:
        """Get total experiences for expert"""
        return len(self.experience_buffer.get(expert_id, []))
    
    def store_experience(self, expert_id: str, experience: Dict[str, Any]):
        """Store experience for future knowledge transfer"""
        if expert_id not in self.experience_buffer:
            self.experience_buffer[expert_id] = deque(maxlen=self.max_buffer_size)
        self.experience_buffer[expert_id].append(experience)
    
    def get_knowledge_summary(self) -> Dict[str, Any]:
        """Get knowledge bank summary"""
        return {
            'total_packages': len(self.knowledge_bank),
            'total_transfers': len(self.transfer_history),
            'avg_survival_score': np.mean([p.survival_score for p in self.knowledge_bank.values()]) if self.knowledge_bank else 0,
            'top_lessons': self._get_top_lessons(5)
        }
    
    def _get_top_lessons(self, n: int) -> List[str]:
        """Get top lessons across all packages"""
        all_lessons = []
        for pkg in self.knowledge_bank.values():
            all_lessons.extend(pkg.lessons_learned)
        
        # Count frequency and return top n
        from collections import Counter
        return [lesson for lesson, _ in Counter(all_lessons).most_common(n)]
