# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/knowledge_transfer.py
# Complete enhanced file v5.0.0 with all improvements

"""
Enhanced Knowledge Transfer Manager v5.0.0
Complete implementation with incremental capture, knowledge validation,
transfer metrics, knowledge decay, cross-domain transfer, and knowledge graph.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict, deque
import hashlib
import json
import math
import networkx as nx

logger = logging.getLogger(__name__)

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class KnowledgePackage:
    """Enhanced knowledge package with versioning and decay"""
    package_id: str
    source_expert_id: str
    source_generation: int
    created_at: datetime
    version: int = 1
    task_patterns: Dict[str, Any] = field(default_factory=dict)
    successful_strategies: List[Dict] = field(default_factory=list)
    failure_patterns: List[Dict] = field(default_factory=list)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    optimized_parameters: Dict[str, Any] = field(default_factory=dict)
    lessons_learned: List[str] = field(default_factory=list)
    total_experiences: int = 0
    survival_score: float = 0.0
    decay_rate: float = 0.01  # Daily decay rate
    
    # NEW: Incremental capture
    is_incremental: bool = False
    parent_package_id: Optional[str] = None
    capture_sequence: int = 0
    
    # NEW: Transfer metadata
    transfer_count: int = 0
    last_transferred: Optional[datetime] = None
    transfer_success_scores: List[float] = field(default_factory=list)
    average_transfer_improvement: float = 0.0
    
    # NEW: Cross-domain tags
    domain_tags: List[str] = field(default_factory=list)
    cross_domain_applicability: Dict[str, float] = field(default_factory=dict)
    
    @property
    def age_days(self) -> float:
        """Age in days"""
        return (datetime.utcnow() - self.created_at).total_seconds() / 86400
    
    @property
    def recency_weight(self) -> float:
        """Calculate recency weight using exponential decay"""
        return math.exp(-self.decay_rate * self.age_days)
    
    @property
    def effective_score(self) -> float:
        """Effective score combining survival and recency"""
        return self.survival_score * self.recency_weight

@dataclass
class TransferRecord:
    """Record of a knowledge transfer event"""
    transfer_id: str
    source_package_id: str
    target_expert_id: str
    timestamp: datetime
    items_transferred: List[str]
    pre_transfer_performance: Optional[float] = None
    post_transfer_performance: Optional[float] = None
    improvement_percentage: float = 0.0
    validation_tasks: int = 0
    successful_transfer: bool = False
    transfer_confidence: float = 0.5
    notes: str = ""

@dataclass
class IncrementalSnapshot:
    """Incremental knowledge snapshot during expert lifetime"""
    snapshot_id: str
    expert_id: str
    timestamp: datetime
    performance_at_capture: float
    strategies_since_last: List[Dict]
    parameter_changes: Dict[str, Any]
    experience_count: int
    sequence_number: int

@dataclass
class CrossDomainMapping:
    """Mapping between knowledge domains"""
    source_domain: str
    target_domain: str
    transferability_score: float
    common_patterns: List[str]
    successful_transfers: int
    total_attempts: int
    last_updated: datetime

# ============================================================================
# Enhanced Knowledge Transfer Manager
# ============================================================================

class KnowledgeTransferManager:
    """
    Enhanced Knowledge Transfer Manager v5.0.0
    
    Complete implementation with:
    - Incremental knowledge capture during expert lifetime
    - Knowledge validation and transfer effectiveness metrics
    - Knowledge decay and recency weighting
    - Cross-domain knowledge transfer
    - Knowledge graph for relationship modeling
    - Competency-based curriculum adaptation
    """
    
    def __init__(self):
        # Knowledge storage
        self.knowledge_bank: Dict[str, KnowledgePackage] = {}
        self.incremental_snapshots: Dict[str, List[IncrementalSnapshot]] = defaultdict(list)
        
        # Transfer history
        self.transfer_history: List[TransferRecord] = []
        
        # Cross-domain mappings
        self.cross_domain_mappings: Dict[Tuple[str, str], CrossDomainMapping] = {}
        
        # Knowledge graph
        self.knowledge_graph = nx.DiGraph()
        
        # Experience replay buffers
        self.experience_buffer: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        
        # Curriculum templates
        self.curriculum_templates: Dict[str, List[Dict]] = {}
        
        # Transfer effectiveness tracking
        self.transfer_effectiveness: Dict[str, List[float]] = defaultdict(list)
        
        # Performance milestones for incremental capture
        self.capture_milestones = [100, 500, 1000, 5000, 10000]
        
        # Validation configuration
        self.validation_enabled = True
        self.validation_task_count = 10
        self.min_improvement_threshold = 0.05  # 5% improvement required
        
        # Decay configuration
        self.decay_enabled = True
        self.default_decay_rate = 0.01
        
        # Start background tasks
        asyncio.create_task(self._knowledge_maintenance_loop())
        
        logger.info("Enhanced Knowledge Transfer Manager v5.0.0 initialized")
    
    # ========================================================================
    # Incremental Knowledge Capture (NEW)
    # ========================================================================
    
    def capture_incremental(self, expert_id: str, expert_instance: Any,
                           force_capture: bool = False) -> Optional[IncrementalSnapshot]:
        """
        Capture incremental knowledge snapshot during expert lifetime.
        
        Triggered by performance milestones or manually.
        """
        # Get current experience count
        total_experiences = self._get_total_experiences(expert_id)
        
        # Check if milestone reached
        should_capture = force_capture or any(
            total_experiences >= milestone and 
            not self._milestone_already_captured(expert_id, milestone)
            for milestone in self.capture_milestones
        )
        
        if not should_capture:
            return None
        
        # Get performance metrics
        performance = 0.5
        if hasattr(expert_instance, 'get_expert_statistics'):
            stats = expert_instance.get_expert_statistics()
            performance = stats.get('success_rate', stats.get('efficiency_rating', 0.5))
        
        # Get strategies since last snapshot
        last_snapshot = self._get_last_snapshot(expert_id)
        new_strategies = self._get_strategies_since(expert_instance, last_snapshot)
        parameter_changes = self._get_parameter_changes(expert_instance, last_snapshot)
        
        # Create snapshot
        sequence = len(self.incremental_snapshots[expert_id]) + 1
        snapshot = IncrementalSnapshot(
            snapshot_id=f"snap_{expert_id}_{sequence}_{datetime.utcnow().timestamp()}",
            expert_id=expert_id,
            timestamp=datetime.utcnow(),
            performance_at_capture=performance,
            strategies_since_last=new_strategies,
            parameter_changes=parameter_changes,
            experience_count=total_experiences,
            sequence_number=sequence
        )
        
        self.incremental_snapshots[expert_id].append(snapshot)
        
        # Update or create knowledge package
        self._update_knowledge_from_snapshot(expert_id, snapshot)
        
        logger.info(
            f"Incremental capture for {expert_id}: "
            f"sequence={sequence}, experiences={total_experiences}, "
            f"performance={performance:.2f}"
        )
        
        return snapshot
    
    def _milestone_already_captured(self, expert_id: str, milestone: int) -> bool:
        """Check if a milestone has already been captured"""
        for snapshot in self.incremental_snapshots.get(expert_id, []):
            if snapshot.experience_count >= milestone:
                return True
        return False
    
    def _get_last_snapshot(self, expert_id: str) -> Optional[IncrementalSnapshot]:
        """Get the last incremental snapshot for an expert"""
        snapshots = self.incremental_snapshots.get(expert_id, [])
        return snapshots[-1] if snapshots else None
    
    def _get_strategies_since(self, expert_instance: Any, 
                             last_snapshot: Optional[IncrementalSnapshot]) -> List[Dict]:
        """Get new strategies since last snapshot"""
        if not hasattr(expert_instance, 'optimization_history'):
            return []
        
        history = list(expert_instance.optimization_history)
        
        if last_snapshot:
            # Filter strategies after last snapshot
            return [
                h for h in history
                if h.get('timestamp', datetime.min) > last_snapshot.timestamp
            ]
        
        return history[-100:] if history else []
    
    def _get_parameter_changes(self, expert_instance: Any,
                              last_snapshot: Optional[IncrementalSnapshot]) -> Dict[str, Any]:
        """Get parameter changes since last snapshot"""
        if not hasattr(expert_instance, 'adaptive_thresholds'):
            return {}
        
        current = expert_instance.adaptive_thresholds
        
        if last_snapshot and last_snapshot.parameter_changes:
            # Calculate deltas
            changes = {}
            for key, value in current.items():
                old_value = last_snapshot.parameter_changes.get(key, value)
                if abs(value - old_value) > 0.01:
                    changes[key] = {'old': old_value, 'new': value, 'delta': value - old_value}
            return changes
        
        return {k: {'old': v, 'new': v, 'delta': 0} for k, v in current.items()}
    
    def _update_knowledge_from_snapshot(self, expert_id: str, snapshot: IncrementalSnapshot):
        """Update knowledge package from incremental snapshot"""
        # Find or create package
        package = self._find_latest_package(expert_id)
        
        if not package:
            package = KnowledgePackage(
                package_id=f"kp_{expert_id}_{datetime.utcnow().timestamp()}",
                source_expert_id=expert_id,
                source_generation=self._get_generation(expert_id),
                created_at=datetime.utcnow(),
                version=1,
                is_incremental=True
            )
        else:
            # Create new version
            package = KnowledgePackage(
                package_id=f"kp_{expert_id}_v{package.version + 1}_{datetime.utcnow().timestamp()}",
                source_expert_id=expert_id,
                source_generation=package.source_generation,
                created_at=datetime.utcnow(),
                version=package.version + 1,
                parent_package_id=package.package_id,
                is_incremental=True,
                capture_sequence=snapshot.sequence_number,
                successful_strategies=package.successful_strategies.copy(),
                failure_patterns=package.failure_patterns.copy(),
                optimized_parameters=package.optimized_parameters.copy(),
                task_patterns=package.task_patterns.copy(),
                lessons_learned=package.lessons_learned.copy(),
                total_experiences=snapshot.experience_count,
                survival_score=package.survival_score,
                domain_tags=package.domain_tags.copy(),
                cross_domain_applicability=package.cross_domain_applicability.copy()
            )
        
        # Update with new strategies
        for strategy in snapshot.strategies_since_last:
            if strategy.get('success', False):
                package.successful_strategies.append(strategy)
            else:
                package.failure_patterns.append(strategy)
        
        # Update parameters
        for key, change in snapshot.parameter_changes.items():
            package.optimized_parameters[key] = change['new']
        
        # Update survival score
        package.survival_score = self._calculate_survival_score(package)
        package.performance_metrics['success_rate'] = snapshot.performance_at_capture
        
        # Store
        self.knowledge_bank[package.package_id] = package
        
        # Update knowledge graph
        self._update_knowledge_graph(package)
    
    def _find_latest_package(self, expert_id: str) -> Optional[KnowledgePackage]:
        """Find the latest knowledge package for an expert"""
        packages = [
            pkg for pkg in self.knowledge_bank.values()
            if pkg.source_expert_id == expert_id
        ]
        
        if not packages:
            return None
        
        # Return latest version
        return max(packages, key=lambda p: p.version)
    
    # ========================================================================
    # Original Knowledge Capture (Enhanced)
    # ========================================================================
    
    def capture_knowledge(self, expert_id: str, expert_instance: Any,
                         domain_tags: Optional[List[str]] = None) -> KnowledgePackage:
        """Capture comprehensive knowledge from expert (enhanced with domain tags)"""
        package = KnowledgePackage(
            package_id=f"kp_{expert_id}_{datetime.utcnow().timestamp()}",
            source_expert_id=expert_id,
            source_generation=self._get_generation(expert_id),
            created_at=datetime.utcnow(),
            total_experiences=self._get_total_experiences(expert_id),
            domain_tags=domain_tags or self._infer_domain_tags(expert_id)
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
                'success_rate': stats.get('success_rate', stats.get('efficiency_rating', 0.5)),
                'avg_latency': stats.get('avg_latency_ms', stats.get('average_latency_ms', 100)),
                'carbon_efficiency': stats.get('carbon_efficiency', 0.5),
                'token_efficiency': stats.get('token_efficiency', stats.get('efficiency_rating', 0.5))
            }
        
        # Capture optimized parameters
        if hasattr(expert_instance, 'adaptive_thresholds'):
            package.optimized_parameters = expert_instance.adaptive_thresholds.copy()
        
        # Generate lessons
        package.lessons_learned = self._generate_lessons(package)
        package.survival_score = self._calculate_survival_score(package)
        
        # Store
        self.knowledge_bank[package.package_id] = package
        
        # Update knowledge graph
        self._update_knowledge_graph(package)
        
        logger.info(
            f"Captured knowledge from {expert_id}: "
            f"{package.total_experiences} experiences, "
            f"score={package.survival_score:.2f}, "
            f"tags={package.domain_tags}"
        )
        
        return package
    
    # ========================================================================
    # Knowledge Validation (NEW)
    # ========================================================================
    
    def validate_knowledge(self, package_id: str, test_tasks: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        Validate that knowledge package contains accurate and useful information.
        
        Returns validation results with confidence score.
        """
        if package_id not in self.knowledge_bank:
            return {'valid': False, 'reason': 'Package not found'}
        
        package = self.knowledge_bank[package_id]
        validation_results = {
            'package_id': package_id,
            'valid': True,
            'issues': [],
            'warnings': [],
            'confidence': 1.0,
            'checks': {}
        }
        
        # Check 1: Data sufficiency
        if package.total_experiences < 50:
            validation_results['warnings'].append(f"Low experience count: {package.total_experiences}")
            validation_results['confidence'] *= 0.7
        
        # Check 2: Strategy diversity
        unique_strategies = len(set(
            s.get('strategy', '') for s in package.successful_strategies
        ))
        if unique_strategies < 3:
            validation_results['warnings'].append(f"Low strategy diversity: {unique_strategies}")
            validation_results['confidence'] *= 0.8
        
        # Check 3: Success rate sanity
        success_rate = package.performance_metrics.get('success_rate', 0)
        if success_rate < 0.5:
            validation_results['issues'].append(f"Low success rate: {success_rate:.2f}")
            validation_results['valid'] = False
            validation_results['confidence'] *= 0.5
        elif success_rate > 0.99:
            validation_results['warnings'].append("Suspiciously high success rate - possible overfitting")
            validation_results['confidence'] *= 0.9
        
        # Check 4: Parameter consistency
        if package.optimized_parameters:
            param_values = list(package.optimized_parameters.values())
            if param_values:
                param_range = max(param_values) - min(param_values)
                if param_range > 10:
                    validation_results['warnings'].append("High parameter variance")
                    validation_results['confidence'] *= 0.85
        
        # Check 5: Age decay
        if package.age_days > 30:
            validation_results['warnings'].append(f"Knowledge is {package.age_days:.0f} days old")
            validation_results['confidence'] *= max(0.3, 1.0 - package.age_days * 0.01)
        
        # Check 6: Cross-validation with test tasks
        if test_tasks and self.validation_enabled:
            cv_result = self._cross_validate(package, test_tasks)
            validation_results['checks']['cross_validation'] = cv_result
            validation_results['confidence'] *= cv_result.get('accuracy', 0.5)
        
        validation_results['checks']['timestamp'] = datetime.utcnow().isoformat()
        
        return validation_results
    
    def _cross_validate(self, package: KnowledgePackage, 
                       test_tasks: List[Dict]) -> Dict[str, Any]:
        """Cross-validate knowledge against test tasks"""
        if not test_tasks:
            return {'accuracy': 0.5, 'reason': 'No test tasks'}
        
        successful_predictions = 0
        total_predictions = 0
        
        for task in test_tasks[:self.validation_task_count]:
            # Check if any successful strategy matches this task
            task_type = task.get('task_type', '')
            task_complexity = task.get('complexity', 0.5)
            
            matching_strategies = [
                s for s in package.successful_strategies
                if s.get('strategy', '') == task_type
            ]
            
            if matching_strategies:
                # Predict success based on historical reward
                avg_reward = np.mean([s.get('reward', 0.5) for s in matching_strategies])
                predicted_success = avg_reward > 0.5
                actual_success = task.get('expected_success', True)
                
                if predicted_success == actual_success:
                    successful_predictions += 1
                total_predictions += 1
        
        accuracy = successful_predictions / max(total_predictions, 1)
        
        return {
            'accuracy': accuracy,
            'predictions': total_predictions,
            'correct': successful_predictions
        }
    
    # ========================================================================
    # Enhanced Knowledge Transfer with Validation (NEW)
    # ========================================================================
    
    def transfer_knowledge(self, source_package_id: str, target_expert: Any,
                          validate: bool = True,
                          test_tasks: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        Enhanced knowledge transfer with validation and metrics.
        
        Returns transfer results with effectiveness scores.
        """
        if source_package_id not in self.knowledge_bank:
            return {'success': False, 'reason': 'Package not found'}
        
        package = self.knowledge_bank[source_package_id]
        
        # Validate before transfer
        if validate:
            validation = self.validate_knowledge(source_package_id, test_tasks)
            if not validation['valid']:
                return {
                    'success': False,
                    'reason': 'Knowledge validation failed',
                    'validation': validation
                }
        
        # Capture pre-transfer performance
        pre_performance = self._measure_performance(target_expert)
        
        transfer_results = {'transferred_items': [], 'failed_items': [], 'validation': None}
        
        # Transfer adaptive thresholds (with decay weighting)
        if package.optimized_parameters:
            if hasattr(target_expert, 'adaptive_thresholds'):
                for key, value in package.optimized_parameters.items():
                    if key in target_expert.adaptive_thresholds:
                        # Blend with decay-weighted value
                        effective_value = value * package.recency_weight
                        target_expert.adaptive_thresholds[key] = (
                            effective_value * 0.7 + target_expert.adaptive_thresholds[key] * 0.3
                        )
                        transfer_results['transferred_items'].append(f'threshold:{key}')
        
        # Transfer curriculum
        if package.successful_strategies:
            curriculum = self._create_adaptive_curriculum(package, target_expert)
            if hasattr(target_expert, 'set_curriculum'):
                target_expert.set_curriculum(curriculum)
                transfer_results['transferred_items'].append('curriculum')
        
        # Transfer experiences
        if hasattr(target_expert, 'memory') and package.source_expert_id in self.experience_buffer:
            for exp in list(self.experience_buffer[package.source_expert_id])[-100:]:
                if hasattr(target_expert, 'memory'):
                    target_expert.memory.append(exp)
            transfer_results['transferred_items'].append('experiences')
        
        # Capture post-transfer performance
        post_performance = self._measure_performance(target_expert)
        
        # Calculate improvement
        improvement = 0.0
        if pre_performance is not None and post_performance is not None and pre_performance > 0:
            improvement = (post_performance - pre_performance) / pre_performance
        
        # Create transfer record
        transfer = TransferRecord(
            transfer_id=f"transfer_{datetime.utcnow().timestamp()}_{hashlib.md5(source_package_id.encode()).hexdigest()[:6]}",
            source_package_id=source_package_id,
            target_expert_id=getattr(target_expert, 'expert_id', 'unknown'),
            timestamp=datetime.utcnow(),
            items_transferred=transfer_results['transferred_items'],
            pre_transfer_performance=pre_performance,
            post_transfer_performance=post_performance,
            improvement_percentage=improvement * 100,
            validation_tasks=len(test_tasks) if test_tasks else 0,
            successful_transfer=improvement > self.min_improvement_threshold,
            transfer_confidence=self._calculate_transfer_confidence(package, improvement)
        )
        
        self.transfer_history.append(transfer)
        
        # Update package transfer metadata
        package.transfer_count += 1
        package.last_transferred = datetime.utcnow()
        package.transfer_success_scores.append(1.0 if transfer.successful_transfer else 0.0)
        package.average_transfer_improvement = (
            package.average_transfer_improvement * (package.transfer_count - 1) + improvement
        ) / package.transfer_count
        
        # Update cross-domain mapping
        if hasattr(target_expert, 'expert_id'):
            source_domain = self._infer_domain(package.source_expert_id)
            target_domain = self._infer_domain(getattr(target_expert, 'expert_id', ''))
            self._update_cross_domain_mapping(source_domain, target_domain, transfer.successful_transfer)
        
        # Update knowledge graph
        self.knowledge_graph.add_edge(
            package.package_id,
            getattr(target_expert, 'expert_id', 'unknown'),
            transfer_id=transfer.transfer_id,
            improvement=improvement
        )
        
        logger.info(
            f"Knowledge transfer: {source_package_id} → {getattr(target_expert, 'expert_id', 'unknown')}: "
            f"{len(transfer_results['transferred_items'])} items, "
            f"improvement={improvement:.1%}"
        )
        
        return {
            'success': True,
            'transfer_id': transfer.transfer_id,
            'items_transferred': transfer_results['transferred_items'],
            'improvement_percentage': improvement * 100,
            'successful_transfer': transfer.successful_transfer,
            'confidence': transfer.transfer_confidence
        }
    
    def _measure_performance(self, expert: Any) -> Optional[float]:
        """Measure expert performance for before/after comparison"""
        if hasattr(expert, 'get_expert_statistics'):
            stats = expert.get_expert_statistics()
            return stats.get('success_rate', stats.get('efficiency_rating', None))
        
        if hasattr(expert, 'success_rate'):
            return expert.success_rate
        
        if hasattr(expert, 'efficiency_score'):
            return expert.efficiency_score
        
        return None
    
    def _calculate_transfer_confidence(self, package: KnowledgePackage, 
                                      improvement: float) -> float:
        """Calculate confidence in transfer quality"""
        confidence = 0.5
        
        # Factor 1: Package survival score
        confidence += package.survival_score * 0.2
        
        # Factor 2: Historical transfer success
        if package.transfer_success_scores:
            avg_success = np.mean(package.transfer_success_scores)
            confidence += avg_success * 0.2
        
        # Factor 3: Recency
        confidence += package.recency_weight * 0.1
        
        # Factor 4: Improvement magnitude
        if improvement > 0.1:
            confidence += 0.1
        
        return min(0.95, confidence)
    
    # ========================================================================
    # Cross-Domain Transfer (NEW)
    # ========================================================================
    
    def find_cross_domain_knowledge(self, target_domain: str, 
                                   min_transferability: float = 0.3) -> List[Dict[str, Any]]:
        """
        Find knowledge packages from other domains that may be transferable.
        
        Returns list of candidate packages with transferability scores.
        """
        candidates = []
        
        for package_id, package in self.knowledge_bank.items():
            source_domain = self._infer_domain(package.source_expert_id)
            
            if source_domain == target_domain:
                continue  # Same domain, not cross-domain
            
            # Calculate transferability
            transferability = self._calculate_transferability(
                source_domain, target_domain, package
            )
            
            if transferability >= min_transferability:
                candidates.append({
                    'package_id': package_id,
                    'source_domain': source_domain,
                    'target_domain': target_domain,
                    'transferability': transferability,
                    'survival_score': package.survival_score,
                    'recency_weight': package.recency_weight,
                    'effective_score': package.effective_score * transferability,
                    'common_patterns': self._find_common_patterns(package, target_domain)
                })
        
        # Sort by effective score
        candidates.sort(key=lambda c: c['effective_score'], reverse=True)
        
        return candidates[:10]
    
    def _calculate_transferability(self, source_domain: str, target_domain: str,
                                  package: KnowledgePackage) -> float:
        """Calculate how transferable knowledge is between domains"""
        # Check existing mapping
        key = (source_domain, target_domain)
        if key in self.cross_domain_mappings:
            mapping = self.cross_domain_mappings[key]
            return mapping.transferability_score
        
        # Calculate from package cross-domain data
        if target_domain in package.cross_domain_applicability:
            return package.cross_domain_applicability[target_domain]
        
        # Default based on domain similarity
        domain_similarities = {
            ('energy', 'data'): 0.6,
            ('data', 'energy'): 0.5,
            ('data', 'iot'): 0.7,
            ('iot', 'data'): 0.5,
            ('energy', 'helium'): 0.8,
            ('helium', 'energy'): 0.7
        }
        
        return domain_similarities.get((source_domain, target_domain), 0.2)
    
    def _find_common_patterns(self, package: KnowledgePackage, 
                             target_domain: str) -> List[str]:
        """Find patterns common between domains"""
        common = []
        
        # Check for resource optimization patterns (universal)
        if any('carbon' in s.get('strategy', '').lower() 
               for s in package.successful_strategies):
            common.append('carbon_optimization')
        
        if any('token' in s.get('strategy', '').lower() 
               for s in package.successful_strategies):
            common.append('token_efficiency')
        
        if any('latency' in s.get('strategy', '').lower() 
               for s in package.successful_strategies):
            common.append('latency_optimization')
        
        return common
    
    def _update_cross_domain_mapping(self, source_domain: str, target_domain: str,
                                    successful: bool):
        """Update cross-domain transfer mapping"""
        key = (source_domain, target_domain)
        
        if key not in self.cross_domain_mappings:
            self.cross_domain_mappings[key] = CrossDomainMapping(
                source_domain=source_domain,
                target_domain=target_domain,
                transferability_score=0.3,
                common_patterns=[],
                successful_transfers=0,
                total_attempts=0,
                last_updated=datetime.utcnow()
            )
        
        mapping = self.cross_domain_mappings[key]
        mapping.total_attempts += 1
        if successful:
            mapping.successful_transfers += 1
        
        # Update transferability score
        mapping.transferability_score = (
            mapping.successful_transfers / max(mapping.total_attempts, 1)
        )
        mapping.last_updated = datetime.utcnow()
    
    # ========================================================================
    # Knowledge Graph (NEW)
    # ========================================================================
    
    def _update_knowledge_graph(self, package: KnowledgePackage):
        """Update knowledge graph with new package"""
        # Add package node
        self.knowledge_graph.add_node(
            package.package_id,
            type='knowledge_package',
            expert_id=package.source_expert_id,
            survival_score=package.survival_score,
            version=package.version,
            domain_tags=package.domain_tags
        )
        
        # Connect to parent package
        if package.parent_package_id:
            self.knowledge_graph.add_edge(
                package.parent_package_id,
                package.package_id,
                relationship='evolved_from'
            )
        
        # Connect to expert
        self.knowledge_graph.add_edge(
            package.package_id,
            package.source_expert_id,
            relationship='captured_from'
        )
    
    def get_knowledge_graph_stats(self) -> Dict[str, Any]:
        """Get knowledge graph statistics"""
        return {
            'nodes': self.knowledge_graph.number_of_nodes(),
            'edges': self.knowledge_graph.number_of_edges(),
            'packages': sum(1 for n, d in self.knowledge_graph.nodes(data=True) 
                          if d.get('type') == 'knowledge_package'),
            'connections': sum(1 for n, d in self.knowledge_graph.nodes(data=True) 
                             if d.get('type') != 'knowledge_package'),
            'cross_domain_edges': len([
                (u, v) for u, v, d in self.knowledge_graph.edges(data=True)
                if d.get('relationship') == 'cross_domain_transfer'
            ])
        }
    
    def find_related_knowledge(self, package_id: str, depth: int = 2) -> List[Dict[str, Any]]:
        """Find related knowledge packages using graph traversal"""
        if package_id not in self.knowledge_graph:
            return []
        
        related = []
        visited = set()
        
        def traverse(node, current_depth):
            if current_depth > depth or node in visited:
                return
            visited.add(node)
            
            for neighbor in self.knowledge_graph.neighbors(node):
                if neighbor in self.knowledge_bank:
                    pkg = self.knowledge_bank[neighbor]
                    related.append({
                        'package_id': neighbor,
                        'expert_id': pkg.source_expert_id,
                        'survival_score': pkg.survival_score,
                        'relationship': 'related',
                        'depth': current_depth
                    })
                traverse(neighbor, current_depth + 1)
        
        traverse(package_id, 0)
        
        # Sort by depth then survival score
        related.sort(key=lambda r: (r['depth'], -r['survival_score']))
        
        return related[:20]
    
    # ========================================================================
    # Adaptive Curriculum (Enhanced)
    # ========================================================================
    
    def _create_adaptive_curriculum(self, package: KnowledgePackage,
                                   target_expert: Any) -> List[Dict]:
        """Create competency-based adaptive curriculum"""
        # Assess current competency
        current_level = self._assess_competency(target_expert)
        
        curriculum = []
        
        # Phase 1: Foundation (if needed)
        if current_level < 0.3:
            curriculum.append({
                'phase': 'foundation',
                'tasks': package.task_patterns.get('simple_tasks', [])[:5],
                'difficulty': 0.2,
                'min_pass_rate': 0.7,
                'source': package.source_expert_id
            })
        
        # Phase 2: Basic
        curriculum.append({
            'phase': 'basic',
            'tasks': package.task_patterns.get('simple_tasks', [])[:10],
            'difficulty': 0.4,
            'min_pass_rate': 0.75,
            'source': package.source_expert_id
        })
        
        # Phase 3: Intermediate
        curriculum.append({
            'phase': 'intermediate',
            'tasks': package.task_patterns.get('medium_tasks', [])[:15],
            'difficulty': 0.6,
            'min_pass_rate': 0.8,
            'source': package.source_expert_id
        })
        
        # Phase 4: Advanced
        if current_level > 0.5:
            curriculum.append({
                'phase': 'advanced',
                'tasks': package.task_patterns.get('complex_tasks', [])[:20],
                'difficulty': 0.85,
                'min_pass_rate': 0.85,
                'source': package.source_expert_id
            })
        
        return curriculum
    
    def _assess_competency(self, expert: Any) -> float:
        """Assess current competency level of an expert"""
        if hasattr(expert, 'success_rate'):
            return expert.success_rate
        
        if hasattr(expert, 'health_score'):
            return expert.health_score
        
        if hasattr(expert, 'efficiency_score'):
            return expert.efficiency_score
        
        return 0.3  # Default beginner
    
    # ========================================================================
    # Utility Methods
    # ========================================================================
    
    def store_experience(self, expert_id: str, experience: Dict[str, Any]):
        """Store experience for future knowledge transfer"""
        self.experience_buffer[expert_id].append(experience)
    
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
            {'strategy': h.get('strategy', 'unknown'), 'conditions': h.get('conditions', {}),
             'reward': h.get('reward', 0)}
            for h in history[-200:]
            if h.get('success', False) and h.get('reward', 0) > 0.7
        ]
    
    def _extract_failure_patterns(self, history: List) -> List[Dict]:
        """Extract patterns that led to failures"""
        return [
            {'strategy': h.get('strategy', 'unknown'), 'conditions': h.get('conditions', {}),
             'reason': h.get('error', 'unknown')}
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
        reasons = defaultdict(int)
        for f in failures:
            reason = f.get('reason', 'unknown')
            reasons[reason] += 1
        return max(reasons, key=reasons.get) if reasons else 'unknown'
    
    def _calculate_survival_score(self, package: KnowledgePackage) -> float:
        """Calculate how valuable this knowledge is for survival"""
        score = 0.0
        score += package.performance_metrics.get('success_rate', 0.5) * 0.35
        score += package.performance_metrics.get('token_efficiency', 0.5) * 0.30
        score += package.performance_metrics.get('carbon_efficiency', 0.5) * 0.20
        score += min(1.0, package.total_experiences / 1000) * 0.15
        return score
    
    def _infer_domain_tags(self, expert_id: str) -> List[str]:
        """Infer domain tags from expert ID"""
        domain_map = {
            'energy': ['energy_optimization', 'renewable', 'power_management'],
            'data': ['data_processing', 'compression', 'streaming'],
            'iot': ['edge_computing', 'mesh_networking', 'sensor_fusion'],
            'quantum': ['quantum_computing', 'optimization', 'error_correction'],
            'helium': ['resource_management', 'cooling', 'conservation']
        }
        
        for key, tags in domain_map.items():
            if key in expert_id.lower():
                return tags
        
        return ['general']
    
    def _infer_domain(self, expert_id: str) -> str:
        """Infer primary domain from expert ID"""
        domains = ['energy', 'data', 'iot', 'quantum', 'helium']
        for domain in domains:
            if domain in expert_id.lower():
                return domain
        return 'general'
    
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
    
    # ========================================================================
    # Background Maintenance
    # ========================================================================
    
    async def _knowledge_maintenance_loop(self):
        """Background knowledge maintenance"""
        while True:
            try:
                # Update decay on all packages
                if self.decay_enabled:
                    for package in self.knowledge_bank.values():
                        # Recalculate effective scores
                        package.survival_score = self._calculate_survival_score(package)
                
                # Clean up old incremental snapshots (keep last 10)
                for expert_id in list(self.incremental_snapshots.keys()):
                    snapshots = self.incremental_snapshots[expert_id]
                    if len(snapshots) > 10:
                        self.incremental_snapshots[expert_id] = snapshots[-10:]
                
                await asyncio.sleep(3600)  # Every hour
                
            except Exception as e:
                logger.error(f"Knowledge maintenance error: {str(e)}")
                await asyncio.sleep(3600)
    
    # ========================================================================
    # Statistics and Reporting
    # ========================================================================
    
    def get_knowledge_summary(self) -> Dict[str, Any]:
        """Get comprehensive knowledge bank summary"""
        packages = list(self.knowledge_bank.values())
        
        return {
            'total_packages': len(packages),
            'total_transfers': len(self.transfer_history),
            'incremental_snapshots': sum(len(s) for s in self.incremental_snapshots.values()),
            'avg_survival_score': np.mean([p.survival_score for p in packages]) if packages else 0,
            'avg_effective_score': np.mean([p.effective_score for p in packages]) if packages else 0,
            'total_experiences': sum(p.total_experiences for p in packages),
            'cross_domain_mappings': len(self.cross_domain_mappings),
            'knowledge_graph': self.get_knowledge_graph_stats(),
            'transfer_success_rate': (
                sum(1 for t in self.transfer_history if t.successful_transfer) / 
                max(len(self.transfer_history), 1)
            ),
            'avg_transfer_improvement': np.mean([t.improvement_percentage for t in self.transfer_history]) 
                if self.transfer_history else 0,
            'top_packages': [
                {
                    'package_id': p.package_id,
                    'expert_id': p.source_expert_id,
                    'survival_score': p.survival_score,
                    'effective_score': p.effective_score,
                    'experiences': p.total_experiences,
                    'transfers': p.transfer_count,
                    'version': p.version,
                    'domain_tags': p.domain_tags,
                    'age_days': p.age_days
                }
                for p in sorted(packages, key=lambda p: p.effective_score, reverse=True)[:10]
            ]
        }
    
    def get_transfer_report(self) -> Dict[str, Any]:
        """Get transfer effectiveness report"""
        recent = self.transfer_history[-50:]
        
        if not recent:
            return {'status': 'No transfers recorded'}
        
        successful = [t for t in recent if t.successful_transfer]
        
        return {
            'total_transfers': len(self.transfer_history),
            'recent_transfers': len(recent),
            'success_rate': len(successful) / max(len(recent), 1),
            'avg_improvement': np.mean([t.improvement_percentage for t in recent]),
            'avg_confidence': np.mean([t.transfer_confidence for t in recent]),
            'best_improvement': max([t.improvement_percentage for t in recent]) if recent else 0,
            'recommendations': self._generate_transfer_recommendations(recent)
        }
    
    def _generate_transfer_recommendations(self, transfers: List[TransferRecord]) -> List[str]:
        """Generate transfer optimization recommendations"""
        recommendations = []
        
        success_rate = sum(1 for t in transfers if t.successful_transfer) / max(len(transfers), 1)
        
        if success_rate < 0.5:
            recommendations.append("Low transfer success rate. Increase validation task count.")
        
        avg_improvement = np.mean([t.improvement_percentage for t in transfers])
        if avg_improvement < 5:
            recommendations.append("Low average improvement. Consider more selective knowledge transfer.")
        
        if not recommendations:
            recommendations.append("Knowledge transfer is performing well.")
        
        return recommendations
    
    def get_cross_domain_report(self) -> Dict[str, Any]:
        """Get cross-domain transfer report"""
        return {
            'mappings': [
                {
                    'source': mapping.source_domain,
                    'target': mapping.target_domain,
                    'transferability': mapping.transferability_score,
                    'success_rate': mapping.successful_transfers / max(mapping.total_attempts, 1),
                    'attempts': mapping.total_attempts
                }
                for mapping in self.cross_domain_mappings.values()
            ],
            'recommendations': [
                f"High transferability: {m.source_domain} → {m.target_domain}"
                for m in self.cross_domain_mappings.values()
                if m.transferability_score > 0.5
            ]
        }
