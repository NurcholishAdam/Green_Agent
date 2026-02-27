# -*- coding: utf-8 -*-
"""
Knowledge Base Integrator & Selective Invocation Policy

Integrates structured knowledge bases and implements intelligent escalation
logic based on confidence, sustainability, and criticality thresholds.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import json


class KnowledgeSourceType(Enum):
    """Types of knowledge sources."""
    ENERGY_STANDARDS = "energy_standards"
    SCIENTIFIC_PAPERS = "scientific_papers"
    CODE_REPOSITORIES = "code_repositories"
    BENCHMARKS = "benchmarks"
    DOCUMENTATION = "documentation"
    REGULATORY = "regulatory"


class EscalationReason(Enum):
    """Reasons for expert escalation."""
    LOW_CONFIDENCE = "low_confidence"
    HIGH_ENERGY = "high_energy"
    CRITICAL_SAFETY = "critical_safety"
    CRITICAL_SECURITY = "critical_security"
    CRITICAL_CORRECTNESS = "critical_correctness"
    DOMAIN_COMPLEXITY = "domain_complexity"
    ETHICAL_CONCERN = "ethical_concern"
    REGULATORY_COMPLIANCE = "regulatory_compliance"


@dataclass
class KnowledgeEntry:
    """Entry from knowledge base."""
    entry_id: str
    source_type: KnowledgeSourceType
    title: str
    content: str
    metadata: Dict[str, Any]
    relevance_score: float
    confidence: float


@dataclass
class EscalationDecision:
    """Decision about whether to escalate to expert."""
    should_escalate: bool
    reasons: List[EscalationReason]
    confidence_score: float
    sustainability_impact: float
    criticality_level: str  # "low", "medium", "high", "critical"
    recommended_expert: str
    estimated_cost_wh: float
    justification: str


class KnowledgeBaseIntegrator:
    """
    Integrates structured knowledge bases.
    
    Features:
    - Multi-source knowledge retrieval
    - Relevance scoring
    - Knowledge caching
    - Update mechanisms
    """
    
    def __init__(
        self,
        knowledge_sources: Dict[KnowledgeSourceType, str],
        enable_caching: bool = True
    ):
        """
        Initialize knowledge base integrator.
        
        Args:
            knowledge_sources: Paths to knowledge sources
            enable_caching: Enable knowledge caching
        """
        self.knowledge_sources = knowledge_sources
        self.enable_caching = enable_caching
        
        # Cache
        self.knowledge_cache: Dict[str, List[KnowledgeEntry]] = {}
        
        # Load knowledge bases
        self.knowledge_bases: Dict[KnowledgeSourceType, Any] = {}
        self._load_knowledge_bases()
        
    def _load_knowledge_bases(self):
        """Load knowledge bases from sources."""
        for source_type, source_path in self.knowledge_sources.items():
            try:
                with open(source_path, 'r') as f:
                    data = json.load(f)
                    self.knowledge_bases[source_type] = data
            except Exception as e:
                print(f"Warning: Failed to load {source_type.value}: {e}")
    
    def query_knowledge(
        self,
        query: str,
        source_types: Optional[List[KnowledgeSourceType]] = None,
        top_k: int = 5
    ) -> List[KnowledgeEntry]:
        """
        Query knowledge bases.
        
        Args:
            query: Query string
            source_types: Specific sources to query (None = all)
            top_k: Number of results to return
            
        Returns:
            List of relevant knowledge entries
        """
        # Check cache
        cache_key = f"{query}:{source_types}"
        if self.enable_caching and cache_key in self.knowledge_cache:
            return self.knowledge_cache[cache_key]
        
        results = []
        
        # Query each source
        sources_to_query = source_types or list(self.knowledge_bases.keys())
        
        for source_type in sources_to_query:
            if source_type not in self.knowledge_bases:
                continue
            
            kb_data = self.knowledge_bases[source_type]
            
            # Simple keyword matching (could be enhanced with embeddings)
            for entry in kb_data.get('entries', []):
                relevance = self._calculate_relevance(query, entry)
                
                if relevance > 0.3:  # Threshold
                    results.append(KnowledgeEntry(
                        entry_id=entry.get('id', ''),
                        source_type=source_type,
                        title=entry.get('title', ''),
                        content=entry.get('content', ''),
                        metadata=entry.get('metadata', {}),
                        relevance_score=relevance,
                        confidence=entry.get('confidence', 0.8)
                    ))
        
        # Sort by relevance
        results.sort(key=lambda x: x.relevance_score, reverse=True)
        results = results[:top_k]
        
        # Cache results
        if self.enable_caching:
            self.knowledge_cache[cache_key] = results
        
        return results
    
    def _calculate_relevance(self, query: str, entry: Dict) -> float:
        """Calculate relevance score (simple keyword matching)."""
        query_words = set(query.lower().split())
        title_words = set(entry.get('title', '').lower().split())
        content_words = set(entry.get('content', '').lower().split())
        
        title_overlap = len(query_words & title_words)
        content_overlap = len(query_words & content_words)
        
        # Weight title matches higher
        score = (title_overlap * 0.7 + content_overlap * 0.3) / max(len(query_words), 1)
        
        return min(score, 1.0)
    
    def add_knowledge_source(
        self,
        source_type: KnowledgeSourceType,
        source_path: str
    ):
        """Add new knowledge source."""
        self.knowledge_sources[source_type] = source_path
        
        try:
            with open(source_path, 'r') as f:
                data = json.load(f)
                self.knowledge_bases[source_type] = data
        except Exception as e:
            print(f"Failed to load {source_type.value}: {e}")


class SelectiveInvocationPolicy:
    """
    Implements selective invocation policy with escalation logic.
    
    Decision Criteria:
    - Confidence threshold: Low agent confidence → escalate
    - Sustainability threshold: High energy task → escalate
    - Criticality threshold: Safety/security/correctness → escalate
    - Domain complexity: Complex domains → escalate
    """
    
    def __init__(
        self,
        confidence_threshold: float = 0.7,
        sustainability_threshold_wh: float = 0.1,
        enable_criticality_check: bool = True,
        enable_sustainability_check: bool = True,
        knowledge_integrator: Optional[KnowledgeBaseIntegrator] = None
    ):
        """
        Initialize selective invocation policy.
        
        Args:
            confidence_threshold: Minimum confidence to avoid escalation
            sustainability_threshold_wh: Maximum energy before escalation
            enable_criticality_check: Enable criticality-based escalation
            enable_sustainability_check: Enable sustainability-based escalation
            knowledge_integrator: Knowledge base for context
        """
        self.confidence_threshold = confidence_threshold
        self.sustainability_threshold = sustainability_threshold_wh
        self.enable_criticality_check = enable_criticality_check
        self.enable_sustainability_check = enable_sustainability_check
        self.knowledge_integrator = knowledge_integrator
        
        # Criticality keywords
        self.critical_keywords = {
            'safety': ['crash', 'segfault', 'memory', 'unsafe', 'critical'],
            'security': ['vulnerability', 'exploit', 'password', 'authentication', 'encryption'],
            'correctness': ['algorithm', 'logic', 'computation', 'accuracy', 'precision']
        }
        
        # Statistics
        self.total_decisions = 0
        self.escalations = 0
        self.escalations_by_reason: Dict[EscalationReason, int] = {}
        
    def decide_escalation(
        self,
        task: str,
        agent_confidence: float,
        estimated_energy_wh: float,
        domain: str,
        context: Optional[Dict[str, Any]] = None
    ) -> EscalationDecision:
        """
        Decide whether to escalate to expert.
        
        Args:
            task: Task description
            agent_confidence: Agent's confidence score
            estimated_energy_wh: Estimated energy consumption
            domain: Task domain
            context: Additional context
            
        Returns:
            Escalation decision
        """
        self.total_decisions += 1
        
        reasons = []
        should_escalate = False
        
        # 1. Confidence check
        if agent_confidence < self.confidence_threshold:
            reasons.append(EscalationReason.LOW_CONFIDENCE)
            should_escalate = True
        
        # 2. Sustainability check
        if self.enable_sustainability_check:
            if estimated_energy_wh > self.sustainability_threshold:
                reasons.append(EscalationReason.HIGH_ENERGY)
                should_escalate = True
        
        # 3. Criticality check
        if self.enable_criticality_check:
            criticality_reasons = self._check_criticality(task)
            reasons.extend(criticality_reasons)
            if criticality_reasons:
                should_escalate = True
        
        # 4. Domain complexity check
        if self._is_complex_domain(domain):
            reasons.append(EscalationReason.DOMAIN_COMPLEXITY)
            should_escalate = True
        
        # 5. Regulatory compliance check
        if self._requires_compliance_check(task):
            reasons.append(EscalationReason.REGULATORY_COMPLIANCE)
            should_escalate = True
        
        # Determine criticality level
        criticality_level = self._determine_criticality_level(reasons, context)
        
        # Recommend expert type
        recommended_expert = self._recommend_expert(reasons, domain)
        
        # Estimate expert cost
        estimated_cost = self._estimate_expert_cost(reasons, criticality_level)
        
        # Build justification
        justification = self._build_justification(
            should_escalate, reasons, agent_confidence,
            estimated_energy_wh, criticality_level
        )
        
        # Update statistics
        if should_escalate:
            self.escalations += 1
            for reason in reasons:
                self.escalations_by_reason[reason] = \
                    self.escalations_by_reason.get(reason, 0) + 1
        
        # Calculate sustainability impact
        sustainability_impact = self._calculate_sustainability_impact(
            estimated_energy_wh, estimated_cost
        )
        
        return EscalationDecision(
            should_escalate=should_escalate,
            reasons=reasons,
            confidence_score=agent_confidence,
            sustainability_impact=sustainability_impact,
            criticality_level=criticality_level,
            recommended_expert=recommended_expert,
            estimated_cost_wh=estimated_cost,
            justification=justification
        )
    
    def _check_criticality(self, task: str) -> List[EscalationReason]:
        """Check if task involves critical concerns."""
        reasons = []
        task_lower = task.lower()
        
        # Check safety
        if any(kw in task_lower for kw in self.critical_keywords['safety']):
            reasons.append(EscalationReason.CRITICAL_SAFETY)
        
        # Check security
        if any(kw in task_lower for kw in self.critical_keywords['security']):
            reasons.append(EscalationReason.CRITICAL_SECURITY)
        
        # Check correctness
        if any(kw in task_lower for kw in self.critical_keywords['correctness']):
            reasons.append(EscalationReason.CRITICAL_CORRECTNESS)
        
        return reasons
    
    def _is_complex_domain(self, domain: str) -> bool:
        """Check if domain is complex."""
        complex_domains = [
            'cryptography', 'quantum', 'distributed_systems',
            'machine_learning', 'medical', 'legal', 'scientific'
        ]
        
        return any(cd in domain.lower() for cd in complex_domains)
    
    def _requires_compliance_check(self, task: str) -> bool:
        """Check if task requires regulatory compliance."""
        compliance_keywords = [
            'medical', 'healthcare', 'financial', 'privacy',
            'gdpr', 'hipaa', 'pci', 'compliance'
        ]
        
        task_lower = task.lower()
        return any(kw in task_lower for kw in compliance_keywords)
    
    def _determine_criticality_level(
        self,
        reasons: List[EscalationReason],
        context: Optional[Dict[str, Any]]
    ) -> str:
        """Determine overall criticality level."""
        critical_reasons = [
            EscalationReason.CRITICAL_SAFETY,
            EscalationReason.CRITICAL_SECURITY,
            EscalationReason.REGULATORY_COMPLIANCE
        ]
        
        if any(r in reasons for r in critical_reasons):
            return "critical"
        
        if EscalationReason.CRITICAL_CORRECTNESS in reasons:
            return "high"
        
        if len(reasons) >= 2:
            return "medium"
        
        return "low"
    
    def _recommend_expert(
        self,
        reasons: List[EscalationReason],
        domain: str
    ) -> str:
        """Recommend expert type based on escalation reasons."""
        # Map reasons to expert types
        if EscalationReason.CRITICAL_SECURITY in reasons:
            return "security_expert"
        
        if EscalationReason.CRITICAL_SAFETY in reasons:
            return "safety_expert"
        
        if EscalationReason.REGULATORY_COMPLIANCE in reasons:
            return "compliance_expert"
        
        if EscalationReason.HIGH_ENERGY in reasons:
            return "sustainability_expert"
        
        # Default to domain expert
        return f"{domain}_expert"
    
    def _estimate_expert_cost(
        self,
        reasons: List[EscalationReason],
        criticality_level: str
    ) -> float:
        """Estimate energy cost of expert consultation."""
        # Base cost
        base_cost = 0.05  # 50 mWh
        
        # Multiply by number of reasons
        cost = base_cost * len(reasons)
        
        # Adjust by criticality
        if criticality_level == "critical":
            cost *= 1.5
        elif criticality_level == "high":
            cost *= 1.3
        
        return cost
    
    def _calculate_sustainability_impact(
        self,
        task_energy: float,
        expert_energy: float
    ) -> float:
        """Calculate sustainability impact of escalation."""
        # Net impact: expert cost - potential savings
        # Assume expert can reduce task energy by 30% on average
        potential_savings = task_energy * 0.3
        net_impact = expert_energy - potential_savings
        
        return net_impact
    
    def _build_justification(
        self,
        should_escalate: bool,
        reasons: List[EscalationReason],
        confidence: float,
        energy: float,
        criticality: str
    ) -> str:
        """Build human-readable justification."""
        if not should_escalate:
            return (
                f"No escalation needed. Agent confidence: {confidence:.2f}, "
                f"Energy: {energy*1000:.1f} mWh, Criticality: {criticality}"
            )
        
        reason_strs = [r.value.replace('_', ' ') for r in reasons]
        reasons_text = ", ".join(reason_strs)
        
        return (
            f"Escalation recommended ({criticality} priority). "
            f"Reasons: {reasons_text}. "
            f"Agent confidence: {confidence:.2f}, "
            f"Energy: {energy*1000:.1f} mWh"
        )
    
    def get_policy_stats(self) -> Dict[str, Any]:
        """Get policy statistics."""
        escalation_rate = (
            self.escalations / self.total_decisions
            if self.total_decisions > 0 else 0
        )
        
        return {
            "total_decisions": self.total_decisions,
            "escalations": self.escalations,
            "escalation_rate": escalation_rate,
            "escalations_by_reason": {
                reason.value: count
                for reason, count in self.escalations_by_reason.items()
            },
            "confidence_threshold": self.confidence_threshold,
            "sustainability_threshold_wh": self.sustainability_threshold
        }
