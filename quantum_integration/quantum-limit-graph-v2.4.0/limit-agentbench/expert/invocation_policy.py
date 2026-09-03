# -*- coding: utf-8 -*-
"""
Knowledge Base Integrator & Selective Invocation Policy (Enhanced)

Integrates structured knowledge bases and implements intelligent escalation
logic based on confidence, sustainability, and criticality thresholds.

Enhancements (enabled via `SelectiveInvocationConfig.use_enhancements`):
  - LIMIT Graph metrics are incorporated into the escalation decision state.
  - MODP (multi‑objective) reward computed from confidence, energy, criticality.
  - RLHF: human feedback score influences the decision via a dedicated teacher.
  - Multi‑Teacher On‑Policy Distillation + MoE: a student model blends
    rule‑based, RLHF, and historical teachers to make the final escalation call.
  - Bio‑inspired Optimisation: evolutionary tuning of the MODP weights.

When enhancements are disabled, the behaviour is identical to the original.
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
from datetime import datetime
import json
import random
import numpy as np
from collections import deque

# ---------------------------------------------------------------------------
# Original enums and dataclasses (unchanged except EscalationDecision)
# ---------------------------------------------------------------------------

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
    # Enhanced optional fields
    graph_metrics: Optional[Dict[str, float]] = None
    modp_score: Optional[float] = None
    human_feedback_score: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Enhanced configuration dataclass
# ---------------------------------------------------------------------------
@dataclass
class SelectiveInvocationConfig:
    """Configuration for enhanced selective invocation policy."""
    use_enhancements: bool = False
    # LIMIT Graph metrics
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    # MODP weights: [confidence, sustainability, criticality, cost]
    modp_weights: Optional[List[float]] = None   # default [0.4, 0.3, 0.2, 0.1]
    # RLHF
    human_feedback_score: float = 0.5
    # Distillation + MoE
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon: float = 0.1
    # Bio‑inspired
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2


# ---------------------------------------------------------------------------
# KnowledgeBaseIntegrator (unchanged, but kept for completeness)
# ---------------------------------------------------------------------------
class KnowledgeBaseIntegrator:
    """
    Integrates structured knowledge bases.
    (Original implementation unchanged.)
    """
    def __init__(self, knowledge_sources: Dict[KnowledgeSourceType, str],
                 enable_caching: bool = True):
        self.knowledge_sources = knowledge_sources
        self.enable_caching = enable_caching
        self.knowledge_cache: Dict[str, List[KnowledgeEntry]] = {}
        self.knowledge_bases: Dict[KnowledgeSourceType, Any] = {}
        self._load_knowledge_bases()

    def _load_knowledge_bases(self):
        for source_type, source_path in self.knowledge_sources.items():
            try:
                with open(source_path, 'r') as f:
                    data = json.load(f)
                    self.knowledge_bases[source_type] = data
            except Exception as e:
                print(f"Warning: Failed to load {source_type.value}: {e}")

    def query_knowledge(self, query: str, source_types: Optional[List[KnowledgeSourceType]] = None,
                        top_k: int = 5) -> List[KnowledgeEntry]:
        cache_key = f"{query}:{source_types}"
        if self.enable_caching and cache_key in self.knowledge_cache:
            return self.knowledge_cache[cache_key]
        results = []
        sources_to_query = source_types or list(self.knowledge_bases.keys())
        for source_type in sources_to_query:
            if source_type not in self.knowledge_bases:
                continue
            kb_data = self.knowledge_bases[source_type]
            for entry in kb_data.get('entries', []):
                relevance = self._calculate_relevance(query, entry)
                if relevance > 0.3:
                    results.append(KnowledgeEntry(
                        entry_id=entry.get('id', ''),
                        source_type=source_type,
                        title=entry.get('title', ''),
                        content=entry.get('content', ''),
                        metadata=entry.get('metadata', {}),
                        relevance_score=relevance,
                        confidence=entry.get('confidence', 0.8)
                    ))
        results.sort(key=lambda x: x.relevance_score, reverse=True)
        results = results[:top_k]
        if self.enable_caching:
            self.knowledge_cache[cache_key] = results
        return results

    def _calculate_relevance(self, query: str, entry: Dict) -> float:
        query_words = set(query.lower().split())
        title_words = set(entry.get('title', '').lower().split())
        content_words = set(entry.get('content', '').lower().split())
        title_overlap = len(query_words & title_words)
        content_overlap = len(query_words & content_words)
        score = (title_overlap * 0.7 + content_overlap * 0.3) / max(len(query_words), 1)
        return min(score, 1.0)

    def add_knowledge_source(self, source_type: KnowledgeSourceType, source_path: str):
        self.knowledge_sources[source_type] = source_path
        try:
            with open(source_path, 'r') as f:
                data = json.load(f)
                self.knowledge_bases[source_type] = data
        except Exception as e:
            print(f"Failed to load {source_type.value}: {e}")


# ---------------------------------------------------------------------------
# Enhanced SelectiveInvocationPolicy
# ---------------------------------------------------------------------------
class SelectiveInvocationPolicy:
    """
    Implements selective invocation policy with escalation logic.

    When enhancements are enabled:
      - A distillation optimizer (with MoE gating) decides whether to escalate.
      - Teachers: rule‑based (original logic), RLHF, historical.
      - State includes graph metrics and human feedback.
      - After decision, a MODP reward is computed and used to update the student.
      - Optionally, an evolutionary algorithm tunes the MODP weights.
    """
    def __init__(
        self,
        confidence_threshold: float = 0.7,
        sustainability_threshold_wh: float = 0.1,
        enable_criticality_check: bool = True,
        enable_sustainability_check: bool = True,
        knowledge_integrator: Optional[KnowledgeBaseIntegrator] = None,
        config: Optional[SelectiveInvocationConfig] = None
    ):
        self.confidence_threshold = confidence_threshold
        self.sustainability_threshold = sustainability_threshold_wh
        self.enable_criticality_check = enable_criticality_check
        self.enable_sustainability_check = enable_sustainability_check
        self.knowledge_integrator = knowledge_integrator
        self.config = config or SelectiveInvocationConfig()
        self.use_enhancements = self.config.use_enhancements

        # Criticality keywords (original)
        self.critical_keywords = {
            'safety': ['crash', 'segfault', 'memory', 'unsafe', 'critical'],
            'security': ['vulnerability', 'exploit', 'password', 'authentication', 'encryption'],
            'correctness': ['algorithm', 'logic', 'computation', 'accuracy', 'precision']
        }

        # Statistics
        self.total_decisions = 0
        self.escalations = 0
        self.escalations_by_reason: Dict[EscalationReason, int] = {}

        # Enhanced components
        self.distillation_optimizer = None
        self.evolutionary_optimizer = None
        if self.use_enhancements:
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.4, 0.3, 0.2, 0.1]
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]
            if self.config.use_distillation:
                self.distillation_optimizer = EscalationDistillationOptimizer(self.config)
            if self.config.use_evolutionary:
                self.evolutionary_optimizer = EvolutionaryMODPWeightsOptimizer(self.config)

    # ------------------------------------------------------------------
    # Original decide_escalation (used when enhancements disabled, or as fallback)
    # ------------------------------------------------------------------
    def _original_decide(self, task: str, agent_confidence: float,
                         estimated_energy_wh: float, domain: str,
                         context: Optional[Dict[str, Any]] = None) -> EscalationDecision:
        # (Copy of original logic)
        reasons = []
        should_escalate = False
        if agent_confidence < self.confidence_threshold:
            reasons.append(EscalationReason.LOW_CONFIDENCE)
            should_escalate = True
        if self.enable_sustainability_check and estimated_energy_wh > self.sustainability_threshold:
            reasons.append(EscalationReason.HIGH_ENERGY)
            should_escalate = True
        if self.enable_criticality_check:
            criticality_reasons = self._check_criticality(task)
            reasons.extend(criticality_reasons)
            if criticality_reasons:
                should_escalate = True
        if self._is_complex_domain(domain):
            reasons.append(EscalationReason.DOMAIN_COMPLEXITY)
            should_escalate = True
        if self._requires_compliance_check(task):
            reasons.append(EscalationReason.REGULATORY_COMPLIANCE)
            should_escalate = True
        criticality_level = self._determine_criticality_level(reasons, context)
        recommended_expert = self._recommend_expert(reasons, domain)
        estimated_cost = self._estimate_expert_cost(reasons, criticality_level)
        justification = self._build_justification(should_escalate, reasons, agent_confidence,
                                                  estimated_energy_wh, criticality_level)
        if should_escalate:
            self.escalations += 1
            for reason in reasons:
                self.escalations_by_reason[reason] = self.escalations_by_reason.get(reason, 0) + 1
        sustainability_impact = self._calculate_sustainability_impact(estimated_energy_wh, estimated_cost)
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

    # ------------------------------------------------------------------
    # Main decide_escalation (enhanced path if enabled)
    # ------------------------------------------------------------------
    def decide_escalation(
        self,
        task: str,
        agent_confidence: float,
        estimated_energy_wh: float,
        domain: str,
        context: Optional[Dict[str, Any]] = None,
        graph_metrics: Optional[Dict[str, float]] = None,
        human_feedback_score: Optional[float] = None
    ) -> EscalationDecision:
        """
        Decide whether to escalate to expert. If enhancements enabled, use
        distillation optimizer; otherwise use original rule-based logic.
        """
        self.total_decisions += 1

        # Use defaults for enhanced inputs if not provided
        if graph_metrics is None:
            graph_metrics = self.config.graph_metrics
        if human_feedback_score is None:
            human_feedback_score = self.config.human_feedback_score

        if self.use_enhancements and self.distillation_optimizer:
            # Build state
            state = EscalationState(
                task=task,
                agent_confidence=agent_confidence,
                estimated_energy_wh=estimated_energy_wh,
                domain=domain,
                graph_metrics=graph_metrics,
                human_feedback=human_feedback_score
            )
            # Use distillation to decide (0 = no escalate, 1 = escalate)
            action, state_vec, teacher_probs = self.distillation_optimizer.select_action(state)
            should_escalate = bool(action == 1)

            # Generate reasons for transparency (based on original logic, but for reporting)
            reasons = []
            if agent_confidence < self.confidence_threshold:
                reasons.append(EscalationReason.LOW_CONFIDENCE)
            if self.enable_sustainability_check and estimated_energy_wh > self.sustainability_threshold:
                reasons.append(EscalationReason.HIGH_ENERGY)
            if self.enable_criticality_check:
                reasons.extend(self._check_criticality(task))
            if self._is_complex_domain(domain):
                reasons.append(EscalationReason.DOMAIN_COMPLEXITY)
            if self._requires_compliance_check(task):
                reasons.append(EscalationReason.REGULATORY_COMPLIANCE)

            criticality_level = self._determine_criticality_level(reasons, context)
            recommended_expert = self._recommend_expert(reasons, domain)
            estimated_cost = self._estimate_expert_cost(reasons, criticality_level)
            justification = self._build_justification(should_escalate, reasons, agent_confidence,
                                                      estimated_energy_wh, criticality_level)
            sustainability_impact = self._calculate_sustainability_impact(estimated_energy_wh, estimated_cost)

            # Compute MODP reward (for learning)
            modp_reward = self._compute_modp_reward(
                should_escalate=should_escalate,
                agent_confidence=agent_confidence,
                estimated_energy_wh=estimated_energy_wh,
                criticality_level=criticality_level,
                estimated_cost=estimated_cost,
                graph_metrics=graph_metrics,
                human_feedback=human_feedback_score
            )

            # Update distillation optimizer with reward
            # Use action as chosen, reward as computed.
            # The reward serves as a signal for training.
            self.distillation_optimizer.update(
                state_vec=state_vec,
                action=action,
                reward=modp_reward,
                next_state_vec=state_vec,   # simplified
                teacher_probs=teacher_probs
            )

            # Update evolutionary optimizer if present
            if self.evolutionary_optimizer:
                self.evolutionary_optimizer.update_fitness(modp_reward)
                best_weights = self.evolutionary_optimizer.get_best_weights()
                # Optionally update config modp_weights for future rewards
                self.config.modp_weights = best_weights

            # Update statistics
            if should_escalate:
                self.escalations += 1
                for reason in reasons:
                    self.escalations_by_reason[reason] = self.escalations_by_reason.get(reason, 0) + 1

            # Build enhanced decision
            decision = EscalationDecision(
                should_escalate=should_escalate,
                reasons=reasons,
                confidence_score=agent_confidence,
                sustainability_impact=sustainability_impact,
                criticality_level=criticality_level,
                recommended_expert=recommended_expert,
                estimated_cost_wh=estimated_cost,
                justification=justification,
                graph_metrics=graph_metrics,
                modp_score=modp_reward,
                human_feedback_score=human_feedback_score,
                distillation_stats={
                    "student_counter": self.distillation_optimizer.counter,
                    "buffer_size": len(self.distillation_optimizer.replay_buffer)
                }
            )
            return decision
        else:
            # Original behavior
            decision = self._original_decide(task, agent_confidence, estimated_energy_wh, domain, context)
            return decision

    # ------------------------------------------------------------------
    # Helper: MODP reward calculation
    # ------------------------------------------------------------------
    def _compute_modp_reward(self, should_escalate: bool, agent_confidence: float,
                             estimated_energy_wh: float, criticality_level: str,
                             estimated_cost: float, graph_metrics: Dict[str, float],
                             human_feedback: float) -> float:
        """
        Compute a multi‑objective reward based on the decision.
        Higher reward means a "good" decision (e.g., correct escalation).
        In a real system, we would compare against actual outcome; here we
        use a heuristic: escalate if conditions warrant, else not.
        """
        # Ideal decision logic: escalate if any critical or low confidence
        should_ideal = (
            agent_confidence < self.confidence_threshold or
            estimated_energy_wh > self.sustainability_threshold or
            criticality_level in ["high", "critical"] or
            estimated_cost > 0.1
        )
        # Correct decision?
        correct = (should_escalate == should_ideal)
        # Base reward for correctness
        reward = 1.0 if correct else -0.5

        # Adjust by human feedback: if human agrees, bonus
        # (Assume human_feedback > 0.5 means human prefers escalation)
        if should_escalate and human_feedback > 0.5:
            reward += 0.2
        elif not should_escalate and human_feedback < 0.5:
            reward += 0.2

        # Graph metrics influence: high centrality increases reward for escalation
        centrality = graph_metrics.get("centrality", 0.5)
        if should_escalate and centrality > 0.7:
            reward += 0.1

        # Clamp to [-1, 1] (or [0,1]? we use [-1,1] for RL)
        return float(np.clip(reward, -1.0, 1.0))

    # ------------------------------------------------------------------
    # Original private methods (used by both paths)
    # ------------------------------------------------------------------
    def _check_criticality(self, task: str) -> List[EscalationReason]:
        reasons = []
        task_lower = task.lower()
        if any(kw in task_lower for kw in self.critical_keywords['safety']):
            reasons.append(EscalationReason.CRITICAL_SAFETY)
        if any(kw in task_lower for kw in self.critical_keywords['security']):
            reasons.append(EscalationReason.CRITICAL_SECURITY)
        if any(kw in task_lower for kw in self.critical_keywords['correctness']):
            reasons.append(EscalationReason.CRITICAL_CORRECTNESS)
        return reasons

    def _is_complex_domain(self, domain: str) -> bool:
        complex_domains = ['cryptography', 'quantum', 'distributed_systems',
                           'machine_learning', 'medical', 'legal', 'scientific']
        return any(cd in domain.lower() for cd in complex_domains)

    def _requires_compliance_check(self, task: str) -> bool:
        compliance_keywords = ['medical', 'healthcare', 'financial', 'privacy',
                               'gdpr', 'hipaa', 'pci', 'compliance']
        task_lower = task.lower()
        return any(kw in task_lower for kw in compliance_keywords)

    def _determine_criticality_level(self, reasons: List[EscalationReason],
                                     context: Optional[Dict[str, Any]]) -> str:
        critical_reasons = [EscalationReason.CRITICAL_SAFETY,
                            EscalationReason.CRITICAL_SECURITY,
                            EscalationReason.REGULATORY_COMPLIANCE]
        if any(r in reasons for r in critical_reasons):
            return "critical"
        if EscalationReason.CRITICAL_CORRECTNESS in reasons:
            return "high"
        if len(reasons) >= 2:
            return "medium"
        return "low"

    def _recommend_expert(self, reasons: List[EscalationReason], domain: str) -> str:
        if EscalationReason.CRITICAL_SECURITY in reasons:
            return "security_expert"
        if EscalationReason.CRITICAL_SAFETY in reasons:
            return "safety_expert"
        if EscalationReason.REGULATORY_COMPLIANCE in reasons:
            return "compliance_expert"
        if EscalationReason.HIGH_ENERGY in reasons:
            return "sustainability_expert"
        return f"{domain}_expert"

    def _estimate_expert_cost(self, reasons: List[EscalationReason],
                              criticality_level: str) -> float:
        base_cost = 0.05
        cost = base_cost * len(reasons)
        if criticality_level == "critical":
            cost *= 1.5
        elif criticality_level == "high":
            cost *= 1.3
        return cost

    def _calculate_sustainability_impact(self, task_energy: float, expert_energy: float) -> float:
        potential_savings = task_energy * 0.3
        return expert_energy - potential_savings

    def _build_justification(self, should_escalate: bool, reasons: List[EscalationReason],
                             confidence: float, energy: float, criticality: str) -> str:
        if not should_escalate:
            return (f"No escalation needed. Agent confidence: {confidence:.2f}, "
                    f"Energy: {energy*1000:.1f} mWh, Criticality: {criticality}")
        reason_strs = [r.value.replace('_', ' ') for r in reasons]
        reasons_text = ", ".join(reason_strs)
        return (f"Escalation recommended ({criticality} priority). "
                f"Reasons: {reasons_text}. "
                f"Agent confidence: {confidence:.2f}, "
                f"Energy: {energy*1000:.1f} mWh")

    def get_policy_stats(self) -> Dict[str, Any]:
        escalation_rate = self.escalations / self.total_decisions if self.total_decisions > 0 else 0
        stats = {
            "total_decisions": self.total_decisions,
            "escalations": self.escalations,
            "escalation_rate": escalation_rate,
            "escalations_by_reason": {r.value: c for r, c in self.escalations_by_reason.items()},
            "confidence_threshold": self.confidence_threshold,
            "sustainability_threshold_wh": self.sustainability_threshold,
        }
        if self.use_enhancements and self.distillation_optimizer:
            stats["distillation_stats"] = {
                "student_counter": self.distillation_optimizer.counter,
                "buffer_size": len(self.distillation_optimizer.replay_buffer)
            }
        return stats


# ---------------------------------------------------------------------------
# Enhanced decision components
# ---------------------------------------------------------------------------

class EscalationState:
    """State representation for distillation."""
    def __init__(self, task: str, agent_confidence: float, estimated_energy_wh: float,
                 domain: str, graph_metrics: Dict[str, float], human_feedback: float):
        self.confidence = agent_confidence
        self.energy = min(estimated_energy_wh / 1.0, 1.0)  # normalize (max 1 Wh)
        self.criticality = self._compute_criticality_score(task)
        self.domain_complexity = self._compute_domain_complexity(domain)
        self.centrality = graph_metrics.get("centrality", 0.5)
        self.connectivity = graph_metrics.get("connectivity", 0.5)
        self.human_feedback = human_feedback

    def _compute_criticality_score(self, task: str) -> float:
        # Simple heuristic: check for critical keywords
        critical_words = ['crash', 'segfault', 'vulnerability', 'exploit', 'password',
                          'medical', 'healthcare', 'financial', 'privacy', 'compliance',
                          'safety', 'security', 'correctness']
        task_lower = task.lower()
        score = 0.0
        for word in critical_words:
            if word in task_lower:
                score += 0.1
        return min(score, 1.0)

    def _compute_domain_complexity(self, domain: str) -> float:
        complex_domains = ['cryptography', 'quantum', 'distributed_systems',
                           'machine_learning', 'medical', 'legal', 'scientific']
        return 0.8 if any(cd in domain.lower() for cd in complex_domains) else 0.2

    def to_vector(self) -> np.ndarray:
        return np.array([
            self.confidence,
            self.energy,
            self.criticality,
            self.domain_complexity,
            self.centrality,
            self.connectivity,
            self.human_feedback,
        ], dtype=np.float32)


class EscalationDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to decide escalation (binary action).
    """
    def __init__(self, config: SelectiveInvocationConfig):
        self.config = config
        self.feature_dim = 7
        self.n_actions = 2  # 0 = no escalate, 1 = escalate
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.train_every = config.train_every
        self.counter = 0
        self.replay_buffer = deque(maxlen=config.replay_size)

        # Student
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

        # Teachers (rule-based, RLHF, historical)
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.gating_lr

    def _rule_teacher(self, state: EscalationState) -> np.ndarray:
        # Original rules: low confidence, high energy, high criticality -> escalate
        should_escalate = (
            state.confidence < 0.7 or
            state.energy > 0.1 or
            state.criticality > 0.3 or
            state.domain_complexity > 0.5
        )
        if should_escalate:
            return np.array([0.1, 0.9])
        else:
            return np.array([0.9, 0.1])

    def _rlhf_teacher(self, state: EscalationState) -> np.ndarray:
        # Human feedback: high -> escalate, low -> don't
        if state.human_feedback > 0.7:
            return np.array([0.2, 0.8])
        elif state.human_feedback < 0.3:
            return np.array([0.8, 0.2])
        else:
            return np.array([0.5, 0.5])

    def _historical_teacher(self, state: EscalationState) -> np.ndarray:
        # Simulate a learned model: centrality high -> escalate
        if state.centrality > 0.7 and state.criticality > 0.4:
            return np.array([0.1, 0.9])
        else:
            return np.array([0.6, 0.4])

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: EscalationState, exploration=True):
        x = state.to_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher(state)
            if len(prob) != self.n_actions:
                prob = np.pad(prob, (0, self.n_actions - len(prob)), 'constant')[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate = self._gate_forward(x)
        teacher_probs = np.sum(gate[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_logits = x @ self.student_weights + self.student_bias
        student_probs = np.exp(student_logits - np.max(student_logits))
        student_probs /= student_probs.sum()

        if exploration and random.random() < self.epsilon:
            action = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_probs

    def update(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.replay_buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = random.sample(self.replay_buffer, min(8, len(self.replay_buffer)))
            for s, a, r, ns, tp in batch:
                # Update student
                logits = s @ self.student_weights + self.student_bias
                cur = np.exp(logits - np.max(logits))
                cur /= cur.sum()
                grad_distill = -(tp - cur)
                one_hot = np.zeros(self.n_actions); one_hot[a] = 1.0
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # Update gating
                gate = self._gate_forward(s)
                combined_teacher = np.sum(gate[:, None] * tp, axis=0)
                error = combined_teacher - cur
                grad_gate = np.dot(tp, error)
                self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
                self.gate_bias -= self.gate_lr * grad_gate


class EvolutionaryMODPWeightsOptimizer:
    """Evolves MODP weights using a simple genetic algorithm."""
    def __init__(self, config: SelectiveInvocationConfig):
        self.n_weights = len(config.modp_weights) if config.modp_weights else 4
        self.population_size = config.population_size
        self.mutation_rate = config.mutation_rate
        self.crossover_rate = config.crossover_rate
        self.elitism = config.elitism
        self.population = [np.random.dirichlet(np.ones(self.n_weights)) for _ in range(self.population_size)]
        self.fitness = np.zeros(self.population_size)
        self.best_weights = self.population[0]
        self.best_fitness = 0.0

    def update_fitness(self, reward: float, index: int = 0):
        self.fitness[index] = reward
        best_idx = int(np.argmax(self.fitness))
        self.best_weights = self.population[best_idx]
        self.best_fitness = self.fitness[best_idx]
        # Evolve
        sorted_indices = np.argsort(self.fitness)[::-1]
        new_pop = [self.population[i] for i in sorted_indices[:self.elitism]]
        while len(new_pop) < self.population_size:
            p1 = self.population[random.randint(0, self.population_size-1)]
            p2 = self.population[random.randint(0, self.population_size-1)]
            if random.random() < self.crossover_rate:
                alpha = random.random()
                child = alpha * p1 + (1 - alpha) * p2
            else:
                child = p1.copy()
            child += np.random.dirichlet(np.ones(self.n_weights)) * self.mutation_rate
            child = child / child.sum()
            new_pop.append(child)
        self.population = new_pop
        self.fitness = np.zeros(self.population_size)

    def get_best_weights(self) -> np.ndarray:
        return self.best_weights
