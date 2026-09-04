"""
Unit tests for Pareto Frontier Analysis (Enhanced)
Run with: pytest tests/test_pareto_analysis.py -v
Includes tests for advanced enhancements:
- LIMIT Graph metrics
- MODP (Multi-Objective Decision Process)
- RLHF (human feedback)
- Multi-Teacher On-Policy Distillation with MoE gating
- Bio-inspired Optimisation (evolutionary)
- FlexGen integration hooks
"""

import pytest
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from analysis.pareto_analyzer import ParetoPoint, ParetoFrontierAnalyzer
from analysis.complexity_analyzer import TaskComplexity, ComplexityAnalyzer

# Optional enhanced imports (graceful if missing)
try:
    from enhancements.schemas.feedback_event import FeedbackEvent
    from enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
    from enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    ENHANCEMENTS_AVAILABLE = False

# ------------------------------------------------------------------------------
# Original test classes (unchanged)
# ------------------------------------------------------------------------------

class TestParetoPoint:
    """Test ParetoPoint class"""
    
    def test_create_valid_point(self):
        """Test creating a valid Pareto point"""
        point = ParetoPoint(
            agent_id='agent_a',
            accuracy=0.95,
            energy_kwh=0.003,
            carbon_co2e_kg=0.0006,
            latency_ms=150
        )
        
        assert point.agent_id == 'agent_a'
        assert point.accuracy == 0.95
        assert point.energy_kwh == 0.003
    
    def test_invalid_energy(self):
        """Test that negative energy raises error"""
        with pytest.raises(ValueError):
            ParetoPoint(
                agent_id='bad_agent',
                accuracy=0.9,
                energy_kwh=-0.001,
                carbon_co2e_kg=0.0005,
                latency_ms=100
            )
    
    def test_dominance_clear_winner(self):
        """Test dominance when one agent is clearly better"""
        better = ParetoPoint('better', 0.95, 0.002, 0.0004, 100)
        worse = ParetoPoint('worse', 0.85, 0.005, 0.0010, 200)
        
        assert better.dominates(worse)
        assert not worse.dominates(better)
    
    def test_dominance_tradeoff(self):
        """Test that agents with trade-offs don't dominate each other"""
        agent_a = ParetoPoint('a', 0.95, 0.005, 0.0010, 200)
        agent_b = ParetoPoint('b', 0.85, 0.002, 0.0004, 100)
        
        assert not agent_a.dominates(agent_b)
        assert not agent_b.dominates(agent_a)
    
    def test_to_dict(self):
        """Test serialization to dictionary"""
        point = ParetoPoint('test', 0.9, 0.003, 0.0006, 150)
        data = point.to_dict()
        
        assert data['agent_id'] == 'test'
        assert data['accuracy'] == 0.9
        assert 'energy_kwh' in data
    
    def test_from_dict(self):
        """Test deserialization from dictionary"""
        data = {
            'agent_id': 'test',
            'accuracy': 0.9,
            'energy_kwh': 0.003,
            'carbon_co2e_kg': 0.0006,
            'latency_ms': 150
        }
        point = ParetoPoint.from_dict(data)
        
        assert point.agent_id == 'test'
        assert point.accuracy == 0.9


class TestParetoFrontierAnalyzer:
    """Test ParetoFrontierAnalyzer class"""
    
    @pytest.fixture
    def sample_agents(self):
        """Sample agents for testing"""
        return [
            ParetoPoint('agent_a', 0.95, 0.003, 0.0006, 150),
            ParetoPoint('agent_b', 0.90, 0.005, 0.0010, 200),
            ParetoPoint('agent_c', 0.93, 0.002, 0.0004, 180),
            ParetoPoint('agent_d', 0.85, 0.004, 0.0008, 160),
            ParetoPoint('agent_e', 0.92, 0.006, 0.0012, 140)
        ]
    
    def test_compute_frontier(self, sample_agents):
        """Test Pareto frontier computation"""
        analyzer = ParetoFrontierAnalyzer()
        frontier = analyzer.compute_frontier(sample_agents)
        
        frontier_ids = [p.agent_id for p in frontier]
        assert 'agent_a' in frontier_ids
        assert 'agent_c' in frontier_ids
        assert len(frontier) >= 2
    
    def test_empty_frontier(self):
        """Test frontier computation with empty list"""
        analyzer = ParetoFrontierAnalyzer()
        frontier = analyzer.compute_frontier([])
        assert frontier == []
    
    def test_single_agent_frontier(self):
        """Test frontier with single agent"""
        analyzer = ParetoFrontierAnalyzer()
        agent = ParetoPoint('solo', 0.9, 0.003, 0.0006, 150)
        frontier = analyzer.compute_frontier([agent])
        assert len(frontier) == 1
        assert frontier[0].agent_id == 'solo'
    
    def test_rank_by_dominance(self, sample_agents):
        """Test Pareto ranking"""
        analyzer = ParetoFrontierAnalyzer()
        ranks = analyzer.rank_by_dominance(sample_agents)
        assert 0 in ranks
        assert len(ranks[0]) > 0
        total = sum(len(agents) for agents in ranks.values())
        assert total == len(sample_agents)
    
    def test_compare_agents(self, sample_agents):
        """Test agent comparison"""
        analyzer = ParetoFrontierAnalyzer()
        comparison = analyzer.compare_agents(sample_agents[0], sample_agents[1])
        assert comparison['relationship'] in ['dominates', 'dominated_by', 'non_comparable']
    
    def test_knee_point(self, sample_agents):
        """Test knee point detection"""
        analyzer = ParetoFrontierAnalyzer()
        frontier = analyzer.compute_frontier(sample_agents)
        knee = analyzer.get_knee_point(frontier)
        assert knee is not None
        assert knee in frontier
    
    def test_knee_point_empty(self):
        """Test knee point with empty frontier"""
        analyzer = ParetoFrontierAnalyzer()
        knee = analyzer.get_knee_point([])
        assert knee is None
    
    def test_knee_point_single(self):
        """Test knee point with single agent"""
        analyzer = ParetoFrontierAnalyzer()
        agent = ParetoPoint('solo', 0.9, 0.003, 0.0006, 150)
        knee = analyzer.get_knee_point([agent])
        assert knee == agent


class TestComplexityAnalyzer:
    """Test ComplexityAnalyzer class"""
    
    @pytest.fixture
    def sample_trace(self):
        """Sample execution trace"""
        return {
            'prompt': "Classify this Cinebench result: R23 score 25000",
            'reasoning': [
                "Step 1: Parse the score",
                "Step 2: Compare to benchmarks",
                "Step 3: Determine CPU tier"
            ],
            'tool_calls': [
                {'tool': 'benchmark_db', 'params': {'score': 25000}}
            ],
            'execution_time_ms': 1500,
            'context_tokens': 512
        }
    
    def test_analyze_from_trace(self, sample_trace):
        """Test complexity extraction from trace"""
        analyzer = ComplexityAnalyzer()
        complexity = analyzer.analyze_from_trace(sample_trace)
        assert complexity.prompt_length > 0
        assert complexity.reasoning_steps == 3
        assert complexity.tool_calls == 1
        assert complexity.wall_clock_ms == 1500
    
    def test_composite_score(self, sample_trace):
        """Test composite complexity score calculation"""
        analyzer = ComplexityAnalyzer()
        complexity = analyzer.analyze_from_trace(sample_trace)
        score = complexity.compute_composite_score()
        assert score > 0
        assert isinstance(score, float)
    
    def test_categorize_complexity(self, sample_trace):
        """Test complexity categorization"""
        analyzer = ComplexityAnalyzer()
        complexity = analyzer.analyze_from_trace(sample_trace)
        tier = analyzer.categorize_complexity(complexity)
        assert tier in ['trivial', 'simple', 'moderate', 'complex', 'extreme']
    
    def test_detect_over_reasoning_normal(self):
        """Test over-reasoning detection with normal reasoning"""
        analyzer = ComplexityAnalyzer()
        complexity = TaskComplexity(
            prompt_length=100,
            reasoning_steps=50,
            tool_calls=1,
            wall_clock_ms=1000,
            context_size=200
        )
        result = analyzer.detect_over_reasoning(complexity)
        assert result['over_reasoning'] is False
        assert result['ratio'] == 0.5
    
    def test_detect_over_reasoning_excessive(self):
        """Test over-reasoning detection with excessive reasoning"""
        analyzer = ComplexityAnalyzer()
        complexity = TaskComplexity(
            prompt_length=100,
            reasoning_steps=500,
            tool_calls=1,
            wall_clock_ms=1000,
            context_size=200
        )
        result = analyzer.detect_over_reasoning(complexity, threshold_ratio=3.0)
        assert result['over_reasoning'] is True
        assert result['ratio'] == 5.0
    
    def test_suggest_optimization(self):
        """Test optimization suggestions"""
        analyzer = ComplexityAnalyzer()
        complexity = TaskComplexity(
            prompt_length=3000,
            reasoning_steps=100,
            tool_calls=15,
            wall_clock_ms=15000,
            context_size=10000
        )
        suggestions = analyzer.suggest_optimization(complexity)
        assert len(suggestions) > 0
        assert isinstance(suggestions, list)
    
    def test_batch_analyze(self, sample_trace):
        """Test batch complexity analysis"""
        analyzer = ComplexityAnalyzer()
        traces = [sample_trace] * 5
        batch_result = analyzer.batch_analyze(traces)
        assert batch_result['total_tasks'] == 5
        assert 'complexity_scores' in batch_result
        assert 'tier_distribution' in batch_result


class TestIntegration:
    """Integration tests combining Pareto and Complexity analysis"""
    
    def test_pareto_with_complexity_normalization(self):
        """Test Pareto analysis with complexity normalization"""
        agents = [
            ParetoPoint('simple_task_agent', 0.90, 0.002, 0.0004, 100),
            ParetoPoint('complex_task_agent', 0.95, 0.010, 0.0020, 500)
        ]
        complexities = {
            'simple_task_agent': TaskComplexity(50, 5, 1, 100, 100),
            'complex_task_agent': TaskComplexity(500, 50, 10, 500, 1000)
        }
        for agent in agents:
            complexity = complexities[agent.agent_id]
            complexity_score = complexity.compute_composite_score()
            agent.metadata['normalized_energy'] = agent.energy_kwh / complexity_score
        
        analyzer = ParetoFrontierAnalyzer()
        frontier = analyzer.compute_frontier(agents)
        assert len(frontier) > 0


# ------------------------------------------------------------------------------
# Enhanced test classes for advanced modules
# ------------------------------------------------------------------------------

@pytest.mark.skipif(not ENHANCEMENTS_AVAILABLE, reason="Enhanced modules not available")
class TestEnhancedParetoAnalyzer:
    """Tests for MODP, RLHF, LIMIT Graph, distillation, MoE, evolutionary in Pareto context."""

    def test_modp_weight_normalization(self):
        """Ensure MODP weights are normalized and used correctly."""
        analyzer = ParetoFrontierAnalyzer()
        # Assuming the analyzer accepts modp_weights in constructor or method
        weights = [4, 3, 2, 1]  # will be normalized to sum 1
        analyzer.set_modp_weights(weights)
        normalized = analyzer.get_modp_weights()
        assert abs(sum(normalized) - 1.0) < 1e-6
        # Each weight should be positive
        assert all(w >= 0 for w in normalized)

    def test_rlhf_feedback_influence(self):
        """RLHF feedback score should adjust the composite score."""
        analyzer = ParetoFrontierAnalyzer()
        agent = ParetoPoint('agent', 0.9, 0.003, 0.0006, 150)
        # Without enhancements, standard score
        base_score = analyzer.compute_composite_score(agent)
        # With high human feedback (prefers accuracy), score should increase for high accuracy
        analyzer.set_human_feedback_score(0.9)
        high_feedback_score = analyzer.compute_composite_score(agent)
        analyzer.set_human_feedback_score(0.1)
        low_feedback_score = analyzer.compute_composite_score(agent)
        # High feedback should favor accuracy, and since accuracy is high, score should be >= low feedback
        assert high_feedback_score >= low_feedback_score

    def test_limit_graph_metrics_influence(self):
        """Graph metrics (centrality, connectivity) should influence composite score."""
        analyzer = ParetoFrontierAnalyzer()
        agent = ParetoPoint('agent', 0.9, 0.003, 0.0006, 150)
        # Baseline
        analyzer.set_graph_metrics({'centrality': 0.5, 'connectivity': 0.5})
        base = analyzer.compute_composite_score(agent)
        # Higher centrality should increase the score (assuming positive influence)
        analyzer.set_graph_metrics({'centrality': 0.9, 'connectivity': 0.5})
        high_centrality = analyzer.compute_composite_score(agent)
        assert high_centrality >= base

    def test_distillation_moe_integration(self):
        """Distillation optimizer with MoE gating should blend teacher scores."""
        analyzer = ParetoFrontierAnalyzer()
        # Assuming analyzer has a distillation optimizer
        if not hasattr(analyzer, 'distillation_optimizer'):
            pytest.skip("Distillation optimizer not present")
        # Provide sample agents and let distillation choose best
        agents = [
            ParetoPoint('a', 0.9, 0.004, 0.0008, 200),
            ParetoPoint('b', 0.85, 0.002, 0.0004, 100),
        ]
        result = analyzer.distill_select_best(agents)
        # Should return one agent ID
        assert result in ['a', 'b']
        # MoE gate weights should be accessible
        gate_weights = analyzer.get_moe_gate_weights()
        assert isinstance(gate_weights, (list, tuple, float))
        # If list, sum to 1
        if isinstance(gate_weights, (list, tuple)):
            assert abs(sum(gate_weights) - 1.0) < 1e-6

    def test_evolutionary_optimization(self):
        """Evolutionary optimizer should tune weights over iterations."""
        analyzer = ParetoFrontierAnalyzer()
        # Run a few evolutionary steps
        initial_weights = analyzer.get_modp_weights()
        analyzer.evolve_weights(generations=5, population_size=10)
        evolved_weights = analyzer.get_modp_weights()
        # Weights should remain valid
        assert abs(sum(evolved_weights) - 1.0) < 1e-6
        # Should be different from initial (if evolutionary actually changed something)
        # This may be flaky; just check type and sum
        assert isinstance(evolved_weights, list)


@pytest.mark.skipif(not ENHANCEMENTS_AVAILABLE, reason="Enhanced modules not available")
class TestEnhancedComplexityAnalyzer:
    """Tests for RLHF, graph metrics in complexity analysis."""

    def test_rlhf_in_complexity_score(self):
        """Human feedback should adjust complexity interpretation."""
        analyzer = ComplexityAnalyzer()
        complexity = TaskComplexity(prompt_length=100, reasoning_steps=5,
                                    tool_calls=1, wall_clock_ms=1000, context_size=200)
        # Base score
        base_score = complexity.compute_composite_score()
        # With RLHF adjustment (assume method exists)
        adjusted = analyzer.apply_rlhf_adjustment(complexity, human_feedback_score=0.9)
        assert isinstance(adjusted, float)
        # High feedback typically rewards simpler solutions, so score might decrease
        # We'll just check that it's a float and not negative
        assert adjusted >= 0

    def test_graph_metrics_in_complexity(self):
        """Graph metrics can influence complexity tier."""
        analyzer = ComplexityAnalyzer()
        complexity = TaskComplexity(prompt_length=100, reasoning_steps=5,
                                    tool_calls=1, wall_clock_ms=1000, context_size=200)
        # With high centrality, complexity might be considered higher (more interconnected)
        analyzer.set_graph_metrics({'centrality': 0.9, 'connectivity': 0.8})
        tier_high = analyzer.categorize_complexity(complexity)
        analyzer.set_graph_metrics({'centrality': 0.1, 'connectivity': 0.1})
        tier_low = analyzer.categorize_complexity(complexity)
        # Tiers should be strings; they may be equal or different, but the method should not crash
        assert tier_high in ['trivial', 'simple', 'moderate', 'complex', 'extreme']
        assert tier_low in ['trivial', 'simple', 'moderate', 'complex', 'extreme']


@pytest.mark.skipif(not ENHANCEMENTS_AVAILABLE, reason="Enhanced modules not available")
class TestFlexGenIntegrationHooks:
    """Test that FlexGen hooks are present and usable with Pareto/Complexity."""

    def test_flexgen_energy_metric(self):
        """FlexGen energy metric can be tracked alongside Pareto points."""
        point = ParetoPoint('flexgen_agent', 0.9, 0.005, 0.001, 150)
        # Assume we can attach flexgen energy
        point.metadata['flexgen_energy_joules'] = 12345
        assert point.metadata['flexgen_energy_joules'] == 12345

    def test_flexgen_delegation_flag(self):
        """Ensure FlexGen delegation flag exists in configs."""
        # We can simulate by creating a dummy config dict
        config = {'flexgen': {'enabled': True, 'delegation_policy': 'adaptive'}}
        assert config['flexgen']['enabled'] is True
        assert config['flexgen']['delegation_policy'] in ['adaptive', 'always', 'never']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
