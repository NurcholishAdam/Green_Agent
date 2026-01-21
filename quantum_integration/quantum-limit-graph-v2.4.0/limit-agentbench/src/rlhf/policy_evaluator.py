"""
Policy Evaluation Environment for Green_Agent

Transforms Green_Agent into an RLHF policy evaluation environment.
Enables testing agents across different execution modes to find
optimal policies for specific deployment scenarios.
"""

from typing import Dict, List, Callable, Optional
import numpy as np
import logging

from .reward_shaper import ExecutionMode, RewardShaper

logger = logging.getLogger(__name__)


class PolicyEvaluationEnvironment:
    """
    Green_Agent as RLHF policy evaluation environment
    
    Provides an environment for evaluating and comparing agent policies
    across different execution modes (eco, fast, accuracy, balanced).
    """
    
    def __init__(self):
        """Initialize policy evaluation environment"""
        # Create reward shapers for each mode
        self.reward_shapers = {
            mode: RewardShaper(mode) 
            for mode in ExecutionMode 
            if mode != ExecutionMode.CUSTOM
        }
        
        self.execution_history: List[Dict] = []
        logger.info("Initialized PolicyEvaluationEnvironment")
    
    def evaluate_policy(self,
                       agent_policy: Callable,
                       tasks: List[Dict],
                       mode: ExecutionMode = ExecutionMode.BALANCED_MODE,
                       verbose: bool = False) -> Dict:
        """
        Evaluate agent policy across tasks in specific mode
        
        Args:
            agent_policy: Callable agent that executes tasks
                         Should return dict with metrics
            tasks: List of evaluation tasks
            mode: Execution mode for reward calculation
            verbose: Print detailed progress
        
        Returns:
            Dict with evaluation results:
            {
                'mode': str,
                'avg_reward': float,
                'avg_task_success': float,
                'total_penalty': float,
                'task_results': List[Dict],
                'summary': Dict
            }
        
        Example:
            def my_agent(task):
                # ... agent logic ...
                return {
                    'accuracy': 0.95,
                    'energy_kwh': 0.003,
                    'carbon_kg': 0.0006,
                    'latency_ms': 150
                }
            
            results = env.evaluate_policy(
                my_agent,
                tasks,
                ExecutionMode.ECO_MODE
            )
            
            print(f"Eco mode reward: {results['avg_reward']:.3f}")
        """
        if mode not in self.reward_shapers:
            raise ValueError(f"Unknown mode: {mode}")
        
        shaper = self.reward_shapers[mode]
        results = []
        
        if verbose:
            print(f"\nEvaluating policy in {mode.value} mode...")
            print(f"Tasks: {len(tasks)}")
        
        for i, task in enumerate(tasks):
            if verbose and (i + 1) % 10 == 0:
                print(f"  Progress: {i + 1}/{len(tasks)}")
            
            try:
                # Execute agent
                result = agent_policy(task)
                
                # Compute reward
                reward_data = shaper.compute_reward(
                    task_success=result.get('accuracy', result.get('task_success', 0.0)),
                    energy_kwh=result.get('energy_kwh', 0.0),
                    carbon_kg=result.get('carbon_kg', 0.0),
                    latency_ms=result.get('latency_ms', 0.0),
                    cost_usd=result.get('cost_usd', 0.0)
                )
                
                results.append({
                    'task_id': task.get('task_id', f"task_{i}"),
                    **reward_data,
                    'metrics': result
                })
                
            except Exception as e:
                logger.error(f"Task {i} failed: {e}")
                results.append({
                    'task_id': task.get('task_id', f"task_{i}"),
                    'reward': 0.0,
                    'error': str(e)
                })
        
        # Aggregate statistics
        rewards = [r['reward'] for r in results if 'reward' in r]
        successes = [r['components']['task_success'] for r in results if 'components' in r]
        penalties = [r['penalties']['total_penalty'] for r in results if 'penalties' in r]
        
        # Extract resource metrics
        energy_values = [r['metrics'].get('energy_kwh', 0) for r in results if 'metrics' in r]
        carbon_values = [r['metrics'].get('carbon_kg', 0) for r in results if 'metrics' in r]
        latency_values = [r['metrics'].get('latency_ms', 0) for r in results if 'metrics' in r]
        
        evaluation_result = {
            'mode': mode.value,
            'total_tasks': len(tasks),
            'successful_tasks': len(rewards),
            'avg_reward': np.mean(rewards) if rewards else 0.0,
            'std_reward': np.std(rewards) if rewards else 0.0,
            'avg_task_success': np.mean(successes) if successes else 0.0,
            'total_penalty': sum(penalties) if penalties else 0.0,
            'task_results': results,
            'summary': {
                'avg_energy_kwh': np.mean(energy_values) if energy_values else 0.0,
                'avg_carbon_kg': np.mean(carbon_values) if carbon_values else 0.0,
                'avg_latency_ms': np.mean(latency_values) if latency_values else 0.0,
                'total_energy_kwh': sum(energy_values),
                'total_carbon_kg': sum(carbon_values)
            }
        }
        
        # Store in history
        self.execution_history.append({
            'mode': mode.value,
            'timestamp': np.datetime64('now'),
            'results': evaluation_result
        })
        
        if verbose:
            print(f"\nResults for {mode.value} mode:")
            print(f"  Average Reward: {evaluation_result['avg_reward']:.3f}")
            print(f"  Average Success: {evaluation_result['avg_task_success']:.2%}")
            print(f"  Total Penalty: {evaluation_result['total_penalty']:.3f}")
        
        logger.info(f"Evaluated policy in {mode.value} mode: "
                   f"avg_reward={evaluation_result['avg_reward']:.3f}")
        
        return evaluation_result
    
    def multi_mode_evaluation(self,
                             agent_policy: Callable,
                             tasks: List[Dict],
                             modes: Optional[List[ExecutionMode]] = None,
                             verbose: bool = False) -> Dict:
        """
        Evaluate agent across all execution modes
        
        This shows how the same agent performs under different
        optimization objectives.
        
        Args:
            agent_policy: Agent to evaluate
            tasks: List of tasks
            modes: List of modes to test (default: all non-custom modes)
            verbose: Print progress
        
        Returns:
            Dict with:
            {
                'evaluations': {mode: results},
                'best_mode': str,
                'mode_comparison': Dict,
                'recommendations': Dict
            }
        
        Example:
            results = env.multi_mode_evaluation(my_agent, tasks)
            
            print(f"Best mode: {results['best_mode']}")
            for mode, eval_result in results['evaluations'].items():
                print(f"{mode}: reward={eval_result['avg_reward']:.3f}")
        """
        if modes is None:
            modes = [ExecutionMode.ECO_MODE, ExecutionMode.FAST_MODE,
                    ExecutionMode.ACCURACY_MODE, ExecutionMode.BALANCED_MODE]
        
        evaluations = {}
        
        for mode in modes:
            if verbose:
                print(f"\n{'='*60}")
            
            evaluations[mode.value] = self.evaluate_policy(
                agent_policy,
                tasks,
                mode,
                verbose=verbose
            )
        
        # Find best mode (highest average reward)
        best_mode = max(evaluations.items(), 
                       key=lambda x: x[1]['avg_reward'])[0]
        
        # Mode comparison
        mode_comparison = self._compare_modes(evaluations)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(evaluations)
        
        return {
            'evaluations': evaluations,
            'best_mode': best_mode,
            'mode_comparison': mode_comparison,
            'recommendations': recommendations
        }
    
    def compare_policies(self,
                        policies: Dict[str, Callable],
                        tasks: List[Dict],
                        mode: ExecutionMode = ExecutionMode.BALANCED_MODE) -> Dict:
        """
        Compare multiple policies in same mode
        
        Args:
            policies: Dict mapping policy_name -> policy_function
            tasks: Evaluation tasks
            mode: Execution mode for comparison
        
        Returns:
            Dict with policy rankings and comparison
        
        Example:
            policies = {
                'aggressive': aggressive_agent,
                'conservative': conservative_agent,
                'adaptive': adaptive_agent
            }
            
            comparison = env.compare_policies(policies, tasks, ExecutionMode.ECO_MODE)
            print(f"Winner: {comparison['best_policy']}")
        """
        results = {}
        
        for policy_name, policy in policies.items():
            logger.info(f"Evaluating policy: {policy_name}")
            results[policy_name] = self.evaluate_policy(
                policy,
                tasks,
                mode,
                verbose=False
            )
        
        # Rank policies by reward
        rankings = sorted(
            results.items(),
            key=lambda x: x[1]['avg_reward'],
            reverse=True
        )
        
        return {
            'mode': mode.value,
            'best_policy': rankings[0][0],
            'rankings': [
                {
                    'rank': i + 1,
                    'policy': name,
                    'avg_reward': result['avg_reward'],
                    'avg_success': result['avg_task_success']
                }
                for i, (name, result) in enumerate(rankings)
            ],
            'detailed_results': results
        }
    
    def adaptive_policy_selection(self,
                                  policies: Dict[str, Callable],
                                  tasks: List[Dict],
                                  selection_strategy: str = 'best_per_mode') -> Dict:
        """
        Select best policy per execution mode
        
        Tests all policies in all modes and recommends which policy
        to use in each deployment scenario.
        
        Args:
            policies: Dict of policy_name -> policy_function
            tasks: Evaluation tasks
            selection_strategy: 'best_per_mode' or 'most_versatile'
        
        Returns:
            Dict with policy recommendations per mode
        
        Example:
            recommendations = env.adaptive_policy_selection(
                policies={'A': agent_a, 'B': agent_b},
                tasks=test_tasks
            )
            
            # Deploy different policies per scenario
            if scenario == 'eco':
                agent = recommendations['eco_mode']['policy']
        """
        # Evaluate all policies in all modes
        all_results = {}
        
        for policy_name, policy in policies.items():
            all_results[policy_name] = self.multi_mode_evaluation(
                policy,
                tasks,
                verbose=False
            )
        
        if selection_strategy == 'best_per_mode':
            recommendations = {}
            
            for mode in [ExecutionMode.ECO_MODE, ExecutionMode.FAST_MODE,
                        ExecutionMode.ACCURACY_MODE, ExecutionMode.BALANCED_MODE]:
                
                mode_key = mode.value
                
                # Find best policy for this mode
                best_policy = max(
                    all_results.items(),
                    key=lambda x: x[1]['evaluations'][mode_key]['avg_reward']
                )
                
                recommendations[mode_key] = {
                    'policy': best_policy[0],
                    'reward': best_policy[1]['evaluations'][mode_key]['avg_reward'],
                    'details': best_policy[1]['evaluations'][mode_key]
                }
            
            return {
                'strategy': 'best_per_mode',
                'recommendations': recommendations,
                'all_results': all_results
            }
        
        elif selection_strategy == 'most_versatile':
            # Find policy with best average reward across all modes
            versatility_scores = {}
            
            for policy_name, results in all_results.items():
                avg_reward = np.mean([
                    results['evaluations'][mode]['avg_reward']
                    for mode in results['evaluations']
                ])
                versatility_scores[policy_name] = avg_reward
            
            most_versatile = max(versatility_scores.items(), key=lambda x: x[1])
            
            return {
                'strategy': 'most_versatile',
                'best_versatile_policy': most_versatile[0],
                'versatility_score': most_versatile[1],
                'versatility_scores': versatility_scores,
                'all_results': all_results
            }
    
    def _compare_modes(self, evaluations: Dict) -> Dict:
        """Compare performance across modes"""
        comparison = {}
        
        for mode, result in evaluations.items():
            comparison[mode] = {
                'avg_reward': result['avg_reward'],
                'avg_success': result['avg_task_success'],
                'avg_energy': result['summary']['avg_energy_kwh'],
                'avg_carbon': result['summary']['avg_carbon_kg'],
                'avg_latency': result['summary']['avg_latency_ms']
            }
        
        return comparison
    
    def _generate_recommendations(self, evaluations: Dict) -> Dict:
        """Generate deployment recommendations"""
        recommendations = {}
        
        # Analyze mode characteristics
        for mode, result in evaluations.items():
            if mode == 'eco':
                if result['summary']['avg_energy_kwh'] < 0.005:
                    recommendations[mode] = "Excellent for batch processing and non-urgent tasks"
                else:
                    recommendations[mode] = "Consider optimizing energy consumption further"
            
            elif mode == 'fast':
                if result['summary']['avg_latency_ms'] < 200:
                    recommendations[mode] = "Suitable for real-time applications"
                else:
                    recommendations[mode] = "Latency may be too high for real-time use"
            
            elif mode == 'accuracy':
                if result['avg_task_success'] > 0.90:
                    recommendations[mode] = "Excellent for research and high-accuracy needs"
                else:
                    recommendations[mode] = "Consider improving accuracy further"
            
            elif mode == 'balanced':
                recommendations[mode] = "Good general-purpose deployment"
        
        return recommendations
    
    def get_execution_history(self) -> List[Dict]:
        """Get history of all evaluations"""
        return self.execution_history
    
    def clear_history(self):
        """Clear execution history"""
        self.execution_history = []
        logger.info("Cleared execution history")
