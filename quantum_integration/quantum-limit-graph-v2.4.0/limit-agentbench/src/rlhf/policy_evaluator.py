# src/rlhf/policy_evaluator.py

class PolicyEvaluationEnvironment:
    """Green_Agent as RLHF policy evaluation environment"""
    
    def evaluate_policy(self, agent_policy, tasks, mode):
        """Evaluate agent policy in specific mode"""
        shaper = RewardShaper(mode)
        
        rewards = []
        for task in tasks:
            result = agent_policy(task)
            
            reward_data = shaper.compute_reward(
                task_success=result['accuracy'],
                energy_kwh=result['energy_kwh'],
                carbon_kg=result['carbon_kg'],
                latency_ms=result['latency_ms']
            )
            
            rewards.append(reward_data)
        
        return {
            'avg_reward': np.mean([r['reward'] for r in rewards]),
            'mode': mode.value,
            'policy_performance': rewards
        }

def compare_policies_across_modes(agent_policies, tasks):
    """Compare policies across all execution modes"""
    env = PolicyEvaluationEnvironment()
    
    results = {}
    for policy_name, policy in agent_policies.items():
        results[policy_name] = {}
        
        for mode in ExecutionMode:
            if mode != ExecutionMode.CUSTOM:
                eval_result = env.evaluate_policy(policy, tasks, mode)
                results[policy_name][mode.value] = eval_result
    
    return results
