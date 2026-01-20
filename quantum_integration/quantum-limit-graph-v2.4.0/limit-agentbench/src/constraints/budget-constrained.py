# Create budget-constrained evaluation suite
class BudgetConstrainedEvaluation:
    """Evaluate agents under budget constraints"""
    
    def __init__(self, budget: Budget):
        self.budget = budget
    
    def evaluate_suite(self, agent, tasks):
        """Run task suite with budget enforcement"""
        enforcer = BudgetEnforcer(self.budget)
        results = []
        
        for task in tasks:
            result = enforcer.execute_with_budget(
                agent_fn=agent.execute,
                task=task,
                estimated_consumption=self._estimate(task)
            )
            
            if result['budget_violated']:
                break  # Stop if budget exceeded
            
            results.append(result)
        
        return {
            'completed_tasks': len(results),
            'total_tasks': len(tasks),
            'budget_report': enforcer.get_budget_report()
        }
