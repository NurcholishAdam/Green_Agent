# Update src/agentbeats/a2a_handler.py

from constraints.budget_enforcer import BudgetEnforcer, Budget

class A2AHandler:
    def __init__(self, budget: Budget = None):
        self.budget_enforcer = BudgetEnforcer(budget) if budget else None
    
    async def send_task_with_budget(self, agent_url: str, task: dict):
        """Send task with budget constraints"""
        if self.budget_enforcer:
            estimated_consumption = self._estimate_consumption(task)
            
            can_execute, violations = self.budget_enforcer.manager.can_execute(
                estimated_consumption
            )
            
            if not can_execute:
                return {
                    'status': 'budget_exceeded',
                    'violations': violations
                }
        
        # Proceed with normal execution
        return await self.send_task(agent_url, task)
