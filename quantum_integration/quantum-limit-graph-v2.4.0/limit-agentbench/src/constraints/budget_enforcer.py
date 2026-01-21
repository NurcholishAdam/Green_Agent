"""
Budget Enforcer for Green_Agent

Enforces budget constraints during agent evaluation.
Provides pre-execution checking and post-execution tracking.
"""

from typing import Dict, Callable, Optional, Any
import asyncio
import time
import logging
from datetime import datetime

from .budget_manager import Budget, BudgetManager, BudgetStatus

logger = logging.getLogger(__name__)


class BudgetEnforcer:
    """
    Enforces budget constraints during agent execution
    
    Wraps agent execution with budget checking and enforcement.
    Prevents execution if budget would be exceeded.
    """
    
    def __init__(self, budget: Budget):
        """
        Initialize budget enforcer
        
        Args:
            budget: Budget constraints to enforce
        """
        self.manager = BudgetManager(budget)
        self.fallback_handlers = {}
        self.pre_execution_hooks = []
        self.post_execution_hooks = []
        
        logger.info(f"Initialized BudgetEnforcer with {budget.name}")
    
    async def execute_with_budget(self,
                                  agent_fn: Callable,
                                  task: Dict,
                                  estimated_consumption: Dict[str, float],
                                  timeout: Optional[float] = None) -> Dict:
        """
        Execute agent function with budget enforcement
        
        Args:
            agent_fn: Agent function to execute (can be sync or async)
            task: Task data to pass to agent
            estimated_consumption: Estimated resource consumption:
                {
                    'energy_wh': float,
                    'carbon_g': float,
                    'latency_ms': float
                }
            timeout: Optional timeout in seconds
        
        Returns:
            Dict with execution results:
            {
                'result': Any,
                'success': bool,
                'budget_violated': bool,
                'violations': List[str],
                'actual_consumption': Dict,
                'remaining_budget': Dict,
                'execution_time_ms': float
            }
        
        Example:
            async def my_agent(task):
                return {'output': 'result', 'accuracy': 0.95}
            
            result = await enforcer.execute_with_budget(
                agent_fn=my_agent,
                task={'input': 'data'},
                estimated_consumption={
                    'energy_wh': 2.0,
                    'carbon_g': 0.4,
                    'latency_ms': 500
                }
            )
            
            if result['success']:
                print(f"Output: {result['result']}")
        """
        start_time = time.time()
        
        # Run pre-execution hooks
        for hook in self.pre_execution_hooks:
            try:
                hook(task, estimated_consumption)
            except Exception as e:
                logger.error(f"Pre-execution hook failed: {e}")
        
        # Check if execution is allowed
        can_execute, violations = self.manager.can_execute(estimated_consumption)
        
        if not can_execute:
            logger.warning(f"Budget check failed: {violations}")
            
            # Try fallback handler
            if 'budget_exceeded' in self.fallback_handlers:
                try:
                    fallback_result = await self._execute_fallback(
                        'budget_exceeded',
                        task,
                        violations
                    )
                    return fallback_result
                except Exception as e:
                    logger.error(f"Fallback handler failed: {e}")
            
            return {
                'result': None,
                'success': False,
                'budget_violated': True,
                'violations': violations,
                'actual_consumption': {},
                'remaining_budget': self.manager.get_remaining_budget(),
                'execution_time_ms': (time.time() - start_time) * 1000,
                'message': f"Budget exceeded for: {', '.join(violations)}",
                'timestamp': datetime.now().isoformat()
            }
        
        # Execute agent with timeout
        try:
            if timeout:
                if asyncio.iscoroutinefunction(agent_fn):
                    result = await asyncio.wait_for(
                        agent_fn(task),
                        timeout=timeout
                    )
                else:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(agent_fn, task),
                        timeout=timeout
                    )
            else:
                if asyncio.iscoroutinefunction(agent_fn):
                    result = await agent_fn(task)
                else:
                    result = await asyncio.to_thread(agent_fn, task)
            
            execution_time_ms = (time.time() - start_time) * 1000
            
            # Extract metrics from result
            actual_consumption = self._extract_consumption(
                result,
                execution_time_ms,
                estimated_consumption
            )
            
            # Record actual consumption
            self.manager.record_consumption(
                actual_consumption,
                metadata={
                    'task_id': task.get('task_id'),
                    'agent_fn': agent_fn.__name__,
                    'timestamp': datetime.now()
                }
            )
            
            # Run post-execution hooks
            for hook in self.post_execution_hooks:
                try:
                    hook(result, actual_consumption)
                except Exception as e:
                    logger.error(f"Post-execution hook failed: {e}")
            
            return {
                'result': result,
                'success': True,
                'budget_violated': False,
                'violations': [],
                'actual_consumption': actual_consumption,
                'remaining_budget': self.manager.get_remaining_budget(),
                'execution_time_ms': execution_time_ms,
                'timestamp': datetime.now().isoformat()
            }
            
        except asyncio.TimeoutError:
            logger.error(f"Execution timeout after {timeout}s")
            
            # Try timeout fallback
            if 'timeout' in self.fallback_handlers:
                try:
                    fallback_result = await self._execute_fallback(
                        'timeout',
                        task,
                        {'timeout_seconds': timeout}
                    )
                    return fallback_result
                except Exception as e:
                    logger.error(f"Timeout fallback failed: {e}")
            
            return {
                'result': None,
                'success': False,
                'budget_violated': False,
                'violations': ['timeout'],
                'actual_consumption': {},
                'remaining_budget': self.manager.get_remaining_budget(),
                'execution_time_ms': (time.time() - start_time) * 1000,
                'error': f"Execution timeout after {timeout}s",
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Execution error: {e}")
            
            # Try error fallback
            if 'error' in self.fallback_handlers:
                try:
                    fallback_result = await self._execute_fallback(
                        'error',
                        task,
                        {'error': str(e)}
                    )
                    return fallback_result
                except Exception as fallback_error:
                    logger.error(f"Error fallback failed: {fallback_error}")
            
            return {
                'result': None,
                'success': False,
                'budget_violated': False,
                'violations': [],
                'actual_consumption': {},
                'remaining_budget': self.manager.get_remaining_budget(),
                'execution_time_ms': (time.time() - start_time) * 1000,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def execute_batch_with_budget(self,
                                  agent_fn: Callable,
                                  tasks: list,
                                  estimated_consumption_per_task: Dict[str, float]) -> Dict:
        """
        Execute batch of tasks with budget enforcement
        
        Stops execution when budget is exhausted.
        
        Args:
            agent_fn: Agent function
            tasks: List of task dictionaries
            estimated_consumption_per_task: Estimated consumption per task
        
        Returns:
            Dict with batch results:
            {
                'completed_tasks': int,
                'total_tasks': int,
                'results': List[Dict],
                'stopped_reason': str,
                'budget_summary': Dict
            }
        """
        results = []
        
        for i, task in enumerate(tasks):
            # Check budget before each task
            can_execute, violations = self.manager.can_execute(
                estimated_consumption_per_task
            )
            
            if not can_execute:
                logger.warning(f"Budget exhausted after {i} tasks: {violations}")
                return {
                    'completed_tasks': i,
                    'total_tasks': len(tasks),
                    'results': results,
                    'stopped_reason': f"Budget exceeded: {violations}",
                    'budget_summary': self.manager.get_summary()
                }
            
            # Execute task
            result = asyncio.run(self.execute_with_budget(
                agent_fn,
                task,
                estimated_consumption_per_task
            ))
            
            results.append(result)
            
            if not result['success']:
                logger.warning(f"Task {i} failed: {result.get('error', 'unknown')}")
        
        return {
            'completed_tasks': len(results),
            'total_tasks': len(tasks),
            'results': results,
            'stopped_reason': 'all_tasks_completed',
            'budget_summary': self.manager.get_summary()
        }
    
    def register_fallback(self, event: str, handler: Callable):
        """
        Register fallback handler for specific events
        
        Args:
            event: Event type ('budget_exceeded', 'timeout', 'error')
            handler: Async function to call when event occurs
        
        Example:
            async def cheap_fallback(task, context):
                # Use cheaper model when budget exceeded
                return await lightweight_model(task)
            
            enforcer.register_fallback('budget_exceeded', cheap_fallback)
        """
        self.fallback_handlers[event] = handler
        logger.info(f"Registered fallback handler for '{event}'")
    
    def add_pre_execution_hook(self, hook: Callable):
        """
        Add hook to run before execution
        
        Args:
            hook: Function(task, estimated_consumption) to call
        """
        self.pre_execution_hooks.append(hook)
    
    def add_post_execution_hook(self, hook: Callable):
        """
        Add hook to run after execution
        
        Args:
            hook: Function(result, actual_consumption) to call
        """
        self.post_execution_hooks.append(hook)
    
    async def _execute_fallback(self,
                                event: str,
                                task: Dict,
                                context: Any) -> Dict:
        """Execute fallback handler"""
        handler = self.fallback_handlers[event]
        
        if asyncio.iscoroutinefunction(handler):
            fallback_result = await handler(task, context)
        else:
            fallback_result = await asyncio.to_thread(handler, task, context)
        
        return {
            'result': fallback_result,
            'success': True,
            'budget_violated': False,
            'violations': [],
            'actual_consumption': {},
            'remaining_budget': self.manager.get_remaining_budget(),
            'fallback_used': event,
            'timestamp': datetime.now().isoformat()
        }
    
    def _extract_consumption(self,
                            result: Any,
                            execution_time_ms: float,
                            estimated: Dict[str, float]) -> Dict[str, float]:
        """
        Extract actual consumption from result
        
        Tries to extract metrics from result, falls back to estimates
        """
        consumption = {
            'energy_wh': estimated.get('energy_wh', 0.0),
            'carbon_g': estimated.get('carbon_g', 0.0),
            'latency_ms': execution_time_ms,
            'cost_usd': estimated.get('cost_usd', 0.0)
        }
        
        # If result has metrics, use them
        if isinstance(result, dict):
            if 'metrics' in result:
                metrics = result['metrics']
                consumption['energy_wh'] = metrics.get('energy_kwh', consumption['energy_wh'] / 1000) * 1000
                consumption['carbon_g'] = metrics.get('carbon_kg', consumption['carbon_g'] / 1000) * 1000
                consumption['latency_ms'] = metrics.get('latency_ms', execution_time_ms)
                consumption['cost_usd'] = metrics.get('cost_usd', consumption['cost_usd'])
        
        return consumption
    
    def get_budget_report(self) -> Dict:
        """
        Generate comprehensive budget report
        
        Returns:
            Dict with budget utilization and history
        """
        summary = self.manager.get_summary()
        
        return {
            **summary,
            'execution_history': [
                {
                    'timestamp': exec['timestamp'].isoformat(),
                    'consumption': exec['consumption'],
                    'metadata': exec['metadata']
                }
                for exec in self.manager.execution_history[-10:]  # Last 10
            ],
            'violation_history': [
                {
                    'timestamp': v['timestamp'].isoformat(),
                    'violations': v['violations'],
                    'estimated': v['estimated_consumption']
                }
                for v in self.manager.violation_history
            ]
        }
    
    def reset(self):
        """Reset budget tracking"""
        self.manager.reset()
        logger.info("Budget enforcer reset")
