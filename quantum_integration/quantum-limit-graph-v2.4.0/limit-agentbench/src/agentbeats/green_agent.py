# Update src/agentbeats/green_agent.py

from analysis.pareto_analyzer import ParetoFrontierAnalyzer, ParetoPoint

class GreenSustainabilityAgent:
    def __init__(self):
        self.pareto_analyzer = ParetoFrontierAnalyzer()
    
    async def score_with_pareto(self, results: List[Dict]) -> Dict:
        """Score agents using Pareto optimality"""
        points = [
            ParetoPoint(
                agent_id=r['agent_id'],
                accuracy=r['accuracy'],
                energy_kwh=r['energy_kwh'],
                carbon_co2e_kg=r['carbon_kg'],
                latency_ms=r['latency_ms']
            ) for r in results
        ]
        
        frontier = self.pareto_analyzer.compute_frontier(points)
        ranks = self.pareto_analyzer.rank_by_dominance(points)
        
        return {
            'frontier': frontier,
            'ranks': ranks,
            'knee_point': self.pareto_analyzer.get_knee_point(frontier)
        }
