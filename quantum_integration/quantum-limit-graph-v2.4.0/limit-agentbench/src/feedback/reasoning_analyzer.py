# Add: src/feedback/reasoning_analyzer.py

class ReasoningTraceAnalyzer:
    """Analyzes agent reasoning for quality metrics"""
    
    def analyze(self, trace: list[dict]) -> dict:
        """
        Returns:
        {
            "coherence_score": float,      # Logical flow
            "factuality_score": float,     # Correctness
            "efficiency_score": float,     # Steps needed
            "energy_awareness": float,     # Considers sustainability
            "step_breakdown": list[dict]   # Per-step analysis
        }
        """
        
    def _assess_energy_awareness(self, trace: list[dict]) -> float:
        """Check if agent considered energy impact in decisions"""
        keywords = ["energy", "efficient", "optimize", "carbon", "sustainable"]
        mentions = sum(
            any(kw in step.get("thought", "").lower() for kw in keywords)
            for step in trace
        )
        return min(mentions / len(trace), 1.0)