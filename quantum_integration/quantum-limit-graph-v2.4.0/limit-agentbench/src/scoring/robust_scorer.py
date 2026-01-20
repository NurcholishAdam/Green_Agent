# Add: src/scoring/robust_scorer.py

class RobustSustainabilityScorer:
    """Handles all failure modes gracefully"""
    
    def score(self, result: dict, ground_truth: dict) -> dict:
        """
        Returns:
        {
            "accuracy": float,
            "partial_credit": float,
            "energy_efficiency": float,
            "sustainability_index": float,
            "failure_category": str | None,
            "penalty": float
        }
        """
        if result["status"] == "timeout":
            return self._handle_timeout(result, ground_truth)
        elif result["status"] == "error":
            return self._handle_error(result, ground_truth)
        elif result["status"] == "oom":
            return self._handle_oom(result)
        else:
            return self._score_success(result, ground_truth)
    
    def _handle_timeout(self, result, ground_truth):
        """Award partial credit for timeout with partial output"""
        partial_output = result.get("partial_output", "")
        similarity = self._compute_similarity(partial_output, ground_truth)
        
        return {
            "accuracy": 0.0,
            "partial_credit": similarity * 0.5,  # 50% max for timeout
            "energy_efficiency": result["energy_used"] / result["time_limit"],
            "sustainability_index": similarity * 0.25,  # Heavily penalized
            "failure_category": "timeout",
            "penalty": 0.5
        }