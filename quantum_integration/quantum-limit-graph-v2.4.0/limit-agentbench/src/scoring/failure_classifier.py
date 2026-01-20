# Add: src/scoring/failure_classifier.py

class FailureClassifier:
    """Classifies agent failures for better debugging"""
    
    CATEGORIES = {
        "timeout": "Agent exceeded time limit",
        "oom": "Out of memory during execution",
        "tool_error": "Failed to use required tools",
        "invalid_output": "Output format incorrect",
        "hallucination": "Generated false information",
        "energy_exceeded": "Exceeded energy budget"
    }
    
    def classify(self, error: Exception, context: dict) -> str:
        """Classify failure type from exception and context"""
