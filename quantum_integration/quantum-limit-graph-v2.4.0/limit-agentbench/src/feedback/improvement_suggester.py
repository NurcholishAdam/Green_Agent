# Add: src/feedback/improvement_suggester.py

class SustainabilityImprovementSuggester:
    """Generates actionable feedback for agents"""
    
    def generate(self, analysis: dict, result: dict) -> list[str]:
        """Generate sustainability-focused improvement suggestions"""
        suggestions = []
        
        # Energy efficiency suggestions
        if result["energy_kwh"] > self.baseline * 1.5:
            suggestions.append(
                f"⚡ Energy usage {result['energy_kwh']:.4f} kWh is 50% above "
                f"baseline {self.baseline:.4f} kWh. Consider:\n"
                f"  • Reducing inference calls\n"
                f"  • Using smaller models for subtasks\n"
                f"  • Caching repeated computations"
            )
        
        # Carbon optimization suggestions
        if result["carbon_co2e_kg"] > self.carbon_threshold:
            suggestions.append(
                f"🌍 Carbon emissions {result['carbon_co2e_kg']:.4f} kg CO2e "
                f"exceed threshold. Try:\n"
                f"  • Scheduling tasks during low-carbon hours\n"
                f"  • Using more efficient hardware profiles\n"
                f"  • Optimizing model selection"
            )
        
        return suggestions