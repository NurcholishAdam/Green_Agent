class MetaCognitiveLayer:
    """
    Self-reflection layer.
    """

    def reflect(self, accuracy: float, energy: float) -> str:
        return (
            f"I achieved accuracy {accuracy:.2f} "
            f"using {energy:.2f}J. "
            "Trade-off evaluated."
        )
