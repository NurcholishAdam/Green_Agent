import json
from pathlib import Path
from typing import Dict

class BioParameterCatalog:
    """Curated catalog of organism‑like efficiency profiles."""
    def __init__(self, catalog_path: Path = Path("./bio_parameters.json")):
        self.catalog_path = catalog_path
        self.parameters = self._load()

    def _load(self) -> Dict:
        if self.catalog_path.exists():
            with open(self.catalog_path) as f:
                return json.load(f)
        # Default catalog
        default = {
            "organism_types": {
                "high_efficiency": {
                    "photosynthetic_efficiency": 0.8,
                    "resilience_to_stress": 0.6,
                    "carbon_fixation_rate": 0.9,
                    "helium_affinity": 0.7
                },
                "high_robustness": {
                    "photosynthetic_efficiency": 0.5,
                    "resilience_to_stress": 0.9,
                    "carbon_fixation_rate": 0.6,
                    "helium_affinity": 0.5
                },
                "low_carbon": {
                    "photosynthetic_efficiency": 0.7,
                    "resilience_to_stress": 0.5,
                    "carbon_fixation_rate": 0.4,
                    "helium_affinity": 0.3
                }
            }
        }
        self.save(default)
        return default

    def save(self, data: Dict):
        with open(self.catalog_path, 'w') as f:
            json.dump(data, f, indent=2)

    def get_parameters(self, organism_type: str) -> Dict:
        return self.parameters.get("organism_types", {}).get(organism_type, {})
