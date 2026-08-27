# community_data.py

"""
Community-driven data sharing for Green Agent.

Users can optionally contribute anonymized data to improve simulations.
"""

import json
from pathlib import Path
from datetime import datetime

class CommunityDataHub:
    """
    Optional community data sharing.
    
    Users can choose to contribute:
    - Anonymized grid carbon observations
    - Helium price observations from invoices
    - Recovery efficiency data
    """
    
    DATA_DIR = Path.home() / '.green_agent' / 'community_data'
    
    @classmethod
    def contribute_carbon_observation(cls, region: str, intensity: float, source: str):
        """Contribute observed carbon intensity (optional)"""
        observation = {
            'timestamp': datetime.now().isoformat(),
            'region': region,
            'intensity': intensity,
            'source': source,
            'anonymous': True
        }
        
        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(cls.DATA_DIR / 'carbon_observations.jsonl', 'a') as f:
            f.write(json.dumps(observation) + '\n')
    
    @classmethod
    def get_community_average(cls, region: str, days: int = 30) -> Optional[float]:
        """Get average intensity from community data (if available)"""
        if not cls.DATA_DIR.exists():
            return None
        
        observations = []
        cutoff = datetime.now().timestamp() - days * 86400
        
        with open(cls.DATA_DIR / 'carbon_observations.jsonl', 'r') as f:
            for line in f:
                data = json.loads(line)
                if data['region'] == region and data['timestamp'] > cutoff:
                    observations.append(data['intensity'])
        
        if observations:
            return sum(observations) / len(observations)
        return None
