# src/enhancements/export_ai_datacenter_data.py
"""
Export AI Data Center Map to CSV

Creates a machine-readable dataset from the AI Data Center Map
for versioning in the GitHub repository.
"""

import csv
from pathlib import Path
import logging
from .ai_data_center_loader import AIDataCenterLoader

logger = logging.getLogger(__name__)


def export_to_csv(output_path: Path = None):
    """Export all projects to CSV"""
    if output_path is None:
        output_path = Path(__file__).parent / "data" / "ai_datacenters_world.csv"
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    loader = AIDataCenterLoader()
    df = loader.to_dataframe()
    df.to_csv(output_path, index=False)
    logger.info(f"Exported {len(df)} projects to {output_path}")
    
    # Print summary
    print(f"\n=== AI Data Center Map Export ===")
    print(f"Total projects: {len(df)}")
    print(f"Total capacity: {df['capacity_mw'].sum():.0f} MW")
    print(f"Countries: {df['location'].str.split(', ').str[-1].nunique()}")
    print(f"Average Green Score: {df['green_score'].mean():.1f}")
    print(f"File saved to: {output_path}")
    
    return output_path


if __name__ == "__main__":
    export_to_csv()
