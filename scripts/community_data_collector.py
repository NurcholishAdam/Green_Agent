#!/usr/bin/env python3
# scripts/community_data_collector.py

"""
Community data collector for Green Agent.
Runs in background to collect and share anonymized data.

Usage:
    python community_data_collector.py --contribute
    python community_data_collector.py --fetch
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.integration.free_apis import CommunityDataHub, FreeAPIManager


async def collect_and_contribute():
    """Collect local observations and contribute to community"""
    print("🌱 Green Agent - Community Data Collector")
    print("=" * 50)
    
    # Initialize manager
    manager = FreeAPIManager()
    
    # Get current observations
    print("\n📊 Collecting current observations...")
    
    # Observe carbon intensity
    intensity, source, conf = await manager.get_carbon_intensity('us-east')
    CommunityDataHub.contribute_carbon_observation('us-east', intensity, source)
    print(f"   ✅ Carbon: {intensity:.0f} gCO2/kWh (source: {source})")
    
    # Observe helium price
    helium = await manager.get_helium_data()
    CommunityDataHub.contribute_helium_observation(helium.spot_price_usd_per_liter, 
                                                    helium.inventory_days,
                                                    helium.source)
    print(f"   ✅ Helium: ${helium.spot_price_usd_per_liter:.2f}/L")
    
    print("\n✅ Data collection complete")
    print("   Thank you for contributing to the Green Agent community!")


async def fetch_and_display():
    """Fetch and display community data"""
    print("🌍 Green Agent - Community Data")
    print("=" * 50)
    
    # Get community averages
    carbon_avg = CommunityDataHub.get_community_carbon_average('us-east')
    if carbon_avg:
        print(f"\n📊 Community Carbon Average (us-east): {carbon_avg:.0f} gCO2/kWh")
    else:
        print("\n📊 No community carbon data yet. Be the first to contribute!")
    
    helium_avg = CommunityDataHub.get_community_helium_price()
    if helium_avg:
        print(f"🎈 Community Helium Average: ${helium_avg:.2f}/L")
    else:
        print("🎈 No community helium data yet. Be the first to contribute!")


def main():
    parser = argparse.ArgumentParser(description='Green Agent Community Data Collector')
    parser.add_argument('--contribute', action='store_true', 
                        help='Collect and contribute observations')
    parser.add_argument('--fetch', action='store_true',
                        help='Fetch and display community data')
    
    args = parser.parse_args()
    
    if args.contribute:
        asyncio.run(collect_and_contribute())
    elif args.fetch:
        asyncio.run(fetch_and_display())
    else:
        print("Please specify --contribute or --fetch")
        parser.print_help()


if __name__ == "__main__":
    main()
