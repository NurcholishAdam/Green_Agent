# File: src/enhancements/node_registry.py
"""
Node Registry – unified descriptor for all compute nodes.
"""

import json
import asyncio
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field
from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from ..database.manager import DatabaseManager

logger = logging.getLogger(__name__)

Base = declarative_base()

class NodeDescriptorDB(Base):
    __tablename__ = 'node_descriptors'
    node_id = Column(String(128), primary_key=True)
    location = Column(String(64))
    energy_efficiency = Column(Float)
    carbon_intensity = Column(Float)
    helium_index = Column(Float)
    material_index = Column(Float)
    cooling_type = Column(String(32))
    renewable_fraction = Column(Float)
    harvester_type = Column(String(32), nullable=True)
    capture_efficiency = Column(Float, nullable=True)
    energy_output_watts = Column(Float, nullable=True)
    availability_pattern = Column(JSON, nullable=True)
    last_updated = Column(DateTime, default=datetime.now)

class NodeDescriptor(BaseModel):
    node_id: str
    location: str
    energy_efficiency: float
    carbon_intensity: float
    helium_index: float
    material_index: float
    cooling_type: str
    renewable_fraction: float
    harvester_type: Optional[str] = None
    capture_efficiency: Optional[float] = None
    energy_output_watts: Optional[float] = None
    availability_pattern: Optional[Dict[str, Any]] = None
    last_updated: datetime = Field(default_factory=datetime.now)

class NodeRegistry:
    """
    Registry for node descriptors, with persistence and periodic refresh.
    """
    def __init__(self, config: Dict[str, Any], db_manager: DatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.cache: Dict[str, NodeDescriptor] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self, refresh_interval: int = 3600):
        """Start background refresh loop."""
        self._running = True
        self._task = asyncio.create_task(self._refresh_loop(refresh_interval))
        logger.info("NodeRegistry started")

    async def _refresh_loop(self, interval: int):
        while self._running:
            try:
                await self._refresh_all_nodes()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Node refresh error: {e}")
                await asyncio.sleep(60)

    async def _refresh_all_nodes(self):
        """Fetch fresh data for all known nodes (e.g., from cloud APIs)."""
        # Placeholder – you would call cloud provider APIs here
        # For demo, we just update timestamps
        async with self._lock:
            for node_id in list(self.cache.keys()):
                self.cache[node_id].last_updated = datetime.now()
        logger.info("Node descriptors refreshed (simulated)")

    async def register_node(self, descriptor: NodeDescriptor) -> bool:
        """Register or update a node descriptor."""
        async with self._lock:
            self.cache[descriptor.node_id] = descriptor
        # Persist to DB
        with self.db_manager.get_session() as session:
            session.execute(
                text("""
                    INSERT OR REPLACE INTO node_descriptors
                    (node_id, location, energy_efficiency, carbon_intensity, helium_index, material_index,
                     cooling_type, renewable_fraction, harvester_type, capture_efficiency, energy_output_watts,
                     availability_pattern, last_updated)
                    VALUES (:node_id, :location, :energy_efficiency, :carbon_intensity, :helium_index, :material_index,
                     :cooling_type, :renewable_fraction, :harvester_type, :capture_efficiency, :energy_output_watts,
                     :availability_pattern, :last_updated)
                """),
                {
                    'node_id': descriptor.node_id,
                    'location': descriptor.location,
                    'energy_efficiency': descriptor.energy_efficiency,
                    'carbon_intensity': descriptor.carbon_intensity,
                    'helium_index': descriptor.helium_index,
                    'material_index': descriptor.material_index,
                    'cooling_type': descriptor.cooling_type,
                    'renewable_fraction': descriptor.renewable_fraction,
                    'harvester_type': descriptor.harvester_type,
                    'capture_efficiency': descriptor.capture_efficiency,
                    'energy_output_watts': descriptor.energy_output_watts,
                    'availability_pattern': json.dumps(descriptor.availability_pattern),
                    'last_updated': datetime.now()
                }
            )
        logger.info(f"Node {descriptor.node_id} registered")
        return True

    async def get_node(self, node_id: str) -> Optional[NodeDescriptor]:
        async with self._lock:
            return self.cache.get(node_id)

    async def list_nodes(self) -> List[str]:
        async with self._lock:
            return list(self.cache.keys())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("NodeRegistry stopped")
