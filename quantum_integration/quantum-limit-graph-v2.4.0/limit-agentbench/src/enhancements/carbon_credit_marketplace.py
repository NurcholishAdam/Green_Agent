#!/usr/bin/env python3
# File: src/enhancements/carbon_credit_marketplace.py
"""
Carbon Credit Marketplace for Green Agent v1.0.0
- Lists offset projects from registry (simulated).
- Purchases and retires carbon credits.
- Integrates with blockchain for audit trail.
- Maintains a local ledger using SQLAlchemy.
"""

import asyncio
import logging
import json
import hashlib
import uuid
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

import aiohttp
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, text
from sqlalchemy.ext.declarative import declarative_base

from ..database.manager import DatabaseManager
from ..blockchain import BlockchainCarbonCredits  # your existing module
from ..carbon_manager import CarbonIntensityManager
from ..unified_sustainability_engine import UnifiedSustainabilityEngine

logger = logging.getLogger(__name__)

Base = declarative_base()

# ======================================================================
# Database Models
# ======================================================================

class CreditTransactionDB(Base):
    __tablename__ = 'credit_transactions'
    id = Column(Integer, primary_key=True)
    tx_id = Column(String(64), unique=True, index=True)
    project_id = Column(String(128))
    amount_kg = Column(Float)
    cost_usd = Column(Float)
    status = Column(String(32))  # 'purchased', 'retired', 'cancelled'
    retires_at = Column(DateTime, nullable=True)
    blockchain_tx_hash = Column(String(128), nullable=True)
    metadata = Column(JSON)
    created_at = Column(DateTime, default=datetime.now)

class CreditProjectDB(Base):
    __tablename__ = 'credit_projects'
    project_id = Column(String(128), primary_key=True)
    name = Column(String(256))
    registry = Column(String(64))
    available_credits_kg = Column(Float)
    price_per_kg_usd = Column(Float)
    verification_status = Column(String(32))
    metadata = Column(JSON)
    last_updated = Column(DateTime, default=datetime.now)

# ======================================================================
# Pydantic Models
# ======================================================================

class CreditProject(BaseModel):
    project_id: str
    name: str
    registry: str
    available_credits_kg: float = Field(ge=0)
    price_per_kg_usd: float = Field(ge=0)
    verification_status: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

class CreditPurchaseRequest(BaseModel):
    project_id: str
    amount_kg: float = Field(gt=0)
    retire_immediately: bool = False
    reason: Optional[str] = None

class CreditRetireRequest(BaseModel):
    tx_id: str
    amount_kg: float = Field(gt=0)
    reason: Optional[str] = None

class CreditTransaction(BaseModel):
    tx_id: str
    project_id: str
    amount_kg: float
    cost_usd: float
    status: str  # 'purchased', 'retired', 'cancelled'
    retires_at: Optional[datetime] = None
    blockchain_tx_hash: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)

# ======================================================================
# Carbon Credit Marketplace
# ======================================================================

class CarbonCreditMarketplace:
    """
    Manages carbon offset projects, purchases, and retirement.
    Integrates with blockchain for audit and with sustainability engine for auto‑offset.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        db_manager: DatabaseManager,
        blockchain: Optional[BlockchainCarbonCredits] = None,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        sustainability_engine: Optional[UnifiedSustainabilityEngine] = None,
    ):
        self.config = config
        self.db_manager = db_manager
        self.blockchain = blockchain
        self.carbon_manager = carbon_manager
        self.sustainability_engine = sustainability_engine

        # Registry simulation data
        self._projects_cache: Dict[str, CreditProject] = {}
        self._last_project_refresh: Optional[datetime] = None
        self._refresh_interval = config.get('refresh_interval_seconds', 3600)

        # For simulated registry, we seed some projects
        self._seed_projects()

        # Auto‑offset settings
        self.auto_offset_enabled = config.get('auto_offset_enabled', False)
        self.auto_offset_threshold_kg = config.get('auto_offset_threshold_kg', 100.0)

        logger.info("CarbonCreditMarketplace initialized")

    # ------------------------------------------------------------------
    # Project Management
    # ------------------------------------------------------------------

    def _seed_projects(self):
        """Seed some dummy projects for simulation."""
        dummy = [
            CreditProject(
                project_id="reforestation_amazon",
                name="Amazon Rainforest Reforestation",
                registry="Verra",
                available_credits_kg=500000,
                price_per_kg_usd=0.15,
                verification_status="verified",
                metadata={"location": "Brazil", "type": "reforestation"}
            ),
            CreditProject(
                project_id="solar_india",
                name="Solar Power India",
                registry="Gold Standard",
                available_credits_kg=300000,
                price_per_kg_usd=0.20,
                verification_status="verified",
                metadata={"location": "India", "type": "renewable_energy"}
            ),
            CreditProject(
                project_id="wind_texas",
                name="Wind Farm Texas",
                registry="Verra",
                available_credits_kg=250000,
                price_per_kg_usd=0.18,
                verification_status="verified",
                metadata={"location": "USA", "type": "renewable_energy"}
            ),
            CreditProject(
                project_id="mangrove_kenya",
                name="Mangrove Restoration Kenya",
                registry="Gold Standard",
                available_credits_kg=150000,
                price_per_kg_usd=0.25,
                verification_status="pending",
                metadata={"location": "Kenya", "type": "blue_carbon"}
            ),
        ]
        for p in dummy:
            self._projects_cache[p.project_id] = p

    async def refresh_projects(self, force: bool = False) -> List[CreditProject]:
        """
        Fetch the latest project list from the registry.
        In simulation, returns cached projects; can be extended to call real API.
        """
        now = datetime.now()
        if not force and self._last_project_refresh and (now - self._last_project_refresh).seconds < self._refresh_interval:
            return list(self._projects_cache.values())

        # In real implementation, you would call a registry API here.
        # For simulation, we just update timestamps and return.
        self._last_project_refresh = now
        return list(self._projects_cache.values())

    async def get_project(self, project_id: str) -> Optional[CreditProject]:
        """Get a project by ID (from cache)."""
        return self._projects_cache.get(project_id)

    async def list_projects(self, status: Optional[str] = None) -> List[CreditProject]:
        """List all projects, optionally filtered by verification status."""
        projects = await self.refresh_projects()
        if status:
            return [p for p in projects if p.verification_status == status]
        return projects

    # ------------------------------------------------------------------
    # Purchase & Retire
    # ------------------------------------------------------------------

    async def purchase_credits(self, request: CreditPurchaseRequest) -> CreditTransaction:
        """
        Purchase carbon credits from a project.
        - Check availability.
        - Compute cost.
        - (Optionally) mint blockchain token.
        - Save transaction to DB.
        - If retire_immediately, retire them.
        """
        project = await self.get_project(request.project_id)
        if not project:
            raise ValueError(f"Project {request.project_id} not found")

        if project.available_credits_kg < request.amount_kg:
            raise ValueError(f"Insufficient credits available: {project.available_credits_kg} kg < {request.amount_kg} kg")

        # Calculate cost
        cost = request.amount_kg * project.price_per_kg_usd

        # Generate transaction ID
        tx_id = f"cc_{uuid.uuid4().hex[:12]}"

        # Create transaction record
        tx = CreditTransaction(
            tx_id=tx_id,
            project_id=request.project_id,
            amount_kg=request.amount_kg,
            cost_usd=cost,
            status='purchased',
            metadata={"reason": request.reason or "unspecified"}
        )

        # Persist to DB
        async with self.db_manager.get_session() as session:
            session.execute(
                text("""
                    INSERT INTO credit_transactions
                    (tx_id, project_id, amount_kg, cost_usd, status, metadata)
                    VALUES (:tx_id, :project_id, :amount_kg, :cost_usd, :status, :metadata)
                """),
                {
                    'tx_id': tx_id,
                    'project_id': request.project_id,
                    'amount_kg': request.amount_kg,
                    'cost_usd': cost,
                    'status': 'purchased',
                    'metadata': json.dumps(tx.metadata)
                }
            )

        # Reduce available credits in project cache
        project.available_credits_kg -= request.amount_kg

        # Blockchain tokenization (if available)
        if self.blockchain:
            # Mint a carbon credit token (you need to implement this in your blockchain module)
            # For now, we simulate a transaction hash
            dummy_hash = f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"
            tx.blockchain_tx_hash = dummy_hash
            # Update DB with tx_hash
            async with self.db_manager.get_session() as session:
                session.execute(
                    text("UPDATE credit_transactions SET blockchain_tx_hash = :tx_hash WHERE tx_id = :tx_id"),
                    {'tx_hash': dummy_hash, 'tx_id': tx_id}
                )

        logger.info(f"Purchased {request.amount_kg} kg credits from {request.project_id} for ${cost:.2f} (tx: {tx_id})")

        # Retire immediately if requested
        if request.retire_immediately:
            await self.retire_credits(CreditRetireRequest(tx_id=tx_id, amount_kg=request.amount_kg))

        return tx

    async def retire_credits(self, request: CreditRetireRequest) -> CreditTransaction:
        """
        Retire (consume) a specified amount of credits from a transaction.
        This effectively offsets emissions.
        """
        async with self.db_manager.get_session() as session:
            result = session.execute(
                text("SELECT * FROM credit_transactions WHERE tx_id = :tx_id"),
                {'tx_id': request.tx_id}
            ).first()
            if not result:
                raise ValueError(f"Transaction {request.tx_id} not found")

            # Convert to dict
            tx_dict = dict(result._mapping)

            if tx_dict['status'] == 'retired':
                raise ValueError(f"Transaction {request.tx_id} already retired")

            remaining = tx_dict['amount_kg'] - (tx_dict.get('retired_kg', 0) or 0)
            if request.amount_kg > remaining:
                raise ValueError(f"Requested {request.amount_kg} kg > available {remaining} kg")

            # Update status (if fully retired) or partial (we'll keep full retirement for simplicity)
            new_status = 'retired' if request.amount_kg == tx_dict['amount_kg'] else 'partial_retired'

            session.execute(
                text("""
                    UPDATE credit_transactions
                    SET status = :status, retires_at = :retires_at
                    WHERE tx_id = :tx_id
                """),
                {
                    'status': new_status,
                    'retires_at': datetime.now(),
                    'tx_id': request.tx_id
                }
            )

        # If auto‑offset is enabled, notify the sustainability engine
        if self.sustainability_engine and self.auto_offset_enabled:
            await self.sustainability_engine.record_offset(request.amount_kg, source=request.tx_id)

        logger.info(f"Retired {request.amount_kg} kg credits from tx {request.tx_id}")
        return await self.get_transaction(request.tx_id)

    async def get_transaction(self, tx_id: str) -> Optional[CreditTransaction]:
        """Retrieve a transaction by ID."""
        async with self.db_manager.get_session() as session:
            result = session.execute(
                text("SELECT * FROM credit_transactions WHERE tx_id = :tx_id"),
                {'tx_id': tx_id}
            ).first()
            if not result:
                return None
            tx_dict = dict(result._mapping)
            return CreditTransaction(
                tx_id=tx_dict['tx_id'],
                project_id=tx_dict['project_id'],
                amount_kg=tx_dict['amount_kg'],
                cost_usd=tx_dict['cost_usd'],
                status=tx_dict['status'],
                retires_at=tx_dict['retires_at'],
                blockchain_tx_hash=tx_dict['blockchain_tx_hash'],
                metadata=json.loads(tx_dict['metadata']),
                created_at=tx_dict['created_at']
            )

    async def get_balance(self) -> Dict[str, Any]:
        """Return the total purchased, retired, and available credits."""
        async with self.db_manager.get_session() as session:
            total_purchased = session.execute(
                text("SELECT SUM(amount_kg) FROM credit_transactions WHERE status != 'cancelled'")
            ).scalar() or 0.0
            total_retired = session.execute(
                text("SELECT SUM(amount_kg) FROM credit_transactions WHERE status = 'retired'")
            ).scalar() or 0.0
            return {
                'total_purchased_kg': total_purchased,
                'total_retired_kg': total_retired,
                'available_kg': total_purchased - total_retired,
                'transactions_count': session.execute(
                    text("SELECT COUNT(*) FROM credit_transactions")
                ).scalar()
            }

    # ------------------------------------------------------------------
    # Auto‑offset Integration
    # ------------------------------------------------------------------

    async def auto_offset(self, emissions_kg: float, reason: str = "auto_offset"):
        """
        Automatically purchase and retire credits to offset emissions.
        If the current balance is insufficient, it will purchase additional credits.
        """
        balance = await self.get_balance()
        available = balance['available_kg']
        if available >= emissions_kg:
            # Use existing credits
            # Find a transaction with available credits and retire
            async with self.db_manager.get_session() as session:
                result = session.execute(
                    text("""
                        SELECT tx_id, amount_kg FROM credit_transactions
                        WHERE status = 'purchased'
                        ORDER BY created_at ASC
                    """)
                ).fetchall()
                to_retire = emissions_kg
                for row in result:
                    tx_id = row[0]
                    amount = row[1]
                    retire_now = min(to_retire, amount)
                    await self.retire_credits(CreditRetireRequest(tx_id=tx_id, amount_kg=retire_now, reason=reason))
                    to_retire -= retire_now
                    if to_retire <= 0:
                        break
        else:
            # Need to buy more credits
            missing = emissions_kg - available
            # Find a project with enough credits
            projects = await self.list_projects(status='verified')
            if not projects:
                logger.warning("No verified projects available for auto‑offset")
                return
            # Buy from the cheapest project
            cheapest = min(projects, key=lambda p: p.price_per_kg_usd)
            await self.purchase_credits(CreditPurchaseRequest(
                project_id=cheapest.project_id,
                amount_kg=missing,
                retire_immediately=True,
                reason=reason
            ))

        logger.info(f"Auto‑offset: offset {emissions_kg} kg CO₂")

    # ------------------------------------------------------------------
    # Background Task for Periodic Auto‑Offset
    # ------------------------------------------------------------------

    async def start_auto_offset_loop(self, interval_seconds: int = 3600):
        """Background loop that periodically checks emissions and offsets if threshold exceeded."""
        while self._running:
            try:
                if self.auto_offset_enabled and self.sustainability_engine:
                    # Get recent emissions (from sustainability engine)
                    recent_emissions = await self.sustainability_engine.get_recent_emissions(hours=24)
                    if recent_emissions > self.auto_offset_threshold_kg:
                        await self.auto_offset(recent_emissions, reason="auto_offset_loop")
                await asyncio.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Auto‑offset loop error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    async def shutdown(self):
        """Clean up resources."""
        self._running = False
        if self._offset_task:
            self._offset_task.cancel()
            try:
                await self._offset_task
            except asyncio.CancelledError:
                pass
        logger.info("CarbonCreditMarketplace shut down")
