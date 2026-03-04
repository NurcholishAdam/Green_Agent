"""
Carbon Ledger Service
======================

Tracks carbon budgets and expenditures per team/model/experiment.

Location: src/governance/carbon_ledger.py
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class CarbonTransaction:
    """Single carbon transaction"""
    transaction_id: str
    timestamp: datetime
    team: str
    task_id: str
    energy_kwh: float
    carbon_kgco2e: float
    cost_usd: float


@dataclass
class TeamBudget:
    """Team carbon budget"""
    team: str
    period: str  # "2026-03"
    budget_kgco2e: float
    used_kgco2e: float
    remaining_kgco2e: float
    num_transactions: int
    
    @property
    def utilization_pct(self) -> float:
        return (self.used_kgco2e / self.budget_kgco2e * 100) if self.budget_kgco2e > 0 else 0


class CarbonLedgerService:
    """Tracks carbon budgets and transactions"""
    
    def __init__(self, ledger_path: Optional[Path] = None):
        self.ledger_path = ledger_path or Path("data/carbon_ledger.json")
        self.transactions: List[CarbonTransaction] = []
        self.team_budgets: Dict[str, TeamBudget] = {}
        self._load_ledger()
        
        logger.info(f"Carbon ledger initialized with {len(self.transactions)} transactions")
    
    def _load_ledger(self):
        """Load ledger from disk"""
        if not self.ledger_path.exists():
            return
        
        try:
            with open(self.ledger_path, 'r') as f:
                data = json.load(f)
                self.transactions = [
                    CarbonTransaction(**t) for t in data.get("transactions", [])
                ]
                self.team_budgets = {
                    team: TeamBudget(**budget)
                    for team, budget in data.get("budgets", {}).items()
                }
        except Exception as e:
            logger.error(f"Failed to load ledger: {e}")
    
    def _save_ledger(self):
        """Save ledger to disk"""
        try:
            self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.ledger_path, 'w') as f:
                json.dump({
                    "transactions": [t.__dict__ for t in self.transactions],
                    "budgets": {team: b.__dict__ for team, b in self.team_budgets.items()}
                }, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save ledger: {e}")
    
    def set_team_budget(
        self,
        team: str,
        period: str,
        budget_kgco2e: float
    ):
        """Set carbon budget for team"""
        self.team_budgets[f"{team}_{period}"] = TeamBudget(
            team=team,
            period=period,
            budget_kgco2e=budget_kgco2e,
            used_kgco2e=0.0,
            remaining_kgco2e=budget_kgco2e,
            num_transactions=0
        )
        self._save_ledger()
    
    def record_transaction(
        self,
        team: str,
        task_id: str,
        energy_kwh: float,
        carbon_kgco2e: float,
        cost_usd: float
    ):
        """Record carbon transaction"""
        transaction = CarbonTransaction(
            transaction_id=f"txn_{len(self.transactions)}",
            timestamp=datetime.now(),
            team=team,
            task_id=task_id,
            energy_kwh=energy_kwh,
            carbon_kgco2e=carbon_kgco2e,
            cost_usd=cost_usd
        )
        
        self.transactions.append(transaction)
        
        # Update team budget
        period = datetime.now().strftime("%Y-%m")
        budget_key = f"{team}_{period}"
        
        if budget_key in self.team_budgets:
            budget = self.team_budgets[budget_key]
            budget.used_kgco2e += carbon_kgco2e
            budget.remaining_kgco2e = budget.budget_kgco2e - budget.used_kgco2e
            budget.num_transactions += 1
        
        self._save_ledger()
        
        logger.info(f"Recorded {carbon_kgco2e:.4f} kgCO2e for team {team}")
    
    def get_team_budget(self, team: str, period: Optional[str] = None) -> Optional[TeamBudget]:
        """Get team budget for period"""
        if period is None:
            period = datetime.now().strftime("%Y-%m")
        
        budget_key = f"{team}_{period}"
        return self.team_budgets.get(budget_key)
    
    def check_budget_available(
        self,
        team: str,
        required_carbon: float,
        period: Optional[str] = None
    ) -> bool:
        """Check if team has budget available"""
        budget = self.get_team_budget(team, period)
        if not budget:
            return True  # No budget set = unlimited
        
        return budget.remaining_kgco2e >= required_carbon
    
    def get_leaderboard(self, period: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get team carbon efficiency leaderboard"""
        if period is None:
            period = datetime.now().strftime("%Y-%m")
        
        teams = [
            {
                "team": budget.team,
                "used_kgco2e": budget.used_kgco2e,
                "utilization_pct": budget.utilization_pct,
                "remaining_kgco2e": budget.remaining_kgco2e
            }
            for key, budget in self.team_budgets.items()
            if budget.period == period
        ]
        
        # Sort by lowest utilization (most efficient)
        teams.sort(key=lambda t: t["utilization_pct"])
        
        return teams


if __name__ == "__main__":
    ledger = CarbonLedgerService()
    
    # Set budget
    ledger.set_team_budget("nlp_research", "2026-03", 20.0)
    
    # Record transaction
    ledger.record_transaction(
        team="nlp_research",
        task_id="bert_sentiment",
        energy_kwh=0.82,
        carbon_kgco2e=0.135,
        cost_usd=0.164
    )
    
    # Check budget
    budget = ledger.get_team_budget("nlp_research", "2026-03")
    print(f"Team: {budget.team}")
    print(f"Used: {budget.used_kgco2e:.3f} / {budget.budget_kgco2e:.3f} kgCO2e")
    print(f"Remaining: {budget.remaining_kgco2e:.3f} kgCO2e")
    print(f"Utilization: {budget.utilization_pct:.1f}%")
