"""
PolicyEnforcementPoint — executes the PolicyDecision.

The PEP is the runtime side of the PDP: it applies the verdict, runs
the obligations, and enforces the fallback actions.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from ..contracts.policy_decision import PolicyDecision, DecisionVerdict
from ..contracts.decision_request import DecisionRequest

logger = logging.getLogger(__name__)


class PolicyEnforcementPoint:
    """
    Consumes a PolicyDecision and dispatches the corresponding action.

    Obligation handlers are registered by name. When the decision
    carries an obligation that has no handler, the PEP logs a warning
    but does not block (the decision already passed the PDP).
    """

    def __init__(self):
        self._obligation_handlers: Dict[str, Callable] = {}
        self._verdict_handlers: Dict[str, Callable] = {}

    def register_obligation(self, name: str, handler: Callable) -> None:
        self._obligation_handlers[name] = handler

    def register_verdict(self, verdict: str, handler: Callable) -> None:
        self._verdict_handlers[verdict] = handler

    async def enforce(
        self,
        decision: PolicyDecision,
        request: DecisionRequest,
        *,
        executor: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Enforce the decision. Returns a summary dict.

        - ALLOW: run all obligations, then call `executor(request)`.
        - MODIFY: run all obligations, then call `executor(modified)`.
        - ESCALATE: return awaiting_human=True; the runtime must not
          proceed until the human gate resolves.
        - FALLBACK: run the fallback action.
        - DENY: raise or return blocked=True.
        """
        outcome: Dict[str, Any] = {
            "verdict": decision.verdict,
            "executed": False,
            "obligations_run": [],
            "blocked": False,
            "awaiting_human": False,
        }

        # --- Obligations ---
        for obligation in decision.obligations:
            handler = self._obligation_handlers.get(obligation.split(":")[0])
            if handler is None:
                logger.debug(f"No handler for obligation '{obligation}'")
                continue
            try:
                res = handler(request, decision)
                if hasattr(res, "__await__"):
                    await res
                outcome["obligations_run"].append(obligation)
            except Exception as e:
                logger.warning(f"Obligation '{obligation}' failed: {e}")

        # --- Verdict dispatch ---
        if decision.verdict == DecisionVerdict.DENY.value:
            outcome["blocked"] = True
            handler = self._verdict_handlers.get("deny")
            if handler:
                await _maybe_await(handler(request, decision))
            return outcome

        if decision.verdict == DecisionVerdict.ESCALATE.value:
            outcome["awaiting_human"] = True
            handler = self._verdict_handlers.get("escalate")
            if handler:
                await _maybe_await(handler(request, decision))
            return outcome

        if decision.verdict == DecisionVerdict.FALLBACK.value:
            handler = self._verdict_handlers.get("fallback")
            if handler:
                await _maybe_await(handler(request, decision))
            return outcome

        # --- ALLOW / MODIFY → run executor ---
        if executor is not None:
            target = decision.modified_request or request.to_dict()
            res = executor(target)
            if hasattr(res, "__await__"):
                res = await res
            outcome["executed"] = True
            outcome["result"] = res

        return outcome


async def _maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value
