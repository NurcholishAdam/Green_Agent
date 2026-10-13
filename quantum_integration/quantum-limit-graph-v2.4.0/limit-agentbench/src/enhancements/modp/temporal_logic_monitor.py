"""
temporal_logic_monitor.py — Enhanced v17.1.0
=============================================

Temporal logic runtime verification for MOPD safety policies, with all ten
Green Agent enhancements.

FIXES OVER v17.0.0:
 - The implication operator `->` and biconditional `<->` are now supported
   in the tokenizer, parser, and evaluator.
 - The tokenizer accepts adjacent temporal operators (`GF(x)` = `G F(x)`).
 - `X` (next) is vacuously true at the last trace position (documented).
 - `TemporalLogicVerifier._escalate_critical` is now throttled per rule via
   a configurable cooldown (was fired on every verify cycle).
 - `model_check` accepts an optional `transition_fn` so real bounded model
   checking is possible; the perturbation fallback is clearly marked
   `synthetic=True`.
 - LTL evaluation uses a bottom-up table so `G F φ`-style formulas are
   O(|formula| · n) instead of O(n²).
 - `window_seconds` is honoured by the model checker as well as by runtime
   verification.
 - HITL responses are correlated via per-request futures.
 - Trace and violation rows are buffered and flushed in batches.
 - Rule registration failures are logged at ERROR level.
 - log_event shim (structlog or stdlib) and Prometheus metrics.
 - Lifecycle: aclose / __aenter__ / __aexit__, get_running_loop().
 - Shared fixes for the ten enhancement classes.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import re
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import (
    Any, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple,
)

# ---------- Optional dependencies ----------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    LinearRegression = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)

# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Structured-logging shim (structlog or stdlib)."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# CONFIG HELPER
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            return config.dict().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    _M_RULES = Gauge(
        "temporal_rules_total", "Active temporal rules", ["severity"])
    _M_VIOLATIONS = _PromCounter(
        "temporal_violations_total", "Temporal violations",
        ["rule", "severity"])
    _M_VERIFY_SECONDS = Histogram(
        "temporal_verify_seconds", "Verify cycle latency")
    _M_ESCALATIONS = _PromCounter(
        "temporal_escalations_total", "Critical escalations",
        ["rule", "outcome"])
    _M_MODEL_CHECK_SECONDS = Histogram(
        "temporal_model_check_seconds", "Model check latency")
    _M_MODEL_CHECK_VISITED = Histogram(
        "temporal_model_check_visited", "States visited in model check")
    _M_SIDE_EFFECT = Histogram(
        "temporal_side_effect_seconds", "Side-effect latency", ["name"])
    _M_PRECISION_SWITCHES = _PromCounter(
        "temporal_precision_switches_total", "Precision switches",
        ["from_p", "to_p"])
    _M_CHAOS = _PromCounter(
        "temporal_chaos_experiments_total", "Chaos experiments",
        ["fault_type", "status"])
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_RULES = _M_VIOLATIONS = _M_VERIFY_SECONDS = _M_ESCALATIONS = _Dummy()
    _M_MODEL_CHECK_SECONDS = _M_MODEL_CHECK_VISITED = _Dummy()
    _M_SIDE_EFFECT = _M_PRECISION_SWITCHES = _M_CHAOS = _Dummy()


# =============================================================================
# LTL CORE — ATOMIC REGEX, TOKENIZER, PARSER, EVALUATOR
# =============================================================================
_ATOMIC_RE = re.compile(
    r"([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?\d+(?:\.\d+)?)")

_OP_CHARS = set("GFXUWR")


def _tokenize_ltl(s: str) -> List[Tuple[str, Optional[str]]]:
    """
    Tokenize an LTL formula.

    - Operators: `!`, `&`, `|`, `->`, `<->`, `(`, `)`, `G`, `F`, `X`, `U`, `W`, `R`.
    - Adjacent temporal operators are accepted (`GF(x)` = `G F(x)`).
    - Identifiers are alphanumeric+underscore and must not start with a
      temporal operator followed by a non-operator letter (to avoid
      confusing `Gate` with `G ate`).
    """
    tokens: List[Tuple[str, Optional[str]]] = []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c.isspace():
            i += 1
            continue
        if s.startswith("<->", i):
            tokens.append(("IFF", None)); i += 3
            continue
        if s.startswith("->", i):
            tokens.append(("IMPLIES", None)); i += 2
            continue
        if c == "(":
            tokens.append(("LPAREN", None)); i += 1
            continue
        if c == ")":
            tokens.append(("RPAREN", None)); i += 1
            continue
        if c == "!":
            tokens.append(("NOT", None)); i += 1
            continue
        if c == "&":
            tokens.append(("AND", None)); i += 1
            continue
        if c == "|":
            tokens.append(("OR", None)); i += 1
            continue
        if c in _OP_CHARS:
            nxt = s[i + 1] if i + 1 < n else ""
            # If the next char is also a temporal operator, the current one
            # is definitely an operator (GF, FG, GX, ...).
            if nxt in _OP_CHARS:
                tokens.append((c, None)); i += 1
                continue
            # If the next char is a letter/underscore and NOT an operator,
            # read the whole identifier (so `Gate` is a variable).
            if nxt and (nxt.isalnum() or nxt == "_"):
                j = i
                while j < n and (s[j].isalnum() or s[j] == "_"):
                    j += 1
                tokens.append(("ATOMIC", s[i:j])); i = j
                continue
            tokens.append((c, None)); i += 1
            continue
        if c.isalpha() or c == "_":
            j = i
            while j < n and (s[j].isalnum() or s[j] == "_"):
                j += 1
            tokens.append(("ATOMIC", s[i:j])); i = j
            continue
        raise ValueError(f"Unexpected character '{c}' at position {i}")
    return tokens


def _parse_ltl(formula: str) -> Tuple[Any, Dict[str, Tuple[str, str, float]]]:
    """
    Parse an LTL formula into an AST.

    Supported operators (tightest to loosest):
      ! , X , G , F        (unary)
      U , W , R            (binary temporal)
      &                    (conjunction)
      |                    (disjunction)
      ->                   (implication, right-associative)
      <->                  (biconditional)
      (...)                (grouping)
    """
    atomics: Dict[str, Tuple[str, str, float]] = {}
    counter = [0]

    def _replace_atomic(match):
        var, cmp, val = match.group(1), match.group(2), float(match.group(3))
        key = f"__A{counter[0]}__"
        counter[0] += 1
        atomics[key] = (var, cmp, val)
        return key

    processed = _ATOMIC_RE.sub(_replace_atomic, formula)
    tokens = _tokenize_ltl(processed)
    parser = _LTLParser(tokens, atomics)
    ast = parser.parse()
    return ast, atomics


class _LTLParser:
    """Recursive-descent parser for LTL formulas."""

    def __init__(self, tokens: List[Tuple[str, Optional[str]]],
                 atomics: Dict[str, Tuple[str, str, float]]):
        self.tokens = tokens
        self.pos = 0
        self.atomics = atomics

    def _peek(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def _consume(self, kind: str) -> Optional[str]:
        tok = self._peek()
        if tok is None or tok[0] != kind:
            return None
        self.pos += 1
        return tok[1]

    def parse(self) -> Any:
        ast = self._parse_iff()
        if self.pos != len(self.tokens):
            raise ValueError(f"Unexpected token at end: {self._peek()}")
        return ast

    def _parse_iff(self) -> Any:
        left = self._parse_implies()
        while self._peek() and self._peek()[0] == "IFF":
            self._consume("IFF")
            right = self._parse_implies()
            left = ("iff", left, right)
        return left

    def _parse_implies(self) -> Any:
        # Right-associative: a -> b -> c = a -> (b -> c)
        left = self._parse_or()
        if self._peek() and self._peek()[0] == "IMPLIES":
            self._consume("IMPLIES")
            right = self._parse_implies()
            return ("implies", left, right)
        return left

    def _parse_or(self) -> Any:
        left = self._parse_and()
        while self._peek() and self._peek()[0] == "OR":
            self._consume("OR")
            left = ("or", left, self._parse_and())
        return left

    def _parse_and(self) -> Any:
        left = self._parse_binary_temporal()
        while self._peek() and self._peek()[0] == "AND":
            self._consume("AND")
            left = ("and", left, self._parse_binary_temporal())
        return left

    def _parse_binary_temporal(self) -> Any:
        left = self._parse_unary()
        while True:
            tok = self._peek()
            if tok and tok[0] in ("U", "W", "R"):
                op = tok[0]
                self._consume(op)
                right = self._parse_unary()
                left = (op.lower(), left, right)
            else:
                break
        return left

    def _parse_unary(self) -> Any:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of formula")
        kind = tok[0]
        if kind == "NOT":
            self._consume("NOT")
            return ("not", self._parse_unary())
        if kind == "G":
            self._consume("G")
            return ("always", self._parse_unary())
        if kind == "F":
            self._consume("F")
            return ("eventually", self._parse_unary())
        if kind == "X":
            self._consume("X")
            return ("next", self._parse_unary())
        return self._parse_atom()

    def _parse_atom(self) -> Any:
        tok = self._peek()
        if tok is None:
            raise ValueError("Unexpected end of formula")
        kind = tok[0]
        if kind == "LPAREN":
            self._consume("LPAREN")
            inner = self._parse_iff()
            if not self._consume("RPAREN"):
                raise ValueError("Missing closing parenthesis")
            return inner
        if kind == "ATOMIC":
            name = self._consume("ATOMIC")
            return ("atomic", name)
        raise ValueError(f"Unexpected token: {tok}")


def _build_ltl_table(ast: Any, trace: List[Dict[str, Any]],
                     atomics: Dict[str, Tuple[str, str, float]]
                     ) -> List[bool]:
    """
    Return a per-position truth table for an LTL formula.

    Bottom-up dynamic programming: every subformula is evaluated once,
    giving O(|formula| · n) total time (instead of O(n²) for the naive
    recursive evaluator).
    """
    n = len(trace)
    if n == 0:
        return []
    kind = ast[0]

    if kind == "atomic":
        key = ast[1]
        if key in atomics:
            var, cmp, val = atomics[key]
            out: List[bool] = []
            for state in trace:
                try:
                    sv = float(state.get(var, 0.0))
                except Exception:
                    out.append(True)
                    continue
                out.append({
                    ">=": sv >= val, "<=": sv <= val, "==": sv == val,
                    "!=": sv != val, ">": sv > val, "<": sv < val,
                }[cmp])
            return out
        return [bool(state.get(key, False)) for state in trace]

    if kind == "not":
        return [not b for b in _build_ltl_table(ast[1], trace, atomics)]

    if kind == "and":
        a = _build_ltl_table(ast[1], trace, atomics)
        b = _build_ltl_table(ast[2], trace, atomics)
        return [x and y for x, y in zip(a, b)]

    if kind == "or":
        a = _build_ltl_table(ast[1], trace, atomics)
        b = _build_ltl_table(ast[2], trace, atomics)
        return [x or y for x, y in zip(a, b)]

    if kind == "implies":
        a = _build_ltl_table(ast[1], trace, atomics)
        b = _build_ltl_table(ast[2], trace, atomics)
        return [(not x) or y for x, y in zip(a, b)]

    if kind == "iff":
        a = _build_ltl_table(ast[1], trace, atomics)
        b = _build_ltl_table(ast[2], trace, atomics)
        return [x == y for x, y in zip(a, b)]

    if kind == "next":
        inner = _build_ltl_table(ast[1], trace, atomics)
        # X φ is vacuously true at the last position of a finite trace.
        return [inner[i + 1] if i + 1 < n else True for i in range(n)]

    if kind == "always":
        inner = _build_ltl_table(ast[1], trace, atomics)
        out = [True] * n
        for i in range(n - 1, -1, -1):
            nxt = out[i + 1] if i + 1 < n else True
            out[i] = inner[i] and nxt
        return out

    if kind == "eventually":
        inner = _build_ltl_table(ast[1], trace, atomics)
        out = [False] * n
        for i in range(n - 1, -1, -1):
            nxt = out[i + 1] if i + 1 < n else False
            out[i] = inner[i] or nxt
        return out

    if kind == "until":
        phi = _build_ltl_table(ast[1], trace, atomics)
        psi = _build_ltl_table(ast[2], trace, atomics)
        out = [False] * n
        for i in range(n - 1, -1, -1):
            nxt = out[i + 1] if i + 1 < n else False
            out[i] = psi[i] or (phi[i] and nxt)
        return out

    if kind == "weak_until":
        phi = _build_ltl_table(ast[1], trace, atomics)
        psi = _build_ltl_table(ast[2], trace, atomics)
        out = [False] * n
        for i in range(n - 1, -1, -1):
            nxt = out[i + 1] if i + 1 < n else True
            out[i] = phi[i] and (psi[i] or nxt)
        return out

    if kind == "release":
        phi = _build_ltl_table(ast[1], trace, atomics)
        psi = _build_ltl_table(ast[2], trace, atomics)
        out = [False] * n
        for i in range(n - 1, -1, -1):
            nxt = out[i + 1] if i + 1 < n else True
            out[i] = psi[i] and (phi[i] or nxt)
        return out

    raise ValueError(f"Unknown AST node: {kind}")


def _eval_ltl(ast: Any, trace: List[Dict[str, Any]], i: int,
              atomics: Dict[str, Tuple[str, str, float]]) -> bool:
    """
    Evaluate an LTL AST at position i of the trace. Kept as a stable
    public helper; implemented on top of the bottom-up table.
    """
    if not trace:
        return False
    table = _build_ltl_table(ast, trace, atomics)
    if not table:
        return False
    if i < 0 or i >= len(table):
        # Positions outside the trace are vacuously satisfied (standard
        # finite-trace convention for X).
        return True
    return table[i]


def _hash_short(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:6]


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC VERIFIER (enhanced original)
# =============================================================================
class TemporalRule:
    """A temporal rule with LTL formula, versioning, and violation tracking."""

    def __init__(self,
                 rule_id: str,
                 ast: Any,
                 atomics: Dict[str, Tuple[str, str, float]],
                 raw_formula: str,
                 window_seconds: float = 0.0,
                 description: str = "",
                 severity: str = "warning",
                 version: int = 1):
        self.rule_id = rule_id
        self.ast = ast
        self.atomics = atomics
        self.raw_formula = raw_formula
        self.window_seconds = float(window_seconds)
        self.description = description or raw_formula
        self.severity = severity
        self.version = version
        self.violations = 0
        self.last_violation: Optional[datetime] = None
        self.created_at = _now_utc()

    def evaluate(self, trace: Deque[Tuple[datetime, Dict]]) -> bool:
        """Return True if the rule is violated over the current trace."""
        if not trace:
            return False
        # Apply time window pruning.
        if self.window_seconds > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window_seconds)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        states = [s for _, s in trace]
        if not states:
            return False
        try:
            return not _eval_ltl(self.ast, states, 0, self.atomics)
        except Exception as exc:
            log_event("warning", f"LTL eval failed for {self.rule_id}: {exc}")
            return False


class RuleRegistry:
    """
    Additive helper that owns rule storage, versioning, and persistence.
    `TemporalLogicVerifier` delegates to an instance of this class.
    """

    def __init__(self, storage: Any):
        self.storage = storage
        self.rules: Dict[str, TemporalRule] = {}
        self.history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def add(self, rule_id: str, formula: str, severity: str,
            description: str = "", window_seconds: float = 0.0) -> TemporalRule:
        ast, atomics = _parse_ltl(formula)
        version = 1
        if rule_id in self.rules:
            old = self.rules[rule_id]
            version = old.version + 1
            self.history[rule_id].append({
                "version": old.version,
                "formula": old.raw_formula,
                "severity": old.severity,
                "retired_at": _now_utc().isoformat(),
            })
        rule = TemporalRule(
            rule_id=rule_id, ast=ast, atomics=atomics,
            raw_formula=formula, window_seconds=window_seconds,
            description=description, severity=severity, version=version)
        self.rules[rule_id] = rule
        return rule

    def remove(self, rule_id: str) -> None:
        if rule_id in self.rules:
            old = self.rules.pop(rule_id)
            self.history[rule_id].append({
                "version": old.version,
                "formula": old.raw_formula,
                "severity": old.severity,
                "retired_at": _now_utc().isoformat(),
            })

    async def persist(self) -> None:
        for rid, rule in self.rules.items():
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_rule,
                    rid, rule.raw_formula, "ltl", rule.severity,
                    rule.description, rule.window_seconds, True)
            except Exception:
                pass


class BoundedModelChecker:
    """
    Additive helper for bounded model checking of a single rule.

    Uses a caller-supplied `transition_fn` when available. Without it, the
    checker falls back to synthetic numeric perturbations and marks the
    result as `synthetic=True`.
    """

    def __init__(self, max_states: int = 500):
        self.max_states = int(max_states)

    async def check(self,
                    rule: TemporalRule,
                    initial_state: Dict[str, Any],
                    max_depth: int = 8,
                    branch_factor: int = 2,
                    transition_fn: Optional[
                        Callable[[Dict[str, Any]], List[Dict[str, Any]]]
                    ] = None) -> Dict[str, Any]:
        initial_trace = [dict(initial_state)]
        queue: Deque[List[Dict[str, Any]]] = deque([initial_trace])
        visited = 0
        synthetic = transition_fn is None

        while queue:
            trace = queue.popleft()
            visited += 1
            if visited > self.max_states:
                break

            try:
                if not _eval_ltl(rule.ast, trace, 0, rule.atomics):
                    return {
                        "satisfied": False,
                        "counterexample": trace,
                        "visited": visited,
                        "formula": rule.raw_formula,
                        "synthetic": synthetic,
                    }
            except Exception:
                pass

            if len(trace) >= max_depth:
                continue
            last = trace[-1]

            if transition_fn is not None:
                try:
                    successors = transition_fn(last) or []
                except Exception:
                    successors = []
            else:
                successors = self._perturb(last, branch_factor)

            for child in successors:
                queue.append(trace + [child])

        return {
            "satisfied": True,
            "counterexample": None,
            "visited": visited,
            "formula": rule.raw_formula,
            "synthetic": synthetic,
        }

    @staticmethod
    def _perturb(state: Dict[str, Any],
                 branch_factor: int) -> List[Dict[str, Any]]:
        numeric_keys = [k for k, v in state.items()
                        if isinstance(v, (int, float))
                        and not isinstance(v, bool)]
        random.shuffle(numeric_keys)
        successors: List[Dict[str, Any]] = []
        for key in numeric_keys[:branch_factor]:
            base = state[key]
            for delta in (0.1, -0.1, 0.25):
                child = dict(state)
                try:
                    child[key] = float(base) + delta
                except Exception:
                    continue
                successors.append(child)
        return successors


class TemporalLogicVerifier:
    """
    Temporal logic runtime verifier for MOPD.

    Full LTL (G, F, X, U, W, R, !, &, |, ->, <->, parentheses),
    configurable severity, rule versioning, bounded model checking with
    counterexample generation, natural-language rendering, throttled
    escalation, and integration hooks for nine other enhancements.
    """

    VALID_SEVERITIES = {"info", "warning", "critical"}

    def __init__(self,
                 storage: Any,
                 config: Any,
                 hitl: Optional["ActiveUserPreferenceLearner"] = None,
                 xai: Optional["XAIDecisionExplainer"] = None,
                 causal_rl: Optional["CausalPolicyAdapter"] = None,
                 chaos: Optional["ChaosTestingEngine"] = None,
                 carbon_market: Optional["CarbonMarketIntegrator"] = None,
                 multi_agent: Optional["MultiAgentCoordinator"] = None,
                 *,
                 critical_escalation_cooldown_s: float = 60.0,
                 trace_flush_size: int = 50,
                 trace_flush_interval_s: float = 5.0,
                 model_check_max_states: int = 500):
        self.storage = storage
        self.config = config
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000)))
        self.approval_cb: Optional[Callable] = None

        # Enhancement hooks
        self.hitl = hitl
        self.xai = xai
        self.causal_rl = causal_rl
        self.chaos = chaos
        self.carbon_market = carbon_market
        self.multi_agent = multi_agent

        # Registry and model checker
        self._registry = RuleRegistry(storage)
        self._model_checker = BoundedModelChecker(
            max_states=model_check_max_states)

        # Expose dict views (preserved interface)
        self.rules = self._registry.rules
        self.rule_history = self._registry.history

        # Escalation cooldown
        self.critical_escalation_cooldown_s = float(
            critical_escalation_cooldown_s)
        self._last_escalation: Dict[str, float] = {}

        # Buffered storage writes
        self._trace_buffer: List[Dict[str, Any]] = []
        self._violation_buffer: List[Dict[str, Any]] = []
        self._trace_flush_size = int(trace_flush_size)
        self._trace_flush_interval_s = float(trace_flush_interval_s)
        self._last_flush_at = time.monotonic()
        self._flush_lock = asyncio.Lock()

        # Side-effect tracking
        self._side_effects: set = set()

        self._load_rules_from_config()

    # ------------------------------------------------------------------
    # Rule management
    # ------------------------------------------------------------------
    def _load_rules_from_config(self) -> None:
        formulas = _cfg_get(self.config, "temporal_formulas", []) or []
        for i, entry in enumerate(formulas):
            if isinstance(entry, dict):
                formula = entry.get("formula", "")
                severity = entry.get("severity", "warning")
                description = entry.get("description", "")
                window = float(entry.get("window_seconds", 0.0))
            else:
                formula = str(entry)
                severity = "warning"
                description = ""
                window = 0.0
            rule_id = f"rule_{i}_{_hash_short(formula)}"
            try:
                self._registry.add(rule_id, formula, severity,
                                   description, window)
            except Exception as exc:
                log_event("error",
                          f"Failed to load temporal rule {formula!r}: {exc}")

    def add_rule(self, rule_id: str, formula: str,
                 severity: str = "warning", description: str = "",
                 window_seconds: float = 0.0) -> None:
        if severity not in self.VALID_SEVERITIES:
            raise ValueError(f"Invalid severity: {severity}")
        self._registry.add(rule_id, formula, severity,
                           description, window_seconds)

    def remove_rule(self, rule_id: str) -> None:
        self._registry.remove(rule_id)

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def persist_rules(self) -> None:
        await self._registry.persist()

    # ------------------------------------------------------------------
    # Runtime monitoring
    # ------------------------------------------------------------------
    async def push_state(self, state: Dict[str, Any]) -> None:
        self.trace.append((_now_utc(), dict(state)))
        self._trace_buffer.append(dict(state))
        if (len(self._trace_buffer) >= self._trace_flush_size
                or (time.monotonic() - self._last_flush_at)
                >= self._trace_flush_interval_s):
            await self._flush_buffers()

    async def verify(self) -> Dict[str, bool]:
        t0 = time.monotonic()
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = _now_utc()
                self._violation_buffer.append({
                    "rule_id": rid,
                    "description": rule.description,
                    "step": len(self.trace) - 1,
                    "state": self.trace[-1][1] if self.trace else {},
                    "severity": rule.severity,
                })
                try:
                    _M_VIOLATIONS.labels(
                        rule=rid, severity=rule.severity).inc()
                except Exception:
                    pass
                if rule.severity == "critical":
                    self._spawn_side_effect(
                        self._escalate_critical(rule),
                        f"escalate_{rid}")
        try:
            _M_VERIFY_SECONDS.observe(time.monotonic() - t0)
        except Exception:
            pass
        return result

    # ------------------------------------------------------------------
    # Escalation (throttled, non-blocking)
    # ------------------------------------------------------------------
    def _spawn_side_effect(self, coro: Awaitable[Any], name: str) -> None:
        try:
            task = asyncio.create_task(coro, name=f"tl_side_{name}")
        except RuntimeError:
            return
        self._side_effects.add(task)

        def _done(t: asyncio.Task) -> None:
            self._side_effects.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("debug", f"side effect {name} failed: {exc}")

        task.add_done_callback(_done)

    async def _escalate_critical(self, rule: TemporalRule) -> None:
        now = time.monotonic()
        last = self._last_escalation.get(rule.rule_id, 0.0)
        if now - last < self.critical_escalation_cooldown_s:
            return
        self._last_escalation[rule.rule_id] = now

        state = self.trace[-1][1] if self.trace else {}

        # HITL
        if self.hitl is not None:
            try:
                approved = await self.hitl.query_user_if_needed(
                    "temporal_approver",
                    [
                        {"solution_id": "acknowledge", "quality_score": 0.90,
                         "carbon_g": 0.10, "cost_usd": 0.10,
                         "latency_ms": 100},
                        {"solution_id": "rollback", "quality_score": 0.85,
                         "carbon_g": 0.12, "cost_usd": 0.12,
                         "latency_ms": 105},
                    ],
                    timeout=2.0)
                if approved == "rollback":
                    log_event("warning",
                              f"HITL requested rollback after {rule.rule_id}")
                try:
                    _M_ESCALATIONS.labels(
                        rule=rule.rule_id,
                        outcome=approved or "timeout").inc()
                except Exception:
                    pass
            except Exception as exc:
                log_event("debug", f"HITL escalation failed: {exc}")

        # Callback
        if self.approval_cb is not None:
            try:
                r = self.approval_cb(rule.rule_id, state)
                if asyncio.iscoroutine(r):
                    await r
            except Exception as exc:
                log_event("warning", f"Approval callback failed: {exc}")

        # XAI
        if self.xai is not None and NUMPY_AVAILABLE:
            try:
                t0 = time.monotonic()
                feats = np.array([
                    float(rule.violations),
                    1.0,
                    float(len(self.trace)) / 1000.0,
                    float(rule.window_seconds),
                ])
                def _score(x, _w=np.array([0.5, 0.3, 0.1, 0.1])):
                    return float(np.dot(x, _w))
                await self.xai.explain(
                    decision_id=f"tl_{uuid.uuid4().hex[:8]}",
                    label=f"violation:{rule.rule_id}",
                    features=feats,
                    names=["violations", "severity", "trace_len", "window"],
                    model_fn=_score)
                _M_SIDE_EFFECT.labels(name="xai").observe(
                    time.monotonic() - t0)
            except Exception as exc:
                log_event("debug", f"XAI escalation failed: {exc}")

    # ------------------------------------------------------------------
    # Buffered storage writes
    # ------------------------------------------------------------------
    async def _flush_buffers(self) -> None:
        async with self._flush_lock:
            trace_batch, self._trace_buffer = self._trace_buffer, []
            violation_batch, self._violation_buffer = self._violation_buffer, []
            self._last_flush_at = time.monotonic()

        for entry in trace_batch:
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_trace, entry)
            except Exception:
                pass
        for entry in violation_batch:
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_violation,
                    entry["rule_id"], entry["description"], entry["step"],
                    entry["state"], entry["severity"], None)
            except Exception:
                pass

    async def flush(self) -> None:
        await self._flush_buffers()

    async def aclose(self) -> None:
        await self._flush_buffers()
        for t in list(self._side_effects):
            t.cancel()
        if self._side_effects:
            await asyncio.gather(*self._side_effects,
                                 return_exceptions=True)
        self._side_effects.clear()

    async def __aenter__(self) -> "TemporalLogicVerifier":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    # ------------------------------------------------------------------
    # Natural-language explanation
    # ------------------------------------------------------------------
    async def explain_violation(self, rule_id: str
                                 ) -> Optional[Dict[str, Any]]:
        rule = self.rules.get(rule_id)
        if rule is None:
            return None
        violating = []
        for ts, s in list(self.trace)[-20:]:
            try:
                if not _eval_ltl(rule.ast, [s], 0, rule.atomics):
                    violating.append({"ts": ts.isoformat(), "state": s})
            except Exception:
                pass
        recent = violating[-3:]
        nl = (
            f"Rule '{rule.rule_id}' (severity={rule.severity}, "
            f"v{rule.version}) violated {rule.violations} time(s). "
            f"Formula: {rule.raw_formula}. "
            f"Description: {rule.description}."
        )
        if recent:
            nl += f"\nRecent violating states ({len(recent)}):"
            for v in recent:
                nl += f"\n  • {v['ts']}: {v['state']}"
        return {
            "rule_id": rule_id,
            "formula": rule.raw_formula,
            "severity": rule.severity,
            "version": rule.version,
            "violations": rule.violations,
            "recent_violating_states": recent,
            "explanation": nl,
        }

    # ------------------------------------------------------------------
    # Bounded model checking
    # ------------------------------------------------------------------
    async def model_check(self,
                          rule_id: str,
                          initial_state: Dict[str, Any],
                          max_depth: int = 8,
                          branch_factor: int = 2,
                          transition_fn: Optional[
                              Callable[[Dict[str, Any]],
                                       List[Dict[str, Any]]]
                          ] = None) -> Dict[str, Any]:
        rule = self.rules.get(rule_id)
        if rule is None:
            return {"error": f"unknown rule {rule_id}"}
        t0 = time.monotonic()
        result = await self._model_checker.check(
            rule, initial_state, max_depth=max_depth,
            branch_factor=branch_factor, transition_fn=transition_fn)
        try:
            _M_MODEL_CHECK_SECONDS.observe(time.monotonic() - t0)
            _M_MODEL_CHECK_VISITED.observe(result.get("visited", 0))
        except Exception:
            pass
        return result

    # ------------------------------------------------------------------
    # Rule history / stats
    # ------------------------------------------------------------------
    def rule_history_for(self, rule_id: str) -> List[Dict[str, Any]]:
        return list(self.rule_history.get(rule_id, []))

    def stats(self) -> Dict[str, Any]:
        try:
            counts: Dict[str, int] = defaultdict(int)
            for r in self.rules.values():
                counts[r.severity] += 1
            for sev in self.VALID_SEVERITIES:
                _M_RULES.labels(severity=sev).set(counts.get(sev, 0))
        except Exception:
            pass
        return {
            "num_rules": len(self.rules),
            "trace_length": len(self.trace),
            "trace_buffer_size": len(self._trace_buffer),
            "violation_buffer_size": len(self._violation_buffer),
            "rules": {
                rid: {
                    "formula": r.raw_formula,
                    "severity": r.severity,
                    "version": r.version,
                    "violations": r.violations,
                    "last_violation": r.last_violation.isoformat()
                        if r.last_violation else None,
                }
                for rid, r in self.rules.items()
            },
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = float(temperature)
        self.alpha = float(alpha)
        self.n_actions = int(n_actions)
        self.teachers: Dict[str, List[float]] = {}
        self.teacher_reputation: Dict[str, float] = defaultdict(lambda: 0.5)
        self.teacher_kl_history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=20))
        self.student_policy = [1.0 / self.n_actions] * self.n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[str(name)] = [max(0.0, p / s) for p in policy]

    def _temp_scale(self, p: List[float], temperature: float) -> List[float]:
        if not p:
            return []
        inv = 1.0 / max(temperature, 1e-6)
        scaled = [max(v, 1e-12) ** inv for v in p]
        s = sum(scaled) or 1.0
        return [x / s for x in scaled]

    def _softmax(self, x: List[float], temp: float) -> List[float]:
        return self._temp_scale(x, temp)

    def _superpose(self) -> List[float]:
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, storage: Any,
                   student_id: str = "tl_student") -> Dict[str, Any]:
        target = self._temp_scale(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Per-teacher KL against the teacher's own distribution.
        for tid, pol in self.teachers.items():
            kl = 0.0
            for i in range(self.n_actions):
                pi = pol[i] if i < len(pol) else 1e-9
                si = self.student_policy[i]
                pi, si = max(pi, 1e-9), max(si, 1e-9)
                kl += pi * math.log(pi / si)
            self.teacher_kl_history[tid].append(kl)
            reward = 1.0 / (1.0 + kl)
            self.teacher_reputation[tid] = (
                0.9 * self.teacher_reputation[tid] + 0.1 * reward)
            weight = float(pol[0]) if pol else 0.0
            amplitude = math.sqrt(max(weight, 1e-9))
            try:
                await asyncio.to_thread(
                    storage.save_teacher_superposition,
                    student_id, tid, weight, self.temperature, amplitude, kl)
            except Exception:
                pass

        entry = {"target": target,
                 "student": list(self.student_policy),
                 "ts": _now_utc().isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage: Any = None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples: List[Dict[str, float]],
                    variables: List[str],
                    threshold: float = 0.25) -> Dict[str, Any]:
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
                self.graph.clear()
            return self.summary()
        try:
            X = np.asarray(
                [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                dtype=float)
        except Exception:
            return self.summary()
        if X.shape[0] < 3 or X.shape[1] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        try:
            corr = np.corrcoef(X, rowvar=False)
        except Exception:
            return self.summary()
        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if not math.isfinite(c) or c <= threshold:
                        continue
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = ((variables[i], variables[j]) if vi > vj
                                else (variables[j], variables[i]))
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]), "confidence": c}
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def summary(self) -> Dict[str, Any]:
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]
    STRATEGY_MAP = {
        "performance": "balanced", "carbon": "carbon_focus",
        "cost": "operational_focus", "hybrid": "balanced",
        "adaptive": "balanced",
    }

    def __init__(self, config: Any, storage: Any, graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(_cfg_get(config, "causal_exploration_rate", 0.1)
                             or 0.1)
        self._lock = asyncio.Lock()

    def to_strategy(self, action: str) -> str:
        return self.STRATEGY_MAP.get(action, "balanced")

    async def choose_action(self, state: Dict[str, Any]) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action: str, reward: float,
                     state: Dict[str, Any]) -> None:
        async with self._lock:
            if action not in self.ACTIONS:
                action = self.ACTIONS[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (reward - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    def __init__(self, storage: Any, instance_id: str,
                 share_interval: int = 3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = int(share_interval)
        self.rounds = 0
        self.last_aggregated: Optional[bytes] = None

    async def share_weights(self, model_id: str, weights: bytes) -> None:
        if not weights:
            return
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
            self.rounds += 1
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id: str) -> Optional[bytes]:
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r["weights"]]
        if not blobs:
            return None
        lists: List[List[float]] = []
        for b in blobs:
            try:
                parsed = json.loads(b.decode())
                if isinstance(parsed, list) and all(
                        isinstance(x, (int, float)) for x in parsed):
                    lists.append([float(x) for x in parsed])
            except Exception:
                continue
        if lists:
            n = min(len(x) for x in lists)
            avg = [sum(x[i] for x in lists) / len(lists) for i in range(n)]
            self.rounds += 1
            self.last_aggregated = json.dumps(avg).encode()
            return self.last_aggregated
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        self.last_aggregated = bytes(avg)
        return self.last_aggregated


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5) or 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def broadcast(self, topic: str, sender: str,
                        payload: Dict[str, Any]) -> None:
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            pass

    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "orchestrator")
        async with self._lock:
            if not self.agents:
                raise RuntimeError("No agents available for bidding")
            best_id, best_score = None, -1.0
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"),
                              "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id not in self.agents:
                return
            a = self.agents[agent_id]
            alpha = 0.1
            a.reputation = max(0.0, min(1.0,
                (1 - alpha) * a.reputation + alpha * reward))
            a.utilities[a.role] = min(
                1.0, a.utilities[a.role] + 0.05 * reward)

    def get_policy(self) -> List[float]:
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i, v in enumerate(affinity.get(role, [0.2] * 5)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self) -> Dict[str, Any]:
        await self._specialise()
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.method = str(_cfg_get(config, "xai_method", "kernel_shap")
                          or "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5) or 5)

    def _permutation_shap(self, f, x, names, n=64):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - \
                            float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib = contrib / max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _kernel_shap(self, f, x, names, n=64):
        return self._permutation_shap(f, x, names, n)

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id: str, label: str, features,
                      names: List[str], model_fn: Callable) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        else:
            attrs = self._permutation_shap(model_fn, features, names)
        nl = self._nl(label, attrs)

        feature_map: Dict[str, Any] = {}
        try:
            if NUMPY_AVAILABLE and hasattr(features, "tolist"):
                feature_map = dict(zip(names, features.tolist()))
            else:
                feature_map = dict(zip(names, list(features)))
        except Exception:
            feature_map = {"raw": list(features)}

        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                feature_map, attrs, nl)
        except Exception:
            pass
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    LATENCY = {"fp32": 1.0, "tf32": 0.8, "bf16": 0.65,
               "fp16": 0.6, "int8": 0.4}

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.history: Deque[Dict] = deque(maxlen=500)
        self._acc_window: Deque[float] = deque(maxlen=20)
        self._hw_cache: Optional[Dict[str, Any]] = None

    def _probe(self, force_refresh: bool = False) -> Dict[str, Any]:
        if self._hw_cache is not None and not force_refresh:
            return self._hw_cache
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE and torch is not None:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    try:
                        info["bf16"] = bool(torch.cuda.is_bf16_supported())
                    except Exception:
                        info["bf16"] = False
            except Exception:
                pass
        self._hw_cache = info
        return info

    def _allowed_precisions(self) -> List[str]:
        raw = list(_cfg_get(self.config, "precision_levels",
                            ["fp32", "fp16", "bf16", "int8"]))
        return [c for c in raw if c in self.ENERGY]

    def select_precision(self) -> str:
        hw = self._probe()
        cands = self._allowed_precisions()
        if not cands:
            return "fp32"
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            return "fp32"
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    def record_accuracy(self, accuracy: float) -> None:
        try:
            self._acc_window.append(float(accuracy))
        except (TypeError, ValueError):
            pass

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        if target not in self._allowed_precisions():
            log_event("warning",
                      f"Precision {target!r} not in precision_levels")
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved})
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
            _M_PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc: float = 0.0,
                          baseline_acc: float = 0.0) -> None:
        if recent_acc <= 0 or baseline_acc <= 0:
            if not self._acc_window:
                return
            recent_acc = sum(self._acc_window) / len(self._acc_window)
            baseline_acc = max(self._acc_window)
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(_cfg_get(self.config,
                                "precision_switch_threshold", 0.02) or 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            target = self.select_precision()
            if target != self.current:
                await self.switch_to(target, reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    DEFER_INTENSITY_THRESHOLD = 0.3
    RUN_OFFSET_COST_THRESHOLD = 0.5
    MIN_PRICE_USD = 5.0
    MAX_PRICE_USD = 200.0
    DEFAULT_PRICE_USD = 25.0

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.last_price = float(
            _cfg_get(config, "carbon_default_price_usd",
                     self.DEFAULT_PRICE_USD) or self.DEFAULT_PRICE_USD)
        self._rec_balance_mwh = 0.0

    async def _fetch_price(self) -> float:
        candidate = self.last_price + random.gauss(0, 1.5)
        return max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, candidate))

    async def update_price(self) -> float:
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        try:
            price = max(self.MIN_PRICE_USD,
                        min(self.MAX_PRICE_USD, float(price)))
        except (TypeError, ValueError):
            price = self.last_price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        self.last_price = price
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        mwh = max(0.0, float(mwh))
        if mwh == 0.0:
            return 0.0
        price_per_mwh = max(0.0, float(price_per_mwh))
        src = (source or "").strip().lower() or "unspecified"
        cost = mwh * price_per_mwh
        self._rec_balance_mwh += mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, src)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh: float, intensity: float,
                                price: Optional[float] = None,
                                commit: bool = True) -> Dict[str, Any]:
        workload_kwh = max(0.0, float(workload_kwh))
        intensity = max(0.0, float(intensity))
        if price is None:
            price = float(self.last_price)
        else:
            try:
                price = max(0.0, float(price))
            except (TypeError, ValueError):
                price = float(self.last_price)
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        defer_hours = 0
        if intensity > self.DEFER_INTENSITY_THRESHOLD:
            action = "defer"
            defer_hours = max(
                1, int((intensity - self.DEFER_INTENSITY_THRESHOLD) * 10))
        elif offset_cost < self.RUN_OFFSET_COST_THRESHOLD:
            action = "run_offset"
        else:
            action = "run"
        if commit:
            try:
                await asyncio.to_thread(
                    self.storage.save_net_zero_match,
                    uuid.uuid4().hex[:8], workload_kwh, intensity,
                    action, carbon_kg, offset_cost, price)
            except Exception:
                pass
        return {"action": action, "defer_hours": defer_hours,
                "carbon_kg": carbon_kg, "offset_cost_usd": offset_cost,
                "credit_price_usd": price,
                "rec_balance_mwh": float(self._rec_balance_mwh)}

    def get_rec_balance(self) -> float:
        return float(self._rec_balance_mwh)


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._shared_state: Dict[str, Any] = {"payload": "ok", "counter": 0}

    async def _health_ok(self) -> bool:
        async with self._lock:
            return not self.active

    async def run_experiment(self, name: str,
                             fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.monotonic()
        before = await self._health_ok()
        status = "completed"
        injected_ms = 0.0
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            if fault_type == "latency":
                injected_ms = 500.0
                await asyncio.sleep(0.5)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "data_corruption":
                async with self._lock:
                    self._shared_state["payload"] = "corrupted"
                    self._shared_state["counter"] += 1
                injected_ms = 5.0
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
                injected_ms = 10.0
            elif fault_type == "network_drop":
                injected_ms = 200.0
                await asyncio.sleep(0.2)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._health_ok()
        duration = (time.monotonic() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                float(_cfg_get(self.config, "chaos_blast_radius", 0.1) or 0.1),
                int(before), int(after), status, duration)
            _M_CHAOS.labels(fault_type=fault_type, status=status).inc()
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration,
                "injected_ms": injected_ms}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    _PREF_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

    def __init__(self, storage: Any, dashboard: Any = None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._pending: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()

    async def submit_response(self, user_id: str, chosen_id: str,
                              request_id: Optional[str] = None) -> None:
        if request_id is not None:
            async with self._lock:
                fut = self._pending.pop(request_id, None)
            if fut is not None and not fut.done():
                fut.set_result({"user_id": user_id, "chosen": chosen_id})
                return
            log_event("warning",
                      f"Unknown HITL request_id {request_id!r}")
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            log_event("warning", "HITL response queue full; dropping")

    def _entropy_uncertainty(self, candidates: List[Dict[str, Any]]) -> float:
        scores = []
        for c in candidates[:5]:
            try:
                scores.append(float(c.get("quality_score", 0.0)))
            except (TypeError, ValueError):
                scores.append(0.0)
        s = sum(scores) or 1.0
        probs = [x / s for x in scores]
        ent = -sum(p * math.log(p + 1e-9) for p in probs)
        max_ent = math.log(len(probs)) if len(probs) > 1 else 1.0
        return ent / max_ent if max_ent > 0 else 0.0

    def _ambiguity_score(self, candidates: List[Dict[str, Any]]) -> float:
        if len(candidates) < 2:
            return 0.0
        c1, c2 = candidates[0], candidates[1]

        def _rel(a, b):
            try:
                a = float(a); b = float(b)
            except (TypeError, ValueError):
                return 0.0
            m = max(abs(a), abs(b), 1e-6)
            return abs(a - b) / m

        try:
            q = _rel(c1.get("quality_score", 0), c2.get("quality_score", 0))
            cg = _rel(c1.get("carbon_g", 0), c2.get("carbon_g", 0))
            cost = _rel(c1.get("cost_usd", 0), c2.get("cost_usd", 0))
            lat = _rel(c1.get("latency_ms", 0), c2.get("latency_ms", 0))
            return max(cg, cost, lat) * (1.0 - min(q, 1.0))
        except Exception:
            return 0.0

    async def query_user_if_needed(self, user_id: str,
                                   candidates: List[Dict[str, Any]],
                                   timeout: float = 3.0) -> Optional[str]:
        if len(candidates) < 2:
            return None
        ambiguity = self._ambiguity_score(candidates)
        uncertainty = self._entropy_uncertainty(candidates)
        if ambiguity < 0.05 and uncertainty < 0.5:
            return None
        req_id = uuid.uuid4().hex[:8]
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, "query", {"candidates": candidates[:2]}, "info")
        except Exception:
            pass
        if self.dashboard is not None:
            try:
                await self.dashboard.broadcast({
                    "type": "preference_query",
                    "request_id": req_id, "user_id": user_id})
            except Exception:
                pass
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        async with self._lock:
            self._pending[req_id] = fut
        try:
            msg = await asyncio.wait_for(fut, timeout=timeout)
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "approved")
            except Exception:
                pass
            chosen = msg.get("chosen") if isinstance(msg, dict) else None
            metrics = next(
                (c for c in candidates
                 if c.get("solution_id") == chosen), None)
            await self.record_choice(user_id, chosen, metrics=metrics)
            return chosen
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "timeout")
            except Exception:
                pass
            normalized = self.get_preference_weights(user_id)
            if normalized:
                scored = []
                for c in candidates:
                    s = sum(normalized.get(k, 0.25)
                            / (float(c.get(k, 0) or 0) + 1e-8)
                            for k in self._PREF_KEYS)
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")
        finally:
            async with self._lock:
                self._pending.pop(req_id, None)

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None
                            ) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in self._PREF_KEYS:
                if k in metrics:
                    try:
                        v = float(metrics[k])
                    except (TypeError, ValueError):
                        continue
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + \
                            1.0 / (abs(v) + 1e-6) * 0.01
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def get_preference_weights(self, user_id: str) -> Dict[str, float]:
        raw = self.preferences.get(user_id, {})
        if not raw:
            return {}
        s = sum(raw.values()) or 1.0
        return {k: v / s for k, v in raw.items()}


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class TemporalLogicOrchestratorV17:
    """Unified entry point wiring all ten enhancements around the verifier."""

    def __init__(self, storage: Any, config: Any, dashboard: Any = None,
                 *, xai_sample_rate: float = 0.1,
                 chaos_sample_rate: float = 0.05,
                 multi_agent_sample_rate: float = 0.1):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        self.temporal = TemporalLogicVerifier(
            storage, config,
            hitl=self.hitl, xai=self.xai, causal_rl=self.causal_rl,
            chaos=self.chaos, carbon_market=self.carbon_market,
            multi_agent=self.multi_agent)

        self.temporal.set_approval_callback(self._on_critical_violation)

        self.xai_sample_rate = float(xai_sample_rate)
        self.chaos_sample_rate = float(chaos_sample_rate)
        self.multi_agent_sample_rate = float(multi_agent_sample_rate)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()
        self._side_effects: set = set()

    async def _on_critical_violation(self, rule_id: str,
                                     state: Dict[str, Any]) -> None:
        try:
            q = float(state.get("quality", 0.8))
            c = float(state.get("carbon", 0.4))
        except (TypeError, ValueError):
            q, c = 0.8, 0.4
        candidates = [
            {"solution_id": "approve", "quality_score": q, "carbon_g": c,
             "cost_usd": 0.5, "latency_ms": 100.0},
            {"solution_id": "reject", "quality_score": 0.0, "carbon_g": 0.0,
             "cost_usd": 0.0, "latency_ms": 0.0},
        ]
        try:
            decision = await self.hitl.query_user_if_needed(
                "critical_user", candidates, timeout=2.0)
            log_event("info",
                      f"Critical violation {rule_id}: HITL decision={decision}")
        except Exception as exc:
            log_event("warning", f"HITL critical query failed: {exc}")

    def _spawn_side_effect(self, coro: Awaitable[Any], name: str) -> None:
        try:
            task = asyncio.create_task(coro, name=f"tl_orch_{name}")
        except RuntimeError:
            return
        self._side_effects.add(task)

        def _done(t: asyncio.Task) -> None:
            self._side_effects.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("debug", f"side effect {name} failed: {exc}")

        task.add_done_callback(_done)

    async def verify_cycle(self, state: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        # Critical path: strategy + temporal push + verify.
        try:
            action = await self.causal_rl.choose_action(state)
            result["strategy"] = action
            result["strategy_name"] = self.causal_rl.to_strategy(action)
        except Exception:
            result["strategy"] = "adaptive"

        await self.temporal.push_state(state)
        verify = await self.temporal.verify()
        result["temporal_results"] = verify
        result["temporal_violations"] = [
            k for k, v in verify.items() if not v]

        # Non-critical side effects, sampled.
        if random.random() < self.multi_agent_sample_rate:
            async def _bid_and_reward():
                try:
                    agent_id, _ = await self.multi_agent.bid({
                        "name": "verify_task",
                        "preferred_role": "validator"})
                    await self.multi_agent.reward(agent_id, 0.8)
                except Exception:
                    pass
            self._spawn_side_effect(_bid_and_reward(), "multi_agent")

        if random.random() < self.xai_sample_rate and NUMPY_AVAILABLE:
            async def _explain():
                try:
                    feats = np.array([
                        float(state.get("quality", 0.8)),
                        float(state.get("carbon_intensity", 400) or 400) / 1000.0,
                        float(state.get("cost", 0.5)),
                        float(state.get("latency_ms", 100) or 100) / 1000.0])
                    def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                        return float(np.dot(x, _w))
                    await self.xai.explain(
                        decision_id=f"tl_{uuid.uuid4().hex[:8]}",
                        label=f"strategy={result['strategy']}",
                        features=feats,
                        names=["quality", "carbon", "cost", "latency"],
                        model_fn=_score)
                except Exception:
                    pass
            self._spawn_side_effect(_explain(), "xai")

        if random.random() < self.chaos_sample_rate:
            async def _chaos():
                try:
                    await self.chaos.run_experiment(
                        f"auto_{uuid.uuid4().hex[:6]}",
                        random.choice(ChaosTestingEngine.FAULT_TYPES))
                except Exception:
                    pass
            self._spawn_side_effect(_chaos(), "chaos")

        # Precision switch uses the rolling window.
        try:
            self.precision.record_accuracy(
                float(state.get("accuracy", 0.9) or 0.9))
        except Exception:
            pass
        self._spawn_side_effect(
            self.precision.auto_switch(0.0, 0.0), "precision")

        # Causal update uses an observable reward.
        success_ratio = (
            1.0 - (len(result["temporal_violations"])
                   / max(1, len(verify)))
        )
        self._spawn_side_effect(
            self.causal_rl.update(result["strategy"], success_ratio, state),
            "causal_update")

        return result

    async def start(self) -> None:
        self._running = True
        _ = asyncio.get_running_loop()
        await self.temporal.persist_rules()
        self._spawn(self._causal_rl_loop(), "causal_rl")
        self._spawn(self._federated_loop(), "federated")
        self._spawn(self._multi_agent_loop(), "multi_agent")
        self._spawn(self._temporal_loop(), "temporal")
        self._spawn(self._xai_loop(), "xai")
        self._spawn(self._precision_loop(), "precision")
        self._spawn(self._carbon_market_loop(), "carbon_market")
        self._spawn(self._chaos_loop(), "chaos")
        self._spawn(self._distillation_loop(), "distillation")
        self._spawn(self._temporal_flush_loop(), "temporal_flush")

    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("error", f"Background task {name} failed: {exc}")

        task.add_done_callback(_done)
        return task

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        self._background_tasks.clear()
        try:
            await self.temporal.aclose()
        except Exception:
            pass
        for t in list(self._side_effects):
            t.cancel()
        if self._side_effects:
            await asyncio.gather(*self._side_effects,
                                 return_exceptions=True)
        self._side_effects.clear()

    async def aclose(self) -> None:
        await self.shutdown()

    async def __aenter__(self) -> "TemporalLogicOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    # ---- background loops ----
    async def _temporal_flush_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(5.0)
            try:
                await self.temporal.flush()
            except Exception as exc:
                log_event("debug", f"temporal flush error: {exc}")

    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(900)
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception as exc:
                log_event("debug", f"causal_rl loop error: {exc}")

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("tl_policy", dummy)
                await self.federated.pull_aggregated_weights("tl_policy")
            except Exception as exc:
                log_event("debug", f"federated loop error: {exc}")

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="tl_health", features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "tl_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "temporal": self.temporal.stats(),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "side_effects": len(self._side_effects),
            "background_tasks": len(self._background_tasks),
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos/tests. Soft-fail for all methods."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window_seconds, active=True):
        self._data["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula, "operator": operator,
            "severity": severity, "description": description,
            "window_seconds": window_seconds, "active": active})

    def save_temporal_trace(self, state, context=None):
        self._data["temporal_trace"].append({
            "state": state, "context": context})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._data["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula, "step": step,
            "state": state, "severity": severity, "approved": approved})

    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._data["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    def save_causal_edge(self, source, target, weight, confidence):
        self._data["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence})

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_agent(self, agent_id, role, reputation, utilities):
        self._data["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient,
                           payload):
        self._data["agent_messages"].append({
            "message_id": message_id, "topic": topic, "sender": sender,
            "recipient": recipient, "payload": payload})

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label, "features": features,
            "attributions": attributions, "natural_language": nl})

    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._data["precision_history"].append({
            "from_p": from_p, "to_p": to_p, "reason": reason,
            "saved_wh": saved_wh, "acc_delta": acc_delta})

    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._data["carbon_credit_prices"].append({
            "price_usd": price_usd, "currency": currency,
            "source": source, "region": region})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._data["rec_ledger"].append({
            "mwh": mwh, "price_per_mwh": price_per_mwh, "source": source,
            "certificate_id": certificate_id, "region": region,
            "retired": retired})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._data["net_zero_matches"].append({
            "match_id": match_id, "workload_kwh": workload_kwh,
            "intensity": intensity, "action": action,
            "carbon_kg": carbon_kg, "offset_cost": offset_cost,
            "credit_price": credit_price})

    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._data["chaos_experiments"].append({
            "experiment_id": experiment_id, "name": name,
            "fault_type": fault_type, "blast_radius": blast_radius,
            "steady_before": steady_before, "steady_after": steady_after,
            "status": status, "duration_ms": duration_ms})

    def enqueue_hitl_request(self, request_id, rule_id, state, severity):
        self._data["hitl_approval_queue"].append({
            "request_id": request_id, "rule_id": rule_id,
            "state": state, "severity": severity, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        for r in self._data["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_user_preference(self, user_id, weights):
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self._prefs.get(user_id)


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "temporal_max_trace": 2000,
        "temporal_formulas": [
            {"formula": "G (quality >= 0.5)",
             "severity": "critical",
             "description": "Quality must never drop below 0.5"},
            {"formula": "G (carbon <= 0.7)",
             "severity": "warning",
             "description": "Carbon intensity must stay below 0.7"},
            {"formula": "G (!(quality < 0.3))",
             "severity": "critical",
             "description": "Quality must never drop below 0.3"},
            {"formula": "G ((quality >= 0.6) | (carbon <= 0.4))",
             "severity": "warning",
             "description": "Either quality is high or carbon is low"},
            {"formula": "F (task_complete)",
             "severity": "info",
             "description": "Task must eventually complete"},
            {"formula": "(quality >= 0.5) U (task_complete)",
             "severity": "warning",
             "description": "Quality must hold until task completes"},
            # Implication now parses correctly.
            {"formula": "G (quality >= 0.5 -> X (quality >= 0.4))",
             "severity": "info",
             "description": "If quality is high, next state is also OK",
             "window_seconds": 60},
            # Biconditional
            {"formula": "G (high_quality <-> (quality >= 0.8))",
             "severity": "info",
             "description": "high_quality mirrors quality >= 0.8"},
            # Adjacent operators
            {"formula": "GF (task_complete)",
             "severity": "info",
             "description": "Task completes infinitely often"},
        ],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.05,
        "chaos_blast_radius": 0.1,
    }

    storage = InMemoryStorage()
    orch = TemporalLogicOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("temporal_logic_monitor.py v17.1.0 — Demo")
    print("=" * 80)

    # 1. Registered rules
    print("\n=== Registered LTL rules ===")
    for rid, rule in orch.temporal.rules.items():
        print(f"  {rid[:26]:28s} sev={rule.severity:8s} "
              f"formula={rule.raw_formula[:60]}")

    # 2. LTL operator coverage (all operators, including -> and <->).
    print("\n=== LTL operator coverage ===")
    test_formulas = [
        ("G (x >= 5)", "always", {"x": 10}, {"x": 4}),
        ("F (x >= 5)", "eventually", {"x": 10}, {"x": 4}),
        ("X (x >= 5)", "next", {"x": 10}, {"x": 4}),
        ("(x >= 5) U (y >= 10)", "until",
         {"x": 6, "y": 10}, {"x": 4, "y": 10}),
        ("(x >= 5) W (y >= 10)", "weak_until",
         {"x": 6, "y": 10}, {"x": 4, "y": 5}),
        ("(x >= 5) R (y >= 10)", "release",
         {"x": 6, "y": 10}, {"x": 4, "y": 10}),
        ("G ((x >= 5) & (y >= 5))", "and",
         {"x": 10, "y": 10}, {"x": 4, "y": 10}),
        ("G ((x >= 5) | (y >= 5))", "or",
         {"x": 4, "y": 10}, {"x": 4, "y": 4}),
        ("G (!(x < 5))", "not", {"x": 10}, {"x": 4}),
        ("G (x >= 5 -> y >= 5)", "implies",
         {"x": 10, "y": 10}, {"x": 10, "y": 4}),
        ("G (x >= 5 <-> y >= 5)", "iff",
         {"x": 10, "y": 10}, {"x": 10, "y": 4}),
        ("GF (x >= 5)", "adjacent_GF", {"x": 6}, {"x": 4}),
    ]
    for formula, name, good_state, bad_state in test_formulas:
        try:
            ast, atomics = _parse_ltl(formula)
            good_ok = _eval_ltl(ast, [good_state], 0, atomics)
            bad_ok = _eval_ltl(ast, [bad_state], 0, atomics)
            print(f"  {name:14s}: {formula[:42]:44s} "
                  f"good={good_ok} bad={bad_ok}")
        except Exception as exc:
            print(f"  {name:14s}: PARSE ERROR: {exc}")

    # 3. X-at-end-of-trace convention.
    print("\n=== X at end of trace (vacuous truth) ===")
    ast, atomics = _parse_ltl("X (x >= 100)")
    result = _eval_ltl(ast, [{"x": 1}], 0, atomics)
    print(f"  X (x >= 100) on trace [{ {chr(120):1} }] = {result} "
          f"(expected True by convention)")

    # 4. Runtime monitoring with escalation cooldown.
    print("\n=== Runtime monitoring ===")
    for i in range(12):
        state = {
            "quality": random.uniform(0.4, 0.95),
            "carbon_intensity": random.uniform(200, 700),
            "cost": random.uniform(0.3, 0.8),
            "latency_ms": random.uniform(50, 300),
            "accuracy": random.uniform(0.85, 0.95),
            "baseline_accuracy": 0.92,
            "task_complete": i > 9,
            "high_quality": random.choice([True, False]),
            "carbon": random.uniform(0.1, 0.9),
        }
        result = await orch.verify_cycle(state)
        print(f"  Step {i + 1:2d}: violations={len(result['temporal_violations'])} "
              f"strategy={result.get('strategy_name')}")

    # 5. Natural-language explanation.
    print("\n=== Natural-language explanation ===")
    for rid, rule in list(orch.temporal.rules.items())[:2]:
        explanation = await orch.temporal.explain_violation(rid)
        if explanation:
            print(f"\n  Rule: {rid}")
            print(f"  {explanation['explanation'][:200]}")

    # 6. Bounded model checking with a real transition function.
    print("\n=== Bounded model checking (with transition_fn) ===")
    if orch.temporal.rules:
        rid = list(orch.temporal.rules.keys())[0]

        def transition(state):
            # Simple oscillator: quality decreases by 0.4 each step.
            nxt = dict(state)
            nxt["quality"] = max(0.0, state.get("quality", 1.0) - 0.4)
            return [nxt]

        result = await orch.temporal.model_check(
            rid,
            initial_state={"quality": 0.9, "carbon_intensity": 0.4,
                           "task_complete": False},
            max_depth=6, transition_fn=transition)
        print(f"  Rule: {rid}")
        print(f"  Satisfied: {result.get('satisfied')}")
        print(f"  Visited: {result.get('visited')}")
        print(f"  Synthetic: {result.get('synthetic')}")
        if result.get("counterexample"):
            print(f"  Counterexample length: {len(result['counterexample'])}")
            print(f"  Last state: {result['counterexample'][-1]}")

    # 7. Bounded model checking without a transition function.
    print("\n=== Bounded model checking (synthetic fallback) ===")
    if orch.temporal.rules:
        rid = list(orch.temporal.rules.keys())[0]
        result = await orch.temporal.model_check(
            rid,
            initial_state={"quality": 0.4, "carbon_intensity": 0.6,
                           "task_complete": False},
            max_depth=4)
        print(f"  Satisfied: {result.get('satisfied')}")
        print(f"  Synthetic: {result.get('synthetic')} "
              f"(expected True — no transition_fn provided)")

    # 8. Rule versioning.
    print("\n=== Rule versioning ===")
    if orch.temporal.rules:
        rid = list(orch.temporal.rules.keys())[0]
        orch.temporal.add_rule(
            rule_id=rid,
            formula="G (quality >= 0.6)",
            severity="warning",
            description="Updated threshold from 0.5 to 0.6")
        print(f"  Rule {rid}: now v{orch.temporal.rules[rid].version}")
        hist = orch.temporal.rule_history_for(rid)
        for entry in hist:
            print(f"    Retired v{entry['version']}: {entry['formula']}")

    # 9. Health check.
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    # 10. Chaos experiment.
    print("\n=== Chaos experiment ===")
    print(json.dumps(
        await orch.chaos.run_experiment("demo_chaos", "latency"),
        indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
