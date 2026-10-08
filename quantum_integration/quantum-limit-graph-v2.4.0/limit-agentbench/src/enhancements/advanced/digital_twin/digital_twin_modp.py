# src/quantum_integration/digital_twin/digital_twin_modp.py

"""Pareto-front MODP solver.

Two algorithms are provided:

1. :meth:`MODPOptimizer.pareto_filter_final_stage` — the *legacy* solver
   (previously called ``solve``). Pareto-filters the states at the
   numerically largest stage. No transition traversal; no backward
   induction. Preserved for callers that depended on the original
   ``solve`` semantics.

2. :meth:`MODPOptimizer.solve` — *genuine backward induction*. Walks
   transitions from the final stage back to the initial state,
   accumulating objective deltas and Pareto-filtering at each state.
   The returned front is rooted at the caller-supplied ``initial_state``.

Both methods return the same dict shape so callers can switch by
changing the method name only.

Thread safety
-------------
The in-process index is guarded by a re-entrant lock. Storage backends
are assumed to be thread-safe on their own; the manager does not wrap
storage calls in the local lock.

Missing-key semantics
---------------------
``_dominates`` treats two objective vectors as incomparable if their
key sets differ. This is the safest default and prevents silent
false-domination when comparing heterogeneous problems.

Minimization
------------
``minimize`` accepts either a ``bool`` (uniform direction for all
objectives) or a ``Mapping[str, bool]`` (per-objective direction). The
mapping form lets you mix, e.g., minimize ``cost`` and maximize
``resilience`` in one solve.
"""

from __future__ import annotations

import logging
import math
import threading
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Union,
    runtime_checkable,
)

from .digital_twin_errors import (
    DigitalTwinInputError,
    DigitalTwinNotFoundError,
)

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

#: Sentinel returned by ``_try_storage`` when the backend does not
#: implement the requested hook.
_NO_STORAGE: Any = object()

#: ``True``/``False`` for uniform direction, mapping for per-objective.
MinimizeSpec = Union[bool, Mapping[str, bool]]


# --------------------------------------------------------------------------- #
# Storage protocol
# --------------------------------------------------------------------------- #

@runtime_checkable
class _ModpStorage(Protocol):
    """Duck-typed protocol for MODP storage backends.

    Every method is optional. The manager probes for individual methods
    at runtime so a backend may implement only a subset.
    """

    # -- problems -------------------------------------------------------- #
    def list_modp_problems(self) -> List[str]: ...

    # -- states ---------------------------------------------------------- #
    def save_modp_state(
        self,
        state_id: str,
        problem_id: str,
        state_attributes: Any,
        objective_values: Any,
        stage: int,
    ) -> None: ...

    def get_modp_states(self, problem_id: str) -> List[Dict[str, Any]]: ...

    def get_modp_state(
        self, problem_id: str, state_id: str,
    ) -> Optional[Dict[str, Any]]: ...

    def remove_modp_state(self, problem_id: str, state_id: str) -> bool: ...

    # -- transitions ----------------------------------------------------- #
    def save_modp_transition(
        self,
        transition_id: str,
        problem_id: str,
        from_state: str,
        to_state: str,
        action: str,
        cost: float,
        objective_deltas: Any,
    ) -> None: ...

    def get_modp_transitions(
        self, problem_id: str,
    ) -> List[Dict[str, Any]]: ...

    def get_modp_transition(
        self, problem_id: str, transition_id: str,
    ) -> Optional[Dict[str, Any]]: ...

    def remove_modp_transition(
        self, problem_id: str, transition_id: str,
    ) -> bool: ...

    # -- policies -------------------------------------------------------- #
    def save_modp_policy(
        self,
        policy_id: str,
        problem_id: str,
        state_id: str,
        action: str,
        expected_objectives: Any,
    ) -> None: ...

    def get_modp_policies(
        self, problem_id: str,
    ) -> List[Dict[str, Any]]: ...

    def get_modp_policy(
        self, problem_id: str, policy_id: str,
    ) -> Optional[Dict[str, Any]]: ...

    def remove_modp_policy(
        self, problem_id: str, policy_id: str,
    ) -> bool: ...

    # -- lifecycle ------------------------------------------------------- #
    def flush(self) -> None: ...
    def close(self) -> None: ...


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #

def _validate_id(name: str, value: Any) -> str:
    """Return ``value`` if it is a non-empty, non-whitespace string."""
    if not isinstance(value, str) or not value.strip():
        raise DigitalTwinInputError(
            f"{name} must be a non-empty, non-whitespace string."
        )
    return value


def _validate_mapping(name: str, value: Any) -> Mapping[str, Any]:
    """Return ``value`` if it is a mapping."""
    if not isinstance(value, ABCMapping):
        raise DigitalTwinInputError(f"{name} must be a mapping.")
    return value


def _validate_finite_float(
    name: str, value: Any, *, allow_none: bool = False,
) -> Optional[float]:
    """Return ``value`` as a finite ``float`` (or ``None`` if allowed).

    Rejects ``bool``, ``NaN``, ``±inf``.
    """
    if value is None:
        if allow_none:
            return None
        raise DigitalTwinInputError(f"{name} must be a finite number.")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DigitalTwinInputError(f"{name} must be a finite number.")
    if not math.isfinite(value):
        raise DigitalTwinInputError(
            f"{name} must be finite (no NaN or infinity)."
        )
    return float(value)


def _validate_stage(name: str, value: Any) -> int:
    """Return ``value`` as a non-negative ``int`` (``bool`` is rejected)."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise DigitalTwinInputError(f"{name} must be a non-negative integer.")
    if value < 0:
        raise DigitalTwinInputError(f"{name} must be non-negative.")
    return int(value)


def _validate_objective_values(
    name: str, value: Any, *, allow_empty: bool = True,
) -> Dict[str, float]:
    """Return a plain ``Dict[str, float]`` with finite numeric values.

    Rejects non-string keys and non-finite floats.
    """
    _validate_mapping(name, value)
    out: Dict[str, float] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k.strip():
            raise DigitalTwinInputError(
                f"{name} keys must be non-empty strings."
            )
        out[k] = _validate_finite_float(f"{name}[{k!r}]", v)  # type: ignore[assignment]
    if not allow_empty and not out:
        raise DigitalTwinInputError(f"{name} must not be empty.")
    return out


# --------------------------------------------------------------------------- #
# Pareto helpers
# --------------------------------------------------------------------------- #

def _resolve_minimize(
    spec: MinimizeSpec, key: str, *, default: bool = True,
) -> bool:
    """Resolve a per-objective minimization flag from ``spec``."""
    if isinstance(spec, bool):
        return spec
    if not isinstance(spec, ABCMapping):
        raise DigitalTwinInputError(
            "minimize must be a bool or a mapping of objective -> bool."
        )
    return bool(spec.get(key, default))


def _dominates(
    a: Mapping[str, float],
    b: Mapping[str, float],
    *,
    minimize: MinimizeSpec = True,
) -> bool:
    """Return ``True`` iff ``a`` Pareto-dominates ``b``.

    Two vectors with differing key sets are treated as incomparable
    (returns ``False`` in both directions).
    """
    if set(a) != set(b):
        return False
    strict = False
    for k in a:
        av = a[k]
        bv = b[k]
        if _resolve_minimize(minimize, k):
            if av > bv:
                return False
            if av < bv:
                strict = True
        else:
            if av < bv:
                return False
            if av > bv:
                strict = True
    return strict


def _pareto_filter(
    points: List["ParetoPoint"], *, minimize: MinimizeSpec = True,
) -> List["ParetoPoint"]:
    """Greedy Pareto filter preserving the first of any tied pair."""
    front: List[ParetoPoint] = []
    for p in points:
        dominated = False
        for q in front:
            if _dominates(
                q.objective_values, p.objective_values, minimize=minimize,
            ):
                dominated = True
                break
        if dominated:
            continue
        front = [
            q for q in front
            if not _dominates(
                p.objective_values, q.objective_values, minimize=minimize,
            )
        ]
        front.append(p)
    return front


def _merge_objectives(
    child: Mapping[str, float],
    delta: Mapping[str, float],
    *,
    add_missing_as_zero: bool = True,
) -> Dict[str, float]:
    """Combine a child's objective values with a transition's deltas.

    Missing keys are treated as ``0.0`` when ``add_missing_as_zero``.
    """
    keys = set(child) | set(delta)
    if add_missing_as_zero:
        return {k: float(child.get(k, 0.0)) + float(delta.get(k, 0.0))
                for k in keys}
    # Strict mode: only keys present in both are combined.
    keys = set(child) & set(delta)
    return {k: float(child[k]) + float(delta[k]) for k in keys}


def _point_to_dict(p: "ParetoPoint") -> Dict[str, Any]:
    """Serialize a ParetoPoint to a plain dict."""
    return {
        "state_id": p.state_id,
        "objective_values": dict(p.objective_values),
        "stage": p.stage,
        "parent_state_id": p.parent_state_id,
        "action": p.action,
    }


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ParetoPoint:
    """A single point on a Pareto front."""

    state_id: str
    objective_values: Mapping[str, float]
    stage: int
    parent_state_id: Optional[str] = None
    action: Optional[str] = None

    _hash: int = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        _validate_id("state_id", self.state_id)
        if (
            isinstance(self.stage, bool)
            or not isinstance(self.stage, int)
            or self.stage < 0
        ):
            raise DigitalTwinInputError(
                "stage must be a non-negative integer."
            )
        if self.parent_state_id is not None:
            _validate_id("parent_state_id", self.parent_state_id)
        if self.action is not None and not isinstance(self.action, str):
            raise DigitalTwinInputError("action must be a string or None.")

        normalized = _validate_objective_values(
            "objective_values", self.objective_values,
        )
        object.__setattr__(
            self, "objective_values", MappingProxyType(normalized),
        )
        object.__setattr__(
            self,
            "_hash",
            hash((
                self.state_id,
                tuple(sorted(normalized.items())),
                self.stage,
                self.parent_state_id,
                self.action,
            )),
        )

    def __hash__(self) -> int:  # type: ignore[override]
        return self._hash


@dataclass(frozen=True)
class ParetoFront:
    """A Pareto front for a single problem."""

    problem_id: str
    points: Tuple[ParetoPoint, ...] = ()

    def __post_init__(self) -> None:
        _validate_id("problem_id", self.problem_id)
        object.__setattr__(self, "points", tuple(self.points))

    def __len__(self) -> int:
        return len(self.points)

    def __iter__(self):
        return iter(self.points)

    def __bool__(self) -> bool:
        return bool(self.points)

    def dominated_by(
        self, other: ParetoPoint, *, minimize: MinimizeSpec = True,
    ) -> Tuple[ParetoPoint, ...]:
        """Return points dominated by ``other``.

        Example
        -------
        >>> front.dominated_by(ParetoPoint(...), minimize={"cost": True})
        """
        return tuple(
            p for p in self.points
            if _dominates(
                other.objective_values, p.objective_values, minimize=minimize,
            )
        )

    def to_list(self) -> List[Dict[str, Any]]:
        """Return the front as plain dicts (serialization-friendly)."""
        return [_point_to_dict(p) for p in self.points]


# --------------------------------------------------------------------------- #
# Optimizer
# --------------------------------------------------------------------------- #

class MODPOptimizer:
    """Multi-objective DP solver with Pareto-front filtering.

    Uses ``storage`` when provided, otherwise keeps an in-process index.
    Both views are consulted by the dispatcher ``_try_storage``: the
    storage path is preferred whenever the backend implements the
    corresponding hook.
    """

    ACTION_SPACE_KEY = "action"

    def __init__(
        self,
        storage: Optional[Any] = None,
        *,
        thread_safe: bool = True,
    ) -> None:
        self.storage = storage
        self._states: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._transitions: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._policies: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._lock = threading.RLock() if thread_safe else _NullLock()
        if storage is not None and not isinstance(storage, _ModpStorage):
            logger.debug(
                "Storage backend %s does not fully implement _ModpStorage; "
                "partial support will be used.",
                type(storage).__name__,
            )

    # -- storage dispatch -------------------------------------------------- #

    def _try_storage(
        self, method_name: str, *args: Any, **kwargs: Any,
    ) -> Any:
        """Call a storage hook if present, else return :data:`_NO_STORAGE`."""
        if self.storage is None:
            return _NO_STORAGE
        fn = getattr(self.storage, method_name, None)
        if fn is None:
            return _NO_STORAGE
        return fn(*args, **kwargs)

    # -- problem discovery ------------------------------------------------- #

    def _all_problem_ids(self) -> List[str]:
        problems = set(self._states) | set(self._transitions) | set(self._policies)
        listed = self._try_storage("list_modp_problems")
        if listed is not _NO_STORAGE:
            problems.update(listed)
        return sorted(problems)

    # -- state existence --------------------------------------------------- #

    def _state_exists(self, problem_id: str, state_id: str) -> bool:
        result = self._try_storage("get_modp_state", problem_id, state_id)
        if result is not _NO_STORAGE:
            return result is not None
        states = self._try_storage("get_modp_states", problem_id)
        if states is not _NO_STORAGE:
            return any(s.get("state_id") == state_id for s in states)
        with self._lock:
            return state_id in self._states.get(problem_id, {})

    def _ensure_state_absent(self, problem_id: str, state_id: str) -> None:
        if self._state_exists(problem_id, state_id):
            raise DigitalTwinInputError(
                f"state {state_id!r} already exists in problem "
                f"{problem_id!r}."
            )

    def _ensure_transition_absent(
        self, problem_id: str, transition_id: str,
    ) -> None:
        result = self._try_storage(
            "get_modp_transition", problem_id, transition_id,
        )
        if result is not _NO_STORAGE:
            if result is not None:
                raise DigitalTwinInputError(
                    f"transition {transition_id!r} already exists in "
                    f"problem {problem_id!r}."
                )
            return
        transitions = self._try_storage("get_modp_transitions", problem_id)
        if transitions is not _NO_STORAGE:
            if any(
                t.get("transition_id") == transition_id for t in transitions
            ):
                raise DigitalTwinInputError(
                    f"transition {transition_id!r} already exists in "
                    f"problem {problem_id!r}."
                )
            return
        with self._lock:
            if transition_id in self._transitions.get(problem_id, {}):
                raise DigitalTwinInputError(
                    f"transition {transition_id!r} already exists in "
                    f"problem {problem_id!r}."
                )

    def _ensure_policy_absent(
        self, problem_id: str, policy_id: str,
    ) -> None:
        result = self._try_storage(
            "get_modp_policy", problem_id, policy_id,
        )
        if result is not _NO_STORAGE:
            if result is not None:
                raise DigitalTwinInputError(
                    f"policy {policy_id!r} already exists in problem "
                    f"{problem_id!r}."
                )
            return
        policies = self._try_storage("get_modp_policies", problem_id)
        if policies is not _NO_STORAGE:
            if any(p.get("policy_id") == policy_id for p in policies):
                raise DigitalTwinInputError(
                    f"policy {policy_id!r} already exists in problem "
                    f"{problem_id!r}."
                )
            return
        with self._lock:
            if policy_id in self._policies.get(problem_id, {}):
                raise DigitalTwinInputError(
                    f"policy {policy_id!r} already exists in problem "
                    f"{problem_id!r}."
                )

    # -- writers ----------------------------------------------------------- #

    def add_state(
        self,
        state_id: str,
        problem_id: str,
        state_attributes: Mapping[str, Any],
        objective_values: Mapping[str, float],
        stage: int,
    ) -> None:
        """Add a state to a problem.

        Raises
        ------
        DigitalTwinInputError
            On duplicate ``state_id`` or invalid input.
        """
        _validate_id("state_id", state_id)
        _validate_id("problem_id", problem_id)
        _validate_mapping("state_attributes", state_attributes)
        stage_v = _validate_stage("stage", stage)
        objectives_v = _validate_objective_values(
            "objective_values", objective_values,
        )
        self._ensure_state_absent(problem_id, state_id)

        attrs_snapshot = {str(k): v for k, v in state_attributes.items()}
        saved = self._try_storage(
            "save_modp_state",
            state_id,
            problem_id,
            attrs_snapshot,
            objectives_v,
            stage_v,
        )
        if saved is not _NO_STORAGE:
            return

        with self._lock:
            self._states.setdefault(problem_id, {})[state_id] = {
                "state_id": state_id,
                "state_attributes": attrs_snapshot,
                "objective_values": objectives_v,
                "stage": stage_v,
            }

    def add_transition(
        self,
        transition_id: str,
        problem_id: str,
        from_state: str,
        to_state: str,
        action: str,
        cost: float,
        objective_deltas: Mapping[str, float],
    ) -> None:
        """Add a transition between two existing states.

        Raises
        ------
        DigitalTwinInputError
            On duplicate ``transition_id``, invalid input, or self-loop.
        DigitalTwinNotFoundError
            If ``from_state`` or ``to_state`` do not exist.
        """
        _validate_id("transition_id", transition_id)
        _validate_id("problem_id", problem_id)
        _validate_id("from_state", from_state)
        _validate_id("to_state", to_state)
        if not isinstance(action, str) or not action.strip():
            raise DigitalTwinInputError(
                "action must be a non-empty, non-whitespace string."
            )
        cost_v = _validate_finite_float("cost", cost)
        deltas_v = _validate_objective_values(
            "objective_deltas", objective_deltas,
        )

        if from_state == to_state:
            raise DigitalTwinInputError(
                f"self-loops are not allowed (from_state == to_state == "
                f"{from_state!r})."
            )
        for role, sid in (("from_state", from_state), ("to_state", to_state)):
            if not self._state_exists(problem_id, sid):
                raise DigitalTwinNotFoundError(
                    f"{role} {sid!r} does not exist in problem "
                    f"{problem_id!r}.",
                    problem_id=problem_id,
                    state_id=sid,
                    role=role,
                )
        self._ensure_transition_absent(problem_id, transition_id)

        saved = self._try_storage(
            "save_modp_transition",
            transition_id,
            problem_id,
            from_state,
            to_state,
            action,
            cost_v,
            deltas_v,
        )
        if saved is not _NO_STORAGE:
            return

        with self._lock:
            self._transitions.setdefault(problem_id, {})[transition_id] = {
                "transition_id": transition_id,
                "from_state": from_state,
                "to_state": to_state,
                "action": action,
                "cost": cost_v,
                "objective_deltas": deltas_v,
            }

    def add_policy(
        self,
        policy_id: str,
        problem_id: str,
        state_id: str,
        action: str,
        expected_objectives: Mapping[str, float],
    ) -> None:
        """Attach a policy recommendation to an existing state."""
        _validate_id("policy_id", policy_id)
        _validate_id("problem_id", problem_id)
        _validate_id("state_id", state_id)
        if not isinstance(action, str) or not action.strip():
            raise DigitalTwinInputError(
                "action must be a non-empty, non-whitespace string."
            )
        objectives_v = _validate_objective_values(
            "expected_objectives", expected_objectives,
        )
        if not self._state_exists(problem_id, state_id):
            raise DigitalTwinNotFoundError(
                f"state {state_id!r} does not exist in problem "
                f"{problem_id!r}.",
                problem_id=problem_id,
                state_id=state_id,
            )
        self._ensure_policy_absent(problem_id, policy_id)

        saved = self._try_storage(
            "save_modp_policy",
            policy_id,
            problem_id,
            state_id,
            action,
            objectives_v,
        )
        if saved is not _NO_STORAGE:
            return

        with self._lock:
            self._policies.setdefault(problem_id, {})[policy_id] = {
                "policy_id": policy_id,
                "state_id": state_id,
                "action": action,
                "expected_objectives": objectives_v,
            }

    # -- readers ----------------------------------------------------------- #

    def get_states(self, problem_id: str) -> List[Dict[str, Any]]:
        """Return all states for ``problem_id`` (empty list if unknown)."""
        _validate_id("problem_id", problem_id)
        result = self._try_storage("get_modp_states", problem_id)
        if result is not _NO_STORAGE:
            return [dict(s) for s in result]
        with self._lock:
            return [dict(s) for s in self._states.get(problem_id, {}).values()]

    def get_state(
        self, problem_id: str, state_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return a single state, or ``None``."""
        _validate_id("problem_id", problem_id)
        _validate_id("state_id", state_id)
        result = self._try_storage("get_modp_state", problem_id, state_id)
        if result is not _NO_STORAGE:
            return None if result is None else dict(result)
        states = self._try_storage("get_modp_states", problem_id)
        if states is not _NO_STORAGE:
            match = next(
                (s for s in states if s.get("state_id") == state_id), None,
            )
            return None if match is None else dict(match)
        with self._lock:
            s = self._states.get(problem_id, {}).get(state_id)
            return None if s is None else dict(s)

    def get_transitions(self, problem_id: str) -> List[Dict[str, Any]]:
        """Return all transitions for ``problem_id`` (empty list if unknown)."""
        _validate_id("problem_id", problem_id)
        result = self._try_storage("get_modp_transitions", problem_id)
        if result is not _NO_STORAGE:
            return [dict(t) for t in result]
        with self._lock:
            return [
                dict(t)
                for t in self._transitions.get(problem_id, {}).values()
            ]

    def get_transition(
        self, problem_id: str, transition_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return a single transition, or ``None``."""
        _validate_id("problem_id", problem_id)
        _validate_id("transition_id", transition_id)
        result = self._try_storage(
            "get_modp_transition", problem_id, transition_id,
        )
        if result is not _NO_STORAGE:
            return None if result is None else dict(result)
        transitions = self._try_storage("get_modp_transitions", problem_id)
        if transitions is not _NO_STORAGE:
            match = next(
                (t for t in transitions
                 if t.get("transition_id") == transition_id),
                None,
            )
            return None if match is None else dict(match)
        with self._lock:
            t = self._transitions.get(problem_id, {}).get(transition_id)
            return None if t is None else dict(t)

    def get_policies(self, problem_id: str) -> List[Dict[str, Any]]:
        """Return all policies for ``problem_id`` (empty list if unknown)."""
        _validate_id("problem_id", problem_id)
        result = self._try_storage("get_modp_policies", problem_id)
        if result is not _NO_STORAGE:
            return [dict(p) for p in result]
        with self._lock:
            return [
                dict(p)
                for p in self._policies.get(problem_id, {}).values()
            ]

    def get_policy(
        self, problem_id: str, policy_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Return a single policy, or ``None``."""
        _validate_id("problem_id", problem_id)
        _validate_id("policy_id", policy_id)
        result = self._try_storage(
            "get_modp_policy", problem_id, policy_id,
        )
        if result is not _NO_STORAGE:
            return None if result is None else dict(result)
        policies = self._try_storage("get_modp_policies", problem_id)
        if policies is not _NO_STORAGE:
            match = next(
                (p for p in policies if p.get("policy_id") == policy_id),
                None,
            )
            return None if match is None else dict(match)
        with self._lock:
            p = self._policies.get(problem_id, {}).get(policy_id)
            return None if p is None else dict(p)

    # -- removers ---------------------------------------------------------- #

    def remove_state(self, problem_id: str, state_id: str) -> bool:
        """Remove a state. Cascades to transitions touching it."""
        _validate_id("problem_id", problem_id)
        _validate_id("state_id", state_id)
        result = self._try_storage(
            "remove_modp_state", problem_id, state_id,
        )
        if result is not _NO_STORAGE:
            return bool(result)
        with self._lock:
            states = self._states.get(problem_id, {})
            if state_id not in states:
                return False
            del states[state_id]
            # Cascade: remove transitions that reference this state.
            transitions = self._transitions.get(problem_id, {})
            stale = [
                tid for tid, t in transitions.items()
                if t["from_state"] == state_id or t["to_state"] == state_id
            ]
            for tid in stale:
                del transitions[tid]
            # Cascade: remove policies for this state.
            policies = self._policies.get(problem_id, {})
            stale_p = [
                pid for pid, p in policies.items()
                if p["state_id"] == state_id
            ]
            for pid in stale_p:
                del policies[pid]
            return True

    def remove_transition(
        self, problem_id: str, transition_id: str,
    ) -> bool:
        """Remove a transition."""
        _validate_id("problem_id", problem_id)
        _validate_id("transition_id", transition_id)
        result = self._try_storage(
            "remove_modp_transition", problem_id, transition_id,
        )
        if result is not _NO_STORAGE:
            return bool(result)
        with self._lock:
            return (
                self._transitions
                .get(problem_id, {})
                .pop(transition_id, None)
                is not None
            )

    def remove_policy(self, problem_id: str, policy_id: str) -> bool:
        """Remove a policy."""
        _validate_id("problem_id", problem_id)
        _validate_id("policy_id", policy_id)
        result = self._try_storage(
            "remove_modp_policy", problem_id, policy_id,
        )
        if result is not _NO_STORAGE:
            return bool(result)
        with self._lock:
            return (
                self._policies
                .get(problem_id, {})
                .pop(policy_id, None)
                is not None
            )

    # -- solvers ----------------------------------------------------------- #

    async def pareto_filter_final_stage(
        self,
        problem_id: str,
        initial_state: Optional[Mapping[str, Any]] = None,
        *,
        max_stages: int = 10,
        minimize: MinimizeSpec = True,
    ) -> Dict[str, Any]:
        """Legacy solver: Pareto-filter over the states at the final stage.

        This is the historical ``solve`` implementation. It does not
        traverse transitions or perform backward induction; it simply
        collects the states whose ``stage`` equals ``max(stage)`` and
        filters them.

        Parameters
        ----------
        problem_id:
            The problem to solve.
        initial_state:
            Ignored (accepted for API compatibility).
        max_stages:
            Must be at least 1. Not enforced against the actual depth in
            this legacy path.
        minimize:
            ``bool`` or per-objective mapping. See module docstring.
        """
        _validate_id("problem_id", problem_id)
        if max_stages < 1:
            raise DigitalTwinInputError("max_stages must be >= 1.")

        states = self.get_states(problem_id)
        if not states:
            raise DigitalTwinNotFoundError(
                f"problem {problem_id!r} has no states.",
                problem_id=problem_id,
            )
        transitions = self.get_transitions(problem_id)

        by_stage: Dict[int, List[Dict[str, Any]]] = {}
        for s in states:
            by_stage.setdefault(int(s.get("stage", 0)), []).append(s)

        final_stage = max(by_stage.keys())
        candidates: List[ParetoPoint] = []
        for s in by_stage.get(final_stage, []):
            objectives_v = _validate_objective_values(
                "objective_values", s.get("objective_values") or {},
            )
            candidates.append(ParetoPoint(
                state_id=s["state_id"],
                objective_values=objectives_v,
                stage=final_stage,
            ))
        front = _pareto_filter(candidates, minimize=minimize)

        return {
            "schema_version": SCHEMA_VERSION,
            "status": "solved",
            "method": "pareto_filter_final_stage",
            "pareto_front": [_point_to_dict(p) for p in front],
            "stages": len(by_stage),
            "stage_counts": {int(k): len(v) for k, v in by_stage.items()},
            "states_considered": len(states),
            "transitions_considered": len(transitions),
        }

    async def solve(
        self,
        problem_id: str,
        initial_state: Mapping[str, Any],
        *,
        max_stages: int = 10,
        minimize: MinimizeSpec = True,
    ) -> Dict[str, Any]:
        """Genuine backward-induction MODP solver.

        Walks transitions from the final stage back to
        ``initial_state``. At each state, the accumulated objective
        vectors of all outgoing transitions are Pareto-filtered to
        produce the state's value front.

        Merging semantics
        -----------------
        For a transition ``t`` from ``s`` to ``s'`` and a point ``p`` on
        ``V[s']``, the candidate vector is
        ``p.objective_values + t.objective_deltas`` (missing keys = 0).

        Parameters
        ----------
        problem_id:
            The problem to solve.
        initial_state:
            A mapping that must contain ``state_id`` (or be resolvable
            when exactly one stage-0 state exists).
        max_stages:
            Upper bound on the number of stages. If the deepest stage
            exceeds this, ``DigitalTwinInputError`` is raised.
        minimize:
            ``bool`` or per-objective mapping.

        Returns
        -------
        dict
            Same shape as :meth:`pareto_filter_final_stage`, plus
            ``initial_state_id``.
        """
        _validate_id("problem_id", problem_id)
        _validate_mapping("initial_state", initial_state)
        if max_stages < 1:
            raise DigitalTwinInputError("max_stages must be >= 1.")

        states = self.get_states(problem_id)
        if not states:
            raise DigitalTwinNotFoundError(
                f"problem {problem_id!r} has no states.",
                problem_id=problem_id,
            )
        transitions = self.get_transitions(problem_id)

        # -- Index states and transitions ---------------------------------- #
        by_stage: Dict[int, List[Dict[str, Any]]] = {}
        by_id: Dict[str, Dict[str, Any]] = {}
        for s in states:
            sid = s["state_id"]
            by_id[sid] = s
            by_stage.setdefault(int(s.get("stage", 0)), []).append(s)

        by_source: Dict[str, List[Dict[str, Any]]] = {}
        for t in transitions:
            by_source.setdefault(t["from_state"], []).append(t)

        if not by_stage:
            raise DigitalTwinNotFoundError(
                f"problem {problem_id!r} has no stage information.",
                problem_id=problem_id,
            )
        final_stage = max(by_stage.keys())
        if final_stage + 1 > max_stages:
            raise DigitalTwinInputError(
                f"problem depth {final_stage + 1} exceeds "
                f"max_stages={max_stages}."
            )

        # -- V[s] = Pareto front at state s --------------------------------- #
        value_front: Dict[str, List[ParetoPoint]] = {}

        # Seed leaf states at the final stage.
        for s in by_stage.get(final_stage, []):
            sid = s["state_id"]
            objectives_v = _validate_objective_values(
                "objective_values", s.get("objective_values") or {},
            )
            value_front[sid] = [ParetoPoint(
                state_id=sid,
                objective_values=objectives_v,
                stage=final_stage,
            )]

        # Backward induction.
        for stage in range(final_stage - 1, -1, -1):
            for s in by_stage.get(stage, []):
                sid = s["state_id"]
                outgoing = by_source.get(sid, [])
                if not outgoing:
                    # Leaf at a non-final stage: use its own objectives.
                    objectives_v = _validate_objective_values(
                        "objective_values",
                        s.get("objective_values") or {},
                    )
                    value_front[sid] = [ParetoPoint(
                        state_id=sid,
                        objective_values=objectives_v,
                        stage=stage,
                    )]
                    continue

                candidates: List[ParetoPoint] = []
                for t in outgoing:
                    child_front = value_front.get(t["to_state"])
                    if not child_front:
                        continue
                    deltas_v = _validate_objective_values(
                        "objective_deltas", t.get("objective_deltas") or {},
                    )
                    for child in child_front:
                        merged = _merge_objectives(
                            child.objective_values, deltas_v,
                        )
                        candidates.append(ParetoPoint(
                            state_id=sid,
                            objective_values=merged,
                            stage=stage,
                            parent_state_id=child.state_id,
                            action=t.get("action"),
                        ))
                value_front[sid] = _pareto_filter(
                    candidates, minimize=minimize,
                )

        # -- Resolve initial state ---------------------------------------- #
        init_id = initial_state.get("state_id") if initial_state else None
        if not init_id:
            stage0 = by_stage.get(0, [])
            if len(stage0) == 1:
                init_id = stage0[0]["state_id"]
            else:
                raise DigitalTwinInputError(
                    "initial_state must provide 'state_id' when the "
                    "problem has multiple (or zero) stage-0 states."
                )
        if init_id not in value_front:
            raise DigitalTwinNotFoundError(
                f"initial state {init_id!r} is not reachable in problem "
                f"{problem_id!r}.",
                problem_id=problem_id,
                state_id=init_id,
            )

        front = tuple(value_front[init_id])
        return {
            "schema_version": SCHEMA_VERSION,
            "status": "solved",
            "method": "backward_induction",
            "pareto_front": [_point_to_dict(p) for p in front],
            "stages": len(by_stage),
            "stage_counts": {int(k): len(v) for k, v in by_stage.items()},
            "states_considered": len(states),
            "transitions_considered": len(transitions),
            "initial_state_id": init_id,
        }

    # -- introspection ----------------------------------------------------- #

    def statistics(self) -> Dict[str, Any]:
        """Return an enriched snapshot of the manager's state."""
        problems = self._all_problem_ids()
        per_problem: Dict[str, Dict[str, int]] = {}
        total_states = total_transitions = total_policies = 0
        for pid in problems:
            ns = len(self.get_states(pid))
            nt = len(self.get_transitions(pid))
            np_ = len(self.get_policies(pid))
            per_problem[pid] = {
                "states": ns,
                "transitions": nt,
                "policies": np_,
            }
            total_states += ns
            total_transitions += nt
            total_policies += np_

        return {
            "schema_version": SCHEMA_VERSION,
            "problems_tracked": len(problems),
            "problems": problems,
            "per_problem": per_problem,
            "states_total": total_states,
            "transitions_total": total_transitions,
            "policies_total": total_policies,
            "uses_storage": self.storage is not None,
            "storage_type": (
                type(self.storage).__name__ if self.storage else None
            ),
        }

    def close(self, *, flush: bool = True) -> None:
        """Flush and/or close the storage backend, if any."""
        if self.storage is None:
            return
        if flush:
            fn = getattr(self.storage, "flush", None)
            if fn is not None:
                fn()
        fn = getattr(self.storage, "close", None)
        if fn is not None:
            fn()

    # -- serialization ----------------------------------------------------- #

    def to_dict(self) -> Dict[str, Any]:
        """Serialize the in-memory index to a plain dict."""
        return {
            "schema_version": SCHEMA_VERSION,
            "states": {
                pid: {sid: dict(s) for sid, s in smap.items()}
                for pid, smap in self._states.items()
            },
            "transitions": {
                pid: {tid: dict(t) for tid, t in tmap.items()}
                for pid, tmap in self._transitions.items()
            },
            "policies": {
                pid: {pid2: dict(p) for pid2, p in pmap.items()}
                for pid, pmap in self._policies.items()
            },
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        storage: Optional[Any] = None,
    ) -> "MODPOptimizer":
        """Rebuild an optimizer from :meth:`to_dict` output."""
        version = int(data.get("schema_version", 0))
        if version > SCHEMA_VERSION:
            raise DigitalTwinInputError(
                f"schema_version {version} is newer than supported "
                f"({SCHEMA_VERSION})."
            )
        obj = cls(storage=storage)
        for pid, smap in (data.get("states") or {}).items():
            for sid, s in smap.items():
                obj.add_state(
                    state_id=sid,
                    problem_id=pid,
                    state_attributes=s.get("state_attributes", {}),
                    objective_values=s.get("objective_values", {}),
                    stage=int(s.get("stage", 0)),
                )
        for pid, tmap in (data.get("transitions") or {}).items():
            for tid, t in tmap.items():
                obj.add_transition(
                    transition_id=tid,
                    problem_id=pid,
                    from_state=t["from_state"],
                    to_state=t["to_state"],
                    action=t["action"],
                    cost=float(t.get("cost", 0.0)),
                    objective_deltas=t.get("objective_deltas", {}),
                )
        for pid, pmap in (data.get("policies") or {}).items():
            for pid2, p in pmap.items():
                obj.add_policy(
                    policy_id=pid2,
                    problem_id=pid,
                    state_id=p["state_id"],
                    action=p["action"],
                    expected_objectives=p.get("expected_objectives", {}),
                )
        return obj

    # -- dunders ----------------------------------------------------------- #

    def __contains__(self, problem_id: object) -> bool:
        if not isinstance(problem_id, str) or not problem_id.strip():
            return False
        return problem_id in self._all_problem_ids()

    def __repr__(self) -> str:
        with self._lock:
            n = len(self._all_problem_ids())
        storage_name = (
            type(self.storage).__name__ if self.storage is not None else None
        )
        return (
            f"{type(self).__name__}(problems={n}, storage={storage_name})"
        )


# --------------------------------------------------------------------------- #
# Fallback no-op lock
# --------------------------------------------------------------------------- #

class _NullLock:
    """A no-op context manager used when ``thread_safe=False``."""

    def __enter__(self) -> "_NullLock":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


__all__ = [
    "SCHEMA_VERSION",
    "MinimizeSpec",
    "MODPOptimizer",
    "ParetoFront",
    "ParetoPoint",
    "_ModpStorage",
]
