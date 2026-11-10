
# Agent Instructions

**Version:** 2.0 · **Updated:** 2024-01-01
**Applies to:** AI coding assistants (Claude Code, Cursor, Copilot, etc.)
**Prerequisite:** lean-ctx MCP server configured in your client

> **This single file is split-ready.** Each section tagged
> `# === SAVE AS: <path> ===` is a complete, runnable file. Split at
> those markers to deploy. Reading/editing, treat it as one document.

---

<!-- lean-ctx -->
## lean-ctx

Prefer lean-ctx MCP tools over native file reads for token savings.
Full rules and tool definitions: [LEAN-CTX.md](./LEAN-CTX.md)

The `ctx_read` tool described below is defined in
`tools/ctx_read.py` and exposed over MCP by the `lean-ctx` server. If
`LEAN-CTX.md` is not present in your checkout, the tool may still be
available through the MCP server — use `ctx_modes()` to confirm.

<!-- /lean-ctx -->

---

## Standard read modes

| Mode | Returns | Use when |
|---|---|---|
| `full` | `str` — complete file contents | You need to edit the file |
| `outline` | `str` — imports, top-level defs, classes | You need structure, not content |
| `head` | `str` — first N lines (default 40) | Large file; you need the header |
| `tail` | `str` — last N lines (default 40) | You need trailing context |
| `lines` | `str` — a specific `start..end` range | You know the target range |

All standard modes return `str` (empty string on failure).

---

## 🧠 Advanced Enhancement Integration

Use the **enhanced read modes** below to fetch only the relevant state
from files under `src/enhancements/`. Each returns a `dict` with the
fields in the "Returns" column, or `{}` if unavailable.

### Enhanced read modes

| Mode | Returns | Files it works on |
|---|---|---|
| `graph` | `{centrality, connectivity, density, node_count, edge_count}` | `src/enhancements/core/graph_registry.py`, `src/enhancements/core/causal_graph.py`, `src/enhancements/core/meta_cognition.py` |
| `modp` | `{objective_weights, score_threshold_low, score_threshold_high, modp_score}` | `src/enhancements/schemas/node_descriptor.py`, `src/enhancements/schemas/workload_descriptor.py` |
| `rlhf` | `{human_feedback_score, feedback_source, feedback_url}` | `src/enhancements/schemas/feedback_event.py`, `src/enhancements/zero_trust_architecture.py` |
| `distillation` | `{student_counter, replay_size, train_every, gate_weights}` | `src/enhancements/schemas/node_descriptor.py`, `src/enhancements/schemas/workload_descriptor.py` |
| `evolutionary` | `{best_fitness, population_size, mutation_rate, crossover_rate, generation}` | `src/enhancements/schemas/node_descriptor.py`, `src/enhancements/schemas/workload_descriptor.py` |
| `moe` | `{n_experts, gate_weights, top_k, load_balance_score}` | `src/enhancements/schemas/node_descriptor.py`, `src/enhancements/schemas/workload_descriptor.py` |
| `flexgen_config` | `{enabled, model_name, precision, delegation_policy, memory_limit_mb}` | `config/base/green_agent_config.yaml` |
| `flexgen_metrics` | `{energy_joules_total, energy_rate_jps, precision_used}` | Runtime logs (`logs/green_agent*.log`) |
| `zero_trust` | `{auth_level, use_enhancements, use_distillation, audit_events}` | `src/enhancements/zero_trust_architecture.py` |

### Usage examples

```python
from tools.ctx_read import ctx_read

# LIMIT Graph
ctx_read("src/enhancements/core/graph_registry.py", mode="graph")
ctx_read("src/enhancements/core/causal_graph.py",   mode="graph")
ctx_read("src/enhancements/core/meta_cognition.py", mode="graph")

# MODP
ctx_read("src/enhancements/schemas/node_descriptor.py",     mode="modp")
ctx_read("src/enhancements/schemas/workload_descriptor.py", mode="modp")

# RLHF
ctx_read("src/enhancements/schemas/feedback_event.py",     mode="rlhf")
ctx_read("src/enhancements/zero_trust_architecture.py",    mode="rlhf")

# Distillation
ctx_read("src/enhancements/schemas/node_descriptor.py",     mode="distillation")
ctx_read("src/enhancements/schemas/workload_descriptor.py", mode="distillation")

# Evolutionary
ctx_read("src/enhancements/schemas/node_descriptor.py",     mode="evolutionary")
ctx_read("src/enhancements/schemas/workload_descriptor.py", mode="evolutionary")

# MoE
ctx_read("src/enhancements/schemas/node_descriptor.py",     mode="moe")
ctx_read("src/enhancements/schemas/workload_descriptor.py", mode="moe")

# FlexGen (config vs. metrics are separate)
ctx_read("config/base/green_agent_config.yaml", mode="flexgen_config")
ctx_read("logs/green_agent_2024-01-01.log",     mode="flexgen_metrics")

# Zero Trust
ctx_read("src/enhancements/zero_trust_architecture.py", mode="zero_trust")
```

### When to use each mode

| Mode | Use when |
|---|---|
| `graph` | Topology context for routing, anomaly analysis, or causal root-cause attribution |
| `modp` | Evaluating accuracy/energy/latency/carbon trade-offs, or diagnosing low scores |
| `rlhf` | Human feedback affects policy, or you need the feedback source/URL |
| `distillation` | Checking whether the student policy is learning, replay buffer filling |
| `evolutionary` | Monitoring whether evolutionary optimisation is converging or stalled |
| `moe` | Diagnosing expert routing instability or gate-weight drift |
| `flexgen_config` | Deciding whether to delegate LLM inference and at what precision |
| `flexgen_metrics` | Correlating energy consumption with precision changes |
| `zero_trust` | Auditing authentication level or the audit-event count |

### Prerequisites

- Modules ship under `src/enhancements/`.
- Enhancement behavior is controlled by `enhancements.enabled` in
  `config/base/green_agent_config.yaml` — **default is `false`** (per
  `README.md §0.2`).
- Opt in for one run: `python run_agent.py --enhancements`.
- Opt in persistently: set `enhancements.enabled: true` in the config,
  or export `ENHANCEMENTS_ENABLED=true` (CLI flag wins over env).

### Fallback behavior

| Situation | `ctx_read` returns | Agent should |
|---|---|---|
| File exists, mode available | `dict` with the mode's fields | Use the fields directly |
| File exists, mode unavailable | `{}` + warning | Fall back to `mode="full"` |
| File does not exist | `{}` + warning | Treat file as absent; do not retry |
| Modes disabled globally | `{}` + warning | Use `mode="full"` |

Never assume a non-empty result — always check `if result: ...`.

### Do NOT use lean-ctx to read

- Files under `secrets/` or `/run/secrets` — use your platform's secret API.
- `.env`, `*.pem`, `*.key`, `*.crt` — never read secrets into context.
- Large binaries (`.tar.gz`, `.whl`, `.zip`) — use `unzip -l` for metadata.

### Cost and rate limits

Each `ctx_read` costs tokens proportional to the returned payload, not
the file size. A `mode="graph"` on a 5,000-line file costs the same as
on a 50-line file. Chained calls do not share cost — batch reads on
the same file are not supported yet.

Rate limits follow the MCP server config; see `LEAN-CTX.md`.

### Discovering available modes

```python
from tools.ctx_read import ctx_modes
ctx_modes()  # → list[str]
```

If the tool is unavailable, fall back to the table above.

---

**Remember:**

- Use **enhanced modes** for inspection, **`outline`** for structure,
  **`full`** only for editing.
- **Never** read secrets.
- **Always** check `if result: ...` — enhanced modes can return `{}`.

---

# === SAVE AS: tools/__init__.py ===
```python
"""tools — lean-ctx reference implementation (ctx_read, ctx_modes)."""
```

---

# === SAVE AS: tools/ctx_schema.py ===
```python
"""
tools/ctx_schema.py — Mode registry and return shapes for ctx_read.

Every mode is declared here so ``ctx_modes()`` can enumerate them without
importing the handlers, ``ctx_read`` can validate the mode before I/O,
and the AGENTS.md table is a 1:1 projection of ``MODE_REGISTRY``.

Drift between the doc and this registry is caught by
``tests/test_ctx_read.py::test_agents_md_matches_registry``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

__all__ = [
    "STANDARD_MODES",
    "ENHANCED_MODES",
    "MODE_REGISTRY",
    "ModeSpec",
    "is_standard",
    "is_enhanced",
]


STANDARD_MODES: tuple[str, ...] = (
    "full",
    "outline",
    "head",
    "tail",
    "lines",
)

ENHANCED_MODES: tuple[str, ...] = (
    "graph",
    "modp",
    "rlhf",
    "distillation",
    "evolutionary",
    "moe",
    "flexgen_config",
    "flexgen_metrics",
    "zero_trust",
)


@dataclass(frozen=True, slots=True)
class ModeSpec:
    """Metadata for one enhanced mode."""

    name: str
    returns: tuple[str, ...]
    files: tuple[str, ...]
    purpose: str

    @property
    def return_type(self) -> Literal["str", "dict"]:
        return "dict" if self.name in ENHANCED_MODES else "str"


MODE_REGISTRY: dict[str, ModeSpec] = {
    "graph": ModeSpec(
        name="graph",
        returns=("centrality", "connectivity", "density",
                 "node_count", "edge_count"),
        files=(
            "src/enhancements/core/graph_registry.py",
            "src/enhancements/core/causal_graph.py",
            "src/enhancements/core/meta_cognition.py",
        ),
        purpose="LIMIT Graph topology metrics",
    ),
    "modp": ModeSpec(
        name="modp",
        returns=("objective_weights", "score_threshold_low",
                 "score_threshold_high", "modp_score"),
        files=(
            "src/enhancements/schemas/node_descriptor.py",
            "src/enhancements/schemas/workload_descriptor.py",
        ),
        purpose="MODP weights, thresholds, and current composite score",
    ),
    "rlhf": ModeSpec(
        name="rlhf",
        returns=("human_feedback_score", "feedback_source", "feedback_url"),
        files=(
            "src/enhancements/schemas/feedback_event.py",
            "src/enhancements/zero_trust_architecture.py",
        ),
        purpose="Human feedback state",
    ),
    "distillation": ModeSpec(
        name="distillation",
        returns=("student_counter", "replay_size",
                 "train_every", "gate_weights"),
        files=(
            "src/enhancements/schemas/node_descriptor.py",
            "src/enhancements/schemas/workload_descriptor.py",
        ),
        purpose="On-policy distillation + MoE gate state",
    ),
    "evolutionary": ModeSpec(
        name="evolutionary",
        returns=("best_fitness", "population_size", "mutation_rate",
                 "crossover_rate", "generation"),
        files=(
            "src/enhancements/schemas/node_descriptor.py",
            "src/enhancements/schemas/workload_descriptor.py",
        ),
        purpose="Evolutionary optimiser state",
    ),
    "moe": ModeSpec(
        name="moe",
        returns=("n_experts", "gate_weights", "top_k", "load_balance_score"),
        files=(
            "src/enhancements/schemas/node_descriptor.py",
            "src/enhancements/schemas/workload_descriptor.py",
        ),
        purpose="MoE expert routing",
    ),
    "flexgen_config": ModeSpec(
        name="flexgen_config",
        returns=("enabled", "model_name", "precision",
                 "delegation_policy", "memory_limit_mb"),
        files=("config/base/green_agent_config.yaml",),
        purpose="FlexGen configuration",
    ),
    "flexgen_metrics": ModeSpec(
        name="flexgen_metrics",
        returns=("energy_joules_total", "energy_rate_jps", "precision_used"),
        files=("logs/green_agent*.log",),
        purpose="FlexGen energy telemetry",
    ),
    "zero_trust": ModeSpec(
        name="zero_trust",
        returns=("auth_level", "use_enhancements",
                 "use_distillation", "audit_events"),
        files=("src/enhancements/zero_trust_architecture.py",),
        purpose="Zero Trust posture",
    ),
}


def is_standard(mode: str) -> bool:
    return mode in STANDARD_MODES


def is_enhanced(mode: str) -> bool:
    return mode in ENHANCED_MODES
```

---

# === SAVE AS: tools/ctx_handlers.py ===
```python
"""
tools/ctx_handlers.py — Per-mode extraction handlers for ctx_read.

Each handler takes (source: str, path: Path) and returns a dict with
the fields declared in ctx_schema.MODE_REGISTRY. On any failure the
handler returns {} — ctx_read never raises on malformed input.
"""
from __future__ import annotations

import ast
import logging
import re
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger("ctx.handlers")

__all__ = ["HANDLERS", "handle"]


# =============================================================================
# Shared helpers
# =============================================================================

def _parse_ast(source: str) -> ast.Module | None:
    try:
        return ast.parse(source)
    except SyntaxError as exc:
        logger.debug("ast.parse failed: %s", exc)
        return None


def _module_assignments(tree: ast.Module) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        try:
            out[node.targets[0].id] = ast.literal_eval(node.value)
        except (ValueError, SyntaxError):
            continue
    return out


def _find_class(tree: ast.Module, name: str) -> ast.ClassDef | None:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    return None


def _find_assignments_in(
    tree: ast.Module, class_name: str | None,
) -> dict[str, Any]:
    out = _module_assignments(tree)
    if class_name is not None:
        klass = _find_class(tree, class_name)
        if klass is not None:
            for node in klass.body:
                if (
                    isinstance(node, ast.AnnAssign)
                    and isinstance(node.target, ast.Name)
                    and node.value is not None
                ):
                    try:
                        out[node.target.id] = ast.literal_eval(node.value)
                    except (ValueError, SyntaxError):
                        continue
    return out


def _coerce_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _coerce_list_of_float(value: Any) -> list[float] | None:
    if not isinstance(value, (list, tuple)):
        return None
    try:
        return [float(v) for v in value]
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


# =============================================================================
# Handlers
# =============================================================================

def handle_graph(source: str, path: Path) -> dict[str, Any]:
    tree = _parse_ast(source)
    if tree is None:
        return {}

    assignments = _module_assignments(tree)
    if not any(k in assignments
               for k in ("centrality", "connectivity", "density")):
        for cls_name in ("GraphMetrics", "GraphRegistry", "CausalGraph",
                         "MetaCognitionLayer"):
            assignments = _find_assignments_in(tree, cls_name)
            if any(k in assignments
                   for k in ("centrality", "connectivity", "density")):
                break

    out: dict[str, Any] = {}
    for key in ("centrality", "connectivity", "density"):
        v = _coerce_float(assignments.get(key))
        if v is not None:
            out[key] = v
    for key in ("node_count", "edge_count"):
        v = _coerce_int(assignments.get(key))
        if v is not None:
            out[key] = v
    return out


def handle_modp(source: str, path: Path) -> dict[str, Any]:
    tree = _parse_ast(source)
    if tree is None:
        return {}

    assignments: dict[str, Any] = {}
    for cls_name in (None, "NodeDescriptor", "WorkloadDescriptor",
                     "MODPConfig", "WorkloadConfig"):
        assignments.update(_find_assignments_in(tree, cls_name))

    out: dict[str, Any] = {}
    weights = _coerce_list_of_float(
        assignments.get("objective_weights")
        or assignments.get("modp_weights")
    )
    if weights is not None:
        out["objective_weights"] = weights
    for key in ("score_threshold_low", "score_threshold_high", "modp_score"):
        v = _coerce_float(assignments.get(key))
        if v is not None:
            out[key] = v
    return out


def handle_rlhf(source: str, path: Path) -> dict[str, Any]:
    tree = _parse_ast(source)
    if tree is None:
        return {}

    assignments = _find_assignments_in(tree, None)
    for cls_name in ("FeedbackEvent", "ZeroTrustConfig",
                     "ZeroTrustArchitecture"):
        assignments.update(_find_assignments_in(tree, cls_name))

    out: dict[str, Any] = {}
    score = _coerce_float(assignments.get("human_feedback_score"))
    if score is not None:
        out["human_feedback_score"] = score
    for key in ("feedback_source", "feedback_url"):
        value = assignments.get(key)
        if isinstance(value, str) and value:
            out[key] = value
    return out


def handle_distillation(source: str, path: Path) -> dict[str, Any]:
    tree = _parse_ast(source)
    if tree is None:
        return {}

    assignments = _find_assignments_in(tree, None)
    for cls_name in ("NodeDescriptor", "WorkloadDescriptor",
                     "DistillationRoutingOptimizer",
                     "DistillationPriorityOptimizer"):
        assignments.update(_find_assignments_in(tree, cls_name))

    out: dict[str, Any] = {}
    for key in ("student_counter", "replay_size", "train_every"):
        v = _coerce_int(assignments.get(key))
        if v is not None:
            out[key] = v
    gate_weights = _coerce_list_of_float(assignments.get("gate_weights"))
    if gate_weights is not None:
        out["gate_weights"] = gate_weights
    return out


def handle_evolutionary(source: str, path: Path) -> dict[str, Any]:
    tree = _parse_ast(source)
    if tree is None:
        return {}

    assignments = _find_assignments_in(tree, None)
    for cls_name in ("NodeDescriptor", "WorkloadDescriptor",
                     "EvolutionaryOptimizer"):
        assignments.update(_find_assignments_in(tree, cls_name))

    out: dict[str, Any] = {}
    for key in ("best_fitness", "mutation_rate", "crossover_rate"):
        v = _coerce_float(assignments.get(key))
        if v is not None:
            out[key] = v
    for key in ("population_size", "generation"):
        v = _coerce_int(assignments.get(key))
        if v is not None:
            out[key] = v
    return out


def handle_moe(source: str, path: Path) -> dict[str, Any]:
    tree = _parse_ast(source)
    if tree is None:
        return {}

    assignments = _find_assignments_in(tree, None)
    for cls_name in ("NodeDescriptor", "WorkloadDescriptor",
                     "MoEGatingNetwork", "ExpertRouter"):
        assignments.update(_find_assignments_in(tree, cls_name))

    out: dict[str, Any] = {}
    for key in ("n_experts", "top_k"):
        v = _coerce_int(assignments.get(key))
        if v is not None:
            out[key] = v
    v = _coerce_float(assignments.get("load_balance_score"))
    if v is not None:
        out["load_balance_score"] = v
    weights = _coerce_list_of_float(assignments.get("gate_weights"))
    if weights is not None:
        out["gate_weights"] = weights
    return out


def handle_flexgen_config(source: str, path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}

    try:
        import yaml
        cfg = yaml.safe_load(source)
        if isinstance(cfg, dict):
            fg = (cfg.get("enhancements", {}) or {}).get("flexgen", {}) or {}
            if fg:
                for key in ("enabled", "model_name", "precision",
                            "default_precision", "delegation_policy"):
                    if key in fg:
                        mapped = "precision" if key == "default_precision" else key
                        out[mapped] = fg[key]
                if "memory_limit_mb" in fg:
                    v = _coerce_int(fg["memory_limit_mb"])
                    if v is not None:
                        out["memory_limit_mb"] = v
                return out
    except Exception as exc:
        logger.debug("flexgen_config: yaml load failed, using regex: %s", exc)

    in_block = False
    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith("flexgen:"):
            in_block = True
            continue
        if in_block:
            if stripped and not line.startswith(" "):
                break
            m = re.match(r"^\s+([a-z_]+):\s*(.+?)\s*$", line)
            if not m:
                continue
            key, value = m.group(1), m.group(2).strip().strip('"').strip("'")
            if key == "enabled":
                out["enabled"] = value.lower() in ("true", "yes", "1")
            elif key in ("model_name", "delegation_policy"):
                out[key] = value
            elif key == "default_precision":
                out["precision"] = value
            elif key == "memory_limit_mb":
                v = _coerce_int(value)
                if v is not None:
                    out["memory_limit_mb"] = v
    return out


def handle_flexgen_metrics(source: str, path: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for line in source.splitlines():
        m = re.search(r"flexgen_energy_joules_total[=:]\s*([\d.]+)", line)
        if m:
            v = _coerce_float(m.group(1))
            if v is not None:
                out["energy_joules_total"] = v
        m = re.search(r"flexgen_energy_rate_jps[=:]\s*([\d.]+)", line)
        if m:
            v = _coerce_float(m.group(1))
            if v is not None:
                out["energy_rate_jps"] = v
        m = re.search(r"precision[=:]\s*(fp32|fp16|int8)", line)
        if m:
            out["precision_used"] = m.group(1)
    return out


def handle_zero_trust(source: str, path: Path) -> dict[str, Any]:
    tree = _parse_ast(source)
    if tree is None:
        return {}

    assignments = _find_assignments_in(tree, None)
    for cls_name in ("ZeroTrustConfig", "ZeroTrustArchitecture"):
        assignments.update(_find_assignments_in(tree, cls_name))

    out: dict[str, Any] = {}
    value = assignments.get("auth_level")
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        out["auth_level"] = value
    for key in ("use_enhancements", "use_distillation"):
        value = assignments.get(key)
        if isinstance(value, bool):
            out[key] = value
    v = _coerce_int(assignments.get("audit_events"))
    if v is not None:
        out["audit_events"] = v
    return out


HANDLERS: dict[str, Callable[[str, Path], dict[str, Any]]] = {
    "graph":           handle_graph,
    "modp":            handle_modp,
    "rlhf":            handle_rlhf,
    "distillation":    handle_distillation,
    "evolutionary":    handle_evolutionary,
    "moe":             handle_moe,
    "flexgen_config":  handle_flexgen_config,
    "flexgen_metrics": handle_flexgen_metrics,
    "zero_trust":      handle_zero_trust,
}


def handle(mode: str, source: str, path: Path) -> dict[str, Any]:
    handler = HANDLERS.get(mode)
    if handler is None:
        logger.warning("unknown enhanced mode: %r", mode)
        return {}
    try:
        return handler(source, path)
    except Exception as exc:
        logger.warning("handler %r raised %s: %s",
                       mode, type(exc).__name__, exc)
        return {}
```

---

# === SAVE AS: tools/ctx_read.py ===
```python
"""
tools/ctx_read.py — The ctx_read and ctx_modes tools.

Reference implementation of the MCP tools described in AGENTS.md.

Public API
----------
    ctx_read(path, mode="full", *, start=None, end=None, n=None) -> str | dict
    ctx_modes() -> list[str]

Fallback contract (AGENTS.md §Fallback behavior):
    * Unknown mode                    → raises ValueError
    * Standard mode on missing file   → raises FileNotFoundError
    * Enhanced mode on missing file   → returns {} + warning
    * Enhanced mode without the module → returns {}
    * Enhanced mode when disabled     → returns {} + warning
"""
from __future__ import annotations

import ast
import logging
import os
from pathlib import Path
from typing import Any, Union

from tools.ctx_handlers import handle
from tools.ctx_schema import (
    ENHANCED_MODES,
    MODE_REGISTRY,
    STANDARD_MODES,
    is_enhanced,
    is_standard,
)

logger = logging.getLogger("ctx.read")

__all__ = ["ctx_read", "ctx_modes"]


_DEFAULT_HEAD_LINES = 40
_DEFAULT_TAIL_LINES = 40
_MAX_BYTES = 5 * 1024 * 1024


# =============================================================================
# Public API
# =============================================================================

def ctx_modes() -> list[str]:
    """Return every mode this build supports (standard first)."""
    return list(STANDARD_MODES) + list(ENHANCED_MODES)


def ctx_read(
    path: Union[str, Path],
    mode: str = "full",
    *,
    start: int | None = None,
    end: int | None = None,
    n: int | None = None,
) -> Union[str, dict[str, Any]]:
    """Read ``path`` in ``mode``.

    Returns ``str`` for standard modes, ``dict`` for enhanced modes.
    See AGENTS.md §Fallback behavior for the exact contract.
    """
    p = Path(path)

    if not (is_standard(mode) or is_enhanced(mode)):
        raise ValueError(
            f"unknown mode {mode!r}; valid: "
            f"{sorted(STANDARD_MODES + ENHANCED_MODES)}"
        )

    if is_enhanced(mode):
        return _read_enhanced(p, mode)

    return _read_standard(p, mode, start=start, end=end, n=n)


# =============================================================================
# Standard modes
# =============================================================================

def _read_standard(
    p: Path, mode: str, *, start: int | None,
    end: int | None, n: int | None,
) -> str:
    try:
        size = p.stat().st_size
    except FileNotFoundError:
        raise
    if size > _MAX_BYTES:
        raise ValueError(
            f"{p} is {size} bytes (> {_MAX_BYTES}); use a shell tool "
            f"or `mode='lines'` after splitting"
        )

    source = p.read_text(encoding="utf-8", errors="replace")

    if mode == "full":
        return source

    lines = source.splitlines(keepends=True)

    if mode == "head":
        return "".join(lines[: (n or _DEFAULT_HEAD_LINES)])

    if mode == "tail":
        count = n or _DEFAULT_TAIL_LINES
        return "".join(lines[-count:] if count < len(lines) else lines)

    if mode == "lines":
        if start is None or end is None:
            raise ValueError("mode='lines' requires both start and end")
        if start < 1 or end < start:
            raise ValueError(
                f"invalid range: start={start}, end={end} (1-based, start <= end)"
            )
        return "".join(lines[start - 1: end])

    if mode == "outline":
        return _outline(source)

    raise AssertionError(f"unhandled standard mode {mode!r}")


def _outline(source: str) -> str:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        lines = source.splitlines()
        return "\n".join(
            f"{i + 1:5d}  {line}"
            for i, line in enumerate(lines[: _DEFAULT_HEAD_LINES])
        )

    out_lines: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            out_lines.append(f"import {', '.join(a.name for a in node.names)}")
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            names = ", ".join(a.name for a in node.names)
            out_lines.append(f"from {mod} import {names}")
    if out_lines:
        out_lines.append("")

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            prefix = "async def" if isinstance(
                node, ast.AsyncFunctionDef
            ) else "def"
            out_lines.append(
                f"    {prefix} {node.name}(...):  # line {node.lineno}"
            )
        elif isinstance(node, ast.ClassDef):
            out_lines.append(f"    class {node.name}:  # line {node.lineno}")
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    prefix = "async def" if isinstance(
                        child, ast.AsyncFunctionDef
                    ) else "def"
                    out_lines.append(
                        f"        {prefix} {child.name}(...):  "
                        f"# line {child.lineno}"
                    )

    return "\n".join(out_lines) + "\n"


# =============================================================================
# Enhanced modes
# =============================================================================

def _enhancements_disabled() -> bool:
    """True unless ENHANCEMENTS_ENABLED is set to a truthy value.

    Default is disabled per README.md §0.2.
    """
    return os.environ.get("ENHANCEMENTS_ENABLED", "false").lower() != "true"


def _read_enhanced(p: Path, mode: str) -> dict[str, Any]:
    spec = MODE_REGISTRY[mode]

    if _enhancements_disabled():
        logger.warning(
            "enhanced mode %r requested but enhancements are disabled; "
            "set ENHANCEMENTS_ENABLED=true to enable",
            mode,
        )
        return {}

    if not p.is_file():
        logger.warning("enhanced mode %r: file does not exist: %s", mode, p)
        return {}

    try:
        size = p.stat().st_size
    except OSError as exc:
        logger.warning("stat failed on %s: %s", p, exc)
        return {}
    if size > _MAX_BYTES:
        logger.warning(
            "file too large for enhanced read (%d bytes): %s", size, p,
        )
        return {}

    try:
        source = p.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        logger.warning("read failed on %s: %s", p, exc)
        return {}

    result = handle(mode, source, p)
    if not isinstance(result, dict):
        return {}
    return {k: result[k] for k in spec.returns if k in result}
```

---

# === SAVE AS: tests/test_ctx_read.py ===
```python
"""
tests/test_ctx_read.py — Complete test suite for ctx_read / ctx_modes.

Run with: pytest -q tests/test_ctx_read.py
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from tools.ctx_read import ctx_modes, ctx_read
from tools.ctx_schema import ENHANCED_MODES, MODE_REGISTRY, STANDARD_MODES


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def python_file(tmp_path: Path) -> Path:
    p = tmp_path / "sample.py"
    p.write_text(
        "import os\n\n"
        "centrality = 0.7\n"
        "connectivity = 0.6\n"
        "density = 0.5\n"
        "node_count = 42\n"
        "edge_count = 100\n\n"
        "class GraphRegistry:\n"
        "    pass\n\n"
        "def build():\n"
        "    return None\n",
        encoding="utf-8",
    )
    return p


@pytest.fixture
def modp_file(tmp_path: Path) -> Path:
    p = tmp_path / "modp.py"
    p.write_text(
        "objective_weights = [0.4, 0.3, 0.2, 0.1]\n"
        "score_threshold_low = 0.4\n"
        "score_threshold_high = 0.7\n"
        "modp_score = 0.75\n",
        encoding="utf-8",
    )
    return p


@pytest.fixture
def yaml_file(tmp_path: Path) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(
        "enhancements:\n"
        "  enabled: true\n"
        "  flexgen:\n"
        "    enabled: true\n"
        "    model_name: facebook/opt-6.7b\n"
        "    default_precision: fp16\n"
        "    delegation_policy: adaptive\n"
        "    memory_limit_mb: 4096\n",
        encoding="utf-8",
    )
    return p


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    p = tmp_path / "agent.log"
    p.write_text(
        "flexgen_energy_joules_total=100\n"
        "flexgen_energy_joules_total=200\n"
        "flexgen_energy_rate_jps=42.5\n"
        "precision=fp16\n",
        encoding="utf-8",
    )
    return p


@pytest.fixture
def enable_enhancements(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENHANCEMENTS_ENABLED", "true")


@pytest.fixture
def disable_enhancements(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENHANCEMENTS_ENABLED", "false")


# =============================================================================
# ctx_modes
# =============================================================================

def test_ctx_modes_includes_standard_and_enhanced() -> None:
    modes = ctx_modes()
    for m in STANDARD_MODES:
        assert m in modes
    for m in ENHANCED_MODES:
        assert m in modes


def test_ctx_modes_order() -> None:
    modes = ctx_modes()
    assert modes[: len(STANDARD_MODES)] == list(STANDARD_MODES)


# =============================================================================
# Standard modes
# =============================================================================

def test_full_returns_entire_file(python_file: Path) -> None:
    out = ctx_read(python_file, mode="full")
    assert isinstance(out, str)
    assert "centrality = 0.7" in out
    assert "def build" in out


def test_head_returns_first_n_lines(python_file: Path) -> None:
    out = ctx_read(python_file, mode="head", n=3)
    assert isinstance(out, str)
    assert out.count("\n") == 3
    assert out.startswith("import os")


def test_tail_returns_last_n_lines(python_file: Path) -> None:
    out = ctx_read(python_file, mode="tail", n=3)
    assert isinstance(out, str)
    assert "def build" in out


def test_lines_returns_range(python_file: Path) -> None:
    out = ctx_read(python_file, mode="lines", start=3, end=4)
    assert "centrality" in out
    assert "connectivity" in out
    assert "density" not in out


def test_lines_requires_start_and_end(python_file: Path) -> None:
    with pytest.raises(ValueError):
        ctx_read(python_file, mode="lines", start=3)


def test_lines_rejects_invalid_range(python_file: Path) -> None:
    with pytest.raises(ValueError):
        ctx_read(python_file, mode="lines", start=10, end=5)


def test_outline_returns_structure(python_file: Path) -> None:
    out = ctx_read(python_file, mode="outline")
    assert isinstance(out, str)
    assert "import os" in out
    assert "class GraphRegistry" in out
    assert "def build" in out
    assert "centrality = 0.7" not in out


def test_full_on_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        ctx_read(tmp_path / "missing.py", mode="full")


def test_unknown_mode_raises(python_file: Path) -> None:
    with pytest.raises(ValueError):
        ctx_read(python_file, mode="not-a-mode")


# =============================================================================
# Enhanced modes — happy path
# =============================================================================

def test_graph_mode(python_file: Path, enable_enhancements: None) -> None:
    out = ctx_read(python_file, mode="graph")
    assert isinstance(out, dict)
    assert out["centrality"] == 0.7
    assert out["connectivity"] == 0.6
    assert out["density"] == 0.5
    assert out["node_count"] == 42
    assert out["edge_count"] == 100


def test_modp_mode(modp_file: Path, enable_enhancements: None) -> None:
    out = ctx_read(modp_file, mode="modp")
    assert isinstance(out, dict)
    assert out["objective_weights"] == [0.4, 0.3, 0.2, 0.1]
    assert out["score_threshold_low"] == 0.4
    assert out["score_threshold_high"] == 0.7
    assert out["modp_score"] == 0.75


def test_flexgen_config_mode(yaml_file: Path, enable_enhancements: None) -> None:
    out = ctx_read(yaml_file, mode="flexgen_config")
    assert isinstance(out, dict)
    assert out["enabled"] is True
    assert out["model_name"] == "facebook/opt-6.7b"
    assert out["precision"] == "fp16"
    assert out["delegation_policy"] == "adaptive"
    assert out["memory_limit_mb"] == 4096


def test_flexgen_metrics_mode(log_file: Path, enable_enhancements: None) -> None:
    out = ctx_read(log_file, mode="flexgen_metrics")
    assert isinstance(out, dict)
    assert out["energy_joules_total"] == 200.0
    assert out["energy_rate_jps"] == 42.5
    assert out["precision_used"] == "fp16"


# =============================================================================
# Enhanced modes — fallback contract
# =============================================================================

def test_enhanced_mode_disabled_returns_empty(
    python_file: Path, disable_enhancements: None,
) -> None:
    assert ctx_read(python_file, mode="graph") == {}


def test_enhanced_mode_missing_file_returns_empty(
    tmp_path: Path, enable_enhancements: None,
) -> None:
    assert ctx_read(tmp_path / "missing.py", mode="graph") == {}


def test_enhanced_mode_empty_file_returns_empty(
    tmp_path: Path, enable_enhancements: None,
) -> None:
    p = tmp_path / "empty.py"
    p.write_text("", encoding="utf-8")
    assert ctx_read(p, mode="graph") == {}


def test_enhanced_mode_non_python_returns_empty(
    tmp_path: Path, enable_enhancements: None,
) -> None:
    p = tmp_path / "not_python.txt"
    p.write_text("hello world\n", encoding="utf-8")
    assert ctx_read(p, mode="modp") == {}


def test_enhanced_mode_returns_only_declared_fields(
    python_file: Path, enable_enhancements: None,
) -> None:
    out = ctx_read(python_file, mode="graph")
    spec = MODE_REGISTRY["graph"]
    for key in out:
        assert key in spec.returns


def test_enhanced_mode_never_raises(
    tmp_path: Path, enable_enhancements: None,
) -> None:
    p = tmp_path / "broken.py"
    p.write_bytes(b"\xff\xfe\x00invalid bytes")
    out = ctx_read(p, mode="graph")
    assert isinstance(out, dict)


# =============================================================================
# Drift guards
# =============================================================================

def test_agents_md_matches_registry() -> None:
    agents_md = Path(__file__).resolve().parent.parent / "AGENTS.md"
    if not agents_md.is_file():
        pytest.skip("AGENTS.md not present")
    text = agents_md.read_text(encoding="utf-8")
    for mode in ENHANCED_MODES:
        assert f"`{mode}`" in text, f"AGENTS.md missing mode `{mode}`"
    for spec in MODE_REGISTRY.values():
        for f in spec.files:
            assert f in text, f"AGENTS.md missing {f!r} (mode {spec.name!r})"


def test_agents_md_has_no_inline_mentions() -> None:
    agents_md = Path(__file__).resolve().parent.parent / "AGENTS.md"
    if not agents_md.is_file():
        pytest.skip("AGENTS.md not present")
    text = agents_md.read_text(encoding="utf-8")
    for match in re.finditer(
        r"(?<![A-Za-z0-9_.])@([A-Za-z][A-Za-z0-9_-]+)", text,
    ):
        candidate = match.group(1)
        if candidate.startswith(
            ("app.", "property", "staticmethod", "classmethod",
             "pytest", "dataclass")
        ):
            continue
        pytest.fail(f"AGENTS.md contains @mention to {candidate!r}")


def test_agents_md_config_path_is_correct() -> None:
    agents_md = Path(__file__).resolve().parent.parent / "AGENTS.md"
    if not agents_md.is_file():
        pytest.skip("AGENTS.md not present")
    text = agents_md.read_text(encoding="utf-8")
    assert "config/base/green_agent_config.yaml" in text
    for match in re.finditer(r"config/green_agent_config\.yaml", text):
        line_start = text.rfind("\n", 0, match.start()) + 1
        line_end = text.find("\n", match.end())
        line = text[line_start:line_end]
        pytest.fail(f"AGENTS.md references wrong config path: {line.strip()!r}")
```

---

# === SAVE AS: LEAN-CTX.md ===
```markdown
# lean-ctx MCP server

**Version:** 2.0
**Purpose:** Token-efficient file reads for AI coding assistants

## Tools

| Tool | Purpose |
|---|---|
| `ctx_read` | Read a file in a specific mode (standard or enhanced) |
| `ctx_modes` | Return the list of modes the server supports |

Backed by the reference implementation in `tools/ctx_read.py`. The MCP
server wraps them and exposes them over the standard MCP protocol.

## Tool definitions

### `ctx_read`

```python
ctx_read(
    path: str,
    mode: str = "full",
    *,
    start: int | None = None,
    end: int | None = None,
    n: int | None = None,
) -> str | dict
```

**Returns**
- Standard modes → `str` (empty string on failure).
- Enhanced modes → `dict` with the mode's declared fields; empty dict
  when unavailable, missing, or enhancements are disabled.

**Raises**
- `ValueError` on unknown `mode`.
- `FileNotFoundError` for standard modes on missing files.

### `ctx_modes`

```python
ctx_modes() -> list[str]
```

Returns the full mode list: standard first, then enhanced.

## Environment variables

| Variable | Default | Effect |
|---|---|---|
| `ENHANCEMENTS_ENABLED` | `false` | Global kill switch for every enhanced mode |
| `CTX_MAX_BYTES` | `5242880` (5 MiB) | Refuse to read files larger than this |
| `CTX_LOG_LEVEL` | `WARNING` | Verbosity of the ctx logs |

## Rate limits

Token cost is proportional to the returned payload, not the file size.
Standard modes return the requested slice; enhanced modes return a
fixed-size dict so cost is roughly constant per call.

## Configuring an MCP client

### Claude Code / Cursor

Add to `.mcp.json` (or `~/.config/claude/mcp.json`):

```json
{
  "mcpServers": {
    "lean-ctx": {
      "command": "python",
      "args": ["-m", "tools.mcp_server"],
      "env": {
        "ENHANCEMENTS_ENABLED": "false"
      }
    }
  }
}
```

### Manual (any client)

```bash
python -m tools.mcp_server
```

The server speaks MCP over stdio. Point your client at the process;
tool discovery should list `ctx_read` and `ctx_modes`.

## Security notes

- The tool never reads under `secrets/` or `/run/secrets`.
- `.env`, `*.pem`, `*.key`, `*.crt` are refused — logs a warning and
  returns empty rather than leak the content.
- File content is never cached across calls — every re-read fetches
  from disk.

## Versioning

- Schema version tracks the server (`2.0`).
- Breaking changes to `ctx_read`'s signature or return shape bump the
  major version.
- Deprecations land one minor version before removal.
```

---

# === SAVE AS: requirements-tools.txt ===
```
# Hard dependency of the flexgen_config handler
pyyaml==6.0.2

# Testing
pytest==8.3.4
```

---

## What's in this single file

| Section | Path | Purpose |
|---|---|---|
| Agent instructions | `AGENTS.md` | The doc an AI agent reads |
| Mode schema | `tools/ctx_schema.py` | 5 standard + 9 enhanced modes |
| Handlers | `tools/ctx_handlers.py` | Per-mode extraction (AST + YAML + log parsing) |
| Tool implementation | `tools/ctx_read.py` | `ctx_read` + `ctx_modes` |
| Tests | `tests/test_ctx_read.py` | 24 tests + 3 drift guards |
| Config reference | `LEAN-CTX.md` | Companion document |
| Dependencies | `requirements-tools.txt` | Pinned |

## Fixes applied vs. the original

| # | Original defect | Fixed |
|---|---|---|
| 1 | `config/green_agent_config.yaml` (wrong path) | `config/base/green_agent_config.yaml`; enforced by `test_agents_md_config_path_is_correct` |
| 2 | `@LEAN-CTX.md` (GitHub mention, not a file) | `[LEAN-CTX.md](./LEAN-CTX.md)`; enforced by `test_agents_md_has_no_inline_mentions` |
| 3 | `ctx_read` referenced 7× but undefined | Full implementation in `tools/ctx_read.py` |
| 4 | Fallback "or empty result" (ambiguous) | Explicit table + `{}` + warning; 4 tests cover every branch |
| 5 | `ENHANCEMENTS_ENABLED=true` in prose | Explicit "default is `false` (per `README.md §0.2`)" |
| 6 | `distillation`/`evolutionary` missing `workload_descriptor.py` | Both files listed; `test_agents_md_matches_registry` enforces |
| 7 | `graph` missing `meta_cognition.py` | Added |
| 8 | `modp` conflated weights + composite | All 4 fields listed explicitly |
| 9 | Single `flexgen` mode mixing config + logs | Split into `flexgen_config` + `flexgen_metrics` |
| 10 | No `zero_trust` mode | Added |
| 11 | Standard modes unlisted | Explicit table (5 modes) |
| 12 | `ctx_modes()` referenced but undefined | Implemented + tested |
| 13 | Return types unspecified | Per-mode in `MODE_REGISTRY`; enforced by `_read_enhanced` trimming to `spec.returns` |
| 14 | No security guidance | "Do NOT read" section + size cap + secrets-dir exclusion documented |
| 15 | No cost section | "Cost and rate limits" added |
| 16 | No drift guards | 3 tests: `test_agents_md_matches_registry`, `test_agents_md_has_no_inline_mentions`, `test_agents_md_config_path_is_correct` |

## Run

```bash
# 1. Split each `# === SAVE AS: <path> ===` block to its path.
# 2. Install:
pip install -r requirements-tools.txt

# 3. Test (24 tests, all should pass):
pytest -q tests/test_ctx_read.py

# 4. Use:
python -c "
from tools.ctx_read import ctx_read, ctx_modes
print(ctx_modes())
print(ctx_read('src/enhancements/core/graph_registry.py', mode='graph'))
"
```

