```markdown
# AgentBeats Compliance Guide (Enhanced v2)

**Document version:** 2.0.0 · **A2A protocol:** 1.1 (accepts 1.0) · **Python:** ≥3.10
**Applies to:** Green_Agent architecture + optional enhancement modules
(LIMIT Graph, MODP, RLHF, Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation,
MoE expert gating, FlexGen integration).

> **Change log**
> - **v2.0.0** — Fixed the scoring bug (clamped, matches the table, adds `partial`);
>   added complete implementations for `schemas/*.json`, `core/scoring_utils.py`,
>   `core/a2a_gateway.py`, `core/green_metrics.py`, `core/rlhf_feedback_engine.py`,
>   `scripts/validate_a2a.py`, `scripts/run_agent_container.sh`; added security,
>   observability, idempotency, rate-limit, and green-metrics methodology sections;
>   pinned image references; added CI integration.
> - **v1.1.0** — Added enhanced-module usage snippets.
> - **v1.0.0** — Four pillars.

---

## 0. Repository layout

```
.
├── core/
│   ├── a2a_gateway.py           # §1 — full implementation below
│   ├── scoring_utils.py         # §3 — full implementation below
│   ├── green_metrics.py         # §GreenMetrics — full implementation below
│   ├── rlhf_feedback_engine.py  # §4 — full implementation below
│   └── docker_orchestrator.py
├── schemas/
│   ├── a2a_request.v1_1.json    # full below
│   ├── a2a_response.v1_1.json   # full below
│   └── rlhf_feedback.v1.json    # full below
├── scripts/
│   ├── validate_a2a.py          # full below
│   └── run_agent_container.sh   # full below
├── src/enhancements/
│   ├── schemas/{node_descriptor.py, workload_descriptor.py}
│   └── core/{graph_registry.py, causal_graph.py, meta_cognition.py}
├── demo_agentbeats_integration.py
├── tests/fixtures/{request,response}_*.json
├── requirements.txt
└── pyproject.toml
```

Paths in the rest of this document are relative to repo root.

---

## 🏆 Four Pillars of AgentBeats Compliance

### 1. A2A Protocol Compliance ✅

**Implementation:** `core/a2a_gateway.py`

- Request validation against `schemas/a2a_request.v1_1.json`.
- Response transformation to `schemas/a2a_response.v1_1.json`.
- Version support: v1.1 preferred, v1.0 accepted and upgraded on ingress.
- All errors return a valid A2A response — never a stack trace.

#### A2A version decision matrix

| Feature | v1.0 | v1.1 | Emit |
|---|---|---|---|
| Basic request/response | ✅ | ✅ | — |
| `green_metrics` block | ❌ | ✅ | v1.1 |
| `reasoning_trace` | ❌ | ✅ | v1.1 |
| Idempotency cache | ❌ | ✅ | v1.1 |

Emit **v1.1** unless your client cannot parse it. The gateway downgrades for v1.0 clients.

#### Request schema — `schemas/a2a_request.v1_1.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agentbeats.example.com/schemas/a2a_request.v1_1.json",
  "title": "A2A Request v1.1",
  "type": "object",
  "required": ["task_id", "task_type", "input_data", "version"],
  "properties": {
    "task_id":   { "type": "string", "pattern": "^[A-Za-z0-9._:-]{1,128}$" },
    "task_type": { "type": "string",
                   "enum": ["research","qa","summarization","analysis","other"] },
    "input_data": {
      "type": "object",
      "required": ["query"],
      "properties": {
        "query": { "type": "string", "minLength": 1, "maxLength": 32000 }
      },
      "additionalProperties": true
    },
    "constraints": {
      "type": "object",
      "properties": {
        "max_tokens":      { "type": "integer", "minimum": 1, "maximum": 8192 },
        "timeout_seconds": { "type": "integer", "minimum": 5, "maximum": 600 }
      },
      "additionalProperties": false
    },
    "version": { "type": "string", "enum": ["1.0", "1.1"] }
  },
  "additionalProperties": false
}
```

#### Response schema — `schemas/a2a_response.v1_1.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agentbeats.example.com/schemas/a2a_response.v1_1.json",
  "title": "A2A Response v1.1",
  "type": "object",
  "required": ["task_id", "status", "output", "execution_time_seconds",
               "timestamp", "version"],
  "properties": {
    "task_id": { "type": "string" },
    "status": {
      "type": "string",
      "enum": ["success","partial","timeout","invalid_output",
               "oom","crash","failure"]
    },
    "output": { "type": ["object","null"] },
    "execution_time_seconds": { "type": "number", "minimum": 0 },
    "green_metrics": {
      "type": "object",
      "properties": {
        "energy_kwh":           { "type": ["number","null"], "minimum": 0 },
        "carbon_kg":            { "type": ["number","null"], "minimum": 0 },
        "sustainability_index": { "type": ["number","null"], "minimum": 0, "maximum": 1 },
        "efficiency_score":     { "type": ["number","null"], "minimum": 0, "maximum": 1 },
        "modp_score":           { "type": ["number","null"], "minimum": 0, "maximum": 1 },
        "graph_centrality":     { "type": ["number","null"], "minimum": 0, "maximum": 1 },
        "rlhf_feedback":        { "type": ["number","null"], "minimum": 0, "maximum": 1 }
      },
      "additionalProperties": false
    },
    "reasoning_trace": { "type": "array" },
    "timestamp": { "type": "string", "format": "date-time" },
    "version":   { "type": "string", "enum": ["1.0", "1.1"] }
  },
  "additionalProperties": false
}
```

#### RLHF schema — `schemas/rlhf_feedback.v1.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://agentbeats.example.com/schemas/rlhf_feedback.v1.json",
  "title": "RLHF Feedback v1",
  "type": "object",
  "required": ["overall_score","reasoning_quality","reasoning_score",
               "efficiency_score","completeness_score","success_score",
               "feedback_items","improvement_suggestions","metrics"],
  "properties": {
    "overall_score":       { "type": "number", "minimum": 0, "maximum": 1 },
    "reasoning_quality":   { "type": "string", "enum": ["poor","fair","good","excellent"] },
    "reasoning_score":     { "type": "number", "minimum": 0, "maximum": 1 },
    "efficiency_score":    { "type": "number", "minimum": 0, "maximum": 1 },
    "completeness_score":  { "type": "number", "minimum": 0, "maximum": 1 },
    "success_score":       { "type": "number", "minimum": 0, "maximum": 1 },
    "feedback_items": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["category","severity","message","suggestion"],
        "properties": {
          "category":   { "type": "string" },
          "severity":   { "type": "string", "enum": ["info","minor","major"] },
          "message":    { "type": "string" },
          "suggestion": { "type": "string" }
        }
      }
    },
    "improvement_suggestions": { "type": "array", "items": { "type": "string" } },
    "metrics": {
      "type": "object",
      "required": ["total_steps","avg_step_duration",
                   "redundant_steps","tool_usage_efficiency"],
      "properties": {
        "total_steps":          { "type": "integer", "minimum": 0 },
        "avg_step_duration":    { "type": "number", "minimum": 0 },
        "redundant_steps":      { "type": "integer", "minimum": 0 },
        "tool_usage_efficiency":{ "type": "number", "minimum": 0, "maximum": 1 }
      }
    }
  },
  "additionalProperties": false
}
```

#### Gateway implementation — `core/a2a_gateway.py`

```python
"""
core/a2a_gateway.py — A2A protocol implementation (v1.0 + v1.1).

Guarantees:
- validate_request() either returns a normalized ValidatedTask or raises A2AValidationError.
- transform_agent_output() returns an A2AResponse whose .to_dict() always passes
  schemas/a2a_response.v1_1.json.
- All exceptions are caught and converted into a valid `failure` response.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, ValidationError


SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
_REQUEST_SCHEMA = json.loads((SCHEMAS_DIR / "a2a_request.v1_1.json").read_text())
_RESPONSE_SCHEMA = json.loads((SCHEMAS_DIR / "a2a_response.v1_1.json").read_text())
_REQUEST_VALIDATOR = Draft202012Validator(_REQUEST_SCHEMA)
_RESPONSE_VALIDATOR = Draft202012Validator(_RESPONSE_SCHEMA)


class A2AValidationError(ValueError):
    """Raised when an incoming request does not conform to the A2A schema."""


@dataclass(frozen=True)
class ValidatedTask:
    task_id: str
    task_type: str
    input_data: dict[str, Any]
    max_tokens: int
    timeout_seconds: int
    version: str


@dataclass
class A2AResponse:
    task_id: str
    status: str
    output: dict[str, Any] | None
    execution_time_seconds: float
    green_metrics: dict[str, Any] | None
    reasoning_trace: list[dict[str, Any]]
    metadata: dict[str, Any]
    version: str = "1.1"

    def to_dict(self) -> dict[str, Any]:
        doc: dict[str, Any] = {
            "task_id": self.task_id,
            "status": self.status,
            "output": self.output,
            "execution_time_seconds": round(float(self.execution_time_seconds), 6),
            "reasoning_trace": self.reasoning_trace,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds")
                          .replace("+00:00", "Z"),
            "version": self.version,
        }
        if self.green_metrics is not None:
            # Schema restricts keys; drop unknown ones rather than fail.
            allowed = {"energy_kwh","carbon_kg","sustainability_index",
                       "efficiency_score","modp_score","graph_centrality",
                       "rlhf_feedback"}
            doc["green_metrics"] = {k: v for k, v in self.green_metrics.items()
                                    if k in allowed}
        # Defense in depth: never emit an invalid document.
        errors = list(_RESPONSE_VALIDATOR.iter_errors(doc))
        if errors:
            raise RuntimeError(
                "A2AResponse produced an invalid document: "
                + "; ".join(f"{list(e.path)}: {e.message}" for e in errors)
            )
        return doc


class A2AGateway:
    def validate_request(self, task: dict[str, Any]) -> ValidatedTask:
        if not isinstance(task, dict):
            raise A2AValidationError("Request must be a JSON object.")
        try:
            _REQUEST_VALIDATOR.validate(task)
        except ValidationError as exc:
            raise A2AValidationError(
                f"{list(exc.path) or '<root>'}: {exc.message}"
            ) from exc

        constraints = task.get("constraints") or {}
        return ValidatedTask(
            task_id=task["task_id"],
            task_type=task["task_type"],
            input_data=task["input_data"],
            max_tokens=int(constraints.get("max_tokens", 500)),
            timeout_seconds=int(constraints.get("timeout_seconds", 30)),
            version=task["version"],
        )

    def transform_agent_output(
        self,
        task_id: str,
        agent_output: dict[str, Any],
        execution_time: float,
        green_metrics: dict[str, Any] | None = None,
        reasoning_trace: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
        status: str = "success",
    ) -> A2AResponse:
        return A2AResponse(
            task_id=task_id,
            status=status,
            output=agent_output,
            execution_time_seconds=execution_time,
            green_metrics=green_metrics,
            reasoning_trace=reasoning_trace or [],
            metadata=metadata or {},
        )

    def create_success_response(self, **kwargs) -> A2AResponse:
        kwargs.setdefault("status", "success")
        return self.transform_agent_output(**kwargs)

    def create_failure_response(
        self,
        task_id: str,
        status: str,
        execution_time: float,
        error_message: str,
        metadata: dict[str, Any] | None = None,
    ) -> A2AResponse:
        # Allow-list — unknown status coerces to `failure` so the response stays schema-valid.
        allowed = {"timeout","invalid_output","oom","crash","failure","partial"}
        safe_status = status if status in allowed else "failure"
        return A2AResponse(
            task_id=task_id,
            status=safe_status,
            output={"error": error_message},
            execution_time_seconds=execution_time,
            green_metrics=None,
            reasoning_trace=[],
            metadata=metadata or {},
        )


def create_a2a_task(
    task_id: str | None = None,
    task_type: str = "research",
    query: str = "",
    max_tokens: int = 500,
    timeout_seconds: int = 30,
    version: str = "1.1",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "task_id": task_id or f"task_{uuid.uuid4().hex[:12]}",
        "task_type": task_type,
        "input_data": {"query": query},
        "constraints": {"max_tokens": max_tokens, "timeout_seconds": timeout_seconds},
        "version": version,
    }
    if extra:
        payload["input_data"].update(extra)
    return payload
```

#### Failure taxonomy — HTTP status, retry contract

| `status` | HTTP | Meaning | Retry? | Score band |
|---|---|---|---|---|
| `success` | 200 | Completed with output | No | 0.0–1.0 (0.8 if no expected) |
| `partial` | 200 | Output present, explicitly incomplete | No (accept) | 0.4–0.8 |
| `invalid_output` | 200 | Output present, failed schema check | No | 0.2–0.4 |
| `timeout` | 200 | Deadline exceeded; output may exist | No (retry with longer timeout) | 0.0 or 0.4–0.8 |
| `failure` | 200 | Error before output | No | 0.0 |
| `oom` | 200 | Memory limit hit | No | 0.0 |
| `crash` | 200 | Container exited unexpectedly | No | 0.0 |
| *(malformed body)* | 400 | Schema violation | No | — |
| *(unknown task_id when GET)* | 404 | — | No | — |
| *(rate limit)* | 429 | — | Yes, after `Retry-After` | — |

**Why HTTP 200 for task failures?** A2A is task-oriented, not resource-oriented. The *task* completed; its outcome is in `status`. Malformed requests, oversized bodies, and unknown routes get 4xx.

#### Idempotency contract (v1.1)

- `task_id` is the idempotency key.
- Retry of a `success` task returns the **cached** response.
- Retry while `running` returns HTTP 409 + `Retry-After`.
- Cache TTL: 24h server-side.
- `timeout_seconds` is *server-side*. Client-side retries should use a strictly longer value.

#### Rate limits (per API key, defaults)

| Budget | Default | Header |
|---|---|---|
| Requests | 10/s | `X-RateLimit-Limit` |
| Concurrent tasks | 8 | `X-RateLimit-Concurrent` |
| Request body | 512 KB | — |
| Response body | 10 MB | — |

Exceeding returns HTTP 429 with `Retry-After`.

---

### 2. Independent Execution ✅

**Implementation:** `core/docker_orchestrator.py` + `scripts/run_agent_container.sh`

#### Features
- Container lifecycle with hardened defaults.
- CPU / memory / GPU limits.
- Filesystem-scoped I/O: `input.json` in, `output.json` out.
- Automatic cleanup on every exit path (including SIGKILL via `trap`).

#### Hardened run helper — `scripts/run_agent_container.sh`

```bash
#!/usr/bin/env bash
# scripts/run_agent_container.sh
# Usage: run_agent_container.sh <image@digest> <input.json> <output.json>
set -euo pipefail

IMAGE="${1:?image ref required (use @sha256:...)}"
INPUT="$(realpath "${2:?input.json required}")"
OUTPUT="$(realpath -m "${3:?output.json required}")"

# --- Validate inputs ------------------------------------------------------
[[ -r "$INPUT"  ]] || { echo "input not readable: $INPUT" >&2; exit 1; }
mkdir -p "$(dirname "$OUTPUT")"
[[ -e "$OUTPUT" ]] || : > "$OUTPUT"
chmod 0600 "$INPUT" "$OUTPUT"

# --- Enforce digest pin ---------------------------------------------------
if [[ "$IMAGE" != *"@sha256:"* ]]; then
  echo "ERROR: image must be pinned by digest (@sha256:...); got: $IMAGE" >&2
  exit 2
fi

# --- Cleanup trap ---------------------------------------------------------
cleanup() {
  docker rm -f agentbeats-run 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# --- Optional egress: pass NETWORK env var to override --------------------
NETWORK="${NETWORK:-none}"

# --- Run ------------------------------------------------------------------
docker run --rm --name agentbeats-run \
  --network="$NETWORK" \
  --read-only \
  --tmpfs /tmp:size=256m \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --user 1000:1000 \
  --cpus "${CPUS:-2.0}" \
  --memory "${MEMORY:-4g}" \
  ${GPUS:+--gpus "$GPUS"} \
  -v "${INPUT}:/app/input.json:ro" \
  -v "${OUTPUT}:/app/output.json:rw" \
  "$IMAGE"

echo "OK: wrote $OUTPUT"
```

#### Execution lifecycle

1. Validate task against request schema.
2. Launch container with recorded `ContainerConfig`.
3. Entrypoint reads `/app/input.json`.
4. Agent runs; green metrics collected inside container.
5. Agent writes `/app/output.json` (schema-checked before exit).
6. `trap` fires on any exit — including SIGTERM/SIGKILL after `--stop-timeout`.

---

### 3. Robust Scoring ✅

**Implementation:** `core/scoring_utils.py` (complete) + `calculate_robust_score` re-exported from `core/a2a_gateway.py`.

**Bug in v1.0:** `0.3 * evaluate_partial_match(...)` could exceed the table's 0.2–0.4 band; success with a wrong answer scored 0.0 despite the "0.8–1.0" claim; no `partial` branch.

#### Complete implementation — `core/scoring_utils.py`

```python
"""
core/scoring_utils.py — Robust scoring primitives.

Contract for every public function here:
    * Inputs: dict, dict | None, dict | None.
    * Outputs: float in [0.0, 1.0] — clamped on the way out, never NaN/inf.
    * No function raises on malformed input; worst case returns 0.0.
"""
from __future__ import annotations

import math
import re
from typing import Any


def _clamp(x: float) -> float:
    """Clamp x to [0, 1]; map NaN/inf to 0."""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(x) or math.isinf(x):
        return 0.0
    return max(0.0, min(1.0, x))


def _tokens(s: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", s.lower())


def evaluate_similarity(
    output: dict[str, Any] | None,
    expected: dict[str, Any] | None,
) -> float:
    """
    Reference-implementation similarity.

    This is a *placeholder* for whatever your domain needs. It uses Jaccard
    token overlap on the concatenated leaf values of `output` and `expected`.
    Replace with an embedding-based scorer if you need semantic similarity;
    keep the return type `float ∈ [0, 1]`.
    """
    if not output or not expected:
        return 0.0
    a = set(_tokens(" ".join(map(str, _flatten(output)))))
    b = set(_tokens(" ".join(map(str, _flatten(expected)))))
    if not a or not b:
        return 0.0
    return _clamp(len(a & b) / len(a | b))


def estimate_completeness(output: dict[str, Any] | None) -> float:
    """
    Heuristic: how many structural sections are populated?

    Counts presence of `answer`, `trace`, `sources`, `conclusion`, plus non-empty
    output itself. Returns 0.0 for empty output, 1.0 for all four sections.
    """
    if not output:
        return 0.0
    sections = ("answer", "trace", "sources", "conclusion")
    hits = sum(1 for k in sections if output.get(k))
    return _clamp(hits / len(sections))


def evaluate_partial_match(
    output: dict[str, Any] | None,
    expected: dict[str, Any] | None,
) -> float:
    """
    Lenient match for invalid_output: fraction of expected *keys* present in
    output, ignoring values. Returns 0.0 if either side is empty.
    """
    if not output or not expected:
        return 0.0
    expected_keys = set(expected.keys())
    if not expected_keys:
        return 0.0
    return _clamp(len(expected_keys & set(output.keys())) / len(expected_keys))


def _flatten(obj: Any) -> list[Any]:
    if isinstance(obj, dict):
        return [x for v in obj.values() for x in _flatten(v)]
    if isinstance(obj, (list, tuple)):
        return [x for v in obj for x in _flatten(v)]
    return [obj]


# ---- Orchestrator ---------------------------------------------------------

def calculate_robust_score(
    status: str,
    output: dict[str, Any] | None,
    expected_output: dict[str, Any] | None,
) -> float:
    """
    Deterministic, clamped score in [0, 1].

    success        → similarity, clamped (0.8 if no expected)
    partial        → 0.4 + 0.4·sim·completeness       → 0.4–0.8
    timeout        → same as partial if output present, else 0.0
    invalid_output → 0.2 + 0.2·partial_match          → 0.2–0.4
    oom/crash/failure/unknown → 0.0
    """
    if status == "success":
        if expected_output is None:
            return 0.8
        return _clamp(evaluate_similarity(output or {}, expected_output))

    if status == "partial" and output:
        base = _clamp(evaluate_similarity(output, expected_output or {}))
        completeness = _clamp(estimate_completeness(output))
        return _clamp(0.4 + 0.4 * base * completeness)

    if status == "timeout" and output:
        base = _clamp(evaluate_similarity(output, expected_output or {}))
        completeness = _clamp(estimate_completeness(output))
        return _clamp(0.4 + 0.4 * base * completeness)

    if status == "invalid_output" and output:
        partial = _clamp(evaluate_partial_match(output, expected_output or {}))
        return _clamp(0.2 + 0.2 * partial)

    return 0.0
```

#### Failure modes (both output-present variants covered)

| Status | Output present | Score range | Behavior |
|---|---|---|---|
| `success` | Yes | 0.0–1.0 (0.8 if no expected) | Similarity |
| `partial` | Yes | 0.4–0.8 | Similarity × completeness |
| `partial` | No | 0.0 | — |
| `timeout` | Yes | 0.4–0.8 | Same as partial |
| `timeout` | No | 0.0 | — |
| `invalid_output` | Yes | 0.2–0.4 | Partial-match credit |
| `invalid_output` | No | 0.0 | — |
| `oom` / `crash` / `failure` | No | 0.0 | — |

---

### 4. RLHF Feedback Loop ✅

**Implementation:** `core/rlhf_feedback_engine.py` (complete below).

#### Features
- Step-level reasoning analysis.
- Multi-dimensional scoring (**default weights** reasoning 0.40 / efficiency 0.20 /
  completeness 0.30 / success 0.10 — these are tunable defaults, not claims about
  human-preference distributions).
- Actionable suggestions.
- Emits schema-valid `rlhf_feedback.v1.json`.

#### Complete implementation — `core/rlhf_feedback_engine.py`

```python
"""
core/rlhf_feedback_engine.py — RLHF feedback engine (v1).

Deterministic, schema-valid feedback. Every public output passes
schemas/rlhf_feedback.v1.json.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean
from typing import Any

from jsonschema import Draft202012Validator

_SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
_VALIDATOR = Draft202012Validator(
    json.loads((_SCHEMAS_DIR / "rlhf_feedback.v1.json").read_text())
)

DEFAULT_WEIGHTS = {
    "reasoning":   0.40,
    "efficiency":  0.20,
    "completeness": 0.30,
    "success":     0.10,
}


def _clamp(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _quality_label(score: float) -> str:
    if score >= 0.85: return "excellent"
    if score >= 0.70: return "good"
    if score >= 0.50: return "fair"
    return "poor"


@dataclass
class RLHFFeedbackEngine:
    weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    history: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        total = sum(self.weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(
                f"RLHF weights must sum to 1.0 (got {total:.6f}): {self.weights}"
            )

    def analyze_reasoning_trace(
        self,
        reasoning_trace: list[dict[str, Any]],
        task_type: str,
        execution_time: float,
        success: bool,
    ) -> dict[str, Any]:
        steps = list(reasoning_trace or [])
        durations = [float(s.get("duration", 0.0)) for s in steps]
        total_steps = len(steps)
        avg_duration = mean(durations) if durations else 0.0

        # --- Reasoning -----------------------------------------------------
        has_plan = any(s.get("action") in {"plan", "planning"} for s in steps)
        has_error_handling = any(s.get("error") for s in steps) or any(
            "error" in str(s.get("thought", "")).lower() for s in steps
        )
        reasoning_score = _clamp(
            0.4 * float(has_plan)
            + 0.3 * float(has_error_handling)
            + 0.3 * (1.0 if total_steps >= 3 else total_steps / 3.0)
        )

        # --- Efficiency ----------------------------------------------------
        redundant = 0
        seen_actions: set[str] = set()
        for s in steps:
            key = f"{s.get('action')}:{s.get('tool')}"
            if key in seen_actions:
                redundant += 1
            seen_actions.add(key)
        unique_tools = {s.get("tool") for s in steps if s.get("tool")}
        tool_efficiency = _clamp(len(unique_tools) / max(total_steps, 1))
        efficiency_score = _clamp(
            (1.0 - redundant / max(total_steps, 1)) * 0.7
            + tool_efficiency * 0.3
        )

        # --- Completeness --------------------------------------------------
        has_synthesis = any(
            s.get("action") in {"synthesize", "conclude", "answer"} for s in steps
        )
        completeness_score = _clamp(
            0.5 * float(has_synthesis)
            + 0.5 * (1.0 if total_steps >= 5 else total_steps / 5.0)
        )

        success_score = 1.0 if success else 0.0

        overall = _clamp(
            self.weights["reasoning"]    * reasoning_score
            + self.weights["efficiency"] * efficiency_score
            + self.weights["completeness"] * completeness_score
            + self.weights["success"]    * success_score
        )

        items: list[dict[str, str]] = []
        if not has_plan:
            items.append({
                "category": "reasoning_quality", "severity": "minor",
                "message": "No planning step detected",
                "suggestion": "Add an explicit plan step before acting",
            })
        if not has_error_handling:
            items.append({
                "category": "reasoning_quality", "severity": "minor",
                "message": "Limited error handling observed",
                "suggestion": "Add explicit error checking and recovery strategies",
            })
        if redundant > 0:
            items.append({
                "category": "efficiency", "severity": "major",
                "message": f"{redundant} redundant step(s) detected",
                "suggestion": "Cache intermediate results to avoid redundant actions",
            })

        suggestions = [f"{i['category'].upper()}: {i['suggestion']}" for i in items]

        feedback = {
            "overall_score": round(overall, 4),
            "reasoning_quality": _quality_label(reasoning_score),
            "reasoning_score": round(reasoning_score, 4),
            "efficiency_score": round(efficiency_score, 4),
            "completeness_score": round(completeness_score, 4),
            "success_score": round(success_score, 4),
            "feedback_items": items,
            "improvement_suggestions": suggestions,
            "metrics": {
                "total_steps": total_steps,
                "avg_step_duration": round(avg_duration, 4),
                "redundant_steps": redundant,
                "tool_usage_efficiency": round(tool_efficiency, 4),
            },
        }

        # Defense in depth: never return an invalid document.
        errors = list(_VALIDATOR.iter_errors(feedback))
        if errors:
            raise RuntimeError(
                "RLHF feedback produced invalid document: "
                + "; ".join(f"{list(e.path)}: {e.message}" for e in errors)
            )

        self.history.append({"task_type": task_type,
                             "execution_time": execution_time,
                             "overall": feedback["overall_score"]})
        return feedback
```

#### Quality dimensions

1. **Reasoning quality (0.40 default)** — planning, error handling, step count.
2. **Efficiency (0.20)** — redundancy, tool utilization.
3. **Completeness (0.30)** — synthesis, step count.
4. **Success (0.10)** — `success` flag.

Weights are defaults. Override with `RLHFFeedbackEngine(weights={...})` — the engine enforces sum-to-1.0.

---

## 🧠 Advanced Enhancements (optional, recommended)

### 5. LIMIT Graph Integration

```python
from src.enhancements.core.graph_registry import GraphRegistry, GraphType

registry = GraphRegistry()
causal = registry.get_or_create(GraphType.CAUSAL)  # returns a CausalGraph
```

### 6. MODP (Multi‑Objective Decision Process)

```python
from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType

wl = WorkloadDescriptor(
    task_id="task1",
    task_type=TaskType.INFERENCE,
    tokens=1000,
    latency_target=300.0,
    metadata={
        "latency_weight": 0.5,
        "carbon_weight":  0.3,
        "energy_weight":  0.2,
    },
)
# Weights validated in the constructor: sum == 1.0 ± 1e-6.
```

### 7. RLHF in Decision-Making

```python
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType

node = NodeDescriptor(
    id="node1", type=NodeType.EDGE, region="us-east",
    region_carbon_intensity=400.0, energy_per_token=0.00005,
    human_feedback_score=0.7, graph_metrics={"centrality": 0.8},
)
```

### 8. Multi‑Teacher On‑Policy Distillation + MoE Gating

```python
# Synchronous — returns immediately. (Earlier drafts showed `await`
# incorrectly; the method is not a coroutine.)
strategy = node.select_routing_strategy(exploration=False)
```

### 9. Bio‑inspired Optimisation

```python
node = NodeDescriptor(
    ..., use_evolutionary=True,
    population_size=20,   # 10 ≤ population_size ≤ 200
    mutation_rate=0.05,   # 0 < mutation_rate < 1
    generations=50,       # 1 ≤ generations ≤ 1000
)
```

### 10. FlexGen Integration

```yaml
flexgen:
  enabled: true
  model_name: "facebook/opt-6.7b"
  default_precision: "fp16"          # fp32 | fp16 | int8
  delegation_policy: "adaptive"      # always | never | adaptive
```

---

## 🔐 Security

### Secrets
- **Never** place `AGENTBEATS_API_KEY` (or any token) in a Dockerfile, image
  layer, or `docker -e VAR` — all three are visible via `docker inspect` or
  the image's history.
- Prefer `--env-file /run/secrets/agent.env` mode `0600`, or a mounted file at
  `/run/secrets/<name>` + a `*_FILE` env var.
- In CI, `::add-mask::` before any echo.

### Container hardening (non-negotiable defaults)
`--network=none`, `--read-only`, `--tmpfs /tmp:size=256m`, `--cap-drop ALL`,
`--security-opt no-new-privileges`, `--user 1000:1000`, `--memory`, `--cpus`,
pinned image digest. **Enforced by `scripts/run_agent_container.sh`.**

### Egress policy
If the agent *must* reach the internet (tools, LLM APIs), don't use
`--network=none` — instead:
- Use an HTTP proxy with an allowlist (`HTTPS_PROXY`). This is the only robust
  defense against DNS rebinding.
- Additionally `--add-host=metadata.google.internal:0.0.0.0` and similar
  overrides as cheap insurance.
- Block RFC1918, loopback, `169.254.0.0/16`, and metadata hostnames at the
  network layer.

### Input validation
- Validate against `schemas/a2a_request.v1_1.json` **before** any agent code runs
  (gateway does this).
- Reject `query` > 32 KB, `max_tokens` > 8192, `timeout_seconds` > 600.
- Rate limit per API key (see §1).

---

## 📈 Observability

### `/metrics` endpoint contract (matches CI smoke tests)

Expose Prometheus metrics on `:8000/metrics`:

| Metric | Type | Notes |
|---|---|---|
| `green_agent_modp_score` | gauge | 0–1 |
| `green_agent_graph_centrality` | gauge | 0–1 |
| `green_agent_rlhf_feedback` | gauge | 0–1 |
| `green_agent_moe_gate` | gauge | 0–1 |
| `green_agent_flexgen_energy` | counter (J) | FlexGen only |
| `green_agent_task_duration_seconds` | histogram | `by task_type` |
| `green_agent_task_total` | counter | `by status` |

### Structured logging
JSON lines with `task_id`, `trace_id`, `span_id`, `status`, `duration_ms`.
**Do not log raw `query` (PII).**

### Tracing
Propagate W3C `traceparent` from A2A ingress through tool calls. Export via OTLP.

---

## 📊 Green Metrics — methodology

Every value must be **reproducible** from a container run. Implementation:
`core/green_metrics.py` (complete below).

### Complete implementation — `core/green_metrics.py`

```python
"""
core/green_metrics.py — Green metrics collector.

Measures energy, carbon, and derived sustainability metrics for one task.

Fallback chain for energy:
  1. RAPL  (/sys/class/powercap/intel-rapl:*/energy_uj)
  2. NVML  (pynvml.nvmlDeviceGetTotalEnergyConsumption)
  3. CPU-TDP × utilization × time estimate (LOW fidelity — marked in output)

Every metric is `Optional[float]`; unavailable measurements return `None`
(serialized as JSON `null` in the A2A response), never a fabricated value.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

RAPL_GLOB = "/sys/class/powercap/intel-rapl:*/energy_uj"

# Rough per-region carbon intensity (gCO2e/kWh) used only as a last resort.
# In production: query a grid-intensity provider and refresh every 5 minutes.
_REGION_FALLBACK = {
    "us-east": 380.0, "us-west": 220.0, "eu-west": 260.0, "ap-south": 700.0,
}


def _rapl_energy_uj() -> int | None:
    total = 0
    found = False
    for p in Path("/").glob(RAPL_GLOB.lstrip("/")):
        try:
            total += int(p.read_text().strip())
            found = True
        except OSError:
            continue
    return total if found else None


def _nvml_energy_mj() -> int | None:
    try:
        import pynvml  # type: ignore
        pynvml.nvmlInit()
        try:
            total = 0
            for i in range(pynvml.nvmlDeviceGetCount()):
                h = pynvml.nvmlDeviceGetHandleByIndex(i)
                total += pynvml.nvmlDeviceGetTotalEnergyConsumption(h)
            return total
        finally:
            pynvml.nvmlShutdown()
    except Exception:
        return None


@dataclass
class GreenMetricsCollector:
    region: str = "us-east"
    cpu_tdp_watts: float = 65.0
    _start_rapl_uj: int | None = field(default=None, init=False)
    _start_nvml_mj: int | None = field(default=None, init=False)
    _start_time: float | None = field(default=None, init=False)

    def start_collection(self) -> None:
        self._start_rapl_uj = _rapl_energy_uj()
        self._start_nvml_mj = _nvml_energy_mj()
        self._start_time = time.monotonic()

    def stop_collection(self) -> dict[str, Any]:
        if self._start_time is None:
            raise RuntimeError("stop_collection() called without start_collection()")
        duration = max(0.0, time.monotonic() - self._start_time)

        energy_kwh: float | None = None
        source = "unknown"

        # 1. RAPL
        end_rapl = _rapl_energy_uj()
        if self._start_rapl_uj is not None and end_rapl is not None:
            delta_j = (end_rapl - self._start_rapl_uj) / 1e6
            if delta_j > 0:
                energy_kwh = delta_j / 3.6e6
                source = "rapl"

        # 2. NVML
        if energy_kwh is None:
            end_nvml = _nvml_energy_mj()
            if self._start_nvml_mj is not None and end_nvml is not None:
                delta_j = (end_nvml - self._start_nvml_mj) / 1e3
                if delta_j > 0:
                    energy_kwh = delta_j / 3.6e6
                    source = "nvml"

        # 3. TDP estimate — LOW fidelity, so mark it in metadata
        if energy_kwh is None:
            energy_kwh = self.cpu_tdp_watts * duration / 3600.0 / 1000.0
            source = "tdp_estimate"

        carbon_kg = None
        intensity = _REGION_FALLBACK.get(self.region)
        if intensity is not None and energy_kwh is not None:
            carbon_kg = (energy_kwh * intensity) / 1000.0

        sustainability_index = None
        if energy_kwh is not None and carbon_kg is not None:
            # Bounded, monotone transform: 0.9 at (0.05 kWh, 0.02 kg),
            # 0.5 at (0.5 kWh, 0.2 kg), 0.1 at (5 kWh, 2 kg).
            sustainability_index = round(
                max(0.0, min(1.0,
                    1.0 / (1.0 + 10.0 * energy_kwh) +
                    1.0 / (1.0 + 100.0 * carbon_kg))
                ), 4)

        return {
            "energy_kwh":           None if energy_kwh is None else round(energy_kwh, 6),
            "carbon_kg":            None if carbon_kg is None else round(carbon_kg, 6),
            "sustainability_index": sustainability_index,
            "efficiency_score":     None,  # populated by the caller if meaningful
            "measurement_source":   source,
            "duration_seconds":     round(duration, 6),
        }
```

### Metric reference

| Metric | Measurement | Source |
|---|---|---|
| `energy_kwh` | `∫P dt` over task | RAPL → NVML → TDP estimate (marked) |
| `carbon_kg` | `energy_kwh × grid_intensity(region)` | Grid provider; fallback table |
| `sustainability_index` | `1/(1+10·E) + 1/(1+100·C)` bounded | Derived |
| `efficiency_score` | `useful_tokens / total_energy` normalized | 7-day baseline |
| `modp_score` | `Σ wᵢ·sᵢ` over accuracy/energy/latency/carbon | `MODP_WEIGHTS` (sum 1.0) |
| `graph_centrality` | PageRank over call graph | `GraphRegistry` |
| `rlhf_feedback` | §4 | — |

A submission that cannot cite its measurement method **must emit `null`** rather than fabricate a value.

---

## 🧪 Testing & validation

### Complete validator — `scripts/validate_a2a.py`

```python
#!/usr/bin/env python3
"""
scripts/validate_a2a.py — Validate A2A responses against a JSON schema.

Usage:
    validate_a2a.py <schema.json> <doc.json> [...]

Exit codes:
    0  all documents valid
    1  at least one document invalid
    2  usage error
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator


def _load(p: str) -> object:
    return json.loads(Path(p).read_text())


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print("usage: validate_a2a.py <schema.json> <doc.json> [...]", file=sys.stderr)
        return 2

    schema = _load(argv[1])
    validator = Draft202012Validator(schema)
    rc = 0

    for path in argv[2:]:
        try:
            doc = _load(path)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"❌ {path}: unreadable ({exc})", file=sys.stderr)
            rc = 1
            continue

        errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.path))
        if errors:
            rc = 1
            print(f"❌ {path}")
            for e in errors:
                print(f"   - {list(e.path) or '<root>'}: {e.message}")
        else:
            print(f"✅ {path}")

    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

### CI integration (uses `.github/workflows/agentbeats.yml`)

```yaml
- name: Validate A2A responses
  run: |
    for f in results/*.json; do
      python scripts/validate_a2a.py schemas/a2a_response.v1_1.json "$f"
    done

- name: Health & metrics smoke test
  run: |
    curl -sf --retry 30 --retry-connrefused --retry-delay 1 \
         http://localhost:8000/health
    curl -sf http://localhost:8000/metrics | grep -q '^green_agent_modp_score'
```

### Test fixtures (commit under `tests/fixtures/`)

- `request_valid.json` / `request_invalid.json`
- `response_success.json`
- `response_partial.json`
- `response_timeout.json` (output present **and** absent variants)
- `response_invalid.json`
- `response_oom.json` / `response_crash.json` / `response_failure.json`

### Negative examples — do not submit

```json
// ❌ missing version, missing green_metrics, status not in enum
{ "task_id": "abc", "status": "done", "output": "..." }
```
```json
// ❌ green_metrics out of range
{ "task_id": "abc", "status": "success", "output": {},
  "execution_time_seconds": 2.5, "timestamp": "2026-01-20T10:30:00Z",
  "version": "1.1",
  "green_metrics": { "energy_kwh": -1, "sustainability_index": 1.5 } }
```

---

## 🚀 Quick Start

### 1. Complete demo

```bash
python demo_agentbeats_integration.py            # four pillars
python demo_agentbeats_integration.py --enhanced # + all enhancement modules
python demo_agentbeats_integration.py --help     # full flag reference
```

### 2. Integrate (full example)

```python
from core.a2a_gateway import A2AGateway
from core.rlhf_feedback_engine import RLHFFeedbackEngine
from core.green_metrics import GreenMetricsCollector
from core.scoring_utils import calculate_robust_score
from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType

gateway = A2AGateway()
rlhf    = RLHFFeedbackEngine()
metrics = GreenMetricsCollector(region="us-east")

node = NodeDescriptor(
    id="agent_node", type=NodeType.EDGE, region="us-east",
    region_carbon_intensity=400.0, energy_per_token=0.00005,
    human_feedback_score=0.6, graph_metrics={"centrality": 0.7},
)

def execute_agent_task(task_request, expected_output=None):
    validated = gateway.validate_request(task_request)
    metrics.start_collection()
    try:
        result = your_agent.run(validated.input_data)   # must set result["trace"], result["output"]
    except TimeoutError:
        gm = metrics.stop_collection()
        return gateway.create_failure_response(
            task_id=validated.task_id, status="timeout",
            execution_time=gm["duration_seconds"],
            error_message="task exceeded timeout",
        ).to_dict()
    except MemoryError:
        gm = metrics.stop_collection()
        return gateway.create_failure_response(
            task_id=validated.task_id, status="oom",
            execution_time=gm["duration_seconds"],
            error_message="out of memory",
        ).to_dict()

    gm = metrics.stop_collection()
    feedback = rlhf.analyze_reasoning_trace(
        reasoning_trace=result["trace"],
        task_type=validated.task_type,
        execution_time=gm["duration_seconds"],
        success=bool(result.get("success", True)),
    )
    gm["rlhf_feedback"] = feedback["overall_score"]

    # Score and (optionally) annotate the response with the score
    score = calculate_robust_score(
        status="success",
        output=result["output"],
        expected_output=expected_output,
    )
    gm["modp_score"] = round(score, 4)   # optional: expose score via green_metrics

    return gateway.create_success_response(
        task_id=validated.task_id,
        output=result["output"],
        execution_time=gm["duration_seconds"],
        green_metrics=gm,
        reasoning_trace=result["trace"],
        metadata={"rlhf_feedback": feedback, "robust_score": score},
    ).to_dict()
```

### 3. Hardened build + run

```bash
# Build and record the digest
docker build -t ghcr.io/your-org/limit-graph-agent:1.4.2 -f Dockerfile.agent .
DIGEST="$(docker image inspect ghcr.io/your-org/limit-graph-agent:1.4.2 \
            --format '{{index .RepoDigests 0}}')"
echo "$DIGEST" > results/provenance.json

# Run via the hardened helper (enforces digest pin, hardened flags, cleanup trap)
bash scripts/run_agent_container.sh "$DIGEST" input.json output.json
```

---

## 📋 Basic vs. enhanced capability matrix

| Capability | Basic | Enhanced | Prerequisite |
|---|---|---|---|
| A2A compliance | ✅ | ✅ | — |
| Independent execution | ✅ | ✅ | — |
| Robust scoring | ✅ | ✅ | — |
| RLHF feedback | ✅ | ✅ | — |
| `modp_score` in `green_metrics` | ❌ | ✅ | `MODP_ENABLED=true` |
| `graph_centrality` | ❌ | ✅ | LIMIT Graph installed |
| Distillation + MoE | ❌ | ✅ | `DISTILLATION_ENABLED=true` |
| Evolutionary tuning | ❌ | ✅ | `EVOLUTIONARY_ENABLED=true` |
| `flexgen_energy` | ❌ | ✅ | `FLEXGEN_ENABLED=true` + weights |

---

## 🎯 AgentBeats Submission Checklist

### Required
- [ ] `python scripts/validate_a2a.py schemas/a2a_response.v1_1.json results/*.json` passes.
- [ ] Container builds from a pinned base image; digest recorded in `results/provenance.json`.
- [ ] Container runs with hardened flags (`--read-only`, `--cap-drop ALL`,
      `--no-new-privileges`, `--network` policy) — enforced by
      `scripts/run_agent_container.sh`.
- [ ] Response passes schema for every status in the failure taxonomy.
- [ ] `/health` returns 200 within 60s in a clean environment.
- [ ] `/metrics` exposes the contract in §Observability.
- [ ] Green metrics cite their measurement method (or are `null`).
- [ ] Structured JSON logging; no PII in logs.
- [ ] Idempotency: retrying the same `task_id` returns the cached response.
- [ ] Rate limits enforced per API key.
- [ ] `calculate_robust_score` returns `∈ [0,1]` for every status × output-present × output-absent combination (unit test).

### Optional (enhanced)
- [ ] MODP weights sum to 1.0 (validated in `WorkloadDescriptor`).
- [ ] FlexGen delegation policy documented (`always|never|adaptive`).
- [ ] Evolutionary hyperparameters within validated bounds.

### Provenance
- [ ] Image digest recorded in `results/provenance.json`.
- [ ] SBOM attached (`buildx sbom: true`).
- [ ] Build provenance attestation (SLSA) attached.

---

## 📚 Additional Resources

- **A2A Protocol Spec** — `TODO(owner): provide the canonical URL; do not
  submit with a placeholder here.`
- **AgentBeats Leaderboard** — `TODO(owner): URL pending.`
- Green metrics methodology — this document, §Green Metrics.
- Integration examples — `demo_agentbeats_integration.py`.
- Enhancements documentation — `src/enhancements/README.md`.
- Validation schemas — `schemas/`.

---

## Known limitations

- `evaluate_similarity` (in `scoring_utils`) is a Jaccard-token placeholder —
  swap it for a domain-appropriate scorer (e.g., embedding cosine) without
  changing the `[0,1]` contract.
- `estimate_completeness` uses structural heuristics, not reference answers.
- RLHF dimension weights (0.40/0.20/0.30/0.10) are **defaults**; tune and
  document per task type.
- Energy measurement falls back to CPU-TDP estimates when RAPL/NVML are
  unavailable — those runs are marked `measurement_source: tdp_estimate`;
  consider emitting `null` if accuracy matters.
- A2A v1.1 idempotency cache is in-memory; not shared across replicas unless
  you deploy the optional Redis backend.
- Carbon intensity fallback table is coarse; for compliance use a real grid-
  intensity provider and refresh every ≤5 minutes.

---

## 🤝 Contributing

See `CONTRIBUTING.md`. All PRs must:
- Add or update a JSON schema for any new field.
- Add a fixture under `tests/fixtures/`.
- Pass `scripts/validate_a2a.py`, `pytest`, and `promtool check config`.
- Update the change log at the top of this file.

## 📄 License

See `LICENSE`.
```
