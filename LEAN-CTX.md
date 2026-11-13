
<!-- lean-ctx-owned: PROJECT-LEAN-CTX.md v9 -->
# lean-ctx — Context Engineering Layer
<!-- lean-ctx-rules-v9 -->

| Field | Value |
|---|---|
| **Rules Version** | v9 |
| **Schema Version** | 1 |
| **Last Updated** | 2026-11-13 |
| **Owner** | lean-ctx |
| **Canonical file** | `PROJECT-LEAN-CTX.md` (this file) |
| **Mirror** | `.cursorrules` — kept in sync; if they diverge, **this file wins** |
| **Companion docs** | `SYMBOLIC_OVERSIGHT_README.md` (v1.2.0) · `Meta-Cognitive Architecture for Green Agent.md` (v1.2.0) · `META_COGNITIVE_ENHANCEMENT_COMPLETE.md` (v1.2.0) · `NEURO_SYMBOLIC_INTEGRATION_COMPLETE.md` (v1.2.0) · `QUICK_START.md` (v1.2.0) · `Scripts and Utilities.md` (v1.2.0) · `REPOSITORY_REFACTORING_GUIDE.md` (v0.2.0) |

---

## Table of Contents

1. [Tool Preference](#tool-preference)
2. [Standard Read Modes](#standard-read-modes)
3. [Enhanced Read Modes (Green Agent)](#enhanced-read-modes-green-agent)
4. [Composite Modes](#composite-modes)
5. [Mode Selection Decision Tree](#mode-selection-decision-tree)
6. [Token Budget Guidance](#token-budget-guidance)
7. [Configuration Matrix](#configuration-matrix)
8. [Fallback & Error Taxonomy](#fallback--error-taxonomy)
9. [File Editing](#file-editing)
10. [Proactive Usage](#proactive-usage)
11. [Cache Invalidation](#cache-invalidation)
12. [When to Skip lean-ctx](#when-to-skip-lean-ctx)
13. [Integration with Advanced Green Agent Enhancements](#integration-with-advanced-green-agent-enhancements)
14. [Canonical Repo Paths](#canonical-repo-paths)
15. [Mode Contract & Versioning](#mode-contract--versioning)
16. [Troubleshooting](#troubleshooting)
17. [Changelog](#changelog)
18. [Reviewer Checklist](#reviewer-checklist)

---

## Tool Preference

PREFER lean-ctx MCP tools over native equivalents for token savings:

| PREFER | OVER | Why |
|--------|------|-----|
| `ctx_read(path, mode="…")` | `Read` / `cat` | Cached, 10+ read modes, re-reads of unchanged files are ~13 tokens (measured on a 500-line Python module) |
| `ctx_shell(command)` | `Shell` / `bash` | Pattern compression for git / npm / cargo output |
| `ctx_search(pattern, path)` | `Grep` / `rg` | Compact, token-efficient results |
| `ctx_tree(path, depth=N)` | `ls` / `find` | Compact directory maps |
| `ctx_edit(path, old_string, new_string)` | `Edit` (when Read unavailable) | Search-and-replace without native Read |

> **Signature:** `ctx_read(path, mode="auto")` — `mode` may be positional or keyword, but keyword is recommended for readability.

---

## Standard Read Modes

| Mode | Purpose | When to use |
|---|---|---|
| `auto` | System selects the optimal mode | Default when unsure |
| `full` | Cached full read | Files you intend to edit |
| `map` | Dependency map + exports | Context-only files where structure matters |
| `signatures` | API surface only (function/class signatures) | You need to call, not read |
| `diff` | Changed lines after edits | Re-read after `ctx_edit` / native `Edit` |
| `aggressive` | Maximum compression | Large context-only files — **may lose critical detail** |
| `entropy` | Highlight high-entropy fragments | Skimming large logs / diffs — **may lose critical detail** |
| `task` | IB-filtered (task-relevant lines) | Active task set with a clear question |
| `reference` | Quote-friendly minimal excerpts | You need verbatim snippets |
| `lines:N-M` | Specific line range | Targeted inspection |

**`map` vs `signatures`:** `map` shows the dependency graph (`imports`, `exports`, `used-by`); `signatures` shows the API shape (`def`, `class`, type hints) without bodies. Use `map` when you need to understand *where* something fits; use `signatures` when you need to *call* it.

**`aggressive` / `entropy` accuracy caveat:** these modes deliberately drop content. Never use them to review code you are about to edit — use `full` + `diff` instead.

---

## Enhanced Read Modes (Green Agent)

These modes read state from the advanced modules under `src/enhancements/`. All
require the corresponding enable flag (see [Configuration Matrix](#configuration-matrix)).
When the flag is missing, the mode **silently falls back** to `full` (or `{}`) —
see [Fallback & Error Taxonomy](#fallback--error-taxonomy).

| Mode | Purpose | Unit / Range | Enable flag | Example |
|---|---|---|---|---|
| `graph` | LIMIT Graph metrics (centrality, connectivity) | — / 0–1 | `LIMIT_GRAPH_ENABLED` | `ctx_read("src/enhancements/core/graph_registry.py", mode="graph")` |
| `routing` | Routing decisions + next-hop probabilities | — / 0–1 | `LIMIT_GRAPH_ENABLED` | `ctx_read("src/enhancements/core/graph_registry.py", mode="routing")` |
| `anomaly` | Anomaly flags, drift scores, alert severity | — / 0–1 | `LIMIT_GRAPH_ENABLED` | `ctx_read("src/enhancements/core/anomaly_detector.py", mode="anomaly")` |
| `modp` | MODP weights + composite score | — / 0–1, higher better | `MODP_ENABLED` | `ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp")` |
| `cost` | Composite cost vector (accuracy / energy / latency / carbon) | mixed | `MODP_ENABLED` | `ctx_read("src/enhancements/schemas/node_descriptor.py", mode="cost")` |
| `rlhf` | Human feedback score | — / 0–1, higher better | `RLHF_ENABLED` | `ctx_read("src/enhancements/schemas/feedback_event.py", mode="rlhf")` |
| `distillation` | Student stats, replay buffer size | ΔL/step ≥ 0 | `DISTILLATION_ENABLED` | `ctx_read("src/enhancements/schemas/node_descriptor.py", mode="distillation")` |
| `evolutionary` | Best fitness + population parameters | — / 0–1, higher better | `EVOLUTIONARY_ENABLED` | `ctx_read("src/enhancements/schemas/node_descriptor.py", mode="evolutionary")` |
| `moe` | MoE gate weights + expert routing stats | — / 0–1 (0 = stable) | `MOE_ENABLED` | `ctx_read("src/enhancements/schemas/node_descriptor.py", mode="moe")` |
| `workload` | WorkloadDescriptor: SLO, batch, priority, tenant | mixed | `MODP_ENABLED` | `ctx_read("src/enhancements/schemas/workload_descriptor.py", mode="workload")` |
| `flexgen` | FlexGen config + **`flexgen_energy_kwh`** (kWh) | kWh ≥ 0 | `FLEXGEN_ENABLED` | `ctx_read("config/green_agent.yaml", mode="flexgen")` |
| `energy` | Energy slice only (kWh, avg watts, p95) | kWh ≥ 0 | `FLEXGEN_ENABLED` | `ctx_read("config/green_agent.yaml", mode="energy")` |
| `zero_trust` | Trust score, attestation, policy decisions | enum | `ZERO_TRUST_ENABLED` | `ctx_read("src/enhancements/zero_trust_architecture.py", mode="zero_trust")` |
| `safety` | Guardrail state, blocked-policy counts | enum | `ZERO_TRUST_ENABLED` | `ctx_read("src/enhancements/core/safety.py", mode="safety")` |
| `privacy` | DP ε/δ budget consumption | float | `DIFFERENTIAL_PRIVACY_ENABLED` | `ctx_read("src/enhancements/core/privacy.py", mode="privacy")` |
| `checkpoint` | Last checkpoint id, step, loss, age | mixed | `TRAINING_ENABLED` | `ctx_read("src/enhancements/core/checkpoints.py", mode="checkpoint")` |
| `training` | Composite: `distillation` + `evolutionary` snapshot | mixed | `DISTILLATION_ENABLED` **and** `EVOLUTIONARY_ENABLED` | `ctx_read("src/enhancements/schemas/node_descriptor.py", mode="training")` |
| `experiment` | A/B arm, p-value, lift | mixed | `TRAINING_ENABLED` | `ctx_read("src/enhancements/core/experiments.py", mode="experiment")` |
| `symbolic` | Symbolic rules + violation summary | list | always on (with `symbolic_policy.yaml`) | `ctx_read("symbolic_policy.yaml", mode="symbolic")` |
| `cache` | Cache hit/miss stats, mode histogram | count / % | always on (lean-ctx core) | `ctx_read("", mode="cache")` |
| `digest` | One-shot snapshot of *all* enabled modes | mixed | union of enabled flags | `ctx_read("src/enhancements/", mode="digest")` |
| `meta` | lean-ctx build + supported modes | dict | always on | `ctx_read("", mode="meta")` |

> **Unit note (v9):** the FlexGen energy metric is **`flexgen_energy_kwh`** (kWh).
> The earlier `flexgen_energy_joules` name is **deprecated** — see the Symbolic README v1.2.

> **State file note:** distillation, evolutionary, and MoE state are read from the
> **descriptor modules** (e.g. `node_descriptor.py`) via the `distillation` /
> `evolutionary` / `moe` modes — **not** from standalone JSON files. Earlier
> drafts of this document referenced invented files (`distillation_log.json`,
> `evolutionary_state.json`, `moe_gating_state.json`, `flexgen_config.yaml`);
> those do not exist and have been removed.

---

## Composite Modes

`mode` accepts `+`-joined unions. Composite reads issue a **single** MCP
round-trip and merge the result object.

| Composite | Equivalent | When to use |
|---|---|---|
| `modp+moe` | MODP score **and** MoE gate weights | Trade-off **and** routing decisions together |
| `graph+routing` | Topology **and** next-hop probabilities | Explain *why* a route was chosen |
| `cost+energy` | Cost vector **and** raw energy | Carbon / latency debugging |
| `modp+rlhf` | MODP **and** feedback score | Policy adjustment review |
| `training` | `distillation+evolutionary` | Convergence monitoring |
| `digest` | All enabled modes | Onboarding, incident triage, state snapshot |

**Example:**

```python
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="cost+energy")
ctx_read("src/enhancements/", mode="digest")
```

---

## Mode Selection Decision Tree

```
Need to EDIT a file?
├── yes → `full` (first read), then `diff` (re-reads)
│         └── do NOT use enhanced or aggressive modes
└── no  → need a single fact?
          ├── routing / topology      → graph | routing | graph+routing
          ├── trade-off / score       → modp | modp+moe | modp+rlhf
          ├── cost / carbon / energy  → cost | energy | cost+energy
          ├── model training state    → distillation | evolutionary | training
          ├── routing weights         → moe
          ├── inference delegation    → flexgen | energy
          ├── security / trust        → zero_trust | safety
          ├── privacy budget          → privacy
          ├── checkpoint state        → checkpoint | experiment
          ├── symbolic rules          → symbolic
          ├── cache health            → cache
          ├── drift / anomaly         → anomaly
          ├── workload / SLO          → workload
          ├── "everything at once"    → digest
          ├── API surface only        → signatures
          ├── structure only          → map
          ├── specific lines          → lines:N-M
          └── unsure                  → auto
```

**Anti-pattern:** never use `full` for files you won't edit — use `map` or `signatures`.

---

## Token Budget Guidance

Approximate token counts, measured on a 4 vCPU / 16 GB host. `full` is
file-size dependent; everything else is bounded by mode.

| Mode | Typical tokens | Use when |
|---|---|---|
| `cache`, `anomaly`, `safety`, `privacy`, `checkpoint`, `experiment`, `meta` | < 100 | quick health checks |
| `energy`, `routing`, `workload` | ~100–250 | targeted diagnosis |
| `modp`, `rlhf`, `zero_trust`, `moe` | ~250–500 | decision input |
| `graph`, `distillation`, `evolutionary` | ~500–1 000 | analysis |
| `flexgen`, `cost` | ~500–1 500 | configuration review |
| `digest`, `training` | ~1 000–3 000 | snapshot / triage |
| `full` | file-size dependent | **only when editing** |

---

## Configuration Matrix

Every enhanced mode requires a corresponding enable flag. Missing flag → mode
silently falls back to `full` (or `{}`) and increments `lean_ctx.fallback.count`.

| Mode(s) | Enable flag |
|---|---|
| all enhanced | `ENHANCEMENTS_ENABLED=true` |
| `graph`, `routing`, `anomaly` | `LIMIT_GRAPH_ENABLED=true` |
| `modp`, `cost`, `workload` | `MODP_ENABLED=true` |
| `rlhf` | `RLHF_ENABLED=true` |
| `distillation`, `training` | `DISTILLATION_ENABLED=true` |
| `evolutionary`, `training` | `EVOLUTIONARY_ENABLED=true` |
| `moe` | `MOE_ENABLED=true` (alias `MOE_GATING_ENABLED`) |
| `flexgen`, `energy` | `FLEXGEN_ENABLED=true` |
| `zero_trust`, `safety` | `ZERO_TRUST_ENABLED=true` |
| `privacy` | `DIFFERENTIAL_PRIVACY_ENABLED=true` |
| `checkpoint`, `experiment` | `TRAINING_ENABLED=true` |
| `symbolic` | always on with `symbolic_policy.yaml` |
| `cache`, `meta` | always on (lean-ctx core) |
| `digest` | union of all enabled modes |

> **Precedence:** env vars are **canonical**; YAML config maps onto them. Env
> wins on conflict. Matches the Dockerfile v2.0 and the Configuration Matrix in
> `SYMBOLIC_OVERSIGHT_README.md` v1.2.

---

## Fallback & Error Taxonomy

| Condition | Behavior | Signal |
|---|---|---|
| Module disabled | Return `{}` (empty result) | `lean_ctx.fallback.count` +1 |
| Module file missing | Return `{}` | `lean_ctx.fallback.count` +1 |
| Schema version mismatch | Best-effort parse + warning in result | `lean_ctx.fallback.schema_warn` |
| Mode unknown to build | Fall through to `full` | `lean_ctx.fallback.unknown_mode` |
| Composite sub-mode fails | Return only successful sub-results | `lean_ctx.fallback.partial` |
| File unreadable | Standard `ctx_read` error | propagate |

All fallbacks are **non-fatal**. Log line format:

```
[lean-ctx] fallback mode=<mode> reason=<reason> path=<path>
```

---

## File Editing

Use native `Edit` / `StrReplace` if available.

**Edit fallback rule (single source of truth):**

1. Prefer **native `Edit` / `StrReplace`** when available.
2. If native `Edit` requires a prior `Read` **and** `Read` is unavailable → use **`ctx_edit`**.
3. **NEVER** loop on `Edit` failures. On the **first** failure, switch to `ctx_edit` immediately.
4. **`Write` / `Delete` / `Glob`** — use **natively**. These tools have no
   cache/compression benefit under lean-ctx, so native use is correct.

---

## Proactive Usage

Use these without being asked:

| Command | Purpose | When |
|---|---|---|
| `ctx_overview(task)` | Summarize the current task + suggested reads | At session start |
| `ctx_compress` | Re-compress accumulated context | When context grows large (> ~50 % of window) |

**Avoid `ctx_compress`** when you are about to edit — compression may drop
verbatim content. Compress after editing, not before.

---

## Cache Invalidation

| Situation | Do this |
|---|---|
| After `ctx_edit` / native `Edit` on a file | `ctx_read(path, mode="diff")` — re-reads ~13 tokens |
| After `git pull` / branch switch | `ctx_read(path, mode="full")` for the changed files |
| After `pip install` (which may regenerate files) | `ctx_read(".", mode="map")` or `ctx_tree(".", depth=2)` |
| After a schema bump (see `mode="meta"`) | Re-read descriptors with `mode="full"` before using enhanced modes |

> Caching is **content-addressed** — a file whose bytes did not change is
> re-read at the same ~13-token cost regardless of which mode you request.

---

## When to Skip lean-ctx

Use native tools directly when:

- The file is **tiny** (< ~50 lines) — cache overhead exceeds benefit.
- The file is **about to be deleted** — no point caching.
- The format is **opaque to compression** (e.g. binary, minified JS, base64 blobs).
- You are **writing** a new file — `Write` natively; lean-ctx adds nothing.
- You are in a **non-MCP** session — native `Read` / `Grep` are fine.

---

## Integration with Advanced Green Agent Enhancements

These additions allow lean-ctx to fetch contextual information from the advanced
modules under `src/enhancements/`. When those modules are enabled, the enhanced
read modes provide direct access to their state, enabling more informed
decision-making and reducing token usage for large configuration or metric dumps.

### How to use (with canonical paths)

```python
# LIMIT Graph — topology + routing
ctx_read("src/enhancements/core/graph_registry.py", mode="graph")
ctx_read("src/enhancements/core/graph_registry.py", mode="routing")
ctx_read("src/enhancements/core/graph_registry.py", mode="graph+routing")

# Anomaly / drift
ctx_read("src/enhancements/core/anomaly_detector.py", mode="anomaly")

# MODP — trade-off vector + composite score
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="cost")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="modp+moe")

# Workload — SLO / batch / priority
ctx_read("src/enhancements/schemas/workload_descriptor.py", mode="workload")

# RLHF — human feedback score
ctx_read("src/enhancements/schemas/feedback_event.py", mode="rlhf")

# Distillation + Evolutionary (state lives in the descriptor)
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="distillation")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="evolutionary")
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="training")

# MoE — gate weights + routing stability
ctx_read("src/enhancements/schemas/node_descriptor.py", mode="moe")

# FlexGen — config + energy
ctx_read("config/green_agent.yaml", mode="flexgen")
ctx_read("config/green_agent.yaml", mode="energy")
ctx_read("config/green_agent.yaml", mode="cost+energy")

# Zero Trust / Safety / Privacy
ctx_read("src/enhancements/zero_trust_architecture.py", mode="zero_trust")
ctx_read("src/enhancements/core/safety.py", mode="safety")
ctx_read("src/enhancements/core/privacy.py", mode="privacy")

# Checkpoint / Experiment
ctx_read("src/enhancements/core/checkpoints.py", mode="checkpoint")
ctx_read("src/enhancements/core/experiments.py", mode="experiment")

# Symbolic rules + violations
ctx_read("symbolic_policy.yaml", mode="symbolic")
ctx_read("symbolic_violations.json", mode="reference")

# Cache + version check
ctx_read("", mode="cache")
ctx_read("", mode="meta")

# One-shot snapshot
ctx_read("src/enhancements/", mode="digest")
```

### Why prefer enhanced modes

- **Token savings** — a `digest` read is ~1 000–3 000 tokens vs. potentially
  tens of thousands for reading all of `src/enhancements/`.
- **Focus** — you get the current state, not the implementation history.
- **Fallback-safe** — missing modules degrade gracefully; you see empty results
  rather than errors.

---

## Canonical Repo Paths

Use these paths when composing `ctx_read` calls. Bare filenames (e.g.
`graph_registry.py`) will **not** resolve — always include the full path.

| Artifact | Path |
|---|---|
| Enhancements root | `src/enhancements/` |
| Enhancement descriptors | `src/enhancements/schemas/{node_descriptor,workload_descriptor,feedback_event}.py` |
| Enhancement core | `src/enhancements/core/{graph_registry,causal_graph,dag_carbon_ledger,anomaly_detector}.py` |
| Zero Trust | `src/enhancements/zero_trust_architecture.py` |
| Symbolic engine | `src/symbolic/symbolic_reasoning_engine.py` |
| Symbolic visualizer | `src/dashboard/symbolic_visualizer.py` |
| Policy feedback | `src/policy/policy_feedback.py` |
| Symbolic rules | `symbolic_policy.yaml` |
| Config | `config/green_agent.yaml` |
| Policies | `green_policy.yaml`, `symbolic_policy.yaml` |
| Build | `Dockerfile`, `pyproject.toml`, `MANIFEST.sha256` |
| Scripts | `scripts/{setup,verify,migrate,cleanup,deploy}.sh`, `scripts/community_data_collector.py` |
| Tests | `test_symbolic_oversight.py` (19 tests), `test_meta_cognitive.py` |

---

## Mode Contract & Versioning

- **Contract:** every mode returns a JSON-serialisable object whose top level
  matches `schemas/<mode>_vN.json`. Backwards-incompatible changes bump `N`.
- **Version check:** `ctx_read("", mode="meta")` reports the lean-ctx build and
  supported modes.
- **Deprecation:** modes are deprecated for **one minor release** before removal;
  a warning is emitted with the replacement mode name.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `ctx_read(..., mode="graph")` returns `{}` | `LIMIT_GRAPH_ENABLED` off | enable flag or use `full` |
| Enhanced mode returns `full` file | mode not supported by build | upgrade lean-ctx; check `mode="meta"` |
| `digest` missing sections | sub-module disabled | enable the missing flag |
| `ctx_edit` repeatedly failing | file has no match / race | re-read via `ctx_read(mode="full")`, then retry once; never loop |
| High token spend | using `full` on large files | switch to enhanced mode |
| `ctx_read("graph_registry.py", ...)` fails to resolve | bare filename | use `src/enhancements/core/graph_registry.py` |
| `flexgen` energy looks wrong (joules) | deprecated unit | use `flexgen_energy_kwh` (kWh) |
| `mode="meta"` shows old version | stale server | restart the lean-ctx MCP server |

---

## Changelog

### v9 (2026-11-13)

**Added**
- Canonical full paths in all §How-to-use examples.
- **New modes:** `routing`, `anomaly`, `cost`, `workload`, `energy`, `zero_trust`, `safety`, `privacy`, `checkpoint`, `experiment`, `symbolic`, `cache`, `digest`, `meta`.
- **Composite modes:** `modp+moe`, `graph+routing`, `cost+energy`, `modp+rlhf`, `training`, `digest`.
- [Token Budget Guidance](#token-budget-guidance) table.
- [Configuration Matrix](#configuration-matrix) mapping modes → enable flags.
- [Fallback & Error Taxonomy](#fallback--error-taxonomy) with log line format.
- [Mode Selection Decision Tree](#mode-selection-decision-tree).
- [Cache Invalidation](#cache-invalidation) guidance.
- [When to Skip lean-ctx](#when-to-skip-lean-ctx) guidance.
- [Canonical Repo Paths](#canonical-repo-paths) table.
- [Mode Contract & Versioning](#mode-contract--versioning).
- [Troubleshooting](#troubleshooting) section.
- [Reviewer Checklist](#reviewer-checklist).
- Metadata block with version, schema, owner, and companion docs.
- Table of Contents.

**Changed**
- Header ownership marker `v1` → **`v9`** (matches `lean-ctx-rules-v9`).
- `ctx_read(path, mode)` → `ctx_read(path, mode="…")` (keyword recommended).
- FlexGen energy unit clarified to **`flexgen_energy_kwh`** (kWh).
- "~13 tokens" claim scoped to "on a 500-line Python module".
- `aggressive` / `entropy` modes now carry an explicit accuracy caveat.
- `map` vs `signatures` distinction documented.
- `Write` / `Delete` / `Glob` native use **justified** (no cache benefit).
- Added `.cursorrules` relationship: mirror, this file wins on divergence.

**Fixed**
- Removed 4 invented state files (`distillation_log.json`,
  `evolutionary_state.json`, `moe_gating_state.json`, `flexgen_config.yaml`).
- Fixed all bare filenames in §How to use (`graph_registry.py` →
  `src/enhancements/core/graph_registry.py`, etc.).
- Removed dangling `<!-- /lean-ctx -->` without matching opening tag.
- Reconciled with `SYMBOLIC_OVERSIGHT_README.md` v1.2, `Meta-Cognitive Architecture
  for Green Agent.md` v1.2, `QUICK_START.md` v1.2, and `Scripts and Utilities.md`
  v1.2.

### v8

- Initial enhanced mode set: `graph`, `modp`, `rlhf`, `distillation`,
  `evolutionary`, `moe`, `flexgen`.

---

## Reviewer Checklist

- [ ] Every `ctx_read` example uses a **full path** (no bare filenames).
- [ ] No references to `distillation_log.json`, `evolutionary_state.json`,
  `moe_gating_state.json`, or `flexgen_config.yaml` remain.
- [ ] All **24 modes** are listed in [Enhanced Read Modes](#enhanced-read-modes-green-agent)
  and mapped in [Configuration Matrix](#configuration-matrix).
- [ ] `digest`, `cache`, `meta`, and composite modes (`modp+moe`,
  `graph+routing`, `cost+energy`) are present and used in examples.
- [ ] FlexGen energy unit is **`flexgen_energy_kwh`** everywhere.
- [ ] Header ownership marker matches the rules version (`v9`).
- [ ] `PROJECT-LEAN-CTX.md` and `.cursorrules` are in sync (or `.cursorrules`
  links here as the canonical source).
- [ ] Fallback taxonomy matches `SYMBOLIC_OVERSIGHT_README.md` v1.2 §Fallback & Error Taxonomy.
- [ ] Configuration Matrix matches `SYMBOLIC_OVERSIGHT_README.md` v1.2 §Configuration Matrix
  and `Dockerfile` v2.0 ARG/ENV names.
- [ ] Troubleshooting covers: empty `{}`, wrong path, wrong unit, stale version.
- [ ] No unsubstantiated token-count claims — "~13 tokens" scoped to a specific case.

---

<!-- /lean-ctx-owned: PROJECT-LEAN-CTX.md v9 -->
