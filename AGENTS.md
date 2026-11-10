
# Agent Instructions

**Version:** 2.0 · **Updated:** 2024-01-01
**Applies to:** AI coding assistants (Claude Code, Cursor, Copilot, etc.)
**Prerequisite:** lean-ctx MCP server configured in your client

---

<!-- lean-ctx -->
## lean-ctx

Prefer lean-ctx MCP tools over native file reads for token savings.
Full rules and tool definitions: [LEAN-CTX.md](./LEAN-CTX.md)

The `ctx_read` tool described below is defined in `tools/ctx_read.py` and
exposed over MCP by the `lean-ctx` server. If `LEAN-CTX.md` is not present
in your checkout, the tool may still be available through the MCP server —
use `ctx_modes()` to confirm.

<!-- /lean-ctx -->

---

## Standard read modes

The `ctx_read` tool supports the following standard modes. These are
the fallback when an enhanced mode is unavailable.

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

When working with files under `src/enhancements/`, use the **enhanced
read modes** below to fetch only the most relevant state. Each returns a
`dict` with the fields listed in the "Returns" column, or `{}` if the
mode is unavailable.

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

# FlexGen (config vs. metrics are separate modes)
ctx_read("config/base/green_agent_config.yaml", mode="flexgen_config")
ctx_read("logs/green_agent_2024-01-01.log",     mode="flexgen_metrics")

# Zero Trust
ctx_read("src/enhancements/zero_trust_architecture.py", mode="zero_trust")
```

### When to use each mode

| Mode | Use when |
|---|---|
| `graph` | You need topology context for routing, anomaly analysis, or causal root-cause attribution |
| `modp` | You're evaluating trade-offs between accuracy, energy, latency, and carbon, or checking why a run scored low |
| `rlhf` | Human feedback influences the current policy, or you need to see the feedback source/URL |
| `distillation` | You're checking whether the student policy is learning and the replay buffer is filling |
| `evolutionary` | You're monitoring whether evolutionary optimisation is converging or has stalled |
| `moe` | You're diagnosing expert routing instability or gate-weight drift |
| `flexgen_config` | You're deciding whether to delegate LLM inference and at what precision |
| `flexgen_metrics` | You're correlating energy consumption with precision changes |
| `zero_trust` | You're auditing authentication level or checking the audit-event count |

### Prerequisites

Enhanced modes require that the corresponding enhancement modules are
**enabled and present**:

- Modules ship with the repo under `src/enhancements/`.
- Enhancement behavior is controlled by `enhancements.enabled` in
  `config/base/green_agent_config.yaml` — **default is `false`** (per
  `README.md §0.2`).
- To opt in for a single run: `python run_agent.py --enhancements`.
- To opt in persistently: set `enhancements.enabled: true` in
  `config/base/green_agent_config.yaml`, or export
  `ENHANCEMENTS_ENABLED=true` (the CLI flag wins over env when both are
  present).

### Fallback behavior

If the target file does not contain the requested module, or
enhancements are disabled:

| Situation | `ctx_read` returns | Agent should |
|---|---|---|
| File exists, mode available | `dict` with the mode's fields | Use the fields directly |
| File exists, mode unavailable | `{}` (empty dict) | Fall back to `mode="full"` and parse manually |
| File does not exist | `{}` (empty dict) + warning in logs | Treat the file as absent; do not retry |
| Modes disabled globally | `{}` (empty dict) + warning in logs | Use `mode="full"` |

Never assume a non-empty result — always check `if result: ...`.

### Do NOT use lean-ctx to read

- Files under `secrets/` or `/run/secrets` — use your platform's secret
  API instead.
- `.env`, `*.pem`, `*.key`, `*.crt` — never read secrets into an
  agent's context, even transiently.
- Large binary files (`.tar.gz`, `.whl`, `.zip`) — use `docker cp` or
  `unzip -l` to inspect metadata instead.

### Cost and rate limits

Each `ctx_read` call costs tokens proportional to the returned payload,
not to the file size. A `mode="graph"` on a 5,000-line file costs the
same as on a 50-line file. Chained calls do not share cost — batch
reads on the same file are not yet supported.

Rate limits follow your MCP server configuration; see `LEAN-CTX.md`.

### Discovering available modes

If this table may be stale, call:

```python
from tools.ctx_read import ctx_modes
ctx_modes()  # returns list[str] of modes the current server supports
```

If the tool is unavailable, fall back to the table above.

---

**Remember:**

- Use **enhanced modes** for inspection (state queries, metric lookups).
- Use **`outline`** for structure.
- Use **`full`** only for files you intend to edit.
- **Never** use lean-ctx to read secrets.
- **Always** check `if result: ...` — the enhanced modes can return `{}`.
