<!-- lean-ctx-owned: PROJECT-LEAN-CTX.md v1 -->
# lean-ctx — Context Engineering Layer
<!-- lean-ctx-rules-v9 -->

PREFER lean-ctx MCP tools over native equivalents for token savings:

## Tool preference:
| PREFER | OVER | Why |
|--------|------|-----|
| `ctx_read(path, mode)` | `Read` / `cat` | Cached, 10 read modes, re-reads ~13 tokens |
| `ctx_shell(command)` | `Shell` / `bash` | Pattern compression for git/npm/cargo output |
| `ctx_search(pattern, path)` | `Grep` / `rg` | Compact, token-efficient results |
| `ctx_tree(path, depth)` | `ls` / `find` | Compact directory maps |
| `ctx_edit(path, old_string, new_string)` | `Edit` (when Read unavailable) | Search-and-replace without native Read |

## ctx_read modes:
- `auto` — auto-select optimal mode (recommended default)
- `full` — cached read (files you edit)
- `map` — deps + exports (context-only files)
- `signatures` — API surface only
- `diff` — changed lines after edits
- `aggressive` — maximum compression (context only)
- `entropy` — highlight high-entropy fragments
- `task` — IB-filtered (task relevant)
- `reference` — quote-friendly minimal excerpts
- `lines:N-M` — specific range
- **`graph`** — LIMIT Graph metrics (centrality, connectivity) from `GraphRegistry` / `CausalGraph`
- **`modp`** — current MODP weights and composite score (from `NodeDescriptor` / `WorkloadDescriptor`)
- **`rlhf`** — human feedback score (from `FeedbackEvent` or config)
- **`distillation`** — distillation student stats (counter, buffer size, MoE gate weights)
- **`evolutionary`** — evolutionary optimizer best fitness, population parameters
- **`moe`** — MoE gating network weights and expert routing stats
- **`flexgen`** — FlexGen configuration and energy metrics

## Mode selection:
1. Editing the file? → `full` first, then `diff` for re-reads
2. Need API surface only? → `map` or `signatures`
3. Large file, context only? → `entropy` or `aggressive`
4. Specific lines? → `lines:N-M`
5. Active task set? → `task`
6. **Need Green Agent enhancement metrics?** → use the corresponding enhanced mode (`graph`, `modp`, `rlhf`, `distillation`, `evolutionary`, `moe`, `flexgen`)
7. Unsure? → `auto` (system selects optimal mode)

Anti-pattern: never use `full` for files you won't edit — use `map` or `signatures`.

## File editing:
Use native Edit/StrReplace if available. If Edit requires Read and Read is unavailable, use ctx_edit.
Write, Delete, Glob → use normally. NEVER loop on Edit failures — switch to ctx_edit immediately.

## Proactive (use without being asked):
- `ctx_overview(task)` at session start
- `ctx_compress` when context grows large

---

## 🧠 Integration with Advanced Green Agent Enhancements

These additions allow lean‑ctx to fetch contextual information from the advanced modules under `src/enhancements/`. When those modules are enabled, the new read modes provide direct access to their state, enabling more informed decision‑making and reducing token usage for large configuration or metric dumps.

### How to use

- **LIMIT Graph** – `ctx_read("graph_registry.py", mode="graph")` retrieves current graph metrics (centrality, connectivity) instead of reading the entire registry implementation.
- **MODP** – `ctx_read("node_descriptor.py", mode="modp")` returns the active MODP weights and composite score without parsing the whole file.
- **RLHF** – `ctx_read("feedback_event.py", mode="rlhf")` extracts the latest human feedback score.
- **Distillation** – `ctx_read("distillation_log.json", mode="distillation")` gives student counter, replay buffer size, and MoE gate weights.
- **Evolutionary** – `ctx_read("evolutionary_state.json", mode="evolutionary")` shows best fitness and population stats.
- **MoE** – `ctx_read("moe_gating_state.json", mode="moe")` returns gate weights and expert routing statistics.
- **FlexGen** – `ctx_read("flexgen_config.yaml", mode="flexgen")` provides model name, precision, delegation policy, and energy metrics.

These modes are designed to integrate seamlessly with the existing lean‑ctx command set, keeping context small while surfacing the most relevant advanced metrics.

<!-- /lean-ctx -->
