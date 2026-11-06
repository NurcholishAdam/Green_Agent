"""
analytics/audit_logger.py — Structured, rotating, PII-safe audit logging.

Emits one JSON object per line (JSONL) suitable for promtail/fluent-bit/jq.
Never logs raw prompts or provider responses — only `prompt_hash`.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

__all__ = ["AuditEvent", "AuditLogger", "TransparencyReport"]


@dataclass
class AuditEvent:
    event_type: str
    task_id: str
    agent_id: str
    timestamp_iso: str
    expert_type: str = ""
    domain: str = ""
    provider: str = ""
    model: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    duration_ms: float = 0.0
    energy_wh: float | None = None
    carbon_kg: float | None = None
    redactions: dict[str, int] = field(default_factory=dict)
    graph_metrics: dict[str, float] = field(default_factory=dict)
    human_feedback_score: float | None = None
    prompt_hash: str = ""
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransparencyReport:
    period_start_iso: str
    period_end_iso: str
    escalations: int
    non_escalations: int
    escalation_rate: float
    expert_cost_usd: float
    energy_wh_total: float
    carbon_kg_total: float
    redactions_applied: dict[str, int] = field(default_factory=dict)
    enhancements: dict[str, Any] = field(default_factory=dict)   # explicit field

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AuditLogger:
    def __init__(
        self,
        log_file: Path,
        *,
        rotate_max_bytes: int = 100 * 1024 * 1024,
        rotate_backups: int = 5,
    ) -> None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        self._handler = RotatingFileHandler(
            log_file, maxBytes=rotate_max_bytes, backupCount=rotate_backups,
        )
        self._handler.setFormatter(logging.Formatter("%(message)s"))
        self._logger = logging.getLogger(f"audit.{log_file.name}")
        self._logger.setLevel(logging.INFO)
        self._logger.addHandler(self._handler)
        self._logger.propagate = False
        self._lock = threading.Lock()

    # --- Logging -----------------------------------------------------------
    def log(self, event: AuditEvent) -> None:
        with self._lock:
            self._logger.info(json.dumps(asdict(event), separators=(",", ":")))

    def log_expert_invocation(
        self,
        *,
        task_id: str,
        agent_id: str,
        timestamp_iso: str,
        provider: str,
        model: str,
        prompt: str,
        input_tokens: int,
        output_tokens: int,
        cost_usd: float,
        duration_ms: float,
        domain: str,
        energy_wh: float | None = None,
        carbon_kg: float | None = None,
        redactions: dict[str, int] | None = None,
        graph_metrics: dict[str, float] | None = None,
        human_feedback_score: float | None = None,
        error: str | None = None,
    ) -> None:
        event = AuditEvent(
            event_type="expert_invocation",
            task_id=task_id,
            agent_id=agent_id,
            timestamp_iso=timestamp_iso,
            expert_type=f"{provider}:{model}",
            domain=domain,
            provider=provider,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost_usd,
            duration_ms=duration_ms,
            energy_wh=energy_wh,
            carbon_kg=carbon_kg,
            redactions=redactions or {},
            graph_metrics=graph_metrics or {},
            human_feedback_score=human_feedback_score,
            prompt_hash=hashlib.sha256(prompt.encode()).hexdigest()[:16],
            error=error,
        )
        self.log(event)

    def log_escalation_decision(
        self,
        *,
        task_id: str,
        agent_id: str,
        timestamp_iso: str,
        should_escalate: bool,
        reasons: list[str],
        domain: str,
    ) -> None:
        self.log(AuditEvent(
            event_type="escalation_decision",
            task_id=task_id,
            agent_id=agent_id,
            timestamp_iso=timestamp_iso,
            domain=domain,
            metadata={"should_escalate": should_escalate, "reasons": reasons},
        ))

    # --- Reporting ---------------------------------------------------------
    def generate_transparency_report(
        self,
        *,
        period_start_iso: str,
        period_end_iso: str,
    ) -> TransparencyReport:
        escalations = 0
        non_escalations = 0
        cost_total = 0.0
        energy_total = 0.0
        carbon_total = 0.0
        redactions: dict[str, int] = {}

        for line in self._iterate_log():
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = evt.get("timestamp_iso", "")
            if not (period_start_iso <= ts <= period_end_iso):
                continue
            if evt.get("event_type") == "escalation_decision":
                if evt.get("metadata", {}).get("should_escalate"):
                    escalations += 1
                else:
                    non_escalations += 1
            elif evt.get("event_type") == "expert_invocation":
                cost_total   += float(evt.get("cost_usd") or 0.0)
                energy_total += float(evt.get("energy_wh") or 0.0)
                carbon_total += float(evt.get("carbon_kg") or 0.0)
                for k, v in (evt.get("redactions") or {}).items():
                    redactions[k] = redactions.get(k, 0) + int(v)

        total = escalations + non_escalations
        return TransparencyReport(
            period_start_iso=period_start_iso,
            period_end_iso=period_end_iso,
            escalations=escalations,
            non_escalations=non_escalations,
            escalation_rate=round(escalations / total, 4) if total else 0.0,
            expert_cost_usd=round(cost_total, 6),
            energy_wh_total=round(energy_total, 6),
            carbon_kg_total=round(carbon_total, 6),
            redactions_applied=redactions,
        )

    def export_report(
        self, report: TransparencyReport, out_path: Path, fmt: str = "json",
    ) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "json":
            out_path.write_text(json.dumps(report.to_dict(), indent=2))
        elif fmt == "jsonl":
            out_path.write_text(json.dumps(report.to_dict()) + "\n")
        else:
            raise ValueError(f"unsupported format: {fmt} (use json|jsonl)")

    def _iterate_log(self):
        path = Path(self._handler.baseFilename)
        if not path.exists():
            return
        with path.open("r", encoding="utf-8") as f:
            yield from f
