"""
runtime/redaction.py — Pre-egress redaction and prompt-injection detection.

Redaction is lossy by design: an expert LLM receives `[REDACTED_EMAIL]`,
never the raw string.

Two concerns are kept separate on purpose:
    * redact()                 — replace sensitive patterns
    * check_prompt_injection() — refuse the call entirely (do not "clean")
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

__all__ = ["RedactionResult", "redact", "check_prompt_injection"]


_PATTERNS: dict[str, re.Pattern[str]] = {
    "EMAIL":       re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "PHONE":       re.compile(r"\b(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{2,4}\)?[\s-]?)?\d{3,4}[\s-]?\d{4}\b"),
    "SSN":         re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "CREDIT_CARD": re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
    "API_KEY":     re.compile(r"\b(?:sk|pk|ghp|AKIA|ASIA|AIza)[A-Za-z0-9_\-]{16,}\b"),
    "BEARER":      re.compile(r"\bBearer\s+[A-Za-z0-9\-._~+/]+=*\b"),
    "AWS_SECRET":  re.compile(r"(?i)aws_secret_access_key\s*=\s*[A-Za-z0-9/+=]{40}"),
    "PRIVKEY":     re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
    "IP":          re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
}

_INJECTION_PATTERNS = [
    re.compile(r"(?i)ignore\s+(?:all\s+)?(?:previous|prior|above)\s+instructions?"),
    re.compile(r"(?i)disregard\s+(?:all\s+)?(?:previous|prior|system)\s+(?:instructions?|prompts?)"),
    re.compile(r"(?i)you\s+are\s+now\s+(?:in\s+)?(?:dan|developer\s+mode)"),
    re.compile(r"(?i)reveal\s+(?:your\s+)?system\s+prompt"),
    re.compile(r"(?i)print\s+(?:your\s+)?(?:api\s+key|secret)"),
]


@dataclass
class RedactionResult:
    text: str
    hits: dict[str, int] = field(default_factory=dict)

    @property
    def was_modified(self) -> bool:
        return bool(self.hits)


def redact(text: str, extra_allowlist: set[str] | None = None) -> RedactionResult:
    """
    Redact PII and secrets. `extra_allowlist` contains literal substrings to
    skip (e.g., public document IDs that look like API keys).
    """
    hits: dict[str, int] = {}
    out = text
    allowlist = extra_allowlist or set()

    for name, pattern in _PATTERNS.items():
        def _sub(m: re.Match[str], _name: str = name) -> str:
            if any(a and a in m.group(0) for a in allowlist):
                return m.group(0)
            hits[_name] = hits.get(_name, 0) + 1
            return f"[REDACTED_{_name}]"
        out = pattern.sub(_sub, out)

    return RedactionResult(text=out, hits=hits)


def check_prompt_injection(text: str) -> str | None:
    """
    Return the matched pattern (truncated) if the text looks like a
    prompt-injection attempt, else None. Callers must refuse the expert call
    when this returns non-None.
    """
    for p in _INJECTION_PATTERNS:
        if p.search(text):
            return p.pattern[:60]
    return None
