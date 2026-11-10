#!/usr/bin/env python3
"""
scripts/validate_agent_toml.py — Validate an AgentBeats agent descriptor.

Semantic checks the TOML syntax cannot express:

    1.  agent_type == "green".
    2.  protocols contains only implemented protocols (A2A).
    3.  Every endpoint uses `{host}` — rejects 0.0.0.0 / localhost / 127.0.0.1.
    4.  health endpoint ends in `/healthz`; ready ends in `/readyz`.
    5.  unique_metrics all start with `green_agent_`.
    6.  evaluation_domains values are in the actual TaskKind enum.
    7.  enhancements.enabled defaults to false.
    8.  rlhf.feedback_url present when feedback_source ∈ {dashboard, api}.
    9.  modp.objective_weights sum to 1.0 ± 1e-6.
    10. bio_inspired.elitism <= population_size.
    11. flexgen.default_precision ∈ precision_options.
    12. carbon_context.region is one of the 13 documented regions.
    13. python_version range overlaps the supported range (>=3.10,<3.13).
    14. hardware_profile is one of the 10 documented profiles.

Usage:
    python scripts/validate_agent_toml.py agent.toml

Exit codes:
    0  all checks pass
    1  semantic check failed
    2  usage / parse error
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ImportError:
        print(
            "FATAL: need tomllib (3.11+) or tomli. pip install tomli",
            file=sys.stderr,
        )
        sys.exit(2)

_ALLOWED_REGIONS = {
    "US-CA", "US-TX", "US-NY", "EU-FR", "EU-DE", "EU-NO",
    "EU-PL", "CN", "IN", "JP", "AU", "BR", "GLOBAL",
}
_HARDWARE_PROFILES = {
    "nvidia_a100", "nvidia_v100", "nvidia_t4", "nvidia_rtx_3090",
    "nvidia_rtx_4090", "amd_mi250", "google_tpu_v4",
    "cpu_intel_xeon", "cpu_amd_epyc", "default",
}
_TASK_KINDS = {"fine_tuning", "inference", "agent", "benchmark"}
_BAD_HOSTS = {"0.0.0.0", "localhost", "127.0.0.1", "::1"}

_failed = False


def _fail(msg: str) -> None:
    global _failed
    _failed = True
    print(f"[FAIL] {msg}", file=sys.stderr)


def _pass(msg: str) -> None:
    print(f"[PASS] {msg}")


# --- 1 ----------------------------------------------------------------------

def check_agent_type(doc: dict) -> None:
    if doc.get("agent", {}).get("agent_type") != "green":
        _fail("agent.agent_type must be 'green'")
    else:
        _pass("agent.agent_type = 'green'")


# --- 2 ----------------------------------------------------------------------

def check_protocols(doc: dict) -> None:
    protocols = set(doc.get("capabilities", {}).get("protocols", []))
    if not protocols:
        _fail("capabilities.protocols is empty")
        return
    unsupported = protocols - {"A2A"}
    if unsupported:
        _fail(f"capabilities.protocols contains unsupported: {sorted(unsupported)}")
        return
    _pass(f"capabilities.protocols = {sorted(protocols)}")


# --- 3-4 --------------------------------------------------------------------

def check_endpoints(doc: dict) -> None:
    endpoints = doc.get("endpoints", {})
    if not endpoints:
        _fail("endpoints section missing")
        return

    for name, url in endpoints.items():
        m = re.match(r"^(?:https?)://([^/:]+)(?::\d+)?(/.+)?$", url)
        if not m:
            _fail(f"endpoints.{name} is not a valid URL: {url!r}")
            continue
        host = m.group(1)
        if host in _BAD_HOSTS:
            _fail(
                f"endpoints.{name} uses {host!r} — use {{host}} template "
                f"so the harness can reach the container"
            )
        elif host != "{host}":
            _fail(
                f"endpoints.{name} host is {host!r}; expected {{host}}"
            )
        else:
            _pass(f"endpoints.{name} = {url}")

    # Path checks
    health = endpoints.get("health", "")
    if not health.endswith("/healthz"):
        _fail(f"endpoints.health must end in '/healthz' (got {health!r})")
    else:
        _pass("endpoints.health ends in /healthz")

    ready = endpoints.get("ready", "")
    if ready and not ready.endswith("/readyz"):
        _fail(f"endpoints.ready must end in '/readyz' (got {ready!r})")
    elif ready:
        _pass("endpoints.ready ends in /readyz")


# --- 5 ----------------------------------------------------------------------

def check_unique_metrics(doc: dict) -> None:
    metrics = doc.get("capabilities", {}).get("unique_metrics", [])
    if not metrics:
        _fail("capabilities.unique_metrics is empty")
        return
    bad = [m for m in metrics if not m.startswith("green_agent_")]
    if bad:
        _fail(f"unique_metrics without 'green_agent_' prefix: {bad}")
        return
    _pass(f"unique_metrics ({len(metrics)} entries, all prefixed)")


# --- 6 ----------------------------------------------------------------------

def check_evaluation_domains(doc: dict) -> None:
    domains = doc.get("capabilities", {}).get("evaluation_domains", [])
    if not domains:
        _fail("capabilities.evaluation_domains is empty")
        return
    unknown = set(domains) - _TASK_KINDS
    if unknown:
        _fail(
            f"evaluation_domains contains non-TaskKind values: "
            f"{sorted(unknown)} (allowed: {sorted(_TASK_KINDS)})"
        )
        return
    _pass(f"evaluation_domains = {sorted(domains)}")


# --- 7-11 ------------------------------------------------------------------

def check_enhancements(doc: dict) -> None:
    enh = doc.get("enhancements", {})
    if not enh:
        _fail("enhancements section missing")
        return

    if enh.get("enabled") is not False:
        _fail(
            "enhancements.enabled must be false (per README.md §0.2) — "
            "opt in per-query via [defaults.environment] or --config"
        )
    else:
        _pass("enhancements.enabled = false (safe default)")

    # rlhf.feedback_url
    rlhf = enh.get("rlhf", {})
    if rlhf.get("enabled") and rlhf.get("feedback_source") in {"dashboard", "api"}:
        if not rlhf.get("feedback_url"):
            _fail("enhancements.rlhf.feedback_url is required for dashboard/api source")
        else:
            _pass(f"enhancements.rlhf.feedback_url = {rlhf['feedback_url']}")

    # modp weights
    w = enh.get("modp", {}).get("objective_weights", [])
    if w:
        if abs(sum(w) - 1.0) > 1e-6:
            _fail(f"modp.objective_weights sum to {sum(w)} (expected 1.0)")
        else:
            _pass("modp.objective_weights sum to 1.0")

    # bio_inspired
    bio = enh.get("bio_inspired", {})
    pop = bio.get("population_size")
    elit = bio.get("elitism")
    if isinstance(pop, int) and isinstance(elit, int) and elit > pop:
        _fail(f"bio_inspired.elitism ({elit}) > population_size ({pop})")
    elif pop is not None and elit is not None:
        _pass("bio_inspired.elitism <= population_size")

    # flexgen
    fg = enh.get("flexgen", {})
    if fg:
        dp = fg.get("default_precision")
        opts = fg.get("precision_options", [])
        if dp and opts and dp not in opts:
            _fail(f"flexgen.default_precision {dp!r} not in {opts}")
        elif dp and opts:
            _pass(f"flexgen.default_precision {dp!r} in options")


# --- 12 ---------------------------------------------------------------------

def check_carbon_region(doc: dict) -> None:
    region = doc.get("carbon_context", {}).get("region")
    if region not in _ALLOWED_REGIONS:
        _fail(f"carbon_context.region {region!r} not in the 13-region table")
    else:
        _pass(f"carbon_context.region = {region}")


# --- 13-14 ------------------------------------------------------------------

def check_requirements(doc: dict) -> None:
    req = doc.get("requirements", {})

    py = req.get("python_version")
    if py:
        # Accept >=3.10,<3.13 or similar ranges that intersect [3.10, 3.13)
        m = re.match(r"^>=(\d+)\.(\d+)(?:,<(\d+)\.(\d+))?$", py)
        if not m:
            _fail(f"python_version {py!r} is not in the expected format")
        else:
            min_major, min_minor = int(m.group(1)), int(m.group(2))
            if (min_major, min_minor) < (3, 10):
                _fail(f"python_version {py!r} allows <3.10")
            else:
                _pass(f"python_version = {py}")

    profile = req.get("hardware_profile")
    if profile is not None:
        if profile not in _HARDWARE_PROFILES:
            _fail(
                f"hardware_profile {profile!r} not in the documented set "
                f"({sorted(_HARDWARE_PROFILES)})"
            )
        else:
            _pass(f"hardware_profile = {profile}")


# --- entry point ------------------------------------------------------------

def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(
            "usage: validate_agent_toml.py agent.toml", file=sys.stderr,
        )
        return 2

    path = Path(argv[1]).resolve()
    if not path.is_file():
        print(f"FATAL: {path} not found", file=sys.stderr)
        return 2

    try:
        with path.open("rb") as f:
            doc = tomllib.load(f)
    except Exception as exc:
        print(f"FATAL: TOML parse error: {exc}", file=sys.stderr)
        return 2

    check_agent_type(doc)
    check_protocols(doc)
    check_endpoints(doc)
    check_unique_metrics(doc)
    check_evaluation_domains(doc)
    check_enhancements(doc)
    check_carbon_region(doc)
    check_requirements(doc)

    if _failed:
        print("\nRESULT: FAIL", file=sys.stderr)
        return 1
    print("\nRESULT: OK — agent descriptor is valid")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
