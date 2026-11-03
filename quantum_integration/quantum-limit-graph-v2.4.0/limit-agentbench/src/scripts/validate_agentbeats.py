# src/scripts/validate_agentbeats.py

"""
AgentBeats Configuration Validator
==================================

Standalone CLI that validates an ``agentbeats.json`` configuration file
against the AgentBeats schema and verifies that the referenced Docker image
is reachable.

Schema expectations
-------------------
- ``queries``              : non-empty list of query objects.
- ``queries[].command``    : non-empty list of non-empty strings (argv).
- ``queries[].environment``: optional dict of string-to-string env vars.
- ``image``                : Docker image reference (``repo/name:tag`` or
  ``repo/name@sha256:<64 hex>``).

The Docker image is verified via ``docker manifest inspect -- <image>``.

Usage
-----
::

    python -m scripts.validate_agentbeats agentbeats.json
    python -m scripts.validate_agentbeats agentbeats.json --skip-docker
    python -m scripts.validate_agentbeats agentbeats.json --json
    python -m scripts.validate_agentbeats agentbeats.json --quiet
    python -m scripts.validate_agentbeats agentbeats.json --log-level DEBUG

Exit codes
----------
- ``0`` — validation passed.
- ``1`` — validation failed (schema or Docker check).
- ``2`` — argument parsing error (raised by ``argparse`` via
  ``SystemExit``).

Enhancements
------------
- ``if __name__ == "__main__"`` guard — the module is importable without
  side effects.
- ``main(argv=None) -> int`` entry point — **returns an int in every
  case** (catches ``SystemExit`` from argparse), so test harnesses can
  call it programmatically.
- ``argparse`` CLI with positional ``config``, ``--skip-docker``,
  ``--timeout``, ``--quiet``, ``--json``, ``--log-level``, and
  ``--verbose`` (legacy alias for ``--log-level DEBUG``).
- ``AgentBeatsValidatorConfig`` — frozen, fully validated (contents of
  every tuple checked, not just its type); ``from_dict(..., strict=True)``
  rejects unknown keys.
- ``ValidationResult`` — frozen, ``to_dict`` / ``from_dict`` (symmetric);
  ``warnings`` field **is populated** for non-fatal issues.
- **Schema fixes**:
  - ``{"command": null}`` no longer passes validation.
  - ``command`` items must be strings.
  - Empty ``queries`` / empty ``command`` produce warnings.
- **Docker hardening**:
  - Image string validated against a strict regex before invocation.
  - ``docker manifest inspect -- <image>`` prevents option injection.
  - ``FileNotFoundError`` and ``OSError`` branches merged (the former is
    a subclass of the latter).
  - Absolute-path ``docker_binary`` gets a distinct error message.
- **File I/O hardening** — open-and-catch instead of ``Path.exists()``
  so broken symlinks, directories, and permission failures produce
  accurate error messages.
- **Structured ``strict`` mode** — ``strict=False`` downgrades
  unknown-key findings from errors to warnings.
- **``--quiet`` prints errors to stderr** so CI logs are not silently
  swallowed; the success message is still suppressed.
- **``--json`` output** no longer uses ``default=str`` — the payload is
  already primitive-only, so future non-primitive additions fail loudly.
- ``GREEN_AGENT_SKIP_DOCKER=1`` env override for environments without
  a Docker daemon.
- ``__repr__`` on the validator; ``__slots__``-free for 3.8
  compatibility.
- Extended ``__main__`` smoke test covering ``--json``, ``--skip-docker``,
  the ``--command:null`` fix, and programmatic ``main([...])`` return
  values.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AgentBeatsValidationError(ValueError):
    """Raised when an AgentBeats configuration fails validation."""


# --------------------------------------------------------------------------- #
# Docker image reference regex
# --------------------------------------------------------------------------- #
# Loose but strict enough to reject option-injection strings and obvious
# typos. Matches:
#   - repo/name
#   - repo/name:tag
#   - registry:5000/repo/name:tag
#   - repo/name@sha256:<64 hex>
_IMAGE_REFERENCE_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._/:@-]*"
    r"(?::[A-Za-z0-9._-]+)?"
    r"(?:@sha256:[0-9a-f]{64})?$"
)


def _is_valid_image_reference(image: str) -> bool:
    """Return True if ``image`` looks like a Docker image reference."""
    if not image or not isinstance(image, str):
        return False
    # Reject leading flag-like tokens (`-x`, `--help`, etc.).
    if image.startswith("-"):
        return False
    return bool(_IMAGE_REFERENCE_RE.match(image))


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AgentBeatsValidatorConfig:
    """Tunable parameters for :class:`AgentBeatsValidator`."""

    config_path: Path = Path("agentbeats.json")
    docker_binary: str = "docker"
    docker_timeout_seconds: float = 30.0
    skip_docker: bool = False
    quiet: bool = False

    # Schema expectations.
    required_top_level_keys: Tuple[str, ...] = ("queries", "image")
    required_query_keys: Tuple[str, ...] = ("command",)

    def __post_init__(self) -> None:
        if not isinstance(self.config_path, Path):
            raise AgentBeatsValidationError(
                "config_path must be a pathlib.Path."
            )
        if not isinstance(self.docker_binary, str) or not self.docker_binary:
            raise AgentBeatsValidationError(
                "docker_binary must be a non-empty string."
            )
        if isinstance(self.docker_timeout_seconds, bool) or not isinstance(
            self.docker_timeout_seconds, (int, float)
        ):
            raise AgentBeatsValidationError(
                "docker_timeout_seconds must be numeric."
            )
        import math
        if not math.isfinite(float(self.docker_timeout_seconds)):
            raise AgentBeatsValidationError(
                "docker_timeout_seconds must be finite."
            )
        if self.docker_timeout_seconds <= 0:
            raise AgentBeatsValidationError(
                "docker_timeout_seconds must be > 0."
            )
        if not isinstance(self.required_top_level_keys, tuple):
            raise AgentBeatsValidationError(
                "required_top_level_keys must be a tuple."
            )
        for k in self.required_top_level_keys:
            if not isinstance(k, str) or not k:
                raise AgentBeatsValidationError(
                    "required_top_level_keys entries must be non-empty "
                    "strings."
                )
        if not isinstance(self.required_query_keys, tuple):
            raise AgentBeatsValidationError(
                "required_query_keys must be a tuple."
            )
        for k in self.required_query_keys:
            if not isinstance(k, str) or not k:
                raise AgentBeatsValidationError(
                    "required_query_keys entries must be non-empty "
                    "strings."
                )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["config_path"] = str(self.config_path)
        return d

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "AgentBeatsValidatorConfig":
        if not isinstance(data, Mapping):
            raise AgentBeatsValidationError(
                "AgentBeatsValidatorConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise AgentBeatsValidationError(
                    f"unknown AgentBeatsValidatorConfig keys: {unknown}"
                )
        try:
            kwargs: Dict[str, Any] = {}
            for k, v in data.items():
                if k not in valid:
                    continue
                if k == "config_path":
                    kwargs[k] = Path(str(v))
                elif k in (
                    "required_top_level_keys", "required_query_keys",
                ):
                    kwargs[k] = tuple(v)
                else:
                    kwargs[k] = v
            return cls(**kwargs)
        except AgentBeatsValidationError:
            raise
        except (TypeError, ValueError) as exc:
            raise AgentBeatsValidationError(
                f"invalid AgentBeatsValidatorConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "AgentBeatsValidatorConfig("
            f"config={str(self.config_path)!r}, "
            f"docker={self.docker_binary!r}, "
            f"timeout={self.docker_timeout_seconds:.1f}s, "
            f"skip_docker={self.skip_docker}, "
            f"quiet={self.quiet})"
        )


# --------------------------------------------------------------------------- #
# Result record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ValidationResult:
    """Immutable summary of a validation run."""

    config_path: str
    image: Optional[str]
    num_queries: int
    docker_checked: bool
    docker_ok: bool
    errors: Tuple[str, ...] = ()
    warnings: Tuple[str, ...] = ()
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @property
    def ok(self) -> bool:
        """True iff there are no errors and (Docker was skipped or passed)."""
        return not self.errors and self._docker_ok_or_skipped()

    def _docker_ok_or_skipped(self) -> bool:
        return (not self.docker_checked) or self.docker_ok

    def to_dict(self) -> Dict[str, Any]:
        return {
            "config_path": self.config_path,
            "image": self.image,
            "num_queries": self.num_queries,
            "docker_checked": self.docker_checked,
            "docker_ok": self.docker_ok,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "ok": self.ok,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ValidationResult":
        if not isinstance(data, Mapping):
            raise AgentBeatsValidationError(
                "ValidationResult.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if ts is None:
                timestamp = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                timestamp = datetime.fromisoformat(ts)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                raise AgentBeatsValidationError(
                    f"timestamp must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                config_path=str(data.get("config_path", "")),
                image=(
                    None if data.get("image") is None
                    else str(data.get("image"))
                ),
                num_queries=int(data.get("num_queries", 0)),
                docker_checked=bool(data.get("docker_checked", False)),
                docker_ok=bool(data.get("docker_ok", False)),
                errors=tuple(str(e) for e in (data.get("errors") or [])),
                warnings=tuple(
                    str(w) for w in (data.get("warnings") or [])
                ),
                timestamp=timestamp,
            )
        except AgentBeatsValidationError:
            raise
        except (TypeError, ValueError) as exc:
            raise AgentBeatsValidationError(
                f"invalid ValidationResult payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "ValidationResult("
            f"config={self.config_path!r}, "
            f"image={self.image!r}, "
            f"queries={self.num_queries}, "
            f"docker_checked={self.docker_checked}, "
            f"errors={len(self.errors)}, "
            f"warnings={len(self.warnings)})"
        )


# --------------------------------------------------------------------------- #
# Validator
# --------------------------------------------------------------------------- #
class AgentBeatsValidator:
    """
    Validates an AgentBeats configuration file.

    See :class:`AgentBeatsValidatorConfig` for tunable parameters.

    ``strict`` mode
    ---------------
    - ``strict=True`` (default) — unknown top-level keys are reported as
      **errors**.
    - ``strict=False`` — unknown top-level keys are reported as
      **warnings**; validation still fails if any real schema errors are
      found.
    """

    def __init__(
        self,
        config: Optional[AgentBeatsValidatorConfig] = None,
        *,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(
            config, AgentBeatsValidatorConfig
        ):
            raise AgentBeatsValidationError(
                f"config must be an AgentBeatsValidatorConfig, got "
                f"{type(config).__name__}."
            )
        self._config = config or AgentBeatsValidatorConfig()
        self._strict = bool(strict)

    @property
    def config(self) -> AgentBeatsValidatorConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    # ---------------------------------------------------------- public API
    def validate(self) -> ValidationResult:
        """
        Run the full validation pipeline.

        Returns
        -------
        ValidationResult
            Structured summary; the ``ok`` property is the pass/fail signal.
        """
        errors: List[str] = []
        warnings: List[str] = []
        image: Optional[str] = None
        num_queries = 0

        cfg = self._config

        # ---- 1. Load the config file ---------------------------------
        try:
            with cfg.config_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            errors.append(f"config file not found: {cfg.config_path}")
            return self._finish(
                errors, warnings, image, num_queries, False, False
            )
        except IsADirectoryError:
            errors.append(
                f"config path is a directory, not a file: {cfg.config_path}"
            )
            return self._finish(
                errors, warnings, image, num_queries, False, False
            )
        except PermissionError as exc:
            errors.append(
                f"cannot read {cfg.config_path}: {exc}"
            )
            return self._finish(
                errors, warnings, image, num_queries, False, False
            )
        except json.JSONDecodeError as exc:
            errors.append(
                f"invalid JSON in {cfg.config_path}: {exc}"
            )
            return self._finish(
                errors, warnings, image, num_queries, False, False
            )
        except OSError as exc:
            errors.append(
                f"could not read {cfg.config_path}: {exc}"
            )
            return self._finish(
                errors, warnings, image, num_queries, False, False
            )

        if not isinstance(data, Mapping):
            errors.append(
                f"root of {cfg.config_path} must be a JSON object, "
                f"got {type(data).__name__}"
            )
            return self._finish(
                errors, warnings, image, num_queries, False, False
            )

        # ---- 2. Top-level keys ---------------------------------------
        for key in cfg.required_top_level_keys:
            if key not in data:
                errors.append(f"missing required top-level key: {key!r}")

        # Unknown top-level keys → error (strict) or warning (non-strict).
        allowed_top = set(cfg.required_top_level_keys) | set(data.keys())
        # Compute the union of "known" keys: required + everything the
        # schema is currently able to interpret. Today that's just
        # required_top_level_keys; add more here if the schema grows.
        known_top = set(cfg.required_top_level_keys)
        unknown_top = [k for k in data if k not in known_top]
        for key in unknown_top:
            msg = f"unexpected top-level key: {key!r}"
            if self._strict:
                errors.append(msg)
            else:
                warnings.append(msg)

        # ---- 3. queries ---------------------------------------------
        queries = data.get("queries")
        if not isinstance(queries, list):
            errors.append("'queries' must be a list")
        else:
            num_queries = len(queries)
            if num_queries == 0:
                warnings.append(
                    "'queries' is empty; the agent has no work to perform"
                )
            for i, q in enumerate(queries):
                if not isinstance(q, Mapping):
                    errors.append(
                        f"queries[{i}] must be an object, "
                        f"got {type(q).__name__}"
                    )
                    continue
                self._validate_query(i, q, errors, warnings)

        # ---- 4. Docker image ----------------------------------------
        image = data.get("image")
        if not isinstance(image, str) or not image:
            errors.append("'image' must be a non-empty string")
            image = None
        elif not _is_valid_image_reference(image):
            errors.append(
                f"'image' is not a valid Docker image reference: {image!r}"
            )
            image = None

        docker_checked = False
        docker_ok = False
        if image is not None:
            if cfg.skip_docker:
                warnings.append(
                    f"Docker check skipped; image {image!r} not verified"
                )
            else:
                docker_checked = True
                docker_ok, docker_err = self._check_docker_image(image)
                if not docker_ok and docker_err:
                    errors.append(docker_err)

        return self._finish(
            errors, warnings, image, num_queries,
            docker_checked, docker_ok,
        )

    # ---------------------------------------------------------- helpers
    def _validate_query(
        self,
        index: int,
        query: Mapping[str, Any],
        errors: List[str],
        warnings: List[str],
    ) -> None:
        """Validate one query object in place."""
        cfg = self._config

        for rk in cfg.required_query_keys:
            if rk not in query:
                errors.append(
                    f"queries[{index}] missing required key: {rk!r}"
                )
                continue
            value = query[rk]
            if not isinstance(value, list):
                errors.append(
                    f"queries[{index}].{rk} must be a list, "
                    f"got {type(value).__name__}"
                )
                continue
            if not value:
                warnings.append(
                    f"queries[{index}].{rk} is an empty list"
                )
                continue
            for j, arg in enumerate(value):
                if not isinstance(arg, str):
                    errors.append(
                        f"queries[{index}].{rk}[{j}] must be a string, "
                        f"got {type(arg).__name__}"
                    )

        env = query.get("environment")
        if env is not None:
            if not isinstance(env, Mapping):
                errors.append(
                    f"queries[{index}].environment must be an object, "
                    f"got {type(env).__name__}"
                )
            else:
                for k, v in env.items():
                    if not isinstance(k, str):
                        errors.append(
                            f"queries[{index}].environment keys must be "
                            f"strings, got {type(k).__name__}"
                        )
                    if not isinstance(v, str):
                        errors.append(
                            f"queries[{index}].environment[{k!r}] must "
                            f"be a string, got {type(v).__name__}"
                        )

    def _check_docker_image(
        self, image: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Run ``docker manifest inspect -- <image>`` and return ``(ok, err)``.

        ``err`` is ``None`` on success; otherwise a human-readable reason.
        """
        cfg = self._config

        # ---- Verify the docker binary ------------------------------ #
        docker_arg = cfg.docker_binary
        looks_like_path = os.sep in docker_arg or (
            os.altsep is not None and os.altsep in docker_arg
        )
        if looks_like_path or Path(docker_arg).is_absolute():
            p = Path(docker_arg)
            if not p.is_file():
                return False, (
                    f"docker binary {docker_arg!r} is not a file"
                )
            if not os.access(str(p), os.X_OK):
                return False, (
                    f"docker binary {docker_arg!r} is not executable"
                )
        else:
            if shutil.which(docker_arg) is None:
                return False, (
                    f"'{docker_arg}' not found on PATH; "
                    f"cannot verify image {image!r}"
                )

        # ---- Invoke docker with `--` to prevent option injection --- #
        argv = [docker_arg, "manifest", "inspect", "--", image]
        try:
            proc = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=cfg.docker_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return False, (
                f"'{docker_arg} manifest inspect {image}' timed out "
                f"after {cfg.docker_timeout_seconds:.1f}s"
            )
        except OSError as exc:
            return False, f"could not execute docker: {exc}"

        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip().splitlines()
            detail = stderr[0] if stderr else f"exit code {proc.returncode}"
            return False, (
                f"docker manifest inspect failed for {image!r}: {detail}"
            )
        return True, None

    def _finish(
        self,
        errors: List[str],
        warnings: List[str],
        image: Optional[str],
        num_queries: int,
        docker_checked: bool,
        docker_ok: bool,
    ) -> ValidationResult:
        return ValidationResult(
            config_path=str(self._config.config_path),
            image=image,
            num_queries=num_queries,
            docker_checked=docker_checked,
            docker_ok=docker_ok,
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "AgentBeatsValidator("
            f"config={str(self._config.config_path)!r}, "
            f"skip_docker={self._config.skip_docker}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _positive_float(raw: str) -> float:
    """argparse ``type`` callable that requires a finite positive float."""
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(
            f"invalid float: {raw!r}"
        ) from None
    import math
    if not math.isfinite(value):
        raise argparse.ArgumentTypeError(
            f"must be finite, got {value!r}"
        )
    if value <= 0:
        raise argparse.ArgumentTypeError(
            f"must be > 0, got {value!r}"
        )
    return value


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="validate_agentbeats",
        description=(
            "Validate an AgentBeats configuration file (agentbeats.json). "
            "Checks the schema and verifies the Docker image is reachable."
        ),
    )
    parser.add_argument(
        "config",
        nargs="?",
        default="agentbeats.json",
        help="Path to the AgentBeats config file (default: agentbeats.json).",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help=(
            "Skip the Docker image check. May also be enabled via "
            "GREEN_AGENT_SKIP_DOCKER=1."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=_positive_float,
        default=30.0,
        help="Docker manifest timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help=(
            "Suppress the success message. Errors are still printed to "
            "stderr; the exit code is the pass/fail signal."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON ValidationResult instead of human-readable output.",
    )
    level_group = parser.add_mutually_exclusive_group()
    level_group.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="Logging level (default: INFO).",
    )
    level_group.add_argument(
        "--verbose",
        action="store_true",
        help="Legacy alias for --log-level DEBUG.",
    )
    return parser


def _emit_human_result(result: ValidationResult, *, quiet: bool) -> None:
    """
    Emit human-readable result to the appropriate streams.

    - Success: stdout, only if ``quiet`` is False.
    - Failure: stderr, always (even in quiet mode).
    - Warnings: stderr, always.
    """
    if result.ok:
        if not quiet:
            print("✅ AgentBeats validation passed")
            for w in result.warnings:
                print(f"  ⚠ {w}", file=sys.stderr)
        return
    print("❌ AgentBeats validation failed:", file=sys.stderr)
    for err in result.errors:
        print(f"  - {err}", file=sys.stderr)
    for w in result.warnings:
        print(f"  ⚠ {w}", file=sys.stderr)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entry point.

    Returns
    -------
    int
        ``0`` on success, ``1`` on validation failure, ``2`` on argument
        error (returned, not raised — ``SystemExit`` from argparse is
        caught and translated).
    """
    parser = _build_arg_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        # argparse uses SystemExit(2) for argument errors and
        # SystemExit(0) for --help. Preserve the exit code.
        return int(exc.code if exc.code is not None else 0)

    log_level_name = "DEBUG" if args.verbose else args.log_level
    logging.basicConfig(
        level=getattr(logging, log_level_name, logging.INFO),
        format="%(levelname)s %(name)s: %(message)s",
    )

    # Environment override for --skip-docker.
    env_skip = os.environ.get("GREEN_AGENT_SKIP_DOCKER") == "1"
    skip_docker = bool(args.skip_docker or env_skip)

    try:
        cfg = AgentBeatsValidatorConfig(
            config_path=Path(args.config),
            docker_timeout_seconds=args.timeout,
            skip_docker=skip_docker,
            quiet=args.quiet,
        )
    except AgentBeatsValidationError as exc:
        # Now reachable — argparse accepted the values but __post_init__
        # rejected them. Report as a CLI usage error and return 2.
        print(f"error: {exc}", file=sys.stderr)
        return 2

    validator = AgentBeatsValidator(cfg)
    result = validator.validate()

    if args.json:
        # Payload is primitive-only; no default= needed.
        print(json.dumps(result.to_dict(), indent=2))
    else:
        _emit_human_result(result, quiet=args.quiet)

    return 0 if result.ok else 1


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AgentBeatsValidationError",
    "AgentBeatsValidator",
    "AgentBeatsValidatorConfig",
    "ValidationResult",
    "main",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m scripts.validate_agentbeats
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    import tempfile

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    with tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)

        # ---- 1. Missing config ---------------------------------- #
        print("missing config:")
        r = AgentBeatsValidator(
            AgentBeatsValidatorConfig(
                config_path=tmpdir / "nope.json", skip_docker=True
            )
        ).validate()
        _check("ok=False", not r.ok)
        _check("error mentions not found",
               any("not found" in e for e in r.errors))

        # ---- 2. Invalid JSON ------------------------------------ #
        print("invalid json:")
        bad_json = tmpdir / "bad.json"
        bad_json.write_text("{not json")
        r = AgentBeatsValidator(
            AgentBeatsValidatorConfig(
                config_path=bad_json, skip_docker=True
            )
        ).validate()
        _check("ok=False", not r.ok)
        _check("error mentions JSON",
               any("JSON" in e for e in r.errors))

        # ---- 3. command: null passes? --------------------------- #
        print("command: null:")
        null_json = tmpdir / "null.json"
        null_json.write_text(
            '{"queries": [{"command": null}], "image": "alpine:3"}'
        )
        r = AgentBeatsValidator(
            AgentBeatsValidatorConfig(
                config_path=null_json, skip_docker=True
            )
        ).validate()
        _check("ok=False", not r.ok)
        _check("error mentions command type",
               any("command must be a list" in e for e in r.errors))

        # ---- 4. command with non-string items ------------------ #
        print("command with ints:")
        ints_json = tmpdir / "ints.json"
        ints_json.write_text(
            '{"queries": [{"command": ["ok", 42]}], "image": "alpine:3"}'
        )
        r = AgentBeatsValidator(
            AgentBeatsValidatorConfig(
                config_path=ints_json, skip_docker=True
            )
        ).validate()
        _check("ok=False", not r.ok)
        _check("error mentions command[1]",
               any("command[1]" in e for e in r.errors))

        # ---- 5. valid config ----------------------------------- #
        print("valid config:")
        valid_json = tmpdir / "valid.json"
        valid_json.write_text(
            '{"queries": [{"command": ["echo", "hi"]}], '
            '"image": "alpine:3.19"}'
        )
        r = AgentBeatsValidator(
            AgentBeatsValidatorConfig(
                config_path=valid_json, skip_docker=True
            )
        ).validate()
        _check("ok=True", r.ok)
        _check("num_queries=1", r.num_queries == 1)
        _check("warning about skipped docker",
               any("skipped" in w.lower() for w in r.warnings))

        # ---- 6. invalid image reference ----------------------- #
        print("invalid image ref:")
        bad_img = tmpdir / "bad_img.json"
        bad_img.write_text(
            '{"queries": [{"command": ["echo"]}], "image": "--help"}'
        )
        r = AgentBeatsValidator(
            AgentBeatsValidatorConfig(
                config_path=bad_img, skip_docker=True
            )
        ).validate()
        _check("ok=False", not r.ok)
        _check("error mentions image",
               any("image" in e.lower() for e in r.errors))

        # ---- 7. strict vs non-strict unknown key --------------- #
        print("unknown key (strict):")
        unknown_json = tmpdir / "unknown.json"
        unknown_json.write_text(
            '{"queries": [{"command": ["echo"]}], '
            '"image": "alpine:3", "extra_field": true}'
        )
        strict_r = AgentBeatsValidator(
            AgentBeatsValidatorConfig(
                config_path=unknown_json, skip_docker=True
            ),
            strict=True,
        ).validate()
        _check("strict: ok=False", not strict_r.ok)
        _check("strict: error mentions extra_field",
               any("extra_field" in e for e in strict_r.errors))

        lenient_r = AgentBeatsValidator(
            AgentBeatsValidatorConfig(
                config_path=unknown_json, skip_docker=True
            ),
            strict=False,
        ).validate()
        _check("lenient: ok=True", lenient_r.ok)
        _check("lenient: warning mentions extra_field",
               any("extra_field" in w for w in lenient_r.warnings))

        # ---- 8. Round-trip ValidationResult -------------------- #
        print("ValidationResult round-trip:")
        rt = ValidationResult.from_dict(r.to_dict())
        _check("round-trip preserves ok", rt.ok == r.ok)
        _check("round-trip preserves num_queries",
               rt.num_queries == r.num_queries)

        # ---- 9. main() returns int ----------------------------- #
        print("main() return type:")
        rc = main([str(valid_json), "--skip-docker", "--quiet"])
        _check("valid returns 0", rc == 0)

        rc = main([str(null_json), "--skip-docker", "--quiet"])
        _check("invalid returns 1", rc == 1)

        # argparse errors are now caught and returned as int, not raised.
        rc = main(["--not-a-flag"])
        _check("bad flag returns 2", rc == 2)

        # ---- 10. --json output is valid JSON ------------------ #
        print("--json output:")
        import io
        import contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = main([str(valid_json), "--skip-docker", "--json"])
        _check("--json returns 0", rc == 0)
        try:
            payload = json.loads(buf.getvalue())
            _check("payload is dict", isinstance(payload, dict))
            _check("payload has ok", "ok" in payload)
            _check("payload ok=True", payload["ok"] is True)
        except json.JSONDecodeError:
            _check("payload is valid JSON", False)

        # ---- 11. --timeout validation ------------------------ #
        print("--timeout validation:")
        rc = main([str(valid_json), "--skip-docker", "--timeout", "-5"])
        _check("negative timeout returns 2", rc == 2)
        rc = main([str(valid_json), "--skip-docker", "--timeout", "abc"])
        _check("non-numeric timeout returns 2", rc == 2)

        # ---- 12. __repr__ / __dir__ -------------------------- #
        print("repr/dir:")
        v = AgentBeatsValidator()
        _check("repr non-empty", bool(repr(v)))
        _check("dir sorted", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
