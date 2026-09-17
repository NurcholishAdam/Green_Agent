# src/scripts/validate_agentbeats.py

"""
AgentBeats Configuration Validator
==================================

Standalone CLI that validates an ``agentbeats.json`` configuration file
against the AgentBeats schema and verifies that the referenced Docker image
is reachable.

Schema expectations
-------------------
- ``queries``         : list of query objects.
- ``queries[].command``: list of strings (the argv for the query).
- ``queries[].environment``: optional dict of env vars.
- ``image``           : Docker image reference (``repo/name:tag``).

The Docker image is verified via ``docker manifest inspect <image>``.

Enhancements
------------
- ``if __name__ == "__main__"`` guard — the module is importable without
  side effects.
- ``main(argv=None) -> int`` entry point for testability and CLI reuse.
- ``argparse`` CLI with ``--config``, ``--image``, ``--skip-docker``,
  ``--timeout``, ``--quiet``, ``--verbose``.
- ``AgentBeatsValidatorConfig`` — frozen, validated.
- ``ValidationResult`` — frozen, serializable summary of a validation run.
- **Replaced bare ``assert``** — explicit :class:`AgentBeatsValidationError`
  is raised so checks survive ``python -O``.
- **Structured logging** — verbose / quiet control via ``logging``.
- **Subprocess timeout** — ``docker manifest inspect`` is bounded.
- Full error handling for ``FileNotFoundError`` / ``json.JSONDecodeError`` /
  ``subprocess.CalledProcessError`` / ``subprocess.TimeoutExpired``.
- ``--quiet`` suppresses the final success message (exit code is the signal).
- ``--json`` emits a machine-readable result on stdout.
- ``__all__`` and a ``__main__`` smoke test.
"""

from __future__ import annotations

import argparse
import json
import logging
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
        if self.docker_timeout_seconds <= 0:
            raise AgentBeatsValidationError(
                "docker_timeout_seconds must be > 0."
            )
        if not isinstance(self.required_top_level_keys, tuple):
            raise AgentBeatsValidationError(
                "required_top_level_keys must be a tuple."
            )
        if not isinstance(self.required_query_keys, tuple):
            raise AgentBeatsValidationError(
                "required_query_keys must be a tuple."
            )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["config_path"] = str(self.config_path)
        return d

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AgentBeatsValidatorConfig":
        if not isinstance(data, Mapping):
            raise AgentBeatsValidationError(
                "AgentBeatsValidatorConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
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
        return not self.errors and (self.skip_docker_ok())

    def skip_docker_ok(self) -> bool:
        """Docker is OK when it was either not checked or verified."""
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

    def __repr__(self) -> str:
        return (
            "ValidationResult("
            f"config={self.config_path!r}, "
            f"image={self.image!r}, "
            f"queries={self.num_queries}, "
            f"docker_checked={self.docker_checked}, "
            f"errors={len(self.errors)})"
        )


# --------------------------------------------------------------------------- #
# Validator
# --------------------------------------------------------------------------- #
class AgentBeatsValidator:
    """
    Validates an AgentBeats configuration file.

    See :class:`AgentBeatsValidatorConfig` for tunable parameters.
    """

    def __init__(
        self,
        config: Optional[AgentBeatsValidatorConfig] = None,
        *,
        strict: bool = True,
    ) -> None:
        self._config = config or AgentBeatsValidatorConfig()
        self._strict = bool(strict)

    @property
    def config(self) -> AgentBeatsValidatorConfig:
        return self._config

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
        if not cfg.config_path.exists():
            errors.append(f"config file not found: {cfg.config_path}")
            return self._finish(errors, warnings, image, num_queries, False, False)

        try:
            with cfg.config_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            errors.append(f"invalid JSON in {cfg.config_path}: {exc}")
            return self._finish(errors, warnings, image, num_queries, False, False)
        except OSError as exc:
            errors.append(f"could not read {cfg.config_path}: {exc}")
            return self._finish(errors, warnings, image, num_queries, False, False)

        if not isinstance(data, Mapping):
            errors.append(
                f"root of {cfg.config_path} must be a JSON object, "
                f"got {type(data).__name__}"
            )
            return self._finish(errors, warnings, image, num_queries, False, False)

        # ---- 2. Required top-level keys ------------------------------
        for key in cfg.required_top_level_keys:
            if key not in data:
                errors.append(f"missing required top-level key: {key!r}")

        # ---- 3. queries ---------------------------------------------
        queries = data.get("queries")
        if not isinstance(queries, list):
            errors.append("'queries' must be a list")
        else:
            num_queries = len(queries)
            for i, q in enumerate(queries):
                if not isinstance(q, Mapping):
                    errors.append(
                        f"queries[{i}] must be an object, "
                        f"got {type(q).__name__}"
                    )
                    continue
                for rk in cfg.required_query_keys:
                    if rk not in q:
                        errors.append(
                            f"queries[{i}] missing required key: {rk!r}"
                        )
                cmd = q.get("command")
                if cmd is not None and not isinstance(cmd, list):
                    errors.append(
                        f"queries[{i}].command must be a list, "
                        f"got {type(cmd).__name__}"
                    )
                env = q.get("environment")
                if env is not None and not isinstance(env, Mapping):
                    errors.append(
                        f"queries[{i}].environment must be an object, "
                        f"got {type(env).__name__}"
                    )

        # ---- 4. Docker image ----------------------------------------
        image = data.get("image")
        if not isinstance(image, str) or not image:
            errors.append("'image' must be a non-empty string")
            image = None

        docker_checked = False
        docker_ok = False
        if image is not None and not cfg.skip_docker:
            docker_checked = True
            docker_ok, docker_err = self._check_docker_image(image)
            if not docker_ok and docker_err:
                errors.append(docker_err)

        return self._finish(
            errors, warnings, image, num_queries,
            docker_checked, docker_ok,
        )

    # ---------------------------------------------------------- helpers
    def _check_docker_image(
        self, image: str,
    ) -> Tuple[bool, Optional[str]]:
        """
        Run ``docker manifest inspect <image>`` and return ``(ok, error)``.

        The error string is ``None`` when the check succeeded; otherwise it
        carries a human-readable reason.
        """
        cfg = self._config

        # Docker binary availability check.
        if shutil.which(cfg.docker_binary) is None:
            return False, (
                f"'{cfg.docker_binary}' not found on PATH; cannot verify "
                f"image {image!r}"
            )

        argv = [cfg.docker_binary, "manifest", "inspect", image]
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
                f"'{cfg.docker_binary} manifest inspect {image}' timed out "
                f"after {cfg.docker_timeout_seconds:.1f}s"
            )
        except FileNotFoundError as exc:
            return False, f"could not execute docker: {exc}"
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


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
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
        help="Skip the Docker image check.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Docker manifest timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress the success message (exit code is the signal).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON ValidationResult instead of human-readable output.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entry point.

    Returns
    -------
    int
        Process exit code: 0 on success, 1 on validation failure,
        2 on argument error.
    """
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        cfg = AgentBeatsValidatorConfig(
            config_path=Path(args.config),
            docker_timeout_seconds=args.timeout,
            skip_docker=args.skip_docker,
            quiet=args.quiet,
        )
    except AgentBeatsValidationError as exc:
        parser.error(str(exc))
        return 2  # pragma: no cover — argparse.error() raises SystemExit

    validator = AgentBeatsValidator(cfg)
    result = validator.validate()

    if args.json:
        print(json.dumps(result.to_dict(), indent=2, default=str))
    elif not args.quiet:
        if result.ok:
            print("✅ AgentBeats validation passed")
        else:
            print("❌ AgentBeats validation failed:")
            for err in result.errors:
                print(f"  - {err}")

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


# --------------------------------------------------------------------------- #
# Smoke test: python -m scripts.validate_agentbeats
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
