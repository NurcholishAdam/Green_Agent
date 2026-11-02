# src/r1/reporting/__init__.py

"""
Reporting package for Green Agent
=================================

Auditable presentation and disclosure layer. Consumes validated outputs
from analysis, policy, verification, and runtime components; produces
stakeholder-specific reports, dashboards, benchmark submissions, and
machine-readable evidence.

Pipeline
--------
::

    EvidenceBundle  (layered_reporter.EvidenceBundle)
        │
        ├── ReportGenerator.render_bundle(...)   → JSON / Markdown / HTML / CSV
        ├── generate_leaderboard(...)            → LeaderboardResult
        └── build_agentbeats_submission(...)     → JSON file
                                                    └── verify_submission(path)

Audiences
---------
``Audience.OPERATOR`` / ``TECHNICAL`` / ``GOVERNANCE`` / ``EXECUTIVE``.
Redaction is applied before every render (see
:meth:`LayeredReporter.redact`). The batch path
(:meth:`ReportGenerator.render_many`) redacts every bundle before
serialising to any format.

Errors
------
All package-level error classes are re-exported here so callers can
write ``except X`` without reaching into submodules:

- :class:`LayeredReporterError` — bundle construction, hashing, freezing
- :class:`ImmutabilityError` — bundle mutated after freeze
- :class:`AgentBeatsError` — submission build / verify failures
- :class:`ReportingPackageError` — aggregation-level failures (missing
  submodule version, incompatible major version, atomic write failure,
  malformed payload)

Utilities
---------
- :func:`load_bundle` — JSON path or dict → :class:`EvidenceBundle`
- :func:`render_to_file` — render + atomic write, returns :class:`Path`
- :func:`render_all_audiences` — ``dict[Audience, str]`` for a bundle
- :func:`provenance` — single SHA-256 over the audit chain
- :func:`validate_reporting` — CI helper: submodule versions

Version
-------
:data:`VERSION` (PEP 440 string); mirrored as ``__version__``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Union

__all__ = [
    # --- Version ---
    "VERSION",
    "__version__",
    # --- Errors ---
    "ReportingPackageError",
    "LayeredReporterError",
    "ImmutabilityError",
    "AgentBeatsError",
    # --- Original exports ---
    "Layer1RawMetrics",
    "Layer2NormalizedMetrics",
    "Layer3ScenarioScore",
    "LayeredReporter",
    "ReportGenerator",
    "generate_leaderboard",
    "build_agentbeats_submission",
    # --- Enhanced contracts ---
    "EvidenceBundle",
    "BundleBuilder",
    "CarbonInstruments",
    "MetricProvenance",
    "ExplanationCard",
    "VerificationEvidence",
    # --- Renderers + format constants ---
    "JSONRenderer",
    "MarkdownRenderer",
    "HTMLRenderer",
    "CSVRenderer",
    "REPORT_FORMATS",
    # --- Leaderboard ---
    "Leaderboard",
    "LeaderboardEntry",
    "LeaderboardResult",
    "RankStatus",
    # --- Benchmarking ---
    "ReproducibilityBundle",
    "verify_submission",
    "verify_submission_detailed",
    # --- Enums ---
    "Audience",
    "ProvenanceKind",
    "TrustLevel",
    # --- Package helpers ---
    "load_bundle",
    "render_to_file",
    "render_all_audiences",
    "provenance",
    "validate_reporting",
]

VERSION = "2.0.0"
__version__ = VERSION

_EXPECTED_MAJOR = VERSION.split(".", 1)[0]

logger = logging.getLogger(__name__)


# =============================================================================
# Package errors
# =============================================================================

class ReportingPackageError(ValueError):
    """Raised by the reporting package aggregator."""


# =============================================================================
# layered_reporter
# =============================================================================

from .layered_reporter import (  # noqa: E402
    # Original exports
    Layer1RawMetrics,
    Layer2NormalizedMetrics,
    Layer3ScenarioScore,
    LayeredReporter,
    # Enhanced contracts
    EvidenceBundle,
    BundleBuilder,
    CarbonInstruments,
    MetricProvenance,
    ExplanationCard,
    VerificationEvidence,
    # Enums
    Audience,
    ProvenanceKind,
    TrustLevel,
    # Errors
    LayeredReporterError,
    ImmutabilityError,
    VERSION as _layered_reporter_version,
)


# =============================================================================
# report_generator
# =============================================================================

from .report_generator import (  # noqa: E402
    ReportGenerator,
    JSONRenderer,
    MarkdownRenderer,
    HTMLRenderer,
    CSVRenderer,
    VERSION as _report_generator_version,
)

# Public copy of the private format tuple — so callers building a CLI
# don't need to reach into a private name in a submodule.
try:
    from .report_generator import _VALID_FORMATS as REPORT_FORMATS  # noqa: E402
except ImportError:  # defensive: submodule may not export it
    REPORT_FORMATS = ("json", "markdown", "html", "csv")


# =============================================================================
# leaderboard
# =============================================================================

from .leaderboard import (  # noqa: E402
    generate_leaderboard,
    Leaderboard,
    LeaderboardEntry,
    LeaderboardResult,
    RankStatus,
    VERSION as _leaderboard_version,
)


# =============================================================================
# agentbeats — accept either filename
# =============================================================================

try:
    from .agentbeats import (  # type: ignore[attr-defined]  # noqa: E402
        build_agentbeats_submission,
        ReproducibilityBundle,
        verify_submission,
        verify_submission_detailed,
        AgentBeatsError,
        VERSION as _agentbeats_version,
    )
except ImportError:  # pragma: no cover — fallback path
    from .agentbeats_submission_builder import (  # type: ignore  # noqa: E402
        build_agentbeats_submission,
        ReproducibilityBundle,
        verify_submission,
        verify_submission_detailed,
        AgentBeatsError,
        VERSION as _agentbeats_version,
    )


# =============================================================================
# __all__ completeness check
# =============================================================================

_missing = [n for n in __all__ if n not in globals()]
if _missing:
    raise ReportingPackageError(
        f"reporting package __all__ references missing names: "
        f"{_missing}. This usually means a submodule renamed a public "
        f"symbol without updating this aggregator."
    )


# =============================================================================
# Convenience helpers
# =============================================================================

def load_bundle(
    source: Union[str, os.PathLike[str], Mapping[str, Any]],
) -> EvidenceBundle:
    """
    Load an :class:`EvidenceBundle` from a JSON path or a payload dict.

    Parameters
    ----------
    source:
        - ``str`` or :class:`os.PathLike` — path to a JSON file
        - :class:`Mapping` — payload passed to
          :meth:`EvidenceBundle.from_dict`

    Raises
    ------
    ReportingPackageError
        On unreadable / invalid JSON, or if the payload is not
        bundle-shaped.
    """
    if isinstance(source, Mapping):
        try:
            return EvidenceBundle.from_dict(source)
        except Exception as exc:
            raise ReportingPackageError(
                f"could not reconstruct EvidenceBundle from payload: "
                f"{exc}"
            ) from exc

    path = Path(source)
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise ReportingPackageError(
            f"could not load bundle from {path}: {exc}"
        ) from exc

    if not isinstance(payload, Mapping):
        raise ReportingPackageError(
            f"bundle file {path} has non-Mapping root "
            f"({type(payload).__name__})."
        )

    try:
        return EvidenceBundle.from_dict(payload)
    except Exception as exc:
        raise ReportingPackageError(
            f"invalid bundle payload in {path}: {exc}"
        ) from exc


def render_to_file(
    bundle: Union[EvidenceBundle, Mapping[str, Any]],
    output_path: Union[str, os.PathLike[str]],
    *,
    audience: Union[Audience, str] = Audience.TECHNICAL,
    format: str = "markdown",
    reporter: Optional[LayeredReporter] = None,
) -> Path:
    """
    Render ``bundle`` and atomically write the result to ``output_path``.

    Parameters
    ----------
    bundle:
        :class:`EvidenceBundle` or a payload mapping (coerced via
        :meth:`EvidenceBundle.from_dict`).
    output_path:
        Destination path. Parent directories are created on demand.
    audience, format:
        Forwarded to :meth:`ReportGenerator.render_bundle`.
    reporter:
        Optional shared :class:`LayeredReporter`.

    Returns
    -------
    Path
        The path that was written.

    Raises
    ------
    ReportingPackageError
        On render failure or I/O error.
    """
    gen = ReportGenerator(reporter)
    try:
        content = gen.render_bundle(
            bundle, audience=audience, format=format,
        )
    except (ValueError, TypeError) as exc:
        raise ReportingPackageError(
            f"render failed: {exc}"
        ) from exc

    out = Path(output_path)
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=out.name + ".", suffix=".tmp",
            dir=str(out.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp, out)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
    except OSError as exc:
        raise ReportingPackageError(
            f"could not write report to {out}: {exc}"
        ) from exc

    logger.info(
        "wrote %s report (%s, %d bytes) to %s",
        format, audience, len(content.encode("utf-8")), out,
    )
    return out


def render_all_audiences(
    bundle: Union[EvidenceBundle, Mapping[str, Any]],
    *,
    format: str = "markdown",
    reporter: Optional[LayeredReporter] = None,
) -> Dict[Audience, str]:
    """
    Render ``bundle`` once per :class:`Audience`.

    Returns
    -------
    dict[Audience, str]
        One rendered string per audience (keyed by the enum value).
    """
    gen = ReportGenerator(reporter)
    return {
        aud: gen.render_bundle(
            bundle, audience=aud, format=format,
        )
        for aud in Audience
    }


def provenance(
    bundle: Union[EvidenceBundle, Mapping[str, Any]],
    *,
    submission_path: Optional[Union[str, os.PathLike[str]]] = None,
) -> str:
    """
    Compute a single SHA-256 over the audit chain.

    The hash covers:

    - ``bundle.content_hash`` (must be populated — call
      :meth:`EvidenceBundle.freeze` first)
    - ``bundle.bundle_version``
    - reporting package ``VERSION``
    - if ``submission_path`` is given, the recomputed content hash of
      that AgentBeats submission (raises if verification fails)

    Returns
    -------
    str
        ``"sha256:..."`` — a single value to record in an external log.

    Raises
    ------
    ReportingPackageError
        If the bundle is not frozen, or the submission fails
        verification.
    """
    if isinstance(bundle, EvidenceBundle):
        bundle_obj = bundle
    else:
        bundle_obj = EvidenceBundle.from_dict(bundle)

    if not bundle_obj.content_hash:
        raise ReportingPackageError(
            "provenance() requires a frozen bundle "
            "(content_hash is empty); call bundle.freeze() first."
        )

    pieces: Dict[str, Any] = {
        "bundle_hash":     bundle_obj.content_hash,
        "bundle_version":  bundle_obj.bundle_version,
        "package_version": VERSION,
    }

    if submission_path is not None:
        check = verify_submission_detailed(str(submission_path))
        if not check["ok"]:
            raise ReportingPackageError(
                f"submission {submission_path} failed verification: "
                f"{check['reason']}"
            )
        pieces["submission_hash"] = check["computed"]

    canonical = json.dumps(
        pieces,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return "sha256:" + hashlib.sha256(
        canonical.encode("ascii")
    ).hexdigest()


# =============================================================================
# Validation helper
# =============================================================================

def validate_reporting() -> Dict[str, str]:
    """
    Verify every reporting submodule is importable and reports a
    compatible major version.

    Returns
    -------
    dict[str, str]
        ``{submodule_name: VERSION}`` for each submodule.

    Raises
    ------
    ReportingPackageError
        If a submodule is missing ``VERSION`` or its major version
        differs from :data:`VERSION`'s major.

    Notes
    -----
    Call from a test fixture or a CLI smoke test — never at import
    time. The package imports cleanly today; this helper exists to
    catch drift when a submodule bumps versions independently.
    """
    modules = {
        "layered_reporter":  _layered_reporter_version,
        "report_generator":  _report_generator_version,
        "leaderboard":       _leaderboard_version,
        "agentbeats":        _agentbeats_version,
    }

    report: Dict[str, str] = {}
    problems = []

    for name, version in modules.items():
        if version is None:
            problems.append(
                f"{name} does not export VERSION."
            )
            continue
        major = str(version).split(".", 1)[0]
        if major != _EXPECTED_MAJOR:
            problems.append(
                f"{name} VERSION {version!r} incompatible with "
                f"reporting package VERSION {VERSION!r} "
                f"(major {major!r} != {_EXPECTED_MAJOR!r})."
            )
            continue
        report[name] = str(version)

    if problems:
        raise ReportingPackageError(
            "reporting package version check failed:\n  - "
            + "\n  - ".join(problems)
        )
    return report


# =============================================================================
# Smoke test: python -m r1.reporting
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    print("VERSION     :", VERSION)
    print("__version__ :", __version__)
    print("formats     :", REPORT_FORMATS)

    # ---- __all__ completeness --------------------------------
    missing = [n for n in __all__ if n not in globals()]
    assert not missing, f"missing from __all__: {missing}"
    print(f"__all__     : {len(__all__)} names all resolve")

    # ---- validate_reporting ----------------------------------
    report = validate_reporting()
    for name, ver in sorted(report.items()):
        print(f"  {name:<18} {ver}")
    assert all(v.startswith(_EXPECTED_MAJOR + ".")
               for v in report.values())

    # ---- Build a demo bundle ---------------------------------
    try:
        from .layered_reporter import (
            BundleBuilder, ExplanationCard, CarbonInstruments,
        )
    except ImportError:
        from r1.reporting.layered_reporter import (  # type: ignore
            BundleBuilder, ExplanationCard, CarbonInstruments,
        )

    result = {
        "accuracy": 0.92,
        "energy_kwh": 0.045,
        "carbon_kg": 0.018,
        "latency_ms": 120.0,
        "task_id": "task-1",
        "run_id": "run-001",
        "policy_version": "v5.0.1",
        "source": "measured",
    }
    instruments = CarbonInstruments()
    instruments.add("rec-1", "rec", 5.0, "MWh")
    bundle = BundleBuilder.build(
        result,
        carbon_instruments=instruments,
        explanation=ExplanationCard(
            decision_id="dec-1",
            selected_action="route_to_lora",
            rationale=["lowest carbon route"],
            confidence=0.92,
        ),
    )

    # ---- render_all_audiences --------------------------------
    rendered = render_all_audiences(bundle, format="markdown")
    assert set(rendered.keys()) == set(Audience)
    print(f"render_all_audiences: {len(rendered)} audiences rendered")

    # ---- load_bundle from dict -------------------------------
    reloaded = load_bundle(bundle.to_dict())
    assert reloaded.content_hash == bundle.content_hash
    print("load_bundle(dict)  : OK")

    # ---- render_to_file --------------------------------------
    import tempfile as _tempfile
    with _tempfile.TemporaryDirectory() as td:
        out = render_to_file(
            bundle,
            Path(td) / "report.md",
            audience=Audience.GOVERNANCE,
            format="markdown",
        )
        assert out.exists()
        print(f"render_to_file     : {out.name} ({out.stat().st_size} B)")

        # load_bundle from path (round-trip)
        with (Path(td) / "bundle.json").open("w") as f:
            json.dump(bundle.to_dict(), f)
        reloaded = load_bundle(Path(td) / "bundle.json")
        assert reloaded.content_hash == bundle.content_hash
        print("load_bundle(path)  : OK")

    # ---- provenance ------------------------------------------
    h = provenance(bundle)
    assert h.startswith("sha256:")
    print(f"provenance         : {h[:20]}...")

    # ---- Error surfaces --------------------------------------
    print("\n--- error surfaces ---")

    # unfrozen bundle → provenance fails
    try:
        import dataclasses
        unfrozen = dataclasses.replace(
            bundle, content_hash=None,
        )
        provenance(unfrozen)
    except ReportingPackageError as exc:
        print("unfrozen provenance:", str(exc)[:60], "...")

    # bad path → load_bundle fails
    try:
        load_bundle("/nonexistent/path/bundle.json")
    except ReportingPackageError as exc:
        print("load_bundle(bad)   :", str(exc)[:60], "...")

    # bad format → render fails
    try:
        render_to_file(bundle, "/tmp/x.bin", format="xml")
    except ReportingPackageError as exc:
        print("render_to_file     :", str(exc)[:60], "...")

    # version drift simulation
    orig = _leaderboard_version
    try:
        import r1.reporting as _self
        _self._leaderboard_version = "3.0.0"
        try:
            validate_reporting()
        except ReportingPackageError as exc:
            print("version mismatch   :", str(exc).splitlines()[0])
    finally:
        _self._leaderboard_version = orig

    # ---- errors are re-exported ------------------------------
    assert issubclass(LayeredReporterError, ValueError)
    assert issubclass(ImmutabilityError, LayeredReporterError)
    assert issubclass(AgentBeatsError, ValueError)
    assert issubclass(ReportingPackageError, ValueError)
    print("\nerror hierarchy    : OK")

    print("\nSmoke test passed.")
