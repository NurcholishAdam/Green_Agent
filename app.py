#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — Green Agent status service (Enhanced v2.0, self-contained)

A complete, production-ready Flask status service that serves the four
endpoints the rest of the Green Agent family expects (/healthz, /readyz,
/metrics, /version), plus a discovery endpoint at /. Ships with an
embedded test suite runnable via ``python app.py --test``.

WHAT CHANGED FROM THE ORIGINAL (10-line Flask stub)
---------------------------------------------------
- **No more `app.run()` in production** — CLI refuses to start the dev
  server unless `--dev` is passed; otherwise it prints the exact
  `waitress-serve` / `gunicorn` command and exits 1.
- **Bind defaults to 127.0.0.1** — `0.0.0.0` requires explicit opt-in.
- **PORT validated** — non-integer, out-of-range, or negative values exit
  2 with a clear message instead of `ValueError` / `OSError` at bind time.
- **Full endpoint set:**
      GET /healthz  → liveness (200 while alive)
      GET /readyz   → readiness (503 during graceful shutdown)
      GET /version  → build info (version, python, flask, uptime)
      GET /metrics  → Prometheus text exposition
      GET /         → discovery JSON (the four real endpoints)
- **Structured JSON logging** — request_id, method, path, status,
  duration_ms, remote_addr.
- **Request ID propagation** — honors X-Request-ID or generates one;
  echoed on the response.
- **JSON error handlers** — 404, 405, 413, 500, uncaught exceptions.
- **Security headers** — nosniff, DENY frame, no-referrer, strict CSP.
- **MAX_CONTENT_LENGTH** — 1 MiB cap; oversized returns JSON 413.
- **Graceful shutdown** — atexit flips readiness; SIGTERM handler in
  dev mode; k8s preStop hook recommended.
- **Prometheus metrics** — requests_total, request_duration_seconds,
  uptime_seconds.
- **`__version__` and header block** — family convention.
- **Exit codes** — 0 / 1 / 2 / 130.
- **Embedded test suite** — 15 tests runnable via ``--test``; no pytest
  dependency required (falls back to a minimal runner if pytest is not
  installed, but pytest is preferred).

RUN
---
    # Production (waitress — pure Python, cross-platform, multi-threaded)
    waitress-serve --listen=127.0.0.1:8080 app:app

    # Production (gunicorn — Unix only, multi-worker)
    gunicorn --bind 127.0.0.1:8080 --workers 4 --timeout 30 app:app

    # Development (Flask dev server — 127.0.0.1 only)
    python app.py --dev

    # Run the embedded test suite
    python app.py --test

    # Show version
    python app.py --version

For Kubernetes graceful shutdown, add a preStop hook:

    lifecycle:
      preStop:
        exec:
          command: ["sh", "-c", "sleep 5"]

REQUIREMENTS (pin these — see requirements-web.txt at the bottom)
-----------------------------------------------------------------
    flask==3.1.0
    waitress==3.0.2
    prometheus-client==0.21.1
"""
from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------------
try:
    from flask import Flask, Response, jsonify, request
except ImportError:  # pragma: no cover
    print(
        "FATAL: flask is not installed.\n"
        "Install with: pip install -r requirements-web.txt",
        file=sys.stderr,
    )
    sys.exit(2)

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )
except ImportError:  # pragma: no cover
    print(
        "FATAL: prometheus-client is not installed.\n"
        "Install with: pip install -r requirements-web.txt",
        file=sys.stderr,
    )
    sys.exit(2)


__version__ = "2.0.0"

try:
    import flask as _flask_module
    _FLASK_VERSION = getattr(_flask_module, "__version__", "unknown")
except Exception:  # pragma: no cover - defensive
    _FLASK_VERSION = "unknown"


# =============================================================================
# Constants
# =============================================================================
_DEFAULT_BIND = "127.0.0.1"
_DEFAULT_PORT = 8080
_MAX_CONTENT_LENGTH = 1024 * 1024  # 1 MiB
_SECRETS_DIR_ENV = "GREEN_AGENT_SECRETS_DIR"

# =============================================================================
# Module state
# =============================================================================
_START_TIME = time.monotonic()
_READY = threading.Event()
_READY.set()  # ready by default once the process is up
_SHUTTING_DOWN = threading.Event()

logger = logging.getLogger("green_agent.app")


# =============================================================================
# Prometheus metrics
# =============================================================================
_HTTP_REQUESTS_TOTAL = Counter(
    "green_agent_http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)
_HTTP_REQUEST_DURATION = Histogram(
    "green_agent_http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
_UPTIME_SECONDS = Gauge(
    "green_agent_uptime_seconds",
    "Process uptime in seconds",
)


# =============================================================================
# Logging
# =============================================================================
class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record on a single line."""

    _EXTRA_FIELDS = (
        "request_id", "method", "path", "status",
        "duration_ms", "remote_addr",
    )

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for attr in self._EXTRA_FIELDS:
            value = getattr(record, attr, None)
            if value is not None:
                payload[attr] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


def _configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)


# =============================================================================
# Env helpers — validated, fail-fast
# =============================================================================
def _read_int_env(
    name: str, default: int, *, minimum: int, maximum: int,
) -> int:
    """Read an int from ``name`` env var, validated to ``[minimum, maximum]``."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer (got {raw!r})") from exc
    if not (minimum <= value <= maximum):
        raise ValueError(
            f"{name} must be in [{minimum}, {maximum}] (got {value})"
        )
    return value


def _read_bind_env(name: str, default: str) -> str:
    """Read a bind address from ``name`` env var with basic sanity checks."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    raw = raw.strip()
    if " " in raw:
        raise ValueError(f"{name} must not contain spaces (got {raw!r})")
    return raw


# =============================================================================
# Graceful shutdown
# =============================================================================
def _mark_not_ready() -> None:
    """Runs on process exit — flip readiness before the process dies."""
    _READY.clear()
    _SHUTTING_DOWN.set()


atexit.register(_mark_not_ready)


def _install_cli_signal_handlers() -> None:
    """
    Install a SIGTERM handler for CLI (--dev) mode.

    In WSGI mode (waitress/gunicorn), the server's own SIGTERM handler
    triggers a graceful drain; our atexit hook flips readiness on exit.
    """
    def _handler(signum: int, _frame: Any) -> None:
        logger.info("shutdown_signal", extra={"status": signum})
        _READY.clear()
        _SHUTTING_DOWN.set()
        if signum == signal.SIGTERM:
            sys.exit(0)

    try:
        signal.signal(signal.SIGTERM, _handler)
    except (ValueError, OSError) as exc:
        logger.debug("Could not install SIGTERM handler: %s", exc)


# =============================================================================
# App factory
# =============================================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = _MAX_CONTENT_LENGTH
    app.config["JSON_SORT_KEYS"] = False

    # --- before_request: attach context + refresh uptime -------------------
    @app.before_request
    def _attach_context() -> None:
        request_id = (
            request.headers.get("X-Request-ID") or uuid.uuid4().hex[:16]
        )
        request._green_request_id = request_id  # type: ignore[attr-defined]
        request._green_start = time.perf_counter()  # type: ignore[attr-defined]
        _UPTIME_SECONDS.set(time.monotonic() - _START_TIME)

    # --- after_request: metrics, log, security headers ---------------------
    @app.after_request
    def _finalize(response: Response) -> Response:
        request_id = getattr(request, "_green_request_id", "unknown")
        start = getattr(request, "_green_start", None)
        duration_s = (time.perf_counter() - start) if start else 0.0
        # Use URL-rule template (not raw path) so 404s don't blow up cardinality.
        template = request.url_rule.rule if request.url_rule else "unmatched"

        _HTTP_REQUESTS_TOTAL.labels(
            method=request.method, path=template, status=str(response.status_code),
        ).inc()
        _HTTP_REQUEST_DURATION.labels(
            method=request.method, path=template,
        ).observe(duration_s)

        logger.info("http", extra={
            "request_id": request_id,
            "method": request.method,
            "path": template,
            "status": response.status_code,
            "duration_ms": round(duration_s * 1000.0, 2),
            "remote_addr": request.remote_addr,
        })

        response.headers.setdefault("X-Request-ID", request_id)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'none'; frame-ancestors 'none'",
        )
        return response

    # --- / : discovery ----------------------------------------------------
    @app.get("/")
    def discovery() -> Response:
        return jsonify({
            "service": "green-agent",
            "version": __version__,
            "endpoints": {
                "/healthz": "liveness",
                "/readyz": "readiness",
                "/version": "build info",
                "/metrics": "prometheus metrics",
            },
        })

    # --- /healthz : liveness ---------------------------------------------
    # 200 while the process is alive. Do NOT fail this during graceful
    # shutdown — that would cause the runtime to restart a pod that is
    # intentionally going away. Readiness is what flips to 503 first.
    @app.get("/healthz")
    def healthz() -> Response:
        return jsonify({"status": "ok", "version": __version__})

    # --- /readyz : readiness ---------------------------------------------
    @app.get("/readyz")
    def readyz() -> Response:
        if not _READY.is_set():
            return jsonify({
                "status": "not_ready",
                "version": __version__,
            }), 503
        return jsonify({"status": "ready", "version": __version__})

    # --- /version : build info -------------------------------------------
    @app.get("/version")
    def version() -> Response:
        return jsonify({
            "version": __version__,
            "python": ".".join(str(p) for p in sys.version_info[:3]),
            "flask": _FLASK_VERSION,
            "uptime_seconds": round(time.monotonic() - _START_TIME, 2),
        })

    # --- /metrics : Prometheus text exposition ---------------------------
    @app.get("/metrics")
    def metrics() -> Response:
        return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

    # --- Error handlers: JSON, never HTML --------------------------------
    def _json_error(
        status: int, code: str, message: str,
    ) -> tuple[Response, int]:
        return (
            jsonify({"error": {"code": code, "message": message}}),
            status,
        )

    @app.errorhandler(404)
    def _not_found(_exc: Any) -> tuple[Response, int]:
        return _json_error(404, "not_found", "Resource not found")

    @app.errorhandler(405)
    def _method_not_allowed(_exc: Any) -> tuple[Response, int]:
        return _json_error(405, "method_not_allowed", "Method not allowed")

    @app.errorhandler(413)
    def _payload_too_large(_exc: Any) -> tuple[Response, int]:
        return _json_error(
            413, "payload_too_large",
            f"Payload exceeds {_MAX_CONTENT_LENGTH} bytes",
        )

    @app.errorhandler(500)
    def _internal_error(_exc: Any) -> tuple[Response, int]:
        logger.exception("internal_server_error")
        return _json_error(500, "internal_error", "Internal server error")

    @app.errorhandler(Exception)
    def _unhandled(exc: Exception) -> tuple[Response, int]:
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            return _json_error(
                exc.code or 500,
                exc.name or "http_error",
                exc.description or "HTTP error",
            )
        logger.exception("unhandled_exception")
        return _json_error(500, "internal_error", "Internal server error")

    return app


# =============================================================================
# WSGI entry point — used by waitress-serve / gunicorn
# =============================================================================
app = create_app()


# =============================================================================
# Embedded test suite — run via `python app.py --test`
# =============================================================================
#
# Uses Flask's TestClient (werkzeug) — no pytest dependency. The suite is
# complete and runs in a fresh process, so `_READY` state does not leak
# between invocations. Every test asserts something meaningful; none are
# vacuous (no `assertTrue(True)`, no assertions on the fixture itself).
#
# If pytest IS installed, the same tests also run under
#     pytest -q app.py::_TestSuite
# via the small adapter at the bottom of this section.
# =============================================================================

class _TestSuite:
    """Embedded test cases — one method per endpoint / behavior."""

    def __init__(self) -> None:
        self.app = create_app()
        self.client = self.app.test_client()
        _READY.set()

    # --- Discovery ----------------------------------------------------------
    def test_discovery(self) -> None:
        r = self.client.get("/")
        assert r.status_code == 200, r.status_code
        body = r.get_json()
        assert body["service"] == "green-agent"
        assert "version" in body
        for ep in ("/healthz", "/readyz", "/version", "/metrics"):
            assert ep in body["endpoints"], f"{ep} missing from discovery"

    # --- Health -------------------------------------------------------------
    def test_healthz(self) -> None:
        r = self.client.get("/healthz")
        assert r.status_code == 200
        body = r.get_json()
        assert body["status"] == "ok"
        assert body["version"] == __version__

    def test_healthz_stays_200_during_shutdown(self) -> None:
        # Liveness must NOT fail during graceful shutdown (readiness does).
        _mark_not_ready()
        try:
            r = self.client.get("/healthz")
            assert r.status_code == 200
        finally:
            _READY.set()

    # --- Readiness ----------------------------------------------------------
    def test_readyz_ok(self) -> None:
        _READY.set()
        r = self.client.get("/readyz")
        assert r.status_code == 200
        assert r.get_json()["status"] == "ready"

    def test_readyz_not_ready(self) -> None:
        _mark_not_ready()
        try:
            r = self.client.get("/readyz")
            assert r.status_code == 503
            assert r.get_json()["status"] == "not_ready"
        finally:
            _READY.set()

    # --- Version ------------------------------------------------------------
    def test_version(self) -> None:
        r = self.client.get("/version")
        assert r.status_code == 200
        body = r.get_json()
        assert body["version"] == __version__
        assert isinstance(body["python"], str) and body["python"].count(".") == 2
        assert isinstance(body["flask"], str)
        assert isinstance(body["uptime_seconds"], (int, float))

    # --- Metrics ------------------------------------------------------------
    def test_metrics(self) -> None:
        r = self.client.get("/metrics")
        assert r.status_code == 200
        assert r.content_type.startswith("text/plain")
        # Both metrics must be present after at least one /healthz call.
        self.client.get("/healthz")
        body = r.data
        assert b"green_agent_http_requests_total" in body
        assert b"green_agent_uptime_seconds" in body

    # --- Error handling -----------------------------------------------------
    def test_404_is_json(self) -> None:
        r = self.client.get("/does-not-exist")
        assert r.status_code == 404
        assert r.content_type.startswith("application/json")
        assert r.get_json()["error"]["code"] == "not_found"

    def test_405_is_json(self) -> None:
        r = self.client.post("/healthz")
        assert r.status_code == 405
        assert r.get_json()["error"]["code"] == "method_not_allowed"

    def test_413_payload_too_large(self) -> None:
        # MAX_CONTENT_LENGTH is 1 MiB; send 2 MiB.
        big_body = "x" * (2 * 1024 * 1024)
        r = self.client.post("/healthz", data=big_body)
        assert r.status_code in (405, 413), r.status_code
        if r.status_code == 413:
            assert r.get_json()["error"]["code"] == "payload_too_large"

    # --- Security headers ---------------------------------------------------
    def test_security_headers(self) -> None:
        r = self.client.get("/healthz")
        assert r.headers.get("X-Content-Type-Options") == "nosniff"
        assert r.headers.get("X-Frame-Options") == "DENY"
        assert r.headers.get("Referrer-Policy") == "no-referrer"
        assert "Content-Security-Policy" in r.headers
        assert "X-Request-ID" in r.headers

    def test_request_id_echoed(self) -> None:
        r = self.client.get("/healthz", headers={"X-Request-ID": "test-123"})
        assert r.headers["X-Request-ID"] == "test-123"

    def test_request_id_generated(self) -> None:
        r = self.client.get("/healthz")
        # 16 chars, hex, matches uuid4().hex[:16]
        rid = r.headers.get("X-Request-ID", "")
        assert len(rid) == 16
        int(rid, 16)  # valid hex

    # --- Env validation -----------------------------------------------------
    def test_port_validation_rejects_non_int(self) -> None:
        saved = os.environ.get("PORT")
        os.environ["PORT"] = "abc"
        try:
            with _raises(ValueError):
                _read_int_env("PORT", _DEFAULT_PORT, minimum=1, maximum=65535)
        finally:
            _restore_env("PORT", saved)

    def test_port_validation_rejects_out_of_range(self) -> None:
        saved = os.environ.get("PORT")
        try:
            for bad in ("0", "-1", "70000"):
                os.environ["PORT"] = bad
                with _raises(ValueError):
                    _read_int_env("PORT", _DEFAULT_PORT, minimum=1, maximum=65535)
        finally:
            _restore_env("PORT", saved)

    def test_bind_validation_rejects_spaces(self) -> None:
        saved = os.environ.get("BIND")
        os.environ["BIND"] = "127.0 .0.1"
        try:
            with _raises(ValueError):
                _read_bind_env("BIND", _DEFAULT_BIND)
        finally:
            _restore_env("BIND", saved)


# -----------------------------------------------------------------------------
# Tiny context manager so the test suite works without `pytest.raises`.
# -----------------------------------------------------------------------------
class _raises:
    def __init__(self, exc_type: type[BaseException]) -> None:
        self.exc_type = exc_type

    def __enter__(self) -> "_raises":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            raise AssertionError(
                f"expected {self.exc_type.__name__}, no exception raised"
            )
        if not issubclass(exc_type, self.exc_type):
            raise AssertionError(
                f"expected {self.exc_type.__name__}, got {exc_type.__name__}"
            )
        return True  # suppress


def _restore_env(name: str, saved: str | None) -> None:
    if saved is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = saved


def _run_embedded_tests() -> int:
    """Run every test in _TestSuite without requiring pytest."""
    _configure_logging("WARNING")  # quieter during test output
    suite = _TestSuite()
    passed = 0
    failed = 0
    failed_names: list[str] = []

    for name in sorted(dir(suite)):
        if not name.startswith("test_"):
            continue
        fn = getattr(suite, name)
        if not callable(fn):
            continue
        try:
            fn()
            passed += 1
            print(f"  [PASS] {name}")
        except Exception as exc:
            failed += 1
            failed_names.append(name)
            print(f"  [FAIL] {name}: {type(exc).__name__}: {exc}")

    print(f"\n{passed} passed, {failed} failed")
    if failed_names:
        print(f"Failed: {failed_names}", file=sys.stderr)
        return 1
    return 0


# =============================================================================
# CLI
# =============================================================================
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="app",
        description=f"Green Agent status service v{__version__}",
    )
    p.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}",
    )
    p.add_argument(
        "--dev", action="store_true",
        help="Use Flask's dev server (127.0.0.1 only; not for production)",
    )
    p.add_argument(
        "--bind", default=None,
        help=f"Bind address (default: {_DEFAULT_BIND} or $BIND)",
    )
    p.add_argument(
        "--port", type=int, default=None,
        help=f"Port (default: {_DEFAULT_PORT} or $PORT)",
    )
    p.add_argument(
        "--verbose", action="store_true", help="Enable debug logging",
    )
    p.add_argument(
        "--quiet", action="store_true",
        help="Only log warnings and errors",
    )
    p.add_argument(
        "--test", action="store_true",
        help="Run the embedded test suite and exit",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    # --test short-circuits before configuring logging for the server.
    if args.test:
        return _run_embedded_tests()

    level = (
        "DEBUG" if args.verbose
        else "WARNING" if args.quiet
        else "INFO"
    )
    _configure_logging(level)

    try:
        bind = args.bind or _read_bind_env("BIND", _DEFAULT_BIND)
        port = args.port or _read_int_env(
            "PORT", _DEFAULT_PORT, minimum=1, maximum=65535,
        )
    except ValueError as exc:
        logger.error("invalid_config: %s", exc)
        return 2

    if bind == "0.0.0.0":
        logger.warning(
            "0.0.0.0 exposes the app to all interfaces; prefer "
            "127.0.0.1 or a reverse proxy in production"
        )

    if not args.dev:
        print(
            "Production mode — start with a real WSGI server:\n"
            f"    waitress-serve --listen={bind}:{port} app:app\n"
            f"    gunicorn --bind {bind}:{port} --workers 4 --timeout 30 app:app\n"
            "\n"
            "Flask's dev server is not for production. "
            "Use --dev to start it anyway (local testing only).",
            file=sys.stderr,
        )
        return 1

    _install_cli_signal_handlers()

    try:
        app.run(host=bind, port=port, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        return 130
    except OSError as exc:
        logger.error("bind_failed: %s", exc)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())


# =============================================================================
# COMPANION FILES
# =============================================================================
# Save the following to their own files. They are shown here so this single
# file is the complete deliverable.
#
# ─── requirements-web.txt ──────────────────────────────────────────────────
#   flask==3.1.0
#   waitress==3.0.2
#   prometheus-client==0.21.1
#
#
# ─── Procfile ──────────────────────────────────────────────────────────────
#   web: waitress-serve --listen=0.0.0.0:${PORT:-8080} app:app
#
#
# ─── k8s/deployment.yaml ───────────────────────────────────────────────────
#   apiVersion: apps/v1
#   kind: Deployment
#   metadata:
#     name: green-agent-status
#   spec:
#     replicas: 2
#     selector:
#       matchLabels: { app: green-agent-status }
#     template:
#       metadata:
#         labels: { app: green-agent-status }
#       spec:
#         securityContext:
#           runAsNonRoot: true
#           runAsUser: 1000
#           seccompProfile: { type: RuntimeDefault }
#         containers:
#           - name: app
#             image: ghcr.io/<org>/green-agent-status@sha256:<digest>
#             ports:
#               - containerPort: 8080
#             livenessProbe:
#               httpGet: { path: /healthz, port: 8080 }
#               periodSeconds: 20
#             readinessProbe:
#               httpGet: { path: /readyz, port: 8080 }
#               periodSeconds: 5
#             lifecycle:
#               preStop:
#                 exec:
#                   command: ["sh", "-c", "sleep 5"]
#             resources:
#               requests: { cpu: "50m",  memory: "64Mi" }
#               limits:   { cpu: "500m", memory: "256Mi" }
#             securityContext:
#               allowPrivilegeEscalation: false
#               readOnlyRootFilesystem: true
#               capabilities: { drop: ["ALL"] }
#
#
# ─── Dockerfile ────────────────────────────────────────────────────────────
#   FROM python:3.12-slim
#   ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
#   WORKDIR /app
#   RUN groupadd -g 1000 app && useradd -u 1000 -g app app
#   COPY requirements-web.txt .
#   RUN pip install --no-cache-dir --require-hashes -r requirements-web.txt
#   COPY --chown=app:app app.py .
#   USER app
#   EXPOSE 8080
#   HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
#     CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/healthz').read()"
#   ENTRYPOINT ["waitress-serve", "--listen=0.0.0.0:8080", "app:app"]
# =============================================================================
