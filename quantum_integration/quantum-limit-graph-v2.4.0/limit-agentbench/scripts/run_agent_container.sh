#!/usr/bin/env bash
# scripts/run_agent_container.sh — Hardened container runner.
#
# Usage:
#   run_agent_container.sh <image@sha256:...> <input.json> <output.json>
#
# Env overrides:
#   CPUS     (default 2.0)
#   MEMORY   (default 4g)
#   NETWORK  (default none; set "proxy" or "bridge" for tools)
#   GPUS     (default unset; set to "all" for GPU tasks)
#
# Guarantees:
#   * Image is pinned by digest (rejects :latest and unqualified tags).
#   * Container runs with --read-only, --cap-drop ALL, --no-new-privileges.
#   * Cleanup via trap on EXIT/INT/TERM — no leaked containers.
set -euo pipefail

IMAGE="${1:?image ref required (must include @sha256:...)}"
INPUT="$(realpath "${2:?input.json required}")"
OUTPUT="$(realpath -m "${3:?output.json required}")"

# --- Digest pin -----------------------------------------------------------
if [[ "$IMAGE" != *"@sha256:"* ]]; then
  echo "ERROR: image must be pinned by digest (@sha256:...); got: $IMAGE" >&2
  exit 2
fi

# --- I/O sanity -----------------------------------------------------------
[[ -r "$INPUT" ]] || { echo "input not readable: $INPUT" >&2; exit 1; }
mkdir -p "$(dirname "$OUTPUT")"
[[ -e "$OUTPUT" ]] || : > "$OUTPUT"
chmod 0600 "$INPUT" "$OUTPUT"

# --- Cleanup --------------------------------------------------------------
cleanup() { docker rm -f agentbeats-run 2>/dev/null || true; }
trap cleanup EXIT INT TERM

# --- Run ------------------------------------------------------------------
docker run --rm --name agentbeats-run \
  --network="${NETWORK:-none}" \
  --read-only \
  --tmpfs /tmp:size=256m \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --user 1000:1000 \
  --cpus "${CPUS:-2.0}" \
  --memory "${MEMORY:-4g}" \
  ${GPUS:+--gpus "$GPUS"} \
  -v "${INPUT}:/app/input.json:ro" \
  -v "${OUTPUT}:/app/output.json:rw" \
  "$IMAGE"

echo "OK: wrote $OUTPUT"
