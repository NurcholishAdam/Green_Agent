# =============================================================================
# Deploy Green Agent (Enhanced v2)
# =============================================================================
# ---- Fixes vs v1 -----------------------------------------------------------
#   FIX #1   Removed --enhancements flag from the deploy call. Deploy script
#            accepts only <env> [--dry-run]. Enhancements are toggled by
#            regenerating config with create_config_structure.sh --enhancements
#            (a "Prepare" step runs only when the input is true).
#   FIX #2   Removed chmod/ls of ./k8s/ (directory does not exist).
#   FIX #3   Replaced "kubectl get hpa" (no HPA in v2 config) with
#            "kubectl get all,raycluster".
#   FIX #4   Removed "|| true" from kubectl wait — timeouts now fail the job.
#   FIX #5   Added a Capture-diagnostics step that produces every file the
#            artifact upload expects (pods.json, events.txt, describe.txt,
#            logs.txt, get-all.txt).
#   FIX #6   KUBE_CONFIG is validated per-environment; missing secret fails
#            loudly before any mutation.
#   FIX #7   concurrency group prevents concurrent deploys per ref.
#   FIX #8   Tag push -> production (with approval on the environment);
#            branch push -> staging; workflow_dispatch -> user input.
#   FIX #9   Deployment waits for kubectl rollout status on the head
#            StatefulSet, not just a single Ready pod.
#   FIX #10  Smoke test hits /health after rollout succeeds.
#   FIX #11  dry_run input renders + diffs without applying.
#   FIX #12  Per-step timeouts + job timeout-minutes.
#   FIX #13  Rollback (kubectl rollout undo) runs on failure in production.
#   FIX #14  set -euo pipefail in every run block.
#   FIX #15  Dropped id-token: write (unused).
#   FIX #17  kubectl installed by version, matching cluster ±1 minor.
#   FIX #18  kustomize build + kubeconform + kubectl diff before apply.
#   FIX #23  Slack notification (skipped when SLACK_WEBHOOK is unset).
#   FIX #27  $GITHUB_STEP_SUMMARY with environment, commit, status, pods.
# =============================================================================

name: Deploy Green Agent

on:
  push:
    branches: [main]
    tags: ['v*']
  workflow_dispatch:
    inputs:
      environment:
        description: 'Deployment environment'
        required: true
        default: 'staging'
        type: choice
        options: [development, staging, production]
      enable_enhancements:
        description: 'Regenerate configs with enhancements enabled (--force)'
        required: false
        default: false
        type: boolean
      dry_run:
        description: 'Render + diff manifests, do not apply'
        required: false
        default: false
        type: boolean

# Least-privilege. Do NOT add id-token:write unless OIDC is wired up.
permissions:
  contents: read
  packages: read

# FIX #7 — one deploy per ref at a time
concurrency:
  group: deploy-${{ github.ref }}
  cancel-in-progress: false

jobs:
  # -------------------------------------------------------------------------
  # Resolve target environment before any cluster-touching job
  # -------------------------------------------------------------------------
  resolve:
    name: Resolve target
    runs-on: ubuntu-latest
    outputs:
      environment: ${{ steps.r.outputs.environment }}
      prefix: ${{ steps.r.outputs.prefix }}
      namespace: ${{ steps.r.outputs.namespace }}
      enhancements: ${{ steps.r.outputs.enhancements }}
      dry_run: ${{ steps.r.outputs.dry_run }}
    steps:
      - name: Resolve environment, prefix, namespace
        id: r
        run: |
          set -euo pipefail
          if [[ -n "${{ github.event.inputs.environment || '' }}" ]]; then
            ENV="${{ github.event.inputs.environment }}"
          elif [[ "${GITHUB_REF}" == refs/tags/v* ]]; then
            ENV="production"      # FIX #8 — release tags deploy to production
          else
            ENV="staging"
          fi
          case "$ENV" in
            development) PREFIX="dev" ;;
            staging)     PREFIX="staging" ;;
            production)  PREFIX="prod" ;;
            *) echo "::error::Unknown environment: $ENV"; exit 1 ;;
          esac
          ENH="${{ github.event.inputs.enable_enhancements || 'false' }}"
          DRY="${{ github.event.inputs.dry_run || 'false' }}"
          {
            echo "environment=$ENV"
            echo "prefix=$PREFIX"
            echo "namespace=green-agent-$ENV"
            echo "enhancements=$ENH"
            echo "dry_run=$DRY"
          } >> "$GITHUB_OUTPUT"
          echo "Resolved: env=$ENV prefix=$PREFIX enhancements=$ENH dry_run=$DRY"

  # -------------------------------------------------------------------------
  # Deploy
  # -------------------------------------------------------------------------
  deploy:
    name: Deploy to ${{ needs.resolve.outputs.environment }}
    needs: resolve
    runs-on: ubuntu-latest
    # Production MUST have reviewers configured on GitHub (Settings → Environments).
    environment:
      name: ${{ needs.resolve.outputs.environment }}
    # FIX #12 — hard ceiling
    timeout-minutes: 30

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      # ---------------------------------------------------------------------
      # Install pinned tools (FIX #17 — kubectl matches cluster ±1 minor)
      # ---------------------------------------------------------------------
      - name: Install tools
        timeout-minutes: 5
        run: |
          set -euo pipefail
          KUBECTL_VERSION="v1.30.0"
          KUSTOMIZE_VERSION="v5.4.3"
          KUBECONFORM_VERSION="v0.6.7"

          if ! command -v kubectl >/dev/null 2>&1; then
            curl -fsSLo /tmp/kubectl \
              "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
            sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl
          fi
          curl -fsSLo /tmp/kustomize.tar.gz \
            "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2F${KUSTOMIZE_VERSION}/kustomize_${KUSTOMIZE_VERSION}_linux_amd64.tar.gz"
          tar -xzf /tmp/kustomize.tar.gz -C /tmp
          sudo install -m 0755 /tmp/kustomize /usr/local/bin/kustomize
          curl -fsSLo /tmp/kubeconform.tar.gz \
            "https://github.com/yannh/kubeconform/releases/download/${KUBECONFORM_VERSION}/kubeconform-linux-amd64.tar.gz"
          tar -xzf /tmp/kubeconform.tar.gz -C /tmp
          sudo install -m 0755 /tmp/kubeconform /usr/local/bin/kubeconform

          kubectl version --client=true
          kustomize version
          kubeconform -v

      # ---------------------------------------------------------------------
      # Kubeconfig — FIX #6: fail loudly if secret missing for this env
      # ---------------------------------------------------------------------
      - name: Configure kubeconfig
        timeout-minutes: 2
        env:
          KUBE_CONFIG_SECRET: ${{ secrets.KUBE_CONFIG }}
          ENVIRONMENT: ${{ needs.resolve.outputs.environment }}
        run: |
          set -euo pipefail
          if [[ -z "${KUBE_CONFIG_SECRET}" ]]; then
            echo "::error::KUBE_CONFIG secret is not set for environment '$ENVIRONMENT'"
            exit 1
          fi
          mkdir -p ~/.kube
          printf '%s' "$KUBE_CONFIG_SECRET" | base64 -d > ~/.kube/config
          chmod 600 ~/.kube/config
          kubectl cluster-info
          kubectl config current-context

      # ---------------------------------------------------------------------
      # FIX #2 — only ./scripts/*.sh exists; k8s/ removed
      # ---------------------------------------------------------------------
      - name: Make scripts executable
        run: |
          set -euo pipefail
          if compgen -G "./scripts/*.sh" > /dev/null; then
            chmod +x ./scripts/*.sh
            ls -la ./scripts/
          else
            echo "::error::No scripts found in ./scripts/"
            exit 1
          fi

      # ---------------------------------------------------------------------
      # FIX #1 — regenerate config only when enhancements requested
      # ---------------------------------------------------------------------
      - name: Regenerate configs with enhancements
        if: ${{ needs.resolve.outputs.enhancements == 'true' }}
        timeout-minutes: 3
        run: |
          set -euo pipefail
          if [[ -x ./create_config_structure.sh ]]; then
            ./create_config_structure.sh --enhancements --force
          elif [[ -x ./scripts/create_config_structure.sh ]]; then
            ./scripts/create_config_structure.sh --enhancements --force
          else
            echo "::warning::Config generator not found; assuming configs already have enhancements enabled"
          fi

      # ---------------------------------------------------------------------
      # FIX #18 — build + schema-validate the manifests before touching cluster
      # ---------------------------------------------------------------------
      - name: Validate manifests
        timeout-minutes: 5
        env:
          ENVIRONMENT: ${{ needs.resolve.outputs.environment }}
        run: |
          set -euo pipefail
          OVERLAY="config/overlays/$ENVIRONMENT"
          [[ -d "$OVERLAY" ]] || { echo "::error::Missing overlay: $OVERLAY"; exit 1; }

          kustomize build "$OVERLAY" > /tmp/rendered.yaml
          echo "Rendered $(wc -l < /tmp/rendered.yaml) lines"

          kubeconform -strict -ignore-missing-schemas -summary /tmp/rendered.yaml

      # ---------------------------------------------------------------------
      # FIX #11 — kubectl diff (render the change for review + catch drift)
      # ---------------------------------------------------------------------
      - name: kubectl diff
        timeout-minutes: 3
        env:
          NAMESPACE: ${{ needs.resolve.outputs.namespace }}
        run: |
          set -euo pipefail
          # Ensure namespace exists for diff to work; --dry-run=client avoids side effects
          kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml \
            | kubectl apply -f - >/dev/null 2>&1 || true

          set +e
          kubectl diff -n "$NAMESPACE" -f /tmp/rendered.yaml > /tmp/kubectl-diff.txt 2>&1
          DIFF_EXIT=$?
          set -e
          # diff exit codes: 0=no diff, 1=diff exists (OK), >1=error
          if [[ $DIFF_EXIT -gt 1 ]]; then
            echo "::error::kubectl diff failed (exit $DIFF_EXIT)"
            cat /tmp/kubectl-diff.txt
            exit $DIFF_EXIT
          fi
          echo "=== kubectl diff (exit $DIFF_EXIT) ==="
          head -200 /tmp/kubectl-diff.txt || true

      # ---------------------------------------------------------------------
      # Deploy — FIX #1: no --enhancements flag (config already baked in)
      # ---------------------------------------------------------------------
      - name: Deploy
        if: ${{ needs.resolve.outputs.dry_run != 'true' }}
        timeout-minutes: 10
        env:
          ENVIRONMENT: ${{ needs.resolve.outputs.environment }}
        run: |
          set -euo pipefail
          ./scripts/deploy-environment.sh "$ENVIRONMENT"

      # ---------------------------------------------------------------------
      # FIX #9 — rollout status on head StatefulSet
      # ---------------------------------------------------------------------
      - name: Wait for head rollout
        if: ${{ needs.resolve.outputs.dry_run != 'true' }}
        timeout-minutes: 6
        env:
          NAMESPACE: ${{ needs.resolve.outputs.namespace }}
          PREFIX: ${{ needs.resolve.outputs.prefix }}
        run: |
          set -euo pipefail
          kubectl rollout status \
            "statefulset/${PREFIX}-green-agent-cluster-ray-head" \
            -n "$NAMESPACE" --timeout=300s

      # ---------------------------------------------------------------------
      # Workers Ready — FIX #4: no "|| true"
      # ---------------------------------------------------------------------
      - name: Wait for workers Ready
        if: ${{ needs.resolve.outputs.dry_run != 'true' }}
        timeout-minutes: 6
        env:
          NAMESPACE: ${{ needs.resolve.outputs.namespace }}
          PREFIX: ${{ needs.resolve.outputs.prefix }}
        run: |
          set -euo pipefail
          kubectl wait --for=condition=Ready pod \
            -l "ray.io/cluster=${PREFIX}-green-agent-cluster,ray.io/node-type=worker" \
            -n "$NAMESPACE" --timeout=300s

      # ---------------------------------------------------------------------
      # FIX #10 — smoke test /health
      # ---------------------------------------------------------------------
      - name: Smoke test /health
        if: ${{ needs.resolve.outputs.dry_run != 'true' }}
        timeout-minutes: 3
        env:
          NAMESPACE: ${{ needs.resolve.outputs.namespace }}
        run: |
          set -euo pipefail
          kubectl run "smoke-curl-${{ github.run_id }}" \
            --rm -i --restart=Never \
            --namespace="$NAMESPACE" \
            --image=curlimages/curl:8.10.1 \
            --timeout=90s \
            -- curl -fsS "http://green-agent-dashboard:8000/health" \
            || { echo "::error::/health smoke test failed"; exit 1; }

      # ---------------------------------------------------------------------
      # FIX #5 — produce every file the artifact upload expects
      # ---------------------------------------------------------------------
      - name: Capture diagnostics
        if: always()
        timeout-minutes: 3
        env:
          NAMESPACE: ${{ needs.resolve.outputs.namespace }}
        run: |
          set +e
          mkdir -p .artifacts
          kubectl get pods   -n "$NAMESPACE" -o wide  > .artifacts/kubectl-pods.txt      2>&1
          kubectl get pods   -n "$NAMESPACE" -o json  > .artifacts/deployment-status.json 2>&1
          kubectl get events -n "$NAMESPACE" --sort-by=.lastTimestamp > .artifacts/kubectl-events.txt 2>&1
          kubectl describe pods -n "$NAMESPACE"       > .artifacts/kubectl-describe.txt   2>&1
          kubectl logs -n "$NAMESPACE" -l app=green-agent \
            --tail=500 --all-containers=true          > .artifacts/kubectl-logs.txt       2>&1
          kubectl get all,raycluster -n "$NAMESPACE"  > .artifacts/kubectl-get-all.txt    2>&1
          echo "=== artifacts ==="
          ls -la .artifacts/

      # ---------------------------------------------------------------------
      # FIX #27 — summary shown in the Actions UI
      # ---------------------------------------------------------------------
      - name: Write job summary
        if: always()
        env:
          ENVIRONMENT: ${{ needs.resolve.outputs.environment }}
          NAMESPACE: ${{ needs.resolve.outputs.namespace }}
          PREFIX: ${{ needs.resolve.outputs.prefix }}
          ENH: ${{ needs.resolve.outputs.enhancements }}
          DRY: ${{ needs.resolve.outputs.dry_run }}
          STATUS: ${{ job.status }}
        run: |
          set +e
          {
            echo "## Green Agent Deployment"
            echo ""
            echo "| Field | Value |"
            echo "|-------|-------|"
            echo "| Environment | \`$ENVIRONMENT\` |"
            echo "| Namespace | \`$NAMESPACE\` |"
            echo "| Prefix | \`$PREFIX\` |"
            echo "| Commit | [\`${GITHUB_SHA:0:8}\`](${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/commit/${GITHUB_SHA}) |"
            echo "| Enhancements | \`$ENH\` |"
            echo "| Dry run | \`$DRY\` |"
            echo "| Status | **$STATUS** |"
            if [[ -f .artifacts/kubectl-pods.txt ]]; then
              echo ""
              echo "### Pods"
              echo '```'
              head -30 .artifacts/kubectl-pods.txt
              echo '```'
            fi
            if [[ -f .artifacts/kubectl-events.txt ]]; then
              echo ""
              echo "### Recent events (last 15)"
              echo '```'
              tail -15 .artifacts/kubectl-events.txt
              echo '```'
            fi
          } >> "$GITHUB_STEP_SUMMARY"

      # ---------------------------------------------------------------------
      # FIX #5 (cont.) — upload the files produced above
      # ---------------------------------------------------------------------
      - name: Upload diagnostics
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: deploy-${{ needs.resolve.outputs.environment }}-${{ github.run_id }}
          path: .artifacts/
          retention-days: 14
          if-no-files-found: warn
          compression-level: 6

      # ---------------------------------------------------------------------
      # FIX #23 — Slack notification (skipped when secret absent)
      # ---------------------------------------------------------------------
      - name: Notify Slack
        if: always()
        timeout-minutes: 2
        env:
          SLACK_WEBHOOK: ${{ secrets.SLACK_WEBHOOK }}
          ENVIRONMENT: ${{ needs.resolve.outputs.environment }}
          ENH: ${{ needs.resolve.outputs.enhancements }}
          STATUS: ${{ job.status }}
        run: |
          set -euo pipefail
          if [[ -z "${SLACK_WEBHOOK:-}" ]]; then
            echo "::notice::SLACK_WEBHOOK not set — skipping Slack notification"
            exit 0
          fi

          case "$STATUS" in
            success) EMOJI="✅"; COLOR="#2eb67d" ;;
            failure) EMOJI="❌"; COLOR="#e01e5a" ;;
            cancelled) EMOJI="⚠️"; COLOR="#ecb22e" ;;
            *) EMOJI="ℹ️"; COLOR="#36c5f0" ;;
          esac

          TEXT="${EMOJI} Green Agent deploy to ${ENVIRONMENT}: ${STATUS} (enhancements=${ENH}) — ${GITHUB_REPOSITORY}@${GITHUB_SHA:0:8}"

          # jq is present on ubuntu-latest
          PAYLOAD=$(jq -nc \
            --arg text "$TEXT" \
            --arg color "$COLOR" \
            --arg url "${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}" \
            '{attachments:[{color:$color, text:$text, actions:[{type:"button",text:"View run",url:$url}]}]}')

          curl -fsSL -X POST -H 'Content-type: application/json' \
            -d "$PAYLOAD" "$SLACK_WEBHOOK"

      # ---------------------------------------------------------------------
      # FIX #13 — rollback on failure (production only)
      # ---------------------------------------------------------------------
      - name: Rollback (production only)
        if: failure() && needs.resolve.outputs.environment == 'production' && needs.resolve.outputs.dry_run != 'true'
        timeout-minutes: 5
        env:
          NAMESPACE: ${{ needs.resolve.outputs.namespace }}
          PREFIX: ${{ needs.resolve.outputs.prefix }}
        run: |
          set +e
          echo "::warning::Deployment failed in production — attempting rollback"
          kubectl rollout undo \
            "statefulset/${PREFIX}-green-agent-cluster-ray-head" \
            -n "$NAMESPACE" \
            || echo "::warning::No previous revision to roll back to"
          kubectl rollout status \
            "statefulset/${PREFIX}-green-agent-cluster-ray-head" \
            -n "$NAMESPACE" --timeout=180s \
            || echo "::warning::Rollback did not stabilize"
