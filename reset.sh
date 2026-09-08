#!/usr/bin/env bash
# reset.sh — tear down the entire plane-track stack and wait for full cleanup.
# Run from the plane-track-k8s/ directory.
#
# Usage:
#   ./reset.sh          interactive (prompts for confirmation)
#   ./reset.sh --yes    non-interactive (CI / scripted)
#   ./reset.sh --help   show this help
set -euo pipefail

NAMESPACE=plane-track
SKIP_CONFIRM=false

# ── Argument parsing ──────────────────────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --yes)  SKIP_CONFIRM=true ;;
    --help) sed -n '2,9p' "$0" | sed 's/^# \?//'; exit 0 ;;
    *) echo "Unknown argument: $arg  (use --help)"; exit 1 ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
die() { echo; echo "ERROR: $*" >&2; exit 1; }

# ── Pre-flight ────────────────────────────────────────────────────────────────
command -v oc &>/dev/null || die "'oc' CLI not found."
oc whoami &>/dev/null     || die "Not logged in to OpenShift. Run 'oc login <server>' first."

# ── Confirmation ──────────────────────────────────────────────────────────────
if ! $SKIP_CONFIRM; then
  echo
  echo "  This will DELETE the namespace '$NAMESPACE' and all its resources."
  echo "  All Kafka data and Flink state will be permanently lost."
  echo
  read -r -p "  Type 'yes' to confirm: " confirm
  [[ "$confirm" == "yes" ]] || { echo "  Aborted."; exit 0; }
fi

# ── Namespace deletion ────────────────────────────────────────────────────────
echo
if ! oc get namespace "$NAMESPACE" &>/dev/null; then
  echo "==> Namespace '$NAMESPACE' does not exist — nothing to delete."
else
  echo "==> Deleting namespace $NAMESPACE ..."
  oc delete namespace "$NAMESPACE" --ignore-not-found --wait=false

  echo "==> Waiting for namespace to be fully removed ..."
  local_timeout=180   # seconds
  local_elapsed=0
  while oc get namespace "$NAMESPACE" &>/dev/null; do
    printf "."
    sleep 3
    local_elapsed=$((local_elapsed + 3))
    if [[ $local_elapsed -ge $local_timeout ]]; then
      echo
      echo "WARNING: namespace '$NAMESPACE' is still terminating after ${local_timeout}s."
      echo "  This can happen on CRC when finalizers are stuck."
      echo "  Force-remove with:"
      echo "    oc get namespace $NAMESPACE -o json \\"
      echo "      | python3 -c \"import sys,json; d=json.load(sys.stdin); d['spec']['finalizers']=[]; print(json.dumps(d))\" \\"
      echo "      | oc replace --raw /api/v1/namespaces/$NAMESPACE/finalize -f -"
      exit 1
    fi
  done
  echo
  echo "==> Namespace $NAMESPACE deleted."
fi

# ── ClusterRoleBinding lives outside the namespace ────────────────────────────
echo "==> Removing ClusterRoleBinding plane-track-anyuid-binding ..."
oc delete clusterrolebinding plane-track-anyuid-binding --ignore-not-found

echo
echo "Reset complete. Run ./start.sh to redeploy from scratch."
