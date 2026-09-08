#!/usr/bin/env bash
# start.sh — deploy the plane-track CEP stack in dependency order.
# Run from the plane-track-k8s/ directory.
#
# Usage:
#   ./start.sh           deploy with real ADS-B data only (simulator off)
#   ./start.sh --sim     deploy and immediately enable the ADS-B simulator
#   ./start.sh --help    show this help
set -euo pipefail

NAMESPACE=plane-track
ENABLE_SIM=false

# ── Argument parsing ──────────────────────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --sim)  ENABLE_SIM=true ;;
    --help) sed -n '2,8p' "$0" | sed 's/^# \?//'; exit 0 ;;
    *) echo "Unknown argument: $arg  (use --help)"; exit 1 ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
step()  { echo; echo "==> $*"; }
info()  { echo "    $*"; }
ok()    { echo "    ✓ $*"; }
die()   { echo; echo "ERROR: $*" >&2; exit 1; }

# ── Pre-flight checks ─────────────────────────────────────────────────────────
step "Pre-flight checks"

command -v oc &>/dev/null || die "'oc' CLI not found. Install it and run 'oc login' first."

# Verify the oc session is authenticated (oc whoami exits non-zero if not logged in)
oc whoami &>/dev/null || die "Not logged in to OpenShift. Run 'oc login <server>' first."
ok "Logged in as: $(oc whoami)  /  server: $(oc whoami --show-server)"

# Warn if there are leftover resources from a previous deploy
if oc get namespace "$NAMESPACE" &>/dev/null; then
  echo
  echo "  WARNING: namespace '$NAMESPACE' already exists."
  echo "  Re-applying manifests on top of existing resources."
  echo "  Run ./reset.sh first for a clean slate."
  echo
fi

# ── Helper functions ──────────────────────────────────────────────────────────
wait_deploy() {
  local name=$1 timeout=${2:-120s}
  info "Waiting for deployment/$name ..."
  oc rollout status -n "$NAMESPACE" "deployment/$name" --timeout="$timeout"
}

wait_job_complete() {
  local name=$1 timeout=${2:-120s}
  # If a completed Job from a previous run exists, delete it first so oc wait
  # doesn't immediately return on the stale Completed condition.
  local phase
  phase=$(oc get job -n "$NAMESPACE" "$name" \
    -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)
  if [[ "$phase" == "True" ]]; then
    info "Job/$name already completed from a previous run — deleting it before re-running ..."
    oc delete job -n "$NAMESPACE" "$name" --ignore-not-found
    sleep 3
  fi
  info "Waiting for job/$name to complete ..."
  oc wait -n "$NAMESPACE" "job/$name" --for=condition=Complete --timeout="$timeout"
}

wait_flink_jobs() {
  local expected=5 max_wait=600 interval=15 elapsed=0
  info "Waiting for $expected Flink CEP jobs to reach RUNNING state (max ${max_wait}s) ..."
  while true; do
    # Capture jobs overview; if the oc/curl fails, default to "0 running".
    # grep -c returns exit 1 when there are no matches — safe because the
    # count is captured via command substitution, not used as a condition.
    local overview running=0
    overview=$(oc exec -n "$NAMESPACE" deploy/flink-jobmanager \
      -c flink-jobmanager -- curl -sf http://localhost:9081/jobs/overview \
      2>/dev/null) || overview=""
    if [[ -n "$overview" ]]; then
      running=$(echo "$overview" | grep -o '"state":"RUNNING"' | wc -l | tr -d '[:space:]')
    fi
    info "RUNNING: $running / $expected"
    [[ "$running" -ge "$expected" ]] && break
    elapsed=$((elapsed + interval))
    if [[ $elapsed -ge $max_wait ]]; then
      echo
      die "Only $running/$expected CEP jobs are RUNNING after ${max_wait}s.
  Check watchdog logs:  oc logs -n $NAMESPACE deploy/flink-jobmanager -c sql-resubmit-watchdog -f
  Check job status:     oc exec -n $NAMESPACE deploy/flink-jobmanager -c flink-jobmanager -- curl -sf http://localhost:9081/jobs/overview"
    fi
    sleep $interval
  done
  ok "All $expected CEP jobs RUNNING."
}

wait_watchdog_ready() {
  # Instead of a fixed sleep, poll the watchdog log until it reports
  # "JobManager ready" — then wait a short margin for the first SQL submission.
  local max_wait=240 interval=10 elapsed=0
  info "Waiting for watchdog to confirm JobManager REST is ready ..."
  while true; do
    local ready
    ready=$(oc logs -n "$NAMESPACE" deploy/flink-jobmanager \
      -c sql-resubmit-watchdog --tail=50 2>/dev/null \
      | grep -c "JobManager ready" || true)
    [[ "$ready" -ge 1 ]] && break
    elapsed=$((elapsed + interval))
    if [[ $elapsed -ge $max_wait ]]; then
      die "Watchdog did not confirm JobManager ready after ${max_wait}s.
  Check: oc logs -n $NAMESPACE deploy/flink-jobmanager -c sql-resubmit-watchdog -f"
    fi
    sleep $interval
  done
  ok "Watchdog confirmed JobManager ready — waiting 30 s for first SQL submission ..."
  sleep 30
}

# ── Deploy ────────────────────────────────────────────────────────────────────
step "Namespace + RBAC"
oc apply -f 00-namespace.yaml
oc apply -f 04-rbac.yaml

step "Kafka broker"
oc apply -f 01-kafka.yaml
wait_deploy broker 180s

step "Schema Registry"
oc apply -f 02-schema-registry.yaml
wait_deploy schema-registry 120s

step "Kafka topics"
oc apply -f 06-kafka-topics.yaml
wait_job_complete kafka-topics-init 120s

step "Lookup CSV data + Flink SQL ConfigMap"
oc apply -f 05-configmap-data-csv.yaml
oc apply -f 08-flink-sql-ory.yaml

step "Flink (JobManager + TaskManager + watchdog sidecar)"
oc apply -f 03-flink.yaml
wait_deploy flink-taskmanager 120s
wait_deploy flink-jobmanager  120s

step "Waiting for watchdog to confirm JobManager is ready"
wait_watchdog_ready

step "Waiting for all 5 CEP jobs to reach RUNNING"
wait_flink_jobs

step "ADS-B producer"
oc apply -f 07-adsb-producer.yaml
wait_deploy adsb-producer 120s

step "Alerts dashboard"
oc apply -f 09-alerts-dashboard.yaml
wait_deploy alerts-dashboard 120s

step "ADS-B simulator"
oc apply -f 10-adsb-simulator.yaml   # always apply at replicas:0 to keep manifest current
if $ENABLE_SIM; then
  oc scale -n "$NAMESPACE" deployment/adsb-simulator --replicas=1
  wait_deploy adsb-simulator 60s
  ok "Simulator enabled (--sim flag set)"
else
  info "Simulator deployed but not started (replicas=0). Use ./sim.sh start to enable."
fi

# ── Summary ───────────────────────────────────────────────────────────────────
DASH_HOST=$(oc get route -n "$NAMESPACE" alerts-dashboard \
  -o jsonpath='{.spec.host}' 2>/dev/null || echo "<route not ready>")
FLINK_HOST=$(oc get route -n "$NAMESPACE" flink-ui \
  -o jsonpath='{.spec.host}' 2>/dev/null || echo "<route not ready>")

echo
echo "============================================================"
echo " plane-track deployed successfully"
echo "============================================================"
echo
echo "  Dashboard  : https://$DASH_HOST"
echo "  Flink UI   : https://$FLINK_HOST"
echo
echo "  Quick commands:"
echo "    Status       : ./status.sh"
echo "    Simulator    : ./sim.sh start | stop | status"
echo "    CEP recovery : oc apply -f flink-sql-resubmit-pod.yaml"
echo "    Watchdog log : oc logs -n $NAMESPACE deploy/flink-jobmanager -c sql-resubmit-watchdog -f"
echo "    Tear down    : ./reset.sh"
echo
