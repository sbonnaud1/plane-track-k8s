#!/usr/bin/env bash
# sim.sh — control the ADS-B simulator pod for the plane-track stack.
# Run from the plane-track-k8s/ directory.
#
# Usage:
#   ./sim.sh start    scale simulator to 1 replica and tail its logs
#   ./sim.sh stop     scale simulator to 0 replicas
#   ./sim.sh restart  stop then start (fresh pod, rotation resets to cycle 0)
#   ./sim.sh status   show current state and recent log lines
#   ./sim.sh --help   show this help
set -euo pipefail

NAMESPACE=plane-track
DEPLOY=adsb-simulator

# ── Argument parsing ──────────────────────────────────────────────────────────
ACTION="${1:-}"
case $ACTION in
  start|stop|restart|status) ;;
  --help) sed -n '2,11p' "$0" | sed 's/^# \?//'; exit 0 ;;
  "")     echo "Usage: ./sim.sh start | stop | restart | status  (--help for details)"; exit 1 ;;
  *)      echo "Unknown action: $ACTION  (use --help)"; exit 1 ;;
esac

# ── Helpers ───────────────────────────────────────────────────────────────────
die()  { echo; echo "ERROR: $*" >&2; exit 1; }
info() { echo "==> $*"; }
ok()   { echo "    ✓ $*"; }

# ── Pre-flight ────────────────────────────────────────────────────────────────
command -v oc &>/dev/null || die "'oc' CLI not found."
oc whoami &>/dev/null     || die "Not logged in to OpenShift. Run 'oc login <server>' first."
oc get namespace "$NAMESPACE" &>/dev/null \
  || die "Namespace '$NAMESPACE' not found. Run ./start.sh first."
oc get deploy -n "$NAMESPACE" "$DEPLOY" &>/dev/null \
  || die "Deployment '$DEPLOY' not found. Run ./start.sh first."

# ── Actions ───────────────────────────────────────────────────────────────────
do_stop() {
  info "Stopping simulator ..."
  oc scale -n "$NAMESPACE" deployment/"$DEPLOY" --replicas=0
  # Wait until the pod is fully gone so a subsequent start gets a clean pod
  local elapsed=0 max_wait=60
  while oc get pods -n "$NAMESPACE" -l app="$DEPLOY" --field-selector=status.phase=Running \
        2>/dev/null | grep -q "$DEPLOY"; do
    sleep 2; elapsed=$((elapsed+2))
    [[ $elapsed -ge $max_wait ]] && { echo "  (pod still terminating — continuing anyway)"; break; }
  done
  ok "Simulator stopped."
}

do_start() {
  info "Starting simulator ..."
  oc scale -n "$NAMESPACE" deployment/"$DEPLOY" --replicas=1

  # Wait for pod to be ready
  info "Waiting for simulator pod to be ready ..."
  if ! oc rollout status -n "$NAMESPACE" "deployment/$DEPLOY" --timeout=60s; then
    echo
    echo "  Simulator pod did not become ready in 60 s."
    echo "  Check: oc logs -n $NAMESPACE deployment/$DEPLOY"
    exit 1
  fi
  ok "Simulator is running."
  echo
  echo "  Scenario rotation (every 20 s):"
  echo "    Cycle 0 — Missed Approach   (SIM001)              → ory_missed_approach"
  echo "    Cycle 1 — Twin Landing      (SIM002 + SIM003)     → ory_twin_landing"
  echo "    Cycle 2 — Rapid Descent     (SIM004)              → ory_rapid_descent"
  echo "    Cycle 3 — Corridor Overload (SIM001+SIM002+SIM003)→ ory_corridor_overload"
  echo "    auto    — Repeat Go-Around  (2nd MA, same ICAO)   → ory_repeated_goaround"
  echo
  echo "  All 5 CEP patterns fire within ~2 minutes."
  echo
  info "Tailing simulator logs (Ctrl-C to stop tailing — simulator keeps running) ..."
  echo
  oc logs -n "$NAMESPACE" deployment/"$DEPLOY" -f
}

do_status() {
  desired=$(oc get deploy -n "$NAMESPACE" "$DEPLOY" \
    -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "?")
  ready=$(oc get deploy -n "$NAMESPACE" "$DEPLOY" \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")

  echo
  if [[ "$desired" == "0" ]]; then
    echo "  Status : STOPPED  (replicas=0)"
  elif [[ "$ready" == "$desired" ]]; then
    echo "  Status : RUNNING  (${ready}/${desired} pods ready)"
  else
    echo "  Status : STARTING (${ready:-0}/${desired} pods ready)"
  fi

  echo
  echo "── Recent simulator logs (last 20 lines) ─────────────────────────────"
  oc logs -n "$NAMESPACE" deployment/"$DEPLOY" --tail=20 2>/dev/null \
    | sed 's/^/  /' \
    || echo "  (no logs available — simulator may be stopped)"
  echo
}

# ── Dispatch ──────────────────────────────────────────────────────────────────
case $ACTION in
  start)   do_start ;;
  stop)    do_stop ;;
  restart) do_stop; echo; do_start ;;
  status)  do_status ;;
esac
