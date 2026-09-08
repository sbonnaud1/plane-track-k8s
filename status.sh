#!/usr/bin/env bash
# status.sh — show the current health of the plane-track stack at a glance.
# Run from the plane-track-k8s/ directory.
#
# Usage:
#   ./status.sh          full status report
#   ./status.sh --help   show this help
set -euo pipefail

NAMESPACE=plane-track

# ── Argument parsing ──────────────────────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --help) sed -n '2,8p' "$0" | sed 's/^# \?//'; exit 0 ;;
    *) echo "Unknown argument: $arg  (use --help)"; exit 1 ;;
  esac
done

# ── Helpers ───────────────────────────────────────────────────────────────────
die()     { echo; echo "ERROR: $*" >&2; exit 1; }
section() { echo; echo "── $* ─────────────────────────────────────────────────"; }
ok()      { printf "  ✓  %-28s %s\n" "$1" "$2"; }
warn()    { printf "  ⚠  %-28s %s\n" "$1" "$2"; }
fail()    { printf "  ✗  %-28s %s\n" "$1" "$2"; }

# ── Pre-flight ────────────────────────────────────────────────────────────────
command -v oc &>/dev/null || die "'oc' CLI not found."
oc whoami &>/dev/null     || die "Not logged in to OpenShift. Run 'oc login <server>' first."

# ── Namespace check ───────────────────────────────────────────────────────────
section "Namespace"
if oc get namespace "$NAMESPACE" &>/dev/null; then
  ok "namespace" "$NAMESPACE exists"
else
  fail "namespace" "$NAMESPACE NOT FOUND — run ./start.sh to deploy"
  exit 1
fi

# ── Deployments ───────────────────────────────────────────────────────────────
section "Deployments"
for deploy in broker schema-registry flink-jobmanager flink-taskmanager \
              adsb-producer alerts-dashboard; do
  ready=$(oc get deploy -n "$NAMESPACE" "$deploy" \
    -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "missing")
  desired=$(oc get deploy -n "$NAMESPACE" "$deploy" \
    -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "?")
  if [[ "$ready" == "$desired" && "$ready" != "0" ]]; then
    ok "$deploy" "${ready}/${desired} ready"
  elif [[ "$ready" == "missing" ]]; then
    fail "$deploy" "deployment not found"
  else
    warn "$deploy" "${ready:-0}/${desired} ready"
  fi
done

# ── Simulator (optional — replicas:0 is normal when not in use) ───────────────
sim_ready=$(oc get deploy -n "$NAMESPACE" adsb-simulator \
  -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "missing")
sim_desired=$(oc get deploy -n "$NAMESPACE" adsb-simulator \
  -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "?")
if [[ "$sim_desired" == "0" ]]; then
  warn "adsb-simulator" "stopped (replicas=0) — use ./sim.sh start to enable"
elif [[ "$sim_ready" == "$sim_desired" ]]; then
  ok "adsb-simulator" "${sim_ready}/${sim_desired} ready  ← RUNNING"
else
  warn "adsb-simulator" "${sim_ready:-0}/${sim_desired} ready"
fi

# ── Flink CEP jobs ────────────────────────────────────────────────────────────
section "Flink CEP jobs"
jobs_json=$(oc exec -n "$NAMESPACE" deploy/flink-jobmanager -c flink-jobmanager \
  -- curl -sf http://localhost:9081/jobs/overview 2>/dev/null || echo '{"jobs":[]}')

for job_name in cep1-missed-approach cep2-twin-landing cep3-rapid-descent \
                cep4-corridor-overload cep5-repeated-goaround; do
  state=$(echo "$jobs_json" \
    | grep -o "\"name\":\"${job_name}\"[^}]*\"state\":\"[^\"]*\"" \
    | grep -o '"state":"[^"]*"' \
    | head -1 \
    | tr -d '"' \
    | sed 's/state://' || echo "")
  if [[ "$state" == "RUNNING" ]]; then
    ok "$job_name" "RUNNING"
  elif [[ -z "$state" ]]; then
    fail "$job_name" "NOT FOUND (not submitted yet?)"
  else
    warn "$job_name" "$state"
  fi
done

# ── Kafka topics ──────────────────────────────────────────────────────────────
section "Kafka topics"
topics_raw=$(oc exec -n "$NAMESPACE" deploy/broker \
  -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list 2>/dev/null \
  || echo "KAFKA_UNAVAILABLE")

if [[ "$topics_raw" == "KAFKA_UNAVAILABLE" ]]; then
  fail "kafka" "broker unreachable"
else
  for topic in adsb_raw ory_missed_approach ory_twin_landing \
               ory_rapid_descent ory_corridor_overload ory_repeated_goaround; do
    if echo "$topics_raw" | grep -qx "$topic"; then
      # Get approximate message count via log-end-offset (sum of all partition offsets).
      # Separate the oc exec from the awk so pipefail doesn't fire on awk's own exit.
      _raw_offset=$(oc exec -n "$NAMESPACE" deploy/broker \
        -- /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
           --bootstrap-server localhost:29092 --topic "$topic" --time -1 \
        2>/dev/null) || _raw_offset=""
      _offset="?"
      if [[ -n "$_raw_offset" ]]; then
        _offset=$(echo "$_raw_offset" | awk -F: '{sum += $3} END {print sum+0}')
      fi
      ok "$topic" "≈ ${_offset} messages"
    else
      fail "$topic" "topic missing"
    fi
  done
fi

# ── Routes (URLs) ─────────────────────────────────────────────────────────────
section "Routes"
for route in alerts-dashboard flink-ui; do
  host=$(oc get route -n "$NAMESPACE" "$route" \
    -o jsonpath='{.spec.host}' 2>/dev/null || echo "")
  if [[ -n "$host" ]]; then
    ok "$route" "https://$host"
  else
    fail "$route" "route not found"
  fi
done

# ── Watchdog last lines ───────────────────────────────────────────────────────
section "Watchdog sidecar (last 5 log lines)"
oc logs -n "$NAMESPACE" deploy/flink-jobmanager \
  -c sql-resubmit-watchdog --tail=5 2>/dev/null \
  | sed 's/^/  /' \
  || echo "  (logs unavailable)"

echo
echo "─────────────────────────────────────────────────────────"
echo "  Full commands:"
echo "    Simulator  : ./sim.sh start | stop | status"
echo "    Recovery   : oc apply -f flink-sql-resubmit-pod.yaml"
echo "    Validation : see README — Validation section"
echo "    Tear down  : ./reset.sh"
echo
