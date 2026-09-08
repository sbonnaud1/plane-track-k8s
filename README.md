# plane-track-k8s

Real-time aviation CEP (Complex Event Processing) pipeline for Paris-Orly airport (ORY), deployed on OpenShift / CRC.

ADS-B state vectors from the [OpenSky Network](https://opensky-network.org/) are streamed through Apache Kafka into five Flink SQL jobs that detect flight anomalies in real time. A Flask dashboard surfaces live alerts with per-event detail drawers.

---

## Architecture

```
OpenSky API
    │  (60 s poll, OAuth2)
    ▼
adsb-producer  ──────────────────────────►  Kafka topic: adsb_raw
                                                    │
                              ┌─────────────────────┼────────────────────────┐
                              ▼                     ▼                        ▼
                         cep1 job             cep2 job               cep3 job
                      Missed Approach       Twin Landing           Rapid Descent
                              │                     │                        │
                              ▼                     ▼                        ▼
                    ory_missed_approach   ory_twin_landing        ory_rapid_descent
                              │
                  ┌───────────┴────────────┐
                  ▼                        ▼
             cep4 job                 cep5 job
          Corridor Overload      Repeated Go-Around
                  │                        │
                  ▼                        ▼
       ory_corridor_overload   ory_repeated_goaround
                  │
      ┌───────────┴─────────────────────────────────┐
      │           (all 5 output topics)              │
      ▼                                              ▼
alerts-dashboard (Flask)                    test_cep.py (validation)
```

All components run in the `plane-track` namespace. No image builds are required — Python scripts are injected via ConfigMaps and executed inside stock `python:3.12-slim` and `flink-sql-client-kafka:1.19.1` images.

---

## Components

### Kubernetes manifests

| Manifest | Component | Notes |
|---|---|---|
| `00-namespace.yaml` | Namespace `plane-track` | |
| `01-kafka.yaml` | Kafka broker (KRaft, no ZooKeeper) | Internal: `broker:29092` |
| `02-schema-registry.yaml` | Confluent Schema Registry | Port 8081 |
| `03-flink.yaml` | Flink JobManager + TaskManager | JM REST: 9081 · 10 task slots · watchdog sidecar |
| `04-rbac.yaml` | ServiceAccount + ClusterRoleBinding | `anyuid` SCC required |
| `05-configmap-data-csv.yaml` | Lookup CSVs | `aircraft_lookup_fr.csv`, `flight_route_ory.csv` |
| `06-kafka-topics.yaml` | Topic creation job | 6 topics, 1 partition, RF 1 |
| `07-adsb-producer.yaml` | ADS-B producer | Polls OpenSky v2 API every 60 s |
| `08-flink-sql-ory.yaml` | Flink SQL CEP jobs | 5 self-contained SQL files |
| `09-alerts-dashboard.yaml` | Flask alerts dashboard | Single-page app, SSE polling |
| `10-adsb-simulator.yaml` | ADS-B simulator | Synthetic events, `replicas: 0` by default |
| `flink-sql-resubmit-pod.yaml` | Manual recovery Pod | Resubmits all 5 CEP jobs |

### Admin scripts

| Script | Purpose |
|---|---|
| `start.sh` | Full stack deploy in dependency order. Pre-flight checks (`oc login`, Job dedup). Pass `--sim` to start the simulator immediately. |
| `reset.sh` | Tear down namespace + ClusterRoleBinding. Interactive by default; pass `--yes` for CI. Includes stuck-namespace recovery hint. |
| `status.sh` | One-shot health report: deployments, Flink CEP job states, Kafka topic message counts, route URLs. |
| `sim.sh` | Simulator lifecycle: `start` · `stop` · `restart` · `status`. Shows scenario rotation table on start. |
| `test_cep.py` | End-to-end CEP validation: reads last 3 messages per output topic, asserts payload semantics. Run inside the cluster. |

---

## CEP Patterns

### CEP1 — Missed Approach (`ory_missed_approach`)
`MATCH_RECOGNIZE` on `adsb_raw` with pattern `A B* C+?` within a 10-minute window:
- **A** — aircraft descending into the ORY corridor (`alt < 914 m`, `vrate < -1 m/s`)
- **B** — continuing descent (`alt < 914 m`, `vrate ≤ 2 m/s`)
- **C** — go-around climb (`vrate > 2 m/s`, `alt > 200 m`)

Bounding box: `lat 48.68–48.77`, `lon 2.33–2.43` · speed filter: `velocity < 154 m/s`

### CEP2 — Twin Landing (`ory_twin_landing`)
Self-join over `HOP(90 s width, 10 s slide)` windows. Fires when two **different** aircraft are simultaneously in the short-final zone (`alt < 500 m`, `on_ground = FALSE`) on the **same runway axis** (true_track ±25° of RWY 06 or RWY 24) with vertical separation < 200 m. Regulatory basis: ICAO Doc 4444 §6.7, ≥ 2 NM longitudinal separation.

### CEP3 — Rapid Descent (`ory_rapid_descent`)
Single-event filter: `baro_altitude < 2000 m` AND `vertical_rate < -18 m/s` (≈ 3 500 ft/min). Fires immediately on each matching message inside the ORY bounding box.

### CEP4 — Corridor Overload (`ory_corridor_overload`)
`TUMBLE(30 s)` window aggregate: fires when `COUNT(DISTINCT icao24) ≥ 3` aircraft are simultaneously in the approach corridor (`alt < 914 m`, `on_ground = FALSE`).

### CEP5 — Repeated Go-Around (`ory_repeated_goaround`)
Self-join on `ory_missed_approach` (Kafka source): fires when the same `icao24` produces two missed-approach events within 30 minutes.

---

## Deployment

### Prerequisites

| Requirement | Details |
|---|---|
| OpenShift Local (CRC) **or** any OpenShift 4.x cluster | CRC 2.x with ≥ 14 GB RAM allocated is recommended |
| `oc` CLI | Authenticated against the target cluster (`oc login`) |
| `anyuid` SCC | Required by Kafka and Flink images; applied automatically by `04-rbac.yaml` |
| Internet access from cluster nodes | Required to pull images from `docker.io` on first deploy |
| OpenSky Network OAuth2 credentials | **Optional** — without them the producer runs in anonymous mode (~1 req/min, rate-limited) |

---

### Step 1 — (Optional) Set OpenSky credentials

If you have an OpenSky Network account, create the secret **after** `start.sh` has created the namespace (or immediately after `start.sh` finishes). The secret is read by the producer at pod startup so it only needs to exist before the producer deployment — not before `start.sh` itself.

```bash
# Run start.sh first, then set the secret if you have credentials:
oc create secret generic opensky-creds -n plane-track \
  --from-literal=client_id=YOUR_CLIENT_ID \
  --from-literal=client_secret=YOUR_CLIENT_SECRET

# Restart the producer so it picks up the new secret:
oc rollout restart -n plane-track deployment/adsb-producer
```

Without this secret the producer runs in anonymous mode automatically — alerts will still appear once real ORY traffic is in the bounding box.

---

### Step 2 — Deploy the stack

Run from the `plane-track-k8s/` directory:

```bash
# Make scripts executable (first time only)
chmod +x start.sh reset.sh status.sh sim.sh

# Deploy with real ADS-B data only (simulator off by default)
./start.sh

# Deploy AND start the ADS-B simulator immediately
./start.sh --sim
```

`start.sh` executes the following sequence automatically and waits at each step:

| Step | What happens | Typical duration |
|---|---|---|
| 1 | Namespace `plane-track` + RBAC created | < 5 s |
| 2 | Kafka broker (KRaft) deployed and ready | ~30 s |
| 3 | Confluent Schema Registry deployed and ready | ~20 s |
| 4 | Kafka topics created (6 topics) | ~15 s |
| 5 | Lookup CSV ConfigMap applied | < 5 s |
| 6 | Flink SQL ConfigMap applied | < 5 s |
| 7 | Flink JobManager + TaskManager + watchdog sidecar deployed | ~30 s |
| 8 | **Watchdog startup delay** (3 min) — the watchdog waits for JM REST to stabilise before submitting SQL | ~200 s |
| 9 | Watchdog submits all 5 CEP jobs sequentially; `start.sh` polls until all 5 are `RUNNING` | ~60–120 s |
| 10 | ADS-B producer deployed | ~20 s |
| 11 | Alerts dashboard deployed | ~20 s |
| 12 | (If `--sim`) Simulator scaled to 1 replica | ~10 s |

**Total: approximately 8–10 minutes on CRC.**

On completion, `start.sh` prints the two URLs:

```
============================================================
 plane-track deployed successfully
============================================================

  Dashboard : https://alerts-dashboard-plane-track.apps-crc.testing
  Flink UI  : https://flink-ui-plane-track.apps-crc.testing
```

---

### Step 3 — Verify CEP jobs are running

Check that all 5 Flink jobs reached `RUNNING` state:

```bash
oc exec -n plane-track deploy/flink-jobmanager -c flink-jobmanager -- \
  curl -sf http://localhost:9081/jobs/overview | python3 -m json.tool
```

Expected: 5 entries with `"state": "RUNNING"` — one per CEP pattern:

```
cep1-missed-approach
cep2-twin-landing
cep3-rapid-descent
cep4-corridor-overload
cep5-repeated-goaround
```

If fewer than 5 are running, follow the watchdog logs (it retries every 5 minutes):

```bash
oc logs -n plane-track deploy/flink-jobmanager -c sql-resubmit-watchdog -f
```

---

### Step 4 — Open the dashboard

```bash
# Get the dashboard URL
oc get route -n plane-track alerts-dashboard -o jsonpath='{.spec.host}'
```

Open `https://<route-host>` in a browser. Four tabs are available:

| Tab | Content |
|---|---|
| 🧪 **Simulated** | Alerts from the ADS-B simulator (`SIM*` aircraft). Start/Stop/Reset buttons control the simulator pod directly from the UI. |
| 📡 **Live — OpenSky ADS-B** | Alerts from real ORY traffic. Appears only when actual aircraft are in the approach bounding box. |
| 🧪 **Simulated Flight Events** | Raw `adsb_raw` vectors from the simulator — shows individual ADS-B messages as they flow through Kafka. |
| 📖 **About & Architecture** | CEP pattern descriptions, thresholds, architecture diagram, and tech stack. |

Each alert card shows type, aircraft ID, key metrics, UTC timestamp, and age. Click **View details →** for a full detail drawer with flight profile diagram.

The **Flink UI** is available at the `flink-ui` route:

```bash
oc get route -n plane-track flink-ui -o jsonpath='{.spec.host}'
```

---

### Step 5 — Enable the simulator (if not using `--sim`)

The simulator starts with `replicas: 0`. Use `sim.sh` to control it:

```bash
./sim.sh start      # scale to 1, wait for pod ready, then tail logs
./sim.sh stop       # scale to 0
./sim.sh restart    # stop + fresh start (rotation resets to cycle 0)
./sim.sh status     # current state + last 20 log lines
```

Or use the **▶ Start / ⏸ Stop** buttons directly in the dashboard **Simulated** tab.

The simulator runs a 4-scenario rotation every 20 seconds:

| Cycle | Scenario | Aircraft | CEP fired |
|---|---|---|---|
| 0 | Missed Approach | `SIM001` | CEP1 → `ory_missed_approach` |
| 1 | Twin Landing | `SIM002` + `SIM003` | CEP2 → `ory_twin_landing` |
| 2 | Rapid Descent | `SIM004` | CEP3 → `ory_rapid_descent` |
| 3 | Corridor Overload | `SIM001` + `SIM002` + `SIM003` | CEP4 → `ory_corridor_overload` |
| — | Repeat Go-Around | auto (after 2nd MA on same ICAO) | CEP5 → `ory_repeated_goaround` |

All 5 CEP patterns fire within the first **2 minutes** after the simulator starts.

---

### Tear down

```bash
# Interactive (prompts for confirmation)
./reset.sh

# Non-interactive (CI / scripted)
./reset.sh --yes
```

`reset.sh` deletes the `plane-track` namespace (all pods, services, ConfigMaps, Kafka data, Flink state) and the `plane-track-anyuid-binding` ClusterRoleBinding. All data is lost. Run `./start.sh` to redeploy from scratch.

---

## Admin scripts quick reference

```bash
# Full health check at a glance
./status.sh

# Simulator lifecycle
./sim.sh start        # scale to 1 + tail logs
./sim.sh stop         # scale to 0
./sim.sh restart      # fresh pod (rotation resets to cycle 0)
./sim.sh status       # current state + last 20 log lines

# All scripts accept --help
./start.sh --help
./reset.sh --help
./status.sh --help
./sim.sh --help
```

---

## Manual job recovery

If a Flink job is lost (TaskManager restart, OOM), the watchdog sidecar resubmits automatically every 5 minutes. For an immediate manual recovery:

```bash
oc apply -f flink-sql-resubmit-pod.yaml
oc logs -n plane-track flink-sql-resubmit -f
```

The pod runs once (`restartPolicy: Never`) and submits all 5 CEP files sequentially.

---

## Validation

Run `test_cep.py` inside the cluster against live Kafka output:

```bash
# Create a temporary ConfigMap with the test script
oc create configmap test-cep-script -n plane-track \
  --from-file=test_cep.py=test_cep.py

# Apply the test pod manifest
oc apply -n plane-track -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: test-cep-runner
  namespace: plane-track
spec:
  restartPolicy: Never
  volumes:
    - name: test-script
      configMap:
        name: test-cep-script
  containers:
    - name: runner
      image: docker.io/python:3.12-slim
      command: ["/bin/bash", "-c"]
      args:
        - "pip install kafka-python-ng -q && python /test/test_cep.py"
      volumeMounts:
        - name: test-script
          mountPath: /test
EOF

# Stream the logs and wait for completion
oc logs -f test-cep-runner -n plane-track

# Clean up
oc delete pod test-cep-runner -n plane-track
oc delete configmap test-cep-script -n plane-track
```

Expected output: `TOTAL: 53 checks — 53 PASS  0 FAIL`

> **Note:** CEP2 now includes a `cep2.has_window` check and CEP3 checks both `latitude` and `longitude`, bringing the total from 50 to 53.

---

## Kafka topics

| Topic | Producer | Consumer |
|---|---|---|
| `adsb_raw` | `adsb-producer`, `adsb-simulator` | All 5 Flink CEP jobs |
| `ory_missed_approach` | Flink cep1 | Flask dashboard, Flink cep5 |
| `ory_twin_landing` | Flink cep2 | Flask dashboard |
| `ory_rapid_descent` | Flink cep3 | Flask dashboard |
| `ory_corridor_overload` | Flink cep4 | Flask dashboard |
| `ory_repeated_goaround` | Flink cep5 | Flask dashboard |

---

## Resource footprint (CRC)

| Component | CPU request | CPU limit | Memory request | Memory limit |
|---|---|---|---|---|
| Kafka broker | 200m | 1000m | 512Mi | 1Gi |
| Flink JobManager | 100m | 1000m | 256Mi | 1Gi |
| Flink watchdog sidecar | 50m | 300m | 128Mi | 512Mi |
| Flink TaskManager | 200m | 1000m | 512Mi | 1Gi |
| adsb-producer | 50m | 200m | 64Mi | 256Mi |
| adsb-simulator | 20m | 100m | 32Mi | 128Mi |
| alerts-dashboard | 50m | 200m | 64Mi | 256Mi |

---

## Tech stack

- **Apache Kafka 3.7** (KRaft, no ZooKeeper) — `apache/kafka:3.7.0`
- **Apache Flink 1.19** — `cnfldemos/flink-kafka:1.19.1-scala_2.12-java17` (JM + TM) · `cnfldemos/flink-sql-client-kafka:1.19.1-scala_2.12-java17` (watchdog + recovery pod)
- **Confluent Schema Registry 7.9** — `confluentinc/cp-schema-registry:7.9.0`
- **Python 3.12** — producer, simulator, dashboard (`kafka-python-ng`, `flask`, `requests`)
- **OpenShift / CRC** — `anyuid` SCC, Routes for external access

---

## Changelog

### 2025 — static audit fixes

| File | Issue | Fix |
|---|---|---|
| `09-alerts-dashboard.yaml` | RGA drawer gap calculation used `new Date("2025-01-15 14:23:01")` (space separator) — invalid in Safari (strict ISO 8601 requires `T`) — `gapMin` displayed `NaN min` | Added `.replace(' ', 'T')` before constructing both `Date` objects |
| `09-alerts-dashboard.yaml` | Twin Landing `atype` card (About tab) stated threshold `alt < 914 m` and described ORY as a "single-runway airport" | Corrected threshold to `alt < 500 m` (matches `ory_short_final` SQL view) and description to "two-runway airport" |
| `09-alerts-dashboard.yaml` | Twin Landing popover stated threshold `alt < 914 m` | Corrected to `alt < 500 m` |
| `09-alerts-dashboard.yaml` | Data Sources section described simulator as "alternates MA/TW every 10 s" | Corrected to "cycles MA → TW → RD → CO every 20 s" |
| `09-alerts-dashboard.yaml` | Kubernetes workloads list included the removed `flink-sql-init` Job | Removed; `flink-jobmanager` entry now mentions the `sql-resubmit-watchdog` sidecar |
| `10-adsb-simulator.yaml` | Rapid Descent scenario comment stated `< -15 m/s` — stale, threshold was raised to −18 m/s | Corrected to `< -18 m/s (CEP3 threshold)` |
| `03-flink.yaml` | Trailing comment referenced the removed `flink-sql-init` Job | Updated to reference the `sql-resubmit-watchdog` sidecar |
| `08-flink-sql-ory.yaml` | CEP4 comment stated "3 distinct aircraft … in 5 min" after the window was reduced | Corrected to "within a 30-second window" |
| `README.md` | Resource table missing CPU limits; JM/TM values did not match manifests; Tech stack listed wrong image names and outdated tags | Corrected all values to match actual manifest specs |

### 2025 — final review fixes

| File | Issue | Fix |
|---|---|---|
| `reset.sh` | `oc delete clusterrolebinding plane-track-anyuid` used the wrong name; the CRB was silently left behind on every teardown | Corrected to `plane-track-anyuid-binding` (matches `04-rbac.yaml`) |
| `09-alerts-dashboard.yaml` | All UI text (About tab, popovers, detail drawers) showed the Rapid Descent threshold as `−15 m/s` | Updated to `−18 m/s (≈ 3 540 ft/min)` to match the actual CEP3 SQL threshold |
| `09-alerts-dashboard.yaml` | Twin Landing About section, popover, and detail drawer labelled the detection window as `TUMBLE(5 min)` | Corrected to `HOP(90 s width, 10 s slide)` to match the actual CEP2 SQL query; source topic in drawer also corrected to `ory_short_final` |
| `08-flink-sql-ory.yaml` + `09-alerts-dashboard.yaml` | CEP4 Corridor Overload used a `TUMBLE(5 min)` window — the demo cadence (~18 s scenario) could wait up to 5 min for an alert, making it appear that CO never fired | Reduced to `TUMBLE(30 s)`; simulator comment and all dashboard labels updated to match |
| `10-adsb-simulator.yaml` | After the Twin Landing scenario, no events flowed through `ory_short_final` (the SIM000 background tick at 8 000 m is above the 500 m filter), so the Flink watermark for CEP2's HOP windows stalled and windows never closed → zero TW alerts | Added a 10-event watermark nudge (`SIMWM1`, 490 m, RWY_06 axis) after each TW scenario; `SIMWM1` icao24 sorts after `SIM003` so it never forms a false pair |
