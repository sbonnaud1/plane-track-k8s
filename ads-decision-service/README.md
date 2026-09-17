# ads-decision-service — flight-alert-classifier

Decision service for the **plane-track-k8s** demo. Classifies CEP alerts from
the Paris-Orly real-time aviation pipeline into severity levels and recommended
ATC actions, using IBM Automation Decision Services (ADS).

---

## Architecture

```
Kafka CEP topics (Flink output)
         │
         ▼
  alerts-dashboard (Flask)
         │
         ▼  POST <ADS runtime>/deploymentSpaces/embedded/decisions/<decisionId>
         │                     /operations/classifyFlightAlert/execute
  IBM ADS Runtime
         │
         ▼
  { severity, recommended_action, escalation_required, rationale, display_color }
         │
         ▼
  Alert card + detail drawer enriched in dashboard UI
```

---

## Decision model files

```
ads-decision-service/
└── src/main/resources/com/ibm/planetrack/
    ├── FlightAlertClassifier.dmn        ← Top-level DRD (Decision Requirements Diagram)
    ├── types/
    │   ├── AlertInput.dmn               ← Input data type definition
    │   └── AlertDecision.dmn            ← Output data type definition
    └── decisions/
        ├── SeverityRules.dmn            ← PRIORITY decision table (severity)
        └── ActionRules.dmn              ← UNIQUE decision table (action + color)
```

---

## Step 1 — Import into IBM ADS

Your ADS instance (Decision Center UI):
```
https://demo-emea-di.decision-prod-eu-de.decision.saas.ibm.com/ads/120000NGXD/plane-track/
```

### Option A — ADS Decision Center UI (recommended)

1. Open Decision Center and navigate to the **plane-track** space:
   ```
   https://demo-emea-di.decision-prod-eu-de.decision.saas.ibm.com/ads/120000NGXD/plane-track/
   ```

2. Click **"Decision services"** → **"Create +"**.

3. Fill in:
   - **Name**: `flight-alert-classifier`
   - **Description**: `CEP alert classifier for Paris-Orly aviation pipeline`

4. Once created, click **"Import"** → **"From file"**.

5. Zip the `ads-decision-service/` directory and upload:
   ```powershell
   # Windows PowerShell (from plane-track-k8s/ directory)
   Compress-Archive -Path .\ads-decision-service\* -DestinationPath .\flight-alert-classifier.zip
   ```
   Or on Linux/macOS:
   ```bash
   cd plane-track-k8s
   zip -r flight-alert-classifier.zip ads-decision-service/
   ```

6. Upload `flight-alert-classifier.zip` in the ADS import dialog.

### Option B — ADS CLI (ads-cli)

```bash
# Authenticate
ads-cli config set-credentials \
  --url https://demo-emea-di.decision-prod-eu-de.decision.saas.ibm.com \
  --apikey YOUR_IBM_API_KEY

# Import project files
ads-cli decision-service create --name flight-alert-classifier
ads-cli decision-service import \
  --name flight-alert-classifier \
  --source ./ads-decision-service/
```

---

## Step 2 — Deploy the decision service

1. In ADS Decision Center, open `flight-alert-classifier`.
2. Click **"Deploy"** → select the **Production** (or **embedded**) deployment space.
3. Wait for the deployment to complete (typically < 2 minutes).

---

## Step 3 — Find the runtime execute URL

After deployment, ADS assigns a `decisionId` to your archive. You need this to build
the execute endpoint.

### Method A — Swagger UI in Decision Center

1. In the deployed service view, click the **"?"** (Help) icon → **"API access"**.
2. The Swagger UI shows the full execute URL for each operation:
   ```
   POST https://<ads-runtime-host>/deploymentSpaces/embedded/decisions/<decisionId>/operations/classifyFlightAlert/execute
   ```
3. Copy that URL — this is your `ADS_ENDPOINT`.

### Method B — Query the runtime REST API

```bash
# List all deployment spaces
curl -H "Authorization: ZenApiKey YOUR_API_KEY" \
  https://<ads-runtime-host>/deploymentSpaces

# List decisions in the embedded space
curl -H "Authorization: ZenApiKey YOUR_API_KEY" \
  https://<ads-runtime-host>/deploymentSpaces/embedded/decisions
```

The `decisionId` returned is typically the JAR archive name, e.g.:
```
flight-alert-classifier-1.0.0
```

So the full execute endpoint is:
```
https://<ads-runtime-host>/deploymentSpaces/embedded/decisions/flight-alert-classifier-1.0.0/operations/classifyFlightAlert/execute
```

> **Note on the runtime hostname:**
> The ADS *Decision Center* (authoring UI) runs at:
> `demo-emea-di.decision-prod-eu-de.decision.saas.ibm.com`
>
> The ADS *runtime* (decision execution) may run at the same hostname or a
> dedicated one. Check the Swagger UI or the "API access" section in Decision
> Center for the exact runtime base URL.

---

## Step 4 — Get your API key

1. In ADS Decision Center, go to **Settings** → **API access** (or click your user
   profile → **Generate API key**).
2. Generate a new ZenApiKey token.
3. Keep it for the next step.

---

## Step 5 — Configure the Flask dashboard

Patch the `alerts-dashboard` deployment with the **full runtime execute URL** and API key:

```bash
oc set env -n plane-track deployment/alerts-dashboard \
  ADS_ENABLED="true" \
  ADS_ENDPOINT="https://<ads-runtime-host>/deploymentSpaces/embedded/decisions/<decisionId>/operations/classifyFlightAlert/execute" \
  ADS_API_KEY="YOUR_ZENAPI_KEY"
```

Then restart the dashboard pod to pick up the new env vars:
```bash
oc rollout restart -n plane-track deployment/alerts-dashboard
```

> If you also re-apply the ConfigMap, rollout restart is sufficient — no need to
> `oc apply` the YAML again unless the Python code itself changed.

---

## REST API reference

### Endpoint

```
POST <ADS_ENDPOINT>
Authorization: ZenApiKey YOUR_API_KEY
Content-Type: application/json
```

### Request body

The ADS runtime expects the DMN input wrapped under the `"input"` key:

```json
{
  "input": {
    "alert": {
      "alert_type":       "RAPID_DESCENT",
      "icao24":           "3c4521",
      "callsign":         "AFR123",
      "baro_altitude":    420.0,
      "vertical_rate":    -26.5,
      "aircraft_count":   0,
      "alt_separation_m": 0,
      "goaround_count":   0,
      "detection_ts":     "2025-06-15T14:32:10Z"
    }
  }
}
```

### `alert_type` values

| Value | CEP | Key metric used |
|---|---|---|
| `MISSED_APPROACH` | CEP1 | `baro_altitude` at go-around |
| `TWIN_LANDING` | CEP2 | `alt_separation_m` between aircraft |
| `RAPID_DESCENT` | CEP3 | `vertical_rate` (m/s) |
| `CORRIDOR_OVERLOAD` | CEP4 | `aircraft_count` |
| `REPEATED_GOAROUND` | CEP5 | `goaround_count` |

### Response body

```json
{
  "output": {
    "severity":            "CRITICAL",
    "recommended_action":  "IMMEDIATE_ATC_INTERVENTION",
    "escalation_required": true,
    "rationale":           "Extreme descent rate below 500 m — immediate ATC intervention required",
    "display_color":       "#f85149"
  },
  "decisionId":    "flight-alert-classifier-1.0.0",
  "executionId":   "abc123"
}
```

> The Flask dashboard reads `response["output"]` to get the enrichment fields.

### `severity` values

| Value | Meaning | Card colour |
|---|---|---|
| `CRITICAL` | Immediate emergency action required | Red `#f85149` |
| `HIGH` | Urgent ATC intervention needed | Yellow `#d29922` |
| `MEDIUM` | Monitoring and potential notification | Amber `#e3b341` |
| `LOW` | Routine logging | Green `#3fb950` |
| `INFO` | Informational, no action | Blue `#79c0ff` |

### `recommended_action` values

| Value | Icon | Description |
|---|---|---|
| `IMMEDIATE_ATC_INTERVENTION` | 🚨 | Emergency — contact aircraft now |
| `ALERT_ATC_SUPERVISOR` | ⚠️ | Escalate to ATC supervisor |
| `NOTIFY_ATC` | 📢 | Notify the duty ATC controller |
| `LOG_AND_MONITOR` | 📋 | Log event, continue monitoring |
| `ROUTINE_LOG` | 📝 | Write to log, no further action |

---

## curl test examples

```bash
# Set these variables first
ADS_ENDPOINT="https://<ads-runtime-host>/deploymentSpaces/embedded/decisions/<decisionId>/operations/classifyFlightAlert/execute"
ADS_API_KEY="YOUR_ZENAPI_KEY"

# Test: Rapid descent (critical)
curl -s -X POST "$ADS_ENDPOINT" \
  -H "Authorization: ZenApiKey $ADS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "input": {
      "alert": {
        "alert_type": "RAPID_DESCENT",
        "icao24": "3c4521", "callsign": "AFR123",
        "baro_altitude": 420.0, "vertical_rate": -28.0,
        "aircraft_count": 0, "alt_separation_m": 0, "goaround_count": 0,
        "detection_ts": "2025-06-15T14:32:10Z"
      }
    }
  }' | python -m json.tool

# Test: Corridor overload (5 aircraft)
curl -s -X POST "$ADS_ENDPOINT" \
  -H "Authorization: ZenApiKey $ADS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "input": {
      "alert": {
        "alert_type": "CORRIDOR_OVERLOAD",
        "icao24": "", "callsign": "",
        "baro_altitude": 0, "vertical_rate": 0,
        "aircraft_count": 5, "alt_separation_m": 0, "goaround_count": 0,
        "detection_ts": "2025-06-15T14:32:10Z"
      }
    }
  }' | python -m json.tool

# Test: Twin landing (60 m separation)
curl -s -X POST "$ADS_ENDPOINT" \
  -H "Authorization: ZenApiKey $ADS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "input": {
      "alert": {
        "alert_type": "TWIN_LANDING",
        "icao24": "3c4521", "callsign": "AFR123",
        "baro_altitude": 200.0, "vertical_rate": -5.0,
        "aircraft_count": 2, "alt_separation_m": 60, "goaround_count": 0,
        "detection_ts": "2025-06-15T14:32:10Z"
      }
    }
  }' | python -m json.tool
```

---

## Decision rules summary

### SeverityRules (PRIORITY hit policy)

| Alert type | Condition | Severity |
|---|---|---|
| `RAPID_DESCENT` | `vertical_rate < -25` | CRITICAL |
| `RAPID_DESCENT` | `vertical_rate < -20` | HIGH |
| `RAPID_DESCENT` | `vertical_rate <= -18` | MEDIUM |
| `TWIN_LANDING` | `alt_separation_m < 80` | CRITICAL |
| `TWIN_LANDING` | `alt_separation_m < 150` | HIGH |
| `TWIN_LANDING` | any | MEDIUM |
| `MISSED_APPROACH` | `baro_altitude < 300` | CRITICAL |
| `MISSED_APPROACH` | `baro_altitude < 600` | HIGH |
| `MISSED_APPROACH` | any | MEDIUM |
| `CORRIDOR_OVERLOAD` | `aircraft_count >= 5` | CRITICAL |
| `CORRIDOR_OVERLOAD` | `aircraft_count >= 4` | HIGH |
| `CORRIDOR_OVERLOAD` | any | MEDIUM |
| `REPEATED_GOAROUND` | `goaround_count >= 3` | HIGH |
| `REPEATED_GOAROUND` | any | MEDIUM |

### ActionRules (UNIQUE hit policy)

| Severity | Alert type | Recommended action | Escalation |
|---|---|---|---|
| CRITICAL | RAPID_DESCENT | IMMEDIATE_ATC_INTERVENTION | true |
| CRITICAL | TWIN_LANDING | IMMEDIATE_ATC_INTERVENTION | true |
| CRITICAL | MISSED_APPROACH | IMMEDIATE_ATC_INTERVENTION | true |
| CRITICAL | CORRIDOR_OVERLOAD | ALERT_ATC_SUPERVISOR | true |
| CRITICAL | REPEATED_GOAROUND | ALERT_ATC_SUPERVISOR | true |
| HIGH | any | ALERT_ATC_SUPERVISOR | true |
| MEDIUM | any | NOTIFY_ATC | false |
| LOW | any | LOG_AND_MONITOR | false |
| INFO | any | ROUTINE_LOG | false |
