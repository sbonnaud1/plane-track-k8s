"""
test_cep.py — end-to-end CEP validation.

Reads the last N messages from each CEP output topic and asserts that the
payload fields satisfy the semantic rules defined in the Flink SQL jobs.

Run inside the cluster (python:3.12-slim image with kafka-python-ng):
  pip install kafka-python-ng -q
  python test_cep.py

Exit code: 0 = all checks passed, 1 = one or more checks failed.
"""
import json
import sys
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP   = "broker:29092"
TIMEOUT_MS  = 6000   # per-topic consumer timeout (ms)
SAMPLE_SIZE = 3      # last N messages checked per topic
RESULTS: list[tuple[str, str, str]] = []


# ── Kafka helper ──────────────────────────────────────────────────────────────

def consume_last(topic: str, n: int = SAMPLE_SIZE) -> list[dict]:
    """Return the last `n` messages from `topic`, or [] if the topic is empty."""
    try:
        c = KafkaConsumer(
            topic,
            bootstrap_servers=BOOTSTRAP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="replace")),
            consumer_timeout_ms=TIMEOUT_MS,
        )
        msgs = list(c)
        c.close()
        return [m.value for m in msgs[-n:]] if msgs else []
    except NoBrokersAvailable:
        print(f"  [ERROR] Cannot reach Kafka at {BOOTSTRAP}", flush=True)
        return []


def _parse_ts(s: str) -> datetime:
    """Parse a Flink TIMESTAMP(3) string with or without sub-second component."""
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unrecognised timestamp format: {s!r}")


# ── Assertion helper ──────────────────────────────────────────────────────────

def check(name: str, cond: bool, detail: str = "") -> None:
    status = "PASS" if cond else "FAIL"
    RESULTS.append((status, name, detail))
    marker = "✓" if cond else "✗"
    print(f"  {marker}  {status:<4}  {name}  {detail}", flush=True)


# ── CEP1: Missed Approach ─────────────────────────────────────────────────────

print("\n=== CEP1  ory_missed_approach ===")
msgs = consume_last("ory_missed_approach")
check("cep1.has_messages", len(msgs) > 0, f"{len(msgs)} msgs")
for j in msgs:
    check("cep1.approach_alt<914",
          j.get("approach_alt", 999) < 914.0,
          f"approach_alt={j.get('approach_alt')}")
    check("cep1.go_around_alt>200",
          j.get("go_around_alt", 0) > 200.0,
          f"go_around_alt={j.get('go_around_alt')}")
    check("cep1.has_callsign",
          bool((j.get("callsign") or "").strip()),
          f"callsign={j.get('callsign')!r}")
    check("cep1.has_timestamps",
          bool(j.get("approach_ts")) and bool(j.get("go_around_ts")),
          "approach_ts + go_around_ts present")


# ── CEP2: Twin Landing ────────────────────────────────────────────────────────

print("\n=== CEP2  ory_twin_landing ===")
msgs = consume_last("ory_twin_landing")
check("cep2.has_messages", len(msgs) > 0, f"{len(msgs)} msgs")
for j in msgs:
    sep = abs((j.get("alt_a") or 0) - (j.get("alt_b") or 0))
    check("cep2.different_aircraft",
          j.get("icao24_a") != j.get("icao24_b"),
          f"A={j.get('icao24_a')}  B={j.get('icao24_b')}")
    check("cep2.alt_sep<200m",
          sep < 200.0,
          f"sep={sep:.0f} m")
    check("cep2.both_below_500m",
          (j.get("alt_a") or 999) < 500 and (j.get("alt_b") or 999) < 500,
          f"alt_a={j.get('alt_a')}  alt_b={j.get('alt_b')}")
    check("cep2.has_window",
          bool(j.get("window_start")) and bool(j.get("window_end")),
          "window_start + window_end present")


# ── CEP3: Rapid Descent ───────────────────────────────────────────────────────

print("\n=== CEP3  ory_rapid_descent ===")
msgs = consume_last("ory_rapid_descent")
check("cep3.has_messages", len(msgs) > 0, f"{len(msgs)} msgs")
for j in msgs:
    check("cep3.vrate<-18",
          (j.get("vertical_rate") or 0) < -18.0,
          f"vrate={j.get('vertical_rate')}")
    check("cep3.alt<2000",
          (j.get("baro_altitude") or 9999) < 2000.0,
          f"alt={j.get('baro_altitude')}")
    check("cep3.has_coords",
          j.get("latitude") is not None and j.get("longitude") is not None,
          f"lat={j.get('latitude')}  lon={j.get('longitude')}")


# ── CEP4: Corridor Overload ───────────────────────────────────────────────────

print("\n=== CEP4  ory_corridor_overload ===")
msgs = consume_last("ory_corridor_overload")
check("cep4.has_messages", len(msgs) > 0, f"{len(msgs)} msgs")
for j in msgs:
    check("cep4.count>=3",
          (j.get("aircraft_count") or 0) >= 3,
          f"count={j.get('aircraft_count')}")
    check("cep4.has_window",
          bool(j.get("window_start")) and bool(j.get("window_end")),
          "window_start + window_end present")


# ── CEP5: Repeated Go-Around ──────────────────────────────────────────────────

print("\n=== CEP5  ory_repeated_goaround ===")
msgs = consume_last("ory_repeated_goaround")
check("cep5.has_messages", len(msgs) > 0, f"{len(msgs)} msgs")
for j in msgs:
    # Guard against missing timestamp fields — a KeyError here would abort the
    # whole test run, masking all subsequent checks.
    ga1_raw = j.get("goaround_1")
    ga2_raw = j.get("goaround_2")
    if not ga1_raw or not ga2_raw:
        check("cep5.has_timestamps", False,
              f"goaround_1={ga1_raw!r}  goaround_2={ga2_raw!r}")
        check("cep5.same_aircraft", bool(j.get("icao24") and j.get("callsign")),
              f"icao={j.get('icao24')}  cs={j.get('callsign')}")
        continue
    try:
        ga1 = _parse_ts(ga1_raw)
        ga2 = _parse_ts(ga2_raw)
    except ValueError as e:
        check("cep5.timestamp_parseable", False, str(e))
        continue

    delta = (ga2 - ga1).total_seconds() / 60
    check("cep5.same_aircraft",
          bool(j.get("icao24")) and bool(j.get("callsign")),
          f"icao={j.get('icao24')}  cs={j.get('callsign')}")
    check("cep5.gap_0_to_30min",
          0 < delta <= 30,
          f"gap={delta:.1f} min")
    check("cep5.ga2_after_ga1",
          ga2 > ga1,
          f"ga1={ga1_raw}  ga2={ga2_raw}")


# ── Summary ───────────────────────────────────────────────────────────────────

passed = sum(1 for s, _, _ in RESULTS if s == "PASS")
failed = sum(1 for s, _, _ in RESULTS if s == "FAIL")

print(f"\n{'='*58}")
print(f"TOTAL: {len(RESULTS)} checks — {passed} PASS  {failed} FAIL")

if failed:
    print("\nFAILURES:")
    for s, name, detail in RESULTS:
        if s == "FAIL":
            print(f"  ✗  {name}  {detail}")

print("=" * 58)
sys.exit(0 if failed == 0 else 1)
