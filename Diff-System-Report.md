# Diff-System-Report v35.1 PID Liveness Hotfix

## Patch target

```text
Base: projektstand_v35_1_preproduction_final_runtime.zip
Patch: v35.1 PID/Actuator-524 liveness stabilization
Scope: Runtime Worker, C2 sensor polling, Thread Management sensor-event gating, PID controller state
```

## User-observed symptom

```text
Sensor register/address:    1
Actuator register/address:  524
Runtime state mapping:      process_state.state_id=1 -> sensor_id=1 -> actuator_id=5
Observed behavior:          actuator 524 eventually stayed at 0 / stopped receiving visible control updates
Last visible runtime write: 2026-05-14 20:44:56 [C2] WRITE aid=5 addr=524 cmd=0.0 ok=True
```

## Differential diagnosis

The provided runtime log contains entries from approximately `20:22:36` to `20:46:57`. The screenshots show later simulator state, but the runtime log does not include entries up to `22:46`; therefore the exact later runtime behavior cannot be proven from the uploaded log alone.

Within the available runtime log:

```text
C2 SENSOR_READ S1@1 entries:          146  first 20:22:45  last 20:46:55
TM SENSOR_EVENT pool.submit S1:       121  first 20:22:45  last 20:44:45
PID_DBG state=1:                      121  first 20:22:45  last 20:44:45
ACTUATOR_TASK addr=524:               121  first 20:22:45  last 20:44:45
WRITE aid=5 addr=524:                 123  first 20:22:36  last 20:44:56
```

Conclusion:

```text
- C2 sensor polling continued.
- Sensor S1 values were still read from Modbus.
- The automation pipeline stopped receiving accepted S1 events after duplicate/zero filtering.
- PID state=1 stopped updating.
- No new actuator task for address 524 was generated after the last accepted event.
- The Modbus write path itself did not show a failed write for address 524 in the uploaded log.
```

## Root cause

The original pipeline treated PID sensor input like a simple event-on-change signal:

```text
_controller_thread:
  publish SENSOR_VALUE_UPDATE only if raw value changed or sensor is critical

thread_management:
  accept automation only if value changed by >= 0.5 percent
  repeated zero values are filtered

PID/filter chain:
  requires repeated cycles to converge and refresh actuator output
```

For `state_id=1`, this created a deterministic freeze condition:

```text
S1@1 stays 16000 -> no repeated PID events
S1@1 changes to 0 -> one event accepted
S1@1 stays 0 -> repeated zero values filtered
PID filtered measurement/integral remain stale/saturated
actuator 524 is no longer refreshed
```

The last PID debug line showed `out=0`, `clamp=min`, and integral clamped at `-10000`, so the old anti-windup mode also made recovery unnecessarily slow.

## Files changed

```text
product/src/core/_controller_thread.py
product/src/core/thread_management.py
product/src/libraries/_automation_system_interface.py
product/config/___device_state_data.json
product/config/___device_state_data_.json
product/tests/test_v35_1_pid_liveness_hotfix.py
reports/diagnostics/PID_ACTUATOR_524_LIVENESS_DIAGNOSIS_V35_1.md
documentation/operations/PID_CONTROL_LIVENESS_V35_1.md
documentation/hardening/PRODUCTION_READINESS_GAP_LIST.md
documentation/release/RELEASE_NOTES_V35_1.md
PROJECT_MANIFEST_V35_1.json
FILE_CHECKSUMS_SHA256_V35_1.txt
```

## Runtime-code changes

### `_controller_thread.py`

```text
- Added _safe_bool.
- Added _normalize_int_list.
- Added _normalize_control_type_set.
- Added _resolve_force_automation_sensor_map.
- Sensors referenced by PID/PI/PD process states are marked as force_automation.
- SENSOR_VALUE_UPDATE payload now contains:
  - critical
  - force_automation
  - automation_reason
- Sensor events are emitted on:
  - raw value change
  - critical sensor
  - forced control-loop sensor
```

### `thread_management.py`

```text
- Added _safe_bool.
- _sensor_value_changed now accepts force=True.
- Forced sensor events bypass the duplicate/zero/0.5-percent filter.
- Added sensor_force_automation runtime stat.
- Added force/reason fields to sensor submit logs.
- Added forced counter to heartbeat diagnostics.
```

### `_automation_system_interface.py`

```text
- create_pid_controller accepts anti_windup_mode.
- PID supports conditional anti-windup.
- Conditional mode rejects additional integral accumulation while output is already saturated in the same direction.
- PID debug snapshot includes integral_rejected and anti_windup_mode.
- PID debug log includes int_reject and aw_mode.
```

## Config changes

Added top-level runtime automation settings:

```json
{
  "automation_settings": {
    "force_automation_for_controlled_sensors": true,
    "force_automation_control_strategy_types": ["PID", "PI", "PD"],
    "force_automation_sensor_ids": [],
    "suppress_duplicates_s": 0.0,
    "pid_control_liveness_patch": "v35.1-pid-liveness"
  }
}
```

Updated PID/PI/PD process states:

```json
{
  "control_strategy": {
    "anti_windup_mode": "conditional",
    "force_automation": true
  }
}
```

Affected active PID states in the checked config:

```text
state_id=1   sensor_ids=[1]   actuator_ids=[5]
state_id=18  sensor_ids=[12]  actuator_ids=[6]
state_id=21  sensor_ids=[20]  actuator_ids=[21]
```

## PostgreSQL documentation correction

The PostgreSQL point is now consistently documented as Gateway/Proxy scope:

```text
Runtime Worker v35.1:
  no local PostgreSQL dependency
  no worker-side backup/restore implementation

Product Proxy Gateway:
  PostgreSQL registry/auth/audit/backup/restore responsibility
```

`documentation/hardening/PRODUCTION_READINESS_GAP_LIST.md` was additionally corrected so it no longer reads like a worker-side PostgreSQL task.

## Expected log after patch

For state `1`:

```text
TM: SENSOR_EVENT -> pool.submit (S1=... C2 job=... force=True reason=control_strategy.PID state=1)
Automation ... PID_DBG state=1 ... int_reject=... aw_mode=conditional
Automation ... ACTUATOR_TASK -> ctrl=2 aid=5 addr=524 f=write_register cmd=...
[C2] WRITE: aid=5 addr=524 f=write_register cmd=... ok=True state_id=1
```

## Validation executed

```text
python3 -m compileall -q product/src product/tests    OK
python3 -m pytest -q                                  348 passed
bash -n installers/*.sh                               OK
bash -n tools/*.sh                                    OK
JSON parse check                                      OK, 9 JSON files parsed
YAML parse check                                      OK, 81 YAML files parsed
ZIP extraction smoke validation                       OK
```

## Remaining field validation

```text
- Run the Modbus simulation server with constant and random-controlled S1 phases.
- Verify actuator 524 receives continued writes during constant S1=16000 and constant S1=0 phases.
- Verify no automation queue backlog grows during a multi-hour soak test.
```

## Follow-up recheck: config chunking / compiled runtime path

A second verification found one additional integration gap in the first PID liveness hotfix:

```text
application/app_config.yaml:
  worker_runtime.config_chunking_enabled: true
```

With controller config chunking enabled, the controller worker receives a per-controller flat chunk instead of the full config. The original chunker filtered `process_states` only by direct controller foreign keys. The current `process_states` rows do not carry `controller_id`, because they reference controllers indirectly through `sensor_ids` and `actuator_ids`.

Before this follow-up fix:

```text
controller 2 chunk process_states: 0
controller 2 force_map: {}
```

That meant the PID liveness code was correct in the full-config path but could be bypassed in the configured chunked runtime path.

### Additional fix

```text
product/src/orchestration/config_chunking.py
```

Process-state chunking now keeps rows when they reference a controller-owned sensor or actuator:

```text
process_state.sensor_ids intersects controller sensor_ids
or
process_state.actuator_ids intersects controller actuator_ids
```

After this follow-up fix:

```text
controller 2 chunk process_states: 10
controller 2 force_map: {1: control_strategy.PID state=1, 20: control_strategy.PID state=21}
```

This keeps PID/PI/PD liveness deterministic in both runtime modes:

```text
config_chunking_enabled=false -> full config path OK
config_chunking_enabled=true  -> chunked config path OK
```

### Additional tests

```text
product/tests/test_v35_1_pid_liveness_hotfix.py
```

Added coverage:

```text
- synthetic chunking test for PID process-state retention by sensor/actuator dependency
- real v35.1 config test proving controller 2 keeps state_id=1 and force-maps sensor 1
```

### Updated validation

```text
python3 -m compileall -q product/src product/tests    OK
python3 -m pytest -q                                  350 passed
bash -n installers/*.sh tools/*.sh                    OK
JSON parse check                                      OK
```
