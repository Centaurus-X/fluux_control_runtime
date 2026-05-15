# PID Control Liveness v35.1

v35.1 keeps PID/PI/PD control loops alive even when a sensor value is constant. This is required because a PID controller is stateful: filters, sample timing, integral memory and actuator refreshes must progress over time.

## Runtime configuration

The worker configuration now supports:

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

Meaning:

```text
force_automation_for_controlled_sensors
  true  -> sensors used by PID/PI/PD process states are evaluated on every poll cycle
  false -> keep old event-on-change behavior except explicitly listed sensor IDs

force_automation_control_strategy_types
  defines which control strategies should receive deterministic liveness events

force_automation_sensor_ids
  optional explicit list of additional sensor IDs

control_strategy.force_automation
  optional per-state override; true forces this state even when the global flag is false,
  false disables automatic forcing for this state
```

## PID anti-windup

PID states may use:

```json
{
  "control_strategy": {
    "type": "PID",
    "anti_windup": true,
    "anti_windup_mode": "conditional"
  }
}
```

`conditional` prevents the integral term from accumulating further in the same direction while the output is already saturated. This avoids long negative recovery tails after a high measurement phase.

## State 1 mapping

For the current C2 runtime profile:

```text
process_state.state_id = 1
sensor_id              = 1
sensor address          = 1
actuator_id             = 5
actuator address         = 524
actuator function        = write_register
```

Expected runtime log after patch:

```text
TM: SENSOR_EVENT -> pool.submit (S1=... C2 job=... force=True reason=control_strategy.PID state=1)
Automation ... PID_DBG state=1 ... int_reject=... aw_mode=conditional
Automation ... ACTUATOR_TASK -> ctrl=2 aid=5 addr=524 f=write_register cmd=...
[C2] WRITE: aid=5 addr=524 f=write_register cmd=... ok=True state_id=1
```

## Operational rule

Do not disable `force_automation_for_controlled_sensors` for long-running PID/PI/PD processes unless another periodic control trigger is configured. Event-on-change alone is correct for simple rules, but it is not sufficient for deterministic continuous control.
