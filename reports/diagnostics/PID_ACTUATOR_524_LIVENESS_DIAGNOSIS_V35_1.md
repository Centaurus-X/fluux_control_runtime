# PID / Actuator 524 Liveness Diagnosis v35.1

Status: **patched**  
Scope: Runtime Worker v35.1, Controller C2, Sensor `sensor_id=1` / Modbus address `1`, Actuator `actuator_id=5` / Modbus address `524`, `process_state.state_id=1`.

## Summary

The actuator did not stop because the Modbus write path itself failed. The available runtime log shows successful writes to address `524` until the last actuator command. After that, C2 sensor polling continued, but no new automation job for sensor `1`, no PID update for state `1`, and no actuator task for actuator `5` were created.

The root cause is the interaction between:

```text
1. controller-thread event gating: publish SENSOR_VALUE_UPDATE only on raw value change or critical sensor
2. thread-management change filter: suppress duplicate/low-delta values, including repeated zero values
3. stateful PID/filter chain: PID needs repeated control cycles even when the raw input is constant
4. integral windup: the old clamp mode could keep the PID output at 0 for a long recovery tail
```

This made the control loop event-driven like a simple threshold rule, although PID control must be time-driven or liveness-forced.

## Log evidence

The provided log covers runtime data from approximately `20:22:36` to `20:46:57`. It does **not** contain runtime entries up to `22:46`, so the later visual simulator state cannot be proven from the runtime log alone.

```text
C2 SENSOR_READ S1@1 entries:          146  first 20:22:45  last 20:46:55
TM SENSOR_EVENT pool.submit S1:       121  first 20:22:45  last 20:44:45
PID_DBG state=1:                      121  first 20:22:45  last 20:44:45
ACTUATOR_TASK addr=524:               121  first 20:22:45  last 20:44:45
WRITE aid=5 addr=524:                 123  first 20:22:36  last 20:44:56
```

Important sequence:

```text
20:42:35  S1@1=16000 -> PID_DBG state=1 -> actuator task/write addr=524
20:42:45  S1@1=16000 -> no new S1 automation job
20:42:55  S1@1=16000 -> no new S1 automation job
...
20:44:35  S1@1=16000 -> no new S1 automation job
20:44:45  S1@1=0     -> one PID update, output still 0 because filtered measurement/integral are still saturated
20:44:55  S1@1=0     -> no new S1 automation job
20:45:05  S1@1=0     -> no new S1 automation job
...
20:46:55  S1@1=0     -> sensor read continues, but no PID/actuator refresh follows
```

The last PID line before the stop shows the controller still saturated at the minimum output:

```text
PID_DBG state=1 sp=554.000 meas=699.819 err=-145.819 dt=0.500(dt_raw=130.002)
P=-1.458186 I=-10.000000 int=-10000.000000 out_pre=-11.458186 out=0.000000 clamp=min
```

The raw sensor value had already reached `0`, but the filtered/scaled PID measurement was still approximately `699.819`. Because duplicate zero values were then filtered away, the filter chain could not converge and the PID could not recover.

## Code/config cause

`process_states.state_id=1` links:

```text
sensor_ids:   [1]
actuator_ids: [5]
PID config:   pid_config_id=1
actuator:     controller_id=2, address=524, function_type=write_register
```

The original controller loop emitted sensor events only here:

```text
if value_changed or critical:
    publish SENSOR_VALUE_UPDATE
```

Sensor `1` was not marked critical. Therefore constant values such as `16000, 16000, 16000` and `0, 0, 0` did not keep the PID alive.

## Patch

Minimal-invasive liveness patch:

```text
product/src/core/_controller_thread.py
- added configurable force map for controlled PID/PI/PD sensors
- PID/PI/PD input sensors now emit SENSOR_VALUE_UPDATE every poll cycle
- event payload includes force_automation and automation_reason

product/src/core/thread_management.py
- forced automation events bypass the 0.5% duplicate/zero filter
- runtime stats include sensor_force_automation
- logs show force=True and reason=control_strategy.PID state=1

product/src/libraries/_automation_system_interface.py
- PID supports anti_windup_mode
- conditional anti-windup prevents negative integral accumulation while output is saturated at min
- debug log includes integral rejection and anti-windup mode

product/config/___device_state_data.json
product/config/___device_state_data_.json
- added automation_settings.force_automation_for_controlled_sensors=true
- added automation_settings.force_automation_control_strategy_types=[PID,PI,PD]
- set anti_windup_mode=conditional for PID process states
```

## Expected result after patch

For state `1`, the normal long-running sequence becomes deterministic:

```text
C2 SensorPoll reads S1@1
  -> emits SENSOR_VALUE_UPDATE force=True reason=control_strategy.PID state=1
  -> Thread Management accepts event even if value is duplicate or zero
  -> Automation worker runs filter + PID update
  -> actuator task for aid=5 / addr=524 is emitted
  -> C2 writes addr=524
```

This keeps actuator `524` refreshed during constant sensor phases and allows the filter/PID state to converge instead of freezing at the last accepted event.

## Remaining external validation

The patch is locally validated by unit tests and static checks. A real Modbus/EMQX runtime soak test is still recommended:

```text
- run at least 2h with controlled random simulator values
- verify actuator 524 receives writes continuously during constant S1 phases
- verify logs contain force=True reason=control_strategy.PID state=1
- verify no queue backlog and no growing ThreadPool inflight count
```
