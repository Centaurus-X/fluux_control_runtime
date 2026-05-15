# Release Notes v35.1

## Summary

v35.1 is a pre-production final runtime migration and hardening release. It corrects the v34.3/v34.4 metadata inconsistency, migrates active runtime identifiers to v35.1 and adds configurable security, monitoring and fieldbus-profile controls without changing the worker into a database-backed system.

## Active markers

```text
Package marker:  v35_1_preproduction_final_runtime
Runtime binding: v35_1_preproduction_final_runtime
Command event:   V35_1_PROXY_RUNTIME_COMMAND_RECEIVED
Systemd service: product-runtime-v35-1.service
Runtime root:    /opt/projektstand_v35_1_preproduction_final_runtime
```

## Compatibility

The runtime accepts these legacy command event names for compatibility:

```text
V34_PROXY_RUNTIME_COMMAND_RECEIVED
V33_PROXY_WORKER_COMMAND_RECEIVED
V32_PROXY_WORKER_COMMAND_RECEIVED
```

## Added

```text
- Worker-side MQTT topic ACL checks.
- Configurable MQTT authentication requirement and environment-based secrets.
- Configurable mTLS mode and certificate rotation metadata.
- Runtime health JSON writer and CLI health checker.
- Fieldbus runtime profile for simulated-controller subset operation.
- Runtime host deploy script.
- systemd/journald/logrotate helper updates.
- PostgreSQL scope clarification.
- Diff-System-Report.md.
- PID/PI/PD control-loop liveness patch for constant sensor phases.
- Conditional PID anti-windup mode for faster recovery from output saturation.
```

## Fixed after runtime observation

```text
- PID-controlled sensors now keep automation alive on every poll cycle instead of only on raw value changes.
- Sensor 1 / process state 1 / actuator 5 address 524 no longer freezes the control chain during constant 16000 or 0 phases.
- Thread Management can bypass the duplicate/zero sensor filter for forced control-loop events.
- PID debug output now records integral rejection and anti-windup mode.
```

## Validation

```text
python -m compileall -q .      OK
python -m pytest -q            348 passed
bash -n installers/*.sh        OK
YAML parse check               OK, 81 YAML files parsed
JSON parse check               OK, 9 JSON files parsed
```
