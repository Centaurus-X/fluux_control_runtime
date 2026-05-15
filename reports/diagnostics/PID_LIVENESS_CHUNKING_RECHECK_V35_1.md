# PID Liveness Chunking Recheck v35.1

## Ergebnis

Die erneute Prüfung hat bestätigt: Der ursprüngliche PID-Liveness-Patch war im Full-Config-Pfad korrekt, musste aber für den tatsächlich konfigurierten Chunking-Pfad erweitert werden.

## Warum war das relevant?

`product/config/application/app_config.yaml` aktiviert:

```text
worker_runtime.config_chunking_enabled: true
```

Dadurch erhält der Controller-Thread nicht zwingend die vollständige Runtime-Konfiguration, sondern einen controller-spezifischen Ausschnitt. Die neue PID-Liveness-Erkennung benötigt `process_states`, um Sensoren mit PID/PI/PD-Regelung als `force_automation` zu markieren.

## Befund vor dem Follow-up-Fix

```text
controller 2 chunk process_states: 0
controller 2 force_map: {}
```

Das hätte bedeutet: Sensor 1 bleibt im Chunking-Betrieb weiterhin ein normaler Event-on-Change-Sensor und könnte bei konstanten Werten wieder aus der Automation herausfallen.

## Fix

`product/src/orchestration/config_chunking.py` wurde minimal-invasiv erweitert:

```text
process_states werden jetzt auch dann in den Controller-Chunk übernommen,
wenn sie sensor_ids oder actuator_ids referenzieren,
die zum Controller gehören.
```

## Befund nach dem Follow-up-Fix

```text
controller 2 chunk process_states: 10
controller 2 force_map: {1: control_strategy.PID state=1, 20: control_strategy.PID state=21}
```

Damit ist die PID-Liveness-Logik auch im kompilierten/chunked Runtime-Pfad aktiv.

## Tests

```text
python3 -m compileall -q product/src product/tests
python3 -m pytest -q
```

Ergebnis:

```text
350 passed
```
