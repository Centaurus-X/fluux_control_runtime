# Runtime Contract Registry

The canonical runtime contract registry is:

```text
contract_registry.yaml
```

<<<<<<< HEAD
The runtime command binding contract intentionally remains:

```text
v34_preproduction_final_runtime
```

This preserves compatibility with the already validated proxy v01.8.6 integration.
=======
The active runtime command binding marker is:

```text
v35_1_preproduction_final_runtime
```

The active command event name is:

```text
V35_1_PROXY_RUNTIME_COMMAND_RECEIVED
```

Legacy V34/V33/V32 command events remain accepted by the worker for compatibility with already validated proxy integrations.
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
