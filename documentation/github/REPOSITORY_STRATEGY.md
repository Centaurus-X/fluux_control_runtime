<<<<<<< HEAD
# Repository Strategy
=======
# Repository Strategy v35.1
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)

## Recommended public repository boundary

Publish the **entire repository root**:

```text
<<<<<<< HEAD
projektstand_v34_3_preproduction_final_runtime/
=======
projektstand_v35_1_preproduction_final_runtime/
>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
```

Do not publish only `product/`.

`product/` contains the runtime executable source tree, but the release also needs installers, service templates, contracts, documentation, acceptance reports, tools and license metadata.

## Do not publish local-only history

The following path is intentionally local-only and excluded by `.gitignore`:

```text
_private/
```

It may contain historical archives, generated backups and old development material. Keep it in local artifact storage or a private repository.

## Multi-repository strategy

The current system should be published as separate repositories:

```text
runtime repository              this package
proxy gateway repository        proxy, PostgreSQL and MQTT rollout package
modbus simulation repository    simulation server and test assets
master repository               later, when the master instance is ready
```

<<<<<<< HEAD
=======
## PostgreSQL ownership

The v35.1 runtime repository does not own PostgreSQL. PostgreSQL backup/restore, registry/auth/audit recovery and database migration checks belong to the external Product Proxy Gateway repository.

>>>>>>> 862ba86 (Release runtime v35.1 preproduction final with PID liveness hotfix)
## Master delivery recommendation

For the current pre-production phase, the proxy gateway repository can continue to contain PostgreSQL and MQTT rollout helpers. A future master repository should be introduced when the master instance is implemented as a first-class component.

Until then, document master-related assumptions in the proxy repository and keep runtime independent.
