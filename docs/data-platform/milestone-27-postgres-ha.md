# Milestone 27 Data Layer HA, Backup, and Restore Drill

## Scope

Every prior milestone's PostgreSQL is a single Compose container with no replication, no automated backup, and no restore path beyond "hope the disk survives" - the weakest link in this entire lab, unexamined since Milestone 1. This milestone builds and proves out the real pattern - CloudNativePG-managed replication, automatic failover, and continuous backup with point-in-time recovery - as a genuinely new, isolated cluster (`postgres-ha` namespace), deliberately **not** a migration of the live `orders`/`payments` database. That data - real output from 26 milestones of validation - stays exactly where it is; this milestone earns the right to propose migrating it by proving the pattern works first, with real measurements, not by gambling with it directly.

## Design

- **CloudNativePG operator** (Helm, `cnpg-system` namespace) manages a 3-instance `Cluster` (`orders-ha`, `postgres-ha` namespace) - one primary, two streaming replicas, automatic leader election and failover on primary loss.
- **MinIO** (`compose/compose.yaml`, wired into K3s the same selectorless-Service pattern as every other Compose-hosted infra service since Milestone 19) provides S3-compatible object storage for Barman Cloud backups - K3s's default `local-path-provisioner` has no CSI snapshot support, so an object store is the practical backup target for a single-node home-lab cluster, not a limitation specific to this exercise.
- **Continuous WAL archiving + on-demand base backups**, both to MinIO, via CNPG's (deprecated-but-still-functional-in-1.30) native `barmanObjectStore` integration - noted as a real forward-looking caveat, not glossed over: `kubectl apply` logs a deprecation warning pointing at the newer Barman Cloud Plugin architecture.
- **`kubernetes/data-platform/`**: the `Cluster`, `Backup`, and a restore-drill `Cluster` template, applied imperatively like the Linkerd/Kyverno cluster policies from Milestones 24-26 - infrastructure, not an Argo CD-managed application. `scripts/postgres-ha-provision.sh` reproduces the whole setup (namespace, MinIO credentials Secret, bucket, cluster) from one command.

## What didn't work

**MinIO's Service lives in `orders-lab`; this cluster's pods live in `postgres-ha` - the short DNS name `minio` silently doesn't resolve across namespaces.** First backup attempt: the rejoining former-primary pod logged `Could not connect to the endpoint URL: "http://minio:9000/postgres-backups"` and never became ready. Fixed by using the fully-qualified `minio.orders-lab.svc.cluster.local`.

**A rapid sequence of chaos (forced failover, a live Cluster patch) wedged the CNPG operator's internal reconciliation state badly enough that a freshly created `Backup` object was never picked up at all - no status, no events, nothing in the logs beyond admission-webhook validation.** Restarting the operator deployment cleared it immediately; the next `Backup` object was picked up within seconds. A real operational lesson about testing chaos and data-durability workflows in the same breath: sequencing matters, and an operator that's mid-reconciling a failover is not necessarily ready to also start a backup.

**The first backup that did run failed with `WAL archive check failed for server orders-ha: Expected empty archive`.** Not a new bug - contamination from the *first* (broken-endpoint) attempt: a handful of WAL segments had actually archived successfully in the brief window before the DNS issue above was diagnosed, then the primary failed over onto a new timeline, and barman-cloud's own safety check (refusing to archive into a non-empty, pre-existing destination without explicit confirmation it's the right lineage) correctly flagged the mismatch. Fixed by clearing the contaminated path in MinIO and letting archiving re-initialize clean - the safety check is working exactly as intended here, not a false positive.

**Point-in-time recovery failed with `no target backup found`, even though the backup completing successfully was already confirmed.** `externalClusters[].barmanObjectStore` defaults its internal `serverName` to the external-cluster entry's own `name` field (`orders-ha-backup-source` in the restore manifest) - but the *source* cluster archived its WALs and backups under **its own** name, `orders-ha`. Without an explicit `serverName: orders-ha` override, CNPG looks in the right bucket and path but under the wrong server-name prefix, and finds nothing. A one-line fix, but one that fails with a message giving zero hint about the actual mismatch - worth documenting for exactly that reason.

## Results

**Failover** (forced by force-deleting the primary pod, `orders-ha-1`):

```
Primary deleted, polling for new primary...
New primary: orders-ha-2
RTO (deletion to first successful write): 59s
```

Pre-failover rows survived intact; a post-failover write against the newly promoted primary succeeded immediately. The failed former primary self-healed back into the cluster as a new replica without any manual intervention, restoring the full 3/3 topology on its own.

**Backup**: a base backup of a small (near-empty demo) database completed in 4 seconds (`2026-07-29T02:32:32Z` to `02:32:36Z`).

**Point-in-time restore drill** - the actual test that matters:

```
# t0: insert a marker row, note now()
INSERT 0 1   -- "last-good-row-before-incident"
2026-07-29 02:32:59.89764+00

# t1 (~5s later): simulate an incident
DROP TABLE demo_orders;
ERROR:  relation "demo_orders" does not exist   -- confirmed gone

# Restore a new Cluster targeting t0
RTO (restore apply to ready): 43s

# Verify against the ORIGINAL cluster's credentials (a recovered cluster's
# data - including role passwords - comes from the backup, not CNPG's own
# freshly-generated Secret for the new Cluster object)
 id |             note              |          created_at
----+-------------------------------+-------------------------------
  1 | before-failover-1             | 2026-07-29 02:16:11.155744+00
  2 | before-failover-2             | 2026-07-29 02:16:11.155744+00
  3 | before-failover-3             | 2026-07-29 02:16:11.155744+00
 34 | after-failover                | 2026-07-29 02:17:26.304457+00
 35 | last-good-row-before-incident | 2026-07-29 02:32:59.68138+00
(5 rows)
```

All five rows present, including the marker inserted seconds before the target time - and the table itself exists, meaning the `DROP TABLE` a few seconds later was correctly excluded from the restore. **RPO: zero data loss up to the chosen recovery point. RTO: 43 seconds** from applying the restore manifest to a queryable, fully-recovered cluster.

### Regression check

`k3s-smoke-test.sh`: passing, unaffected - this milestone's entire footprint is a new, isolated namespace with no wiring into `orders-lab`'s live Postgres, Orders.Api, or any other existing component.

## What this milestone deliberately doesn't do

The live `orders`/`payments` Postgres - the one actually backing every other milestone's validated behavior - was never touched. Migrating it is a legitimate next step now that this pattern is proven, but it's a distinct decision with its own risk profile (a real cutover with real downtime or dual-write complexity, on data nothing else in this lab can regenerate) that deserves to be made deliberately, not folded into "implement the HA pattern."

## Running the experiment

```bash
scripts/postgres-ha-provision.sh   # namespace, MinIO bucket/credentials, 3-instance cluster

# Failover
kubectl delete pod orders-ha-1 -n postgres-ha --grace-period=0 --force
kubectl get cluster orders-ha -n postgres-ha -w

# Backup
kubectl apply -f kubernetes/data-platform/postgres-ha-backup.yaml
kubectl get backup orders-ha-backup-1 -n postgres-ha -w

# Restore drill - copy kubernetes/data-platform/postgres-ha-restore-template.yaml,
# fill in a real recoveryTarget.targetTime, then:
kubectl apply -f <your-copy>.yaml
```
