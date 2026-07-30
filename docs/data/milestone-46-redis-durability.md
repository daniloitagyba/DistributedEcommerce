# Milestone 46: Redis Durability for Cart.Service, Measured Not Assumed

## Scope

Milestone 42 promoted Redis to the system of record for carts - no Postgres fallback, no cache-aside factory, "if this data is lost, the cart is simply gone" (`CartStore`'s own class comment calls this an acceptable trade for ephemeral, low-value state). What was never measured, in the four milestones since: exactly how much is lost, and how, when Redis actually crashes. `compose/compose.yaml`'s `redis` service has run `--save "" --appendonly no` since Milestone 1 - persistence fully off - inherited from when Redis was only ever a cache (Milestone 9) in front of durable Postgres, where losing the cache costs a slower read, not data. That assumption stopped being true at Milestone 42 and nobody revisited the config.

This milestone kills Redis mid-load against real carts and counts survivors, then compares that against AOF `everysec` and `always`, with real loss counts and real latency numbers - not the "RPO should be about a second" hand-wave.

## Method

`scripts/cart-redis-durability-test.sh <mode> <cart_count>`: creates N carts against the live `cart-service` ClusterIP (20-way parallel `PUT`), `docker kill -s SIGKILL`s the `redis` container mid-flight (a hard crash, no graceful save - not `docker stop`, which would give Redis a chance to save on its own terms), waits for it to come back, then re-reads all N carts and counts how many still have their item.

## What didn't work: `CONFIG SET` doesn't survive the restart being tested

First attempt configured each persistence mode at runtime via `redis-cli CONFIG SET appendonly yes` etc., then killed the process. **Every mode - including AOF `always` - showed 300/300 carts lost.** That result would have meant AOF doesn't work at all, which is wrong, and the real bug was in the test: Docker's `restart: unless-stopped` policy restarts the *same* container by re-executing its literal `command:` from `compose.yaml`. That command still said `--appendonly no` - a `redis-cli CONFIG SET` only changes the running process's in-memory config, and `CONFIG REWRITE` (which would persist it) requires the server to have been started from a config *file*, not command-line args, so it silently no-ops here. The runtime toggle never survived the very restart the test depends on.

Fixed by parameterizing the mode into `compose.yaml`'s actual startup command (`REDIS_SAVE`/`REDIS_APPENDONLY`/`REDIS_APPENDFSYNC` env vars) and using `docker compose up -d --force-recreate redis` to change mode, so the mode being tested is what the container actually restarts with - not a config a restart discards. Worth documenting on its own: any Redis-in-Docker durability config that's only ever applied via `CONFIG SET` (a runbook step, an operator script) has this exact hole and will look correct until the container actually restarts.

## Results

300 carts created, then `redis` hard-killed mid-flight, for each mode:

| Mode | Carts lost | Recovery time |
|---|---|---|
| `--save "" --appendonly no` (the config since Milestone 1) | **300/300 (100%)** | 5.8s |
| RDB `save 60 1` | **300/300 (100%)** | 5.8s |
| AOF `appendfsync everysec` | **0/300 (0%)** | 5.7s |
| AOF `appendfsync always` | **0/300 (0%)** | 5.7s |

The RDB result isn't a bug - it's RDB behaving exactly as documented. `save 60 1` snapshots at most once every 60 seconds; killing within a few seconds of the writes means no snapshot had happened yet, so there was nothing on disk to recover. RDB's protection is bounded by the snapshot interval, full stop - it protects data *older* than the last snapshot, not data written since. A cart store crashing shortly after a burst of writes is exactly the case RDB doesn't cover.

Neither AOF mode lost anything in this test, at any cart count down to 10 (tried explicitly to catch `everysec`'s theoretical up-to-1-second exposure window - a background thread fsyncs roughly once per second regardless of write timing, so a crash landing before the first tick could in principle lose that window's writes; this test's timing never landed inside it). That theoretical gap is real and documented Redis behavior, just not one this harness could reliably reproduce in a shell script - the latency measurement below is what actually distinguishes the two modes.

**Latency** (`PUT /carts/{id}/items/{sku}`, 200 sequential requests per mode, live ClusterIP):

| Mode | p50 | p95 | p99 | max |
|---|---|---|---|---|
| No persistence (baseline) | 4.0ms | 5.3ms | 11.0ms | 41.8ms |
| AOF `everysec` | 4.2ms | 5.6ms | 6.0ms | 6.7ms |
| AOF `always` | 4.4ms | 9.7ms | **70.5ms** | 70.6ms |

`everysec` costs nothing measurable over no persistence at all - the fsync happens on a background thread, off the request path. `always` fsyncs on every write, in line, and it shows: a ~6-12x p99 tax for a durability window that, per the loss numbers above, didn't measurably improve on `everysec` in this test. `everysec`'s trade is the right default for this workload: durability for a workload that tolerates losing at most ~1 second of writes on a crash that also requires a pod-level retry anyway, at effectively the same latency as no persistence.

## Fix applied

`compose/compose.yaml`'s `redis` service now defaults to `--appendonly yes --appendfsync everysec` (previously `--appendonly no`), with a named `redis-data` volume mounted at `/data` - without one, AOF durability is fake on any Redis upgrade or `docker compose down`, since the file would live only in the ephemeral container layer. Mode stays parameterized via `REDIS_SAVE`/`REDIS_APPENDONLY`/`REDIS_APPENDFSYNC` so this test script keeps being able to flip it. Re-ran the full 300-cart kill test against the new *default* config (no env override) as final validation: **0/300 lost**, 5.7s recovery.

## Running it

```bash
scripts/cart-redis-durability-test.sh none 300           # the old default - 100% loss
scripts/cart-redis-durability-test.sh rdb 300            # periodic snapshot - still 100% loss on a fresh-write crash
scripts/cart-redis-durability-test.sh aof-everysec 300    # the new default - 0% loss
scripts/cart-redis-durability-test.sh aof-always 300      # 0% loss, real p99 latency tax, no measured durability win here
```
