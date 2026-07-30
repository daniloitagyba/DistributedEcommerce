# Milestone 48: Fencing Tokens for Redis Distributed Locks

## Scope

`RedisOrderCache` (`apps/src/Orders.Infrastructure/Caching/RedisOrderCache.cs`) and `RedisIdempotencyStore` (`apps/src/Orders.Infrastructure/Idempotency/RedisIdempotencyStore.cs`) both take a Redis lock (`LockTakeAsync`/`LockReleaseAsync`, a timeout-bounded `SET NX`) before doing slow work - a Postgres read, order creation - and then write the result. Neither checks whether it still holds the lock at the moment it writes. This is the exact hazard Martin Kleppmann's critique of Redlock-style locking describes: a holder paused past its lock's timeout (a slow dependency, GC, CPU contention) has no way to know it lost the lock, and writes anyway when it resumes.

## The race

1. Holder A acquires the lock, starts its slow work (e.g. a Postgres read for `RedisOrderCache`, order creation for `RedisIdempotencyStore`).
2. A stalls past the lock's timeout - the lock expires.
3. Holder B acquires the now-free lock, does its own work, writes its (correct, current) result, releases.
4. A resumes, oblivious, and writes *its* (now stale) result - silently overwriting B's newer value with old data.

For `RedisOrderCache` this means a stale cached order can be served for up to the cache TTL. For `RedisIdempotencyStore` it's sharper: two concurrent requests with the same idempotency key can each successfully create a real order in Postgres, and this race decides - by write-timing accident, not by which request actually "won" - which of the two order IDs the idempotency record points to afterward. Fencing tokens don't prevent the duplicate order creation (a different, out-of-scope fix - a unique constraint at the domain layer), but they do make the outcome deterministic and correct: whichever write actually happened *later* is the one that survives, never an earlier one arriving out of order.

## Design

`BuildingBlocks/RedisFencedWrite.cs` adds two operations, shared by both stores:

- **`NextFenceTokenAsync`** - a plain `INCR` against a per-resource sequence key (`orders:cache-fence-seq:{orderId}` / `orders:idempotency-fence-seq:{key}`), drawn once per successful lock acquisition. Strictly increasing, independent of the lock itself.
- **`FencedSetAsync`** - a Lua script (atomic, single round trip) that writes the value *and* records the token that produced it, but only if that token is not older than whatever token is already recorded. A stale write is rejected at the point of write - the only place this can actually be enforced, since neither the lock's acquisition nor its (already-expired) release has any way to know a newer holder exists.

Both `RedisOrderCache` and `RedisIdempotencyStore` now draw a token immediately after acquiring the lock and use `FencedSetAsync` instead of a raw `StringSetAsync` for the final write. A rejected write increments a new counter, `orders.redis.fenced_write_rejected` (tagged `store=order-cache|idempotency`) - expected to be near-zero in steady state; a nonzero rate under sustained load is a signal that the lock timeout is tuned too aggressively for how long the guarded work actually takes, not that anything is broken.

## Proof

Rather than a live, timing-dependent chaos demo (hitting the exact race window against a real deployment is inherently flaky), `RedisFencedWriteTests` reproduces the hazard deterministically against a real Testcontainers Redis - a stronger proof than a live demo would give, because it isn't at the mercy of scheduling luck:

```
StaleHolderWriteIsRejectedAfterNewerHolderAlreadyWrote:
  tokenA = NextFenceToken()   // holder A acquires first
  tokenB = NextFenceToken()   // A stalls; B's lock timeout fires, B acquires
  FencedSet(tokenB, "B's fresh value")  -> applied = true
  FencedSet(tokenA, "A's stale value")  -> applied = false   <- rejected
  final stored value: "B's fresh value"

WritesInAcquisitionOrderAllSucceed:
  both writes in order -> both applied, final value is the later one
```

Both pass against a real Redis container, alongside the existing `RedisOrderCacheTests` (3/3) and the full `Orders.UnitTests` suite (28/28) - the fencing change doesn't alter any previously-passing behavior, it only closes the gap where a stale write used to silently win.

## Running it

```bash
cd apps
dotnet test tests/Orders.IntegrationTests/Orders.IntegrationTests.csproj --filter 'FullyQualifiedName~RedisFencedWriteTests'
```
