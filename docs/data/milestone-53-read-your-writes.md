# Milestone 53: Read-Your-Writes Across Postgres Read Replicas

## Scope

`orders-api` runs 2 replicas, and Milestone 27's `postgres-ha` CNPG cluster has 2 streaming read replicas alongside its primary - built and proven to fail over correctly, but never actually used to serve a read. Every GET in this codebase, live or in that cluster, goes to the primary. This milestone routes a read to a replica and proves what that would actually cost: a classic read-your-writes violation, then the standard fix.

## Design: deterministic, not raced

Real replication lag on a single Docker host is sub-millisecond - not a reliable window to reproduce a stale read against, and getting "lucky" or "unlucky" on timing would make the proof unconvincing either way. Instead of racing it, Postgres's own `pg_wal_replay_pause()`/`pg_wal_replay_resume()` (native functions, not a chaos tool) pause WAL replay on one replica entirely - a deterministic, on/off lag with no timing luck involved.

Runs against Milestone 27's already-isolated `postgres-ha` cluster (`orders-ha-rw`/`-ro`/`-r` CNPG-managed services) - not the live orders/payments Postgres - same reasoning as every other isolated demo cluster built for a similarly disruptive experiment in this lab.

## Method

1. Write a row via `orders-ha-rw` (the primary), capture `pg_current_wal_lsn()` at that moment.
2. Pause WAL replay on one replica (`orders-ha-1`).
3. Write a second, distinct row via the primary; capture its LSN too.
4. Read for that second row directly against the **paused** replica - simulating an app that routed a GET there under `secondaryPreferred` immediately after the write.
5. Resume replay, then poll the replica's `pg_last_wal_replay_lsn()` until it reaches the write's LSN (`pg_wal_lsn_diff(replay_lsn, write_lsn) >= 0`) - the fix: gate the read on an LSN token, not a fixed sleep.
6. Read again.

## Results

```
Primary WAL LSN at write time: 0/B032520
[replica paused, second write happens]
Row found on paused replica: 0        <- stale, the write is invisible
[replay resumed]
Replica caught up (replay_lsn=0/B0325E0 >= write_lsn=0/B0325E0) after 1s
Row found on replica after LSN-gated wait: 1   <- correct
```

The immediate read against the replica missed a write the primary had already committed - the textbook read-your-writes hazard, produced on demand rather than argued about. Gating the read on the replica's own replay position catching up to the write's LSN - instead of just reading immediately, or sleeping some arbitrary fixed duration and hoping - made the second read correct. This is the standard fix pattern (an LSN token, or equivalently a "read-my-writes" session that pins to the primary for a bounded window after a write) for any code path that routes reads to replicas for scale but still needs a caller to see their own writes.

## What this means for this codebase

Nothing here currently routes GETs to a replica, so nothing is broken today - the CQRS read-model projection lag (`orders.projection.lag_ms`, from Milestone 13) is a related but different mechanism, measuring lag on a purpose-built async projection, not a synchronous replica read. If `orders-api`'s GET endpoints, or a future read-heavy service, ever start routing reads to `postgres-ha`'s replicas for scale, this milestone's LSN-gating pattern - or simpler, pinning a session to the primary for some bounded window right after that session's own write - is the concrete fix, not an assumption that `readPreference`-style routing is free.

## Running it

```bash
scripts/postgres-read-your-writes-test.sh
```
