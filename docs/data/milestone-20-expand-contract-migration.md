# Milestone 20 Zero-Downtime Expand/Contract Schema Migration

## Scope

Every prior schema migration in this lab has been additive - a new column, a new index, a new table - the kind of change that's safe by construction. This milestone validates a genuinely breaking one under continuous live traffic: `orders.amount` (`numeric(18,2)`, dollars/reais) becomes `orders.amount_cents` (`bigint`, integer cents) - the classic real-world motivation being that storing money as a floating-point-adjacent decimal invites rounding bugs that an integer-cents representation avoids. The point isn't the specific representation change; it's proving the four-phase expand/contract sequence (expand -> dual-write -> backfill -> cut over -> contract) can run against a live, 191,619-row table with zero failed requests at every step, not just zero *planned* downtime.

## Design

- **The domain model never changes.** `Order.Amount` stays `decimal` throughout - in the C# entity, the HTTP API (`OrderResponse.Amount`), and the Avro wire contract from Milestone 19 (which already encodes amount as a string, not tied to any particular Postgres representation). This migration is entirely contained inside `OrdersDbContext`'s EF mapping, which is exactly the payoff of Milestone 18's layering: a storage-representation change is invisible outside `Orders.Infrastructure`.
- **Phase 1, expand + dual-write, shipped as one deploy** (a legitimate simplification of the canonical 5-step pattern used in real practice when the new column is nullable and nothing reads it yet): a migration adds `amount_cents bigint NULL`, and `OrdersDbContext` gains an EF shadow property (`Property<long?>("AmountCents")`) populated via a `SaveChangesAsync` override on every insert/update - infrastructure-only, no `Order.cs` change.
- **Phase 2, backfill**, is pure SQL (`scripts/expand-contract-backfill.sh`), not a deploy: a loop of `UPDATE ... LIMIT 5000 ... FOR UPDATE SKIP LOCKED` batches against `WHERE amount_cents IS NULL`, small enough per-batch to avoid a long-held lock, looping until zero rows remain pending.
- **Phase 3, cutover**, replaces the shadow property with `Order.Amount`'s *primary* EF mapping switching to `amount_cents` via `HasConversion(amount => cents, cents => amount)` - one property, one column, both read and write, no more shadow state.
- **Phase 4, contract**, drops the now-fully-unused `amount` column.
- **`dotnet-ef` (installed fresh on the server for this milestone)** generated the expand migration correctly from a real model diff, but produced *empty* `Up()`/`Down()` bodies for both the cutover and contract migrations - the model diff for "column renamed with a value converter" and "drop an already-unmapped column" isn't something the tool can infer purely from `OnModelCreating`, since nothing in the C# model references the old column at that point. Both migrations needed their DDL operations hand-written directly against `MigrationBuilder`, using the pattern the tool got right on the first, genuinely-inferable migration as a template.

## What didn't work

**Argo CD's `selfHeal` had nothing to fight this time - a deliberate check, not luck.** Every phase here changes only image content under the same static tag (`milestone-7`), never a Kubernetes resource spec, so unlike Milestone 19 there was no manifest drift for Argo CD to revert mid-deploy. Confirmed directly rather than assumed.

**Cutting over reads without relaxing the old column's constraint breaks INSERT, not just SELECT.** The cutover-phase integration test failed immediately with `23502: null value in column "amount" of relation "orders" violates not-null constraint` - `amount` had carried `NOT NULL` since the very first migration in this repo, and simply making the application stop writing to it (because `Order.Amount` no longer maps to that column at all) doesn't relax a constraint the column still enforces. This would have broken every new order on the live system the moment the cutover deployed, not just in the test database - caught by the integration test suite before it ever reached the live deploy, which is exactly the kind of thing that test exists to catch. Fixed by relaxing `amount` to nullable as part of the cutover migration (not the contract migration that drops it) - the column stays present and inspectable for one more phase, just no longer required.

**`dotnet ef migrations remove` followed by `migrations add` doesn't reliably regenerate the master model snapshot in one clean pass when files are being hand-edited and synced between two machines.** After hand-editing the first cutover migration to defer the `DropColumn`, re-syncing the *entire* `Migrations/` directory from the Mac back to the server (to push that edit) also overwrote the master `OrdersDbContextModelSnapshot.cs` with a stale local copy from before the edit - `dotnet ef migrations add` had correctly updated it on the server, but that update was never pulled back before the next push. The result was EF Core's `PendingModelChangesWarning` failing three integration tests with a real, if initially confusing, signal: the snapshot and the live model genuinely disagreed. Fixed by removing and cleanly regenerating the migration, this time syncing the *whole* migrations directory in both directions rather than cherry-picking individual files - the actual lesson being that model snapshot files are a single shared source of truth and can't be safely partial-synced.

## Results

### Phase-by-phase validation, all under continuous k6 load

| Phase | Load profile | Result |
| --- | --- | --- |
| Expand + dual-write (deploy) | `soak`, 5 VUs, 5m | `failed_rate=0`, 0/1444 interrupted iterations; dual-write confirmed correct on new rows (`amount=49.90` -> `amount_cents=4990`) |
| Backfill (pure SQL, no deploy) | `baseline`, 10 VUs, ~70s, concurrent | `failed_rate=0`; 191,505 pending rows backfilled in **13.5 seconds** with zero measurable effect on request latency (`create_p99_ms=23.2`, `get_p99_ms=7.0`) |
| Cutover (deploy) | `soak`, 5 VUs, 5m | `failed_rate=0`, 0/1436 interrupted iterations; live spot-check confirmed `amount` NULL / `amount_cents` populated on new rows, reads correctly reconstructing the decimal via the value converter |
| Contract / drop column (deploy) | `baseline`, 10 VUs, ~70s | `failed_rate=0`, 0/446 interrupted iterations, no threshold crossings at all - `\d orders` confirms only `amount_cents bigint NOT NULL` remains |

Every phase shows `failed_rate=0` - the actual zero-downtime claim. A couple of phases (expand, cutover) briefly crossed a k6 p99 latency *threshold* during the literal moment of pod replacement (e.g. `get_p99_ms=942` vs a 750ms threshold) - consistent with the tail-latency variance already documented for ordinary rolling restarts elsewhere in this lab, not a fault in the migration itself.

### Final state

| Check | Result |
| --- | --- |
| `dotnet test` (all 4 migrations applied in sequence on a fresh database) | 24 + 7 passing |
| `k3s-smoke-test.sh` (post-contract) | Passed |
| `k6-run.sh saga` (post-contract) | `failed_rate=0`; `saga_correct_outcome_rate=99.50%` (399/401) - consistent with this lab's already-documented pre-existing tail-latency flakiness in the saga poll window |
| `\d orders` | `amount` column gone; `amount_cents bigint NOT NULL` is the only money column |

## Running the experiment

```bash
# Phase 1 (expand + dual-write): after adding the shadow-property migration
scripts/k3s-build-images.sh && scripts/k3s-deploy.sh

# Phase 2 (backfill): pure SQL, safe to run against live traffic
scripts/expand-contract-backfill.sh

# Phase 3 (cutover): after switching Amount's EF mapping to amount_cents
scripts/k3s-build-images.sh && scripts/k3s-deploy.sh

# Phase 4 (contract): after adding the DropColumn migration
scripts/k3s-build-images.sh && scripts/k3s-deploy.sh
```
