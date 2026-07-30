# Milestone 57: Jepsen/Elle-Style Linearizability Check of Inventory

## Scope

`scripts/inventory-reservation-concurrency-test.sh` (Milestone 41) proves oversell-safety by checking the **final** stock count after a burst of concurrent reservations - it can't distinguish a correct execution from a broken one that happens to net the same final number (e.g., two overlapping reservations both incorrectly succeeding while a later one incorrectly fails, cancelling out in the total). This milestone records the full operation **history** - every reservation's real invocation and completion wall-clock time, and its outcome - and checks it against the actual definition of linearizability for this resource, not just its ending count.

## The model, and the actual correctness condition

Inventory reservation, for a single SKU, behaves like a decrement-only counting resource: each request either decrements it by 1 (`reserved=true`) or leaves it unchanged (`reserved=false`), starting from a known initial quantity. For exactly this resource shape, a history is linearizable if and only if:

1. **No successful reservation is forced, by real-time precedence, to occur *after* a failed one** - i.e., no operation that failed can have already completed before a later one that succeeded started. Such a pair would mean the resource became *more* available after a rejection with nothing releasing stock in between - impossible for a decrement-only counter, and a genuine violation, not an artifact of how it's checked.
2. **Total successes never exceed the initial quantity.**

Given (1), any topological order placing every successful operation before every failed one is consistent with the observed real-time constraints, and simulating the counter under exactly that shape always reproduces every observed outcome - this is checked directly in `O(n^2)` (every pair, once), not by enumerating orderings. Worth stating plainly: this is **not** a general-purpose linearizability checker like Jepsen's Knossos or Elle - it's the exact, provably sufficient condition for this specific resource model (a single decrementing counter, no releases in the tested window), scoped intentionally rather than reimplementing general-purpose machinery.

## What didn't work: the first version of the checker was wrong, not the system

The first implementation picked *one* candidate ordering - completion order - as a stand-in for "a valid linearization" and checked only that. Against a 25-request burst for 15 available units, it reported `NOT LINEARIZABLE` - but investigating showed all 25 requests were invoked within ~3ms of each other, well before any of them completed (a ~115ms spread later): every pair of operations was mutually concurrent, with no real-time ordering constraint between any of them at all. Completion order (driven by which Kafka reply-topic partition a given reply happened to land on, not by the true decision order inside Inventory.Service) was simply the wrong witness to check - rejecting it proved nothing about the system, only that one arbitrarily chosen candidate ordering didn't fit. Replaced with the direct pairwise condition above, which is correct for any `n` without needing to guess a witness ordering at all.

## Results

**9 concurrent requests against 15 available units** (no contention): all 9 reserved, linearizable.

**25 concurrent requests against 15 available units** (real contention - the interesting case):

```
Successful reservations: 15 (initial quantity: 15)
==> LINEARIZABLE: a valid sequential ordering (respecting real-time precedence)
    reproduces every observed outcome.
```

Exactly 15 of 25 succeeded, none oversold, and - the actual point of this milestone over Milestone 41's final-count check - no pair of operations violated real-time precedence (no failed reservation "unfailed" itself relative to a later success). The checker's own correctness was verified against two synthetic histories before trusting it against the real system: a hand-built history with a real-time-forced false-then-true pair correctly came back `NOT LINEARIZABLE`, and one without it correctly came back `LINEARIZABLE`.

## Running it

```bash
docker run --rm --network <compose-project>_backend -v "$(pwd)/scripts":/scripts <python-image-with-confluent-kafka> \
  python3 /scripts/inventory_linearizability_check.py <sku> <initial_quantity> <concurrent_request_count>
```

Seed the SKU's `available_quantity` in Postgres to a known value first; the script reports the full history and the linearizability verdict, exiting non-zero if either the oversell invariant or linearizability fails.
