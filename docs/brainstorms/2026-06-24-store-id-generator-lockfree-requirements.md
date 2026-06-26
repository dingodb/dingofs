---
date: 2026-06-24
topic: store-id-generator-lockfree
---

# Lock-free fast path for StoreAutoIncrementIdGenerator

## Problem Frame

`StoreAutoIncrementIdGenerator` (`src/mds/filesystem/id_generator.cc:222`) holds a
single `bthread_mutex_` for the entire `GenID` call. It backs the **slice-id
generator** — the hottest allocation path, since every write allocates slices
(`src/mds/filesystem/filesystem.cc:1292,2218,2891,2909,4172`; created at
`id_generator.cc:584`) — and the fs-id generator (`id_generator.cc:558`).

Two costs hurt under high concurrency:
1. Every handout (even the trivial `next_id_ += num` fast path) serializes on the
   single mutex, causing cache-line ping-pong and bthread mutex overhead.
2. When a bundle is exhausted, the refilling thread runs the slow KV transaction
   `AllocateIds` (a remote txn with a retry loop) **while still holding the
   mutex**, blocking every other caller for the full storage round-trip.

A sibling class `ShardStoreAutoIncrementIdGenerator` already shards inode IDs
across 64 RWLocks (`id_generator.cc:401`), but the user chose to keep a **single
counter** for the store generator to preserve near-monotonic IDs with minimal
gaps, replacing the mutex with an atomic bump-allocator.

## Requirements

**Hot path (lock-free)**
- R1. The steady-state `GenID` path (bundle has room) must allocate IDs without
  acquiring any mutex, using an atomic compare-and-swap loop on `next_id_`.
- R2. The `min_slice_id` floor must be honored: the returned id must be
  `>= min_slice_id`, computed as `start = max(next_id_, min_slice_id)` inside the
  CAS loop so the floor composes with the atomic claim without burning IDs.
- R3. Boundary races (multiple threads near bundle exhaustion) must not "burn" or
  skip IDs beyond the necessary batch gaps — a failed CAS retries rather than
  consuming an ID. (This rules out a plain `fetch_add`, which hands out IDs past
  `last_alloc_id_` to racing threads.)

**Refill path (non-blocking, rare)**
- R4. When the bundle is exhausted, **exactly one** thread performs the storage
  transaction, elected via an in-flight refill flag/mutex with a double-check so
  concurrent exhausted threads do not issue redundant transactions.
- R5. Exhausted threads that are **not** the elected refiller must **not block on
  the storage IO**. They back off (yield/retry) and re-attempt the lock-free fast
  path; once the refiller publishes the new `last_alloc_id_` / `next_id_` they
  succeed without having issued their own txn. This is the core fix: no caller is
  ever blocked behind remote KV latency while holding or waiting on an IO lock.
- R6. Memory ordering must ensure a thread that observes a grown `last_alloc_id_`
  also observes the consistent `next_id_` bundle start (publish `next_id_` before
  `last_alloc_id_`; use acquire/release as appropriate).
- R9. When exhaustion is triggered by a `min_slice_id` above the current bundle,
  the refill reservation start must be computed from the same floor:
  `start = max(observed next_id_, observed last_alloc_id_, persisted counter, min_slice_id)`
  before publishing the new bundle, so the floor is preserved and the refill does
  not loop or allocate below `min_slice_id`.
- R10. Refill-failure semantics: nothing is published to `next_id_` /
  `last_alloc_id_` until the storage transaction commits. On failure, the
  in-flight refill flag is cleared so a later caller can retry; waiting callers
  return `false` (matching today's behavior) rather than spinning indefinitely,
  and no partial publication or ID reuse can occur.
- R11. Bound the CAS / refill-wait retry: a caller spinning on a failed CAS or
  waiting for an in-flight refill must yield/back off (bthread-friendly) and have
  a bounded number of attempts, so heavy contention or a slow/failing refill
  cannot livelock or starve bthreads.

**Preserve existing behavior**
- R7. Preserve the existing guards and external contract: `num == 0` rejected,
  `is_destroyed_` handling, `Init`/`Destroy`/`Describe`, and the
  `GenID(num, id)` → `GenID(num, 0, id)` delegation.
- R8. IDs remain globally unique and persisted-monotonic (the storage counter is
  monotonically advanced to reserve ranges and is never behind any handed-out ID),
  with gaps no larger than today's batch-boundary gaps (a far-ahead `min_slice_id`
  is an intentional, allowed gap).
- R12. Lifecycle safety: removing the mutex from the fast path must not introduce a
  `Destroy`/`GenID` data race. Define the rule explicitly — either `Destroy` is
  only called after callers are quiesced, or `is_destroyed_` is an atomic checked
  such that a `GenID` that passes the check cannot hand out an ID after the key is
  deleted. Whichever rule is chosen must be stated and tested.

## Success Criteria
- No mutex is acquired on the common-case allocation path (verifiable by reading
  the code / profiling); the mutex is taken only on bundle refill.
- A multi-threaded test (many bthreads calling `GenID` concurrently) produces a
  set of IDs with **no duplicates**, all `>= min_slice_id` when a floor is passed,
  and matches the count requested.
- A refill-boundary concurrency test forces repeated bundle exhaustion under many
  bthreads and verifies: no duplicate IDs, exactly one storage transaction per
  refill (no redundant txns), mixed `num` sizes across the boundary, a
  `min_slice_id` above the current bundle, persisted-counter advancement after
  refill, and max-gap assertions against current behavior.
- Existing `test/unit/mds/filesystem/test_id_generator.cc` still passes; a new
  concurrency test for the store generator is added.
- Measurable throughput improvement under concurrent slice allocation vs. the
  current single-mutex implementation, measured with a defined benchmark
  (workload, thread/core counts, baseline) reporting throughput, CAS-retry counts,
  and p99 latency. The result is also compared against the existing sharded
  generator so the single-atomic-vs-sharding tradeoff is quantified.

## Scope Boundaries
- Only `StoreAutoIncrementIdGenerator` changes. `CoorAutoIncrementIdGenerator`
  and `ShardStoreAutoIncrementIdGenerator` are left as-is.
- No change to the on-disk format, the `MetaCodec` key/value encoding, the
  `IdGenerator` interface, or the `min_slice_id` semantics.
- Not consolidating the duplicate store/shard classes in this effort.

## Key Decisions
- **Single atomic counter over sharding**: keeps IDs near-monotonic with minimal
  gaps and simpler reasoning. Accepted tradeoff — the hot path is still a single
  shared atomic cache line, so it scales less than 64-way sharding under extreme
  core counts. This is acceptable because the dominant cost being removed is the
  mutex held during remote IO, not raw atomic throughput. Benchmarks (see Success
  Criteria) must confirm the win; if the single atomic fails the targets, fall
  back to the sharded approach.
- **CAS loop, not `fetch_add`**: needed to honor `min_slice_id` and to avoid
  burning IDs on boundary races (R2, R3).
- **Non-blocking refill**: only the elected refiller touches storage; other
  exhausted callers back off and retry the fast path rather than queuing behind
  the IO. This is what actually eliminates the original convoy (R4, R5, R10).

## Alternatives Considered
- **Just release the mutex around `AllocateIds`** (keep the rest of the single-mutex
  design): smallest change, removes the lock-during-IO convoy, but every handout
  still serializes on the mutex — only a partial fix for the high-concurrency
  complaint. Rejected as insufficient on its own.
- **Reuse `ShardStoreAutoIncrementIdGenerator` for slice/fs IDs** (already used by
  inodes): proven, scales best under high core counts. Rejected here because it
  hands IDs from 64 independent bundles, giving non-monotonic IDs with larger
  gaps; the user prefers near-monotonic slice IDs. Kept as the fallback if
  benchmarks show the single atomic does not scale.
- **Atomic bump-allocator with non-blocking refill** (chosen): lock-free fast path,
  near-monotonic IDs, no caller blocked on IO. Higher concurrency-correctness risk
  than reuse, mitigated by the explicit ordering/failure/lifecycle requirements
  and the concurrency tests above.

## Dependencies / Assumptions
- `AllocateIds` already advances the persisted counter to `start + size`; the
  refill path reuses this logic, adapted to publish into atomics only after a
  successful commit (R10).
- `min_slice_id` is only non-zero on the `AllocSliceId` RPC path
  (`src/mds/service/mds_service.cc:2126`); inode/fs paths pass 0.

## Outstanding Questions

### Deferred to Planning
- [Affects R6][Technical] Exact atomic types/orderings (`std::atomic<uint64_t>`
  for both `next_id_` and `last_alloc_id_`) and how `Describe`/`Destroy` read them
  consistently.
- [Affects R11][Technical] Concrete backoff strategy for CAS losers and
  refill-waiters (bthread yield vs. short sleep) and the retry bound.
- [Affects Success Criteria][Needs research] Benchmark workload, baseline, core
  counts, and the minimum acceptable throughput/p99 thresholds that decide whether
  the single atomic is kept or the sharded fallback is adopted.

## Next Steps
-> /ce-plan for structured implementation planning
