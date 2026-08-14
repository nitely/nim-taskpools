# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## Spawn-then-spin: the taskpool usage pattern nimbus-eth1 relies on and this
## suite otherwise never exercises.
##
## Every other test here waits by calling `sync` (which drains the caller's own
## deque via `completeFuture` step 2.2) or `syncAll` (which drains in step 1).
## nimbus-eth1 does neither at two of its three spawn sites: it spawns a batch
## and then busy-waits on a **side channel**, so the spawning thread never
## enters the scheduler at all.
##
## Why that is a distinct regime
## -----------------------------
## The spawns come from the pool-owning thread, so the tasks land in *its own*
## Chase-Lev deque. `schedule` notifies only on the empty -> non-empty
## transition, so a batch of N tasks is published with a **single** wakeup. The
## spawner then spends a long time in application code without draining, which
## leaves the whole batch reachable only by thieves.
##
## That is the same "work stranded behind an owner who will not drain it" shape
## that #59 fixed *inside* `completeFuture`; an application-level spin loop
## bypasses that fix entirely.
##
## Modelled from (read-only analysis, no nimbus code is imported):
##
## * `execution_chain/core/executor/process_block_parallel.nim:159`
##   `withSenderParallel` - spawns one task per transaction, then spins on a
##   per-entry `senderReady` atomic while executing each transaction, and only
##   calls `sync` in a `finally` at the very end.  -> scenario A
##
## * `execution_chain/db/aristo/aristo_compute.nim:395`
##   `computeKeyImpl` - spawns up to 16 tasks, then polls `isReady()` in a loop
##   while draining side-channel queues, calling `sync` only once a future has
##   already reported ready.  -> scenario B
##
## Thread counts
## -------------
## nimbus sizes its pool `max(min(countProcessors(), 16), 2)`, so a 2-core CI
## runner gets **2 threads**: the root plus exactly one worker. That is the
## sharpest configuration for this pattern - with a single thief there is no
## wake-chain to paper over a dropped notification. `tests/utils.nim` uses
## `max(2, countProcessors())`, so the rest of the suite effectively never runs
## it. This test sweeps 2, 3, 4 and the host count.
##
## Note this is a *structural* test, not a weak-memory one: the pattern is
## architecture-independent. Build it with `-d:taskpoolsDebugStall` to get a
## per-worker dump instead of a bare hang.
##
## Tunables: -d:tpStressRounds:N -d:tpStressBudgetMs:N -d:tpStressTimeoutMs:N

{.push raises: [], gcsafe.}

import
  std/[atomics, random],
  ../../taskpools,
  ../utils,
  ./watchdog

const
  rounds {.intdefine: "tpStressRounds".} = 200_000
  budgetMs {.intdefine: "tpStressBudgetMs".} = 20_000
  timeoutMs {.intdefine: "tpStressTimeoutMs".} = 60_000
  maxBatch = 256
  maxPollBatch = 16   ## aristo spawns at most one task per nibble

type
  Entry = object
    ## One transaction's slot. `ready` is the side channel the spawner spins
    ## on - it is published by the task *before* the task finishes, exactly as
    ## `recoverAndPrefetchTask` publishes the sender before prefetching.
    ready {.align: 64.}: Atomic[bool]
    value: Atomic[int]

var
  tp: Taskpool
  entries: array[maxBatch, Entry]
  postWork: Atomic[int]     ## work the task does *after* publishing the sender
  offloaded: Atomic[int]    ## tasks not yet ready when the spawner began waiting
  batchesRun: Atomic[int]

proc senderTask(i: int): bool =
  # Publish through the side channel first, then keep working - this is what
  # makes the spawner's spin overlap with the task still holding the runtime.
  entries[i].value.store(i * 2 + 1, moRelease)
  entries[i].ready.store(true, moRelease)
  spinFor(postWork.load(moRelaxed))
  true

proc pollTask(i: int): int =
  spinFor(postWork.load(moRelaxed))
  i * 3 + 1

# Scenario A: withSenderParallel - spin on a side-channel flag
# ------------------------------------------------------------------------------

proc runSideChannelBatch(n: int, bodySpin: int) =
  for i in 0 ..< n:
    entries[i].ready.store(false, moRelease)
    entries[i].value.store(0, moRelease)

  var futs: array[maxBatch, Flowvar[bool]]

  # All N land in the spawner's own deque; only the first push wakes anybody.
  for i in 0 ..< n:
    futs[i] = tp.spawn senderTask(i)

  # Deliberately NOT sync: wait on the side channel, so the spawner never
  # enters completeFuture and never drains the deque it just filled.
  for i in 0 ..< n:
    if not entries[i].ready.load(moAcquire):
      discard offloaded.fetchAdd(1, moRelaxed)
    while not entries[i].ready.load(moAcquire):
      cpuRelax()
    doAssert entries[i].value.load(moAcquire) == i * 2 + 1
    spinFor(bodySpin)   # "body": execute the transaction

  # Only now, in the equivalent of the `finally`.
  for i in 0 ..< n:
    doAssert sync(futs[i])
  discard batchesRun.fetchAdd(1, moRelaxed)

# Scenario B: aristo computeKeyImpl - poll isReady()
# ------------------------------------------------------------------------------

proc runPollBatch(n: int) =
  var futs: array[maxPollBatch, Flowvar[int]]
  for i in 0 ..< n:
    futs[i] = tp.spawn pollTask(i)

  # Poll isReady without ever entering the scheduler, as aristo does while it
  # drains its side-channel queues.
  var pending = n
  var spun = false
  while pending > 0:
    pending = 0
    for i in 0 ..< n:
      if not futs[i].isReady():
        inc pending
    if pending > 0:
      spun = true
      cpuRelax()
  if spun:
    discard offloaded.fetchAdd(1, moRelaxed)

  for i in 0 ..< n:
    doAssert sync(futs[i]) == i * 3 + 1
  discard batchesRun.fetchAdd(1, moRelaxed)

# Drivers
# ------------------------------------------------------------------------------

proc sweep(threads: int, wd: var Watchdog) {.raises: [CatchableError].} =
  let budget = startBudget(budgetMs)
  var
    rng = initRand(0xB10CC5 + threads)
    done = 0

  tp = Taskpool.new(threads)
  for r in 0 ..< rounds:
    # Sweep batch size and the two spin lengths so the spawner's arrival at the
    # side channel lands all over the tasks' lifetimes.
    postWork.store(rng.rand(128), moRelaxed)
    runSideChannelBatch(1 + rng.rand(min(maxBatch, 64) - 1), rng.rand(64))
    runPollBatch(1 + rng.rand(maxPollBatch - 1))
    inc done
    wd.beat()
    if budget.expired():
      break
  tp.syncAll()
  tp.shutdown()

  echo "  threads=", threads, ": ", done, " rounds in ", budget.elapsedMs(), " ms"

proc churn(threads: int, wd: var Watchdog) {.raises: [CatchableError].} =
  ## nimbus creates a pool per EEST test unit and shuts it down again, so the
  ## pool is torn down right after doing real work with deep deques and workers
  ## mid-steal - unlike tests/stress/test_shutdown.nim, which churns pools that
  ## have only ever run trivial tasks.
  let budget = startBudget(budgetMs)
  var
    rng = initRand(0xC40FFE + threads)
    done = 0

  for r in 0 ..< rounds:
    tp = Taskpool.new(threads)
    postWork.store(rng.rand(128), moRelaxed)
    for _ in 0 .. rng.rand(3):
      runSideChannelBatch(1 + rng.rand(15), rng.rand(32))
      runPollBatch(1 + rng.rand(maxPollBatch - 1))
    tp.syncAll()
    tp.shutdown()
    inc done
    wd.beat()
    if budget.expired():
      break

  echo "  threads=", threads, ": ", done, " create/work/shutdown cycles in ",
       budget.elapsedMs(), " ms"

proc main() {.raises: [CatchableError].} =
  # 2 is the nimbus-on-a-2-core-runner case: root plus a single thief, no
  # wake-chain to recover a dropped notification.
  let threadCounts = [2, 3, 4, numThreads()]

  echo "spawn-then-spin (side channel + isReady polling)"
  block:
    var wd: Watchdog
    wd.start("spawn-then-spin batches", timeoutMs)
    for t in threadCounts:
      sweep(t, wd)
    wd.stop()

  echo "pool churn with real work in flight"
  block:
    var wd: Watchdog
    wd.start("spawn-then-spin pool churn", timeoutMs)
    for t in threadCounts:
      churn(t, wd)
    wd.stop()

  echo "batches=", batchesRun.load(moAcquire),
       " offloaded-waits=", offloaded.load(moAcquire),
       " (times the spawner had to wait on a task it was holding in its deque)"
  doAssert offloaded.load(moAcquire) > 0,
    "the spawner never had to wait: the batch always completed before it looked, " &
    "so the pattern under test was not exercised (raise -d:tpStressRounds)"

main()

{.pop.}
