# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## Weak-memory stress test, driven through the public taskpools API.
##
## Same failure modes as `test_arm_flowvar_wakeup` and `test_arm_backoff_wakeup`
## but reached the way a user would reach them. Each scenario is built so that a
## thread is *forced* onto the parking path rather than reaching it by luck.
##
## 1. `external`  - external (non-worker) threads spawn and immediately sync.
##                  An external thread has no deque and nothing to steal, so
##                  `completeFuture` always takes the
##                  `sleepUntilReady(waiterID = 0)` branch. The task body is
##                  empty, so the worker's `setCompleted()` lands right on top
##                  of the external thread's registration. This is the tightest
##                  reproducer of the flowvar handshake bug via the public API.
##
## 2. `worker`    - a worker parks on a flowvar (`waiterID = ctx.id`).
##                  A parent task spawns a child, waits until *another* worker
##                  has actually picked the child up (so the parent's own deque
##                  is empty and there is nothing left to steal), then releases
##                  the child right as it calls `sync`. That forces
##                  `completeFuture` down to the `sleepUntilReady(ctx.id)`
##                  branch with the child completing at the same instant.
##
## 3. `shutdown`  - create/spawn/shutdown churn. This is the *only* coverage of
##                  the `shutdown` path: `syncAll` only ever calls `wake()` and
##                  never sets the terminate flags, whereas `shutdown` stores
##                  them and then relies on a single `wakeAll()` with no retry.
##                  A worker that misses it never re-checks `terminate` and
##                  `barrier.wait()` hangs forever.
##
##                  It is also the most expensive scenario per race - it spawns
##                  and joins a whole pool every round - and the least dense:
##                  `EventCount.wakeAll` itself is hammered far harder, and
##                  without any thread churn, by test_arm_backoff_wakeup. So it
##                  is kept deliberately short (`-d:tpStressShutdownMs`) and the
##                  pool is capped: what a shutdown race needs is a high *rate*
##                  of shutdowns, not big pools.
##
## Every scenario stops at whichever comes first, its iteration count or its
## time budget, so wall time does not explode on a many-core machine.
##
## Tunables: -d:tpStressIters:N -d:tpStressRounds:N
##           -d:tpStressBudgetMs:N (per scenario)
##           -d:tpStressShutdownMs:N (pool churn only, defaults to budget/8)
##           -d:tpStressTimeoutMs:N (watchdog stall threshold)

{.push raises: [], gcsafe.}

import
  std/[atomics, random],
  ../../taskpools,
  ../utils,
  ./watchdog

const
  iters {.intdefine: "tpStressIters".} = 200_000
  rounds {.intdefine: "tpStressRounds".} = 100_000
  budgetMs {.intdefine: "tpStressBudgetMs".} = 20_000
  # Pool churn is the most expensive scenario per race (it spawns and joins a
  # whole pool per round) and the *least* dense: `EventCount.wakeAll` itself is
  # hammered ~100x harder by test_arm_backoff_wakeup. Keep it as a thin
  # integration check over the real `shutdown` path and give it a small slice.
  shutdownBudgetMs {.intdefine: "tpStressShutdownMs".} = budgetMs div 8
  timeoutMs {.intdefine: "tpStressTimeoutMs".} = 60_000
  maxExternal = 16
  maxPairs = 64

var tp: Taskpool

# Scenario 1: external threads parking on a flowvar (waiterID = 0)
# ------------------------------------------------------------------------------

type
  ExtState = object
    quitting {.align: 64.}: Atomic[bool]
    notReady {.align: 64.}: Atomic[int]
    total {.align: 64.}: Atomic[int]

var ext: ExtState

proc trivial(x: int): int =
  # Deliberately empty: we want `setCompleted` to fire as close as possible to
  # the awaiting thread's registration.
  x

proc externalFn(s: ptr ExtState) {.thread.} =
  var i = 0
  while not s.quitting.load(moAcquire):
    let fv = tp.spawn trivial(i)
    if not fv.isReady:
      discard s.notReady.fetchAdd(1, moRelaxed)
    doAssert sync(fv) == i
    discard s.total.fetchAdd(1, moRelease)
    inc i

proc stressExternalSync(numExternal: int, wd: var Watchdog)
       {.raises: [ResourceExhaustedError].} =
  var threads: array[maxExternal, Thread[ptr ExtState]]
  for i in 0 ..< numExternal:
    createThread(threads[i], externalFn, addr ext)

  let
    budget = startBudget(budgetMs)
    target = numExternal * iters
  var
    last = 0
    spins = 0

  while true:
    let now = ext.total.load(moAcquire)
    if now != last:
      last = now
      wd.beat()
    if now >= target:
      break
    inc spins
    if (spins and 0x3FF) == 0 and budget.expired():
      break
    cpuRelax()

  ext.quitting.store(true, moRelease)
  for i in 0 ..< numExternal:
    joinThread(threads[i])

  echo "external sync: ", ext.total.load(moAcquire), " spawn+sync across ",
       numExternal, " external threads in ", budget.elapsedMs(), " ms, ",
       ext.notReady.load(moAcquire), " reached the parking path"
  doAssert ext.notReady.load(moAcquire) > 0,
    "no sync ever blocked: the parking path was never exercised"

# Scenario 2: a worker parking on a flowvar (waiterID = worker id)
# ------------------------------------------------------------------------------

type
  Cell = object
    started {.align: 64.}: Atomic[bool]
    gate: Atomic[bool]
    jitter: Atomic[int]

var
  cells: array[maxPairs, Cell]
  workerParked: Atomic[int]

proc child(i: int): int =
  # Tell the parent we are running on a *different* worker than it is.
  cells[i].started.store(true, moRelease)
  while not cells[i].gate.load(moAcquire):
    cpuRelax()
  spinFor(cells[i].jitter.load(moRelaxed))
  i

proc parent(i: int): int =
  let fv = tp.spawn child(i)

  # Wait until another worker actually stole the child. From here our own deque
  # is empty and every other deque is empty too, so `sync` below has no work to
  # steal and no work of its own: it must park on the flowvar.
  while not cells[i].started.load(moAcquire):
    cpuRelax()

  # Release the child as we head into `sync`, so its `setCompleted()` races our
  # `sleepUntilComplete()`. The child's jitter sweeps the relative timing.
  cells[i].gate.store(true, moRelease)

  if not fv.isReady:
    discard workerParked.fetchAdd(1, moRelaxed)
  doAssert sync(fv) == i
  i

proc stressWorkerPark(pairs: int, wd: var Watchdog) =
  var
    rng = initRand(0xBADC0DE)
    fvs: array[maxPairs, Flowvar[int]]
    done = 0

  let budget = startBudget(budgetMs)

  for r in 0 ..< rounds:
    for i in 0 ..< pairs:
      cells[i].started.store(false, moRelease)
      cells[i].gate.store(false, moRelease)
      cells[i].jitter.store(rng.rand(96), moRelaxed)
    for i in 0 ..< pairs:
      fvs[i] = tp.spawn parent(i)
    for i in 0 ..< pairs:
      doAssert sync(fvs[i]) == i
    inc done
    wd.beat()
    if budget.expired():
      break

  echo "worker park: ", done, " rounds x ", pairs, " pairs in ",
       budget.elapsedMs(), " ms, ", workerParked.load(moAcquire),
       " parents reached the parking path"
  doAssert workerParked.load(moAcquire) > 0,
    "no parent ever blocked: the worker parking path was never exercised"

# Scenario 3: create / shutdown churn
# ------------------------------------------------------------------------------

proc nothing() =
  discard

proc retint(): int =
  42

proc stressShutdown(poolThreads: int, wd: var Watchdog)
       {.raises: [CatchableError].} =
  let budget = startBudget(shutdownBudgetMs)
  var done = 0

  for _ in 0 ..< rounds:
    var pool = Taskpool.new(poolThreads)
    pool.spawn nothing()
    doAssert sync(pool.spawn(retint())) == 42
    pool.syncAll()
    pool.shutdown()
    inc done
    wd.beat()
    if budget.expired():
      break

  echo "shutdown churn: ", done, " create/shutdown cycles of ", poolThreads,
       " threads in ", budget.elapsedMs(), " ms"

proc main() {.raises: [CatchableError].} =
  let n = numThreads()

  block:
    var wd: Watchdog
    wd.start("taskpool external spawn+sync", timeoutMs)
    tp = Taskpool.new(n)
    stressExternalSync(min(maxExternal, max(2, n div 2)), wd)
    tp.syncAll()
    tp.shutdown()
    wd.stop()

  block:
    # A parent spins while its child runs, so a pair needs two runners. Keep one
    # runner spare so the pool cannot starve itself.
    let pairs = max(1, min(maxPairs, (n - 1) div 2))
    var wd: Watchdog
    wd.start("taskpool worker parked on flowvar", timeoutMs)
    tp = Taskpool.new(n)
    stressWorkerPark(pairs, wd)
    tp.syncAll()
    tp.shutdown()
    wd.stop()

  when not defined(windows):
    # TODO: leaks thread handles on Windows, see nim-lang/Nim#23350
    var wd: Watchdog
    wd.start("taskpool create/shutdown churn", timeoutMs)
    stressShutdown(min(n, 8), wd)
    wd.stop()

main()

{.pop.}
