# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## Invariant test for `EventCount.wake` (backoff.nim).
##
## Rather than waiting for a deadlock, this asserts the invariant that a lost
## wakeup violates, so a failure is observable in milliseconds instead of
## hanging the process:
##
##   INVARIANT: work is available, the producer is not mid-publish, and yet
##              *every* consumer is committed-asleep with no pre-waiters.
##
## If that state persists, nobody is left to take the work and nobody will be
## notified: the pool is wedged. The monitor detects it, counts it, then calls
## `wakeAll()` to unstick the run so we measure a *rate* instead of stopping at
## the first occurrence.
##
## What is under test
## ------------------
## The EventCount park protocol has an unstated precondition: after taking a
## ticket, a consumer must perform an *exhaustive* check of the condition before
## calling `sleep()`. `wake()` wakes exactly ONE thread, so if that thread's
## search spuriously reports "no work" and it parks again, the notification is
## destroyed. Nothing in `backoff.nim` can recover it - the epoch only guards the
## window between `sleepy()` and `sleep()`, not "woke, searched, missed, re-parked".
##
## `trySteal` violates that precondition:
##
##   while not ctx.victims.isEmpty():
##     let target = ctx.victims.randomPick(ctx.rng)
##     let stolenTask = ctx.otherDeques[target].steal()
##     if not stolenTask.isNil: return stolenTask
##     ctx.victims.excl(target)      # ONE CAS attempt, then drop the victim
##   return nil
##
## Chase-Lev `steal()` returns nil both when the deque is empty AND when it lost
## its CAS to a concurrent `pop()`/`steal()`. `trySteal` cannot tell those apart,
## so it reports "nothing to steal" while work is plainly available.
##
## Why this became a hang at bc3bc86: that commit made notifications *scarce*.
## The old EventNotifier called `notify()` on every push, so a destroyed
## notification was replaced by the next one. bc3bc86 changed `schedule` to wake
## only on the empty -> non-empty transition, `submitTask` likewise, and deleted
## `drainInjectionQueue`'s wake loop entirely. A batch of work may now carry a
## single notification, and destroying it is fatal.
##
## `-d:tpStressLossProb:N` models the spurious-failure rate (0 disables, giving
## the exhaustive search that the protocol actually requires).
##
## Tunables: -d:tpStressIters:N -d:tpStressBudgetMs:N -d:tpStressLossy:0|1
##           -d:tpStressLossProb:N -d:tpStressStuckMs:N

{.push raises: [], gcsafe.}

import
  std/[atomics, os],
  system/ansi_c,
  ../../taskpools/backoff,
  ../utils,
  ./watchdog

const
  iters {.intdefine: "tpStressIters".} = 2_000_000
  budgetMs {.intdefine: "tpStressBudgetMs".} = 20_000
  stuckMsThreshold {.intdefine: "tpStressStuckMs".} = 250
  lossy {.intdefine: "tpStressLossy".} = 1
  lossProb {.intdefine: "tpStressLossProb".} = 0
    ## Model `steal()` losing its CAS to the deque owner's concurrent `pop()`:
    ## a 1-in-`lossProb` chance that a take fails though work is available.
    ##
    ## Default 0 (exhaustive search): asserts the EventCount protocol itself is
    ## sound, and guards against a regression in `backoff.nim`.
    ## `-d:tpStressLossProb:8` models `trySteal`'s single-attempt-per-victim
    ## behaviour and *reproduces the wedge* - that run is expected to fail until
    ## `trySteal` distinguishes "empty" from "lost the race".
  maxConsumers = 64
  pollMs = 5

type
  Shared = object
    ec {.align: 64.}: EventCount
    work {.align: 64.}: Atomic[int]
    consumed {.align: 64.}: Atomic[int]
    producerBusy {.align: 64.}: Atomic[bool]
    violations {.align: 64.}: Atomic[int]
    quitting: Atomic[bool]
    monitorQuit: Atomic[bool]
    numConsumers: int

var
  sh: Shared
  seedCounter: Atomic[uint64]

proc nextRand(state: var uint64): uint64 {.inline.} =
  state = state xor (state shl 13)
  state = state xor (state shr 7)
  state = state xor (state shl 17)
  state

proc tryTake(s: ptr Shared, rng: var uint64): bool =
  ## Models `trySteal` against a single victim.
  var cur = s.work.load(moAcquire)
  if cur <= 0:
    return false
  when lossProb != 0:
    if (nextRand(rng) mod lossProb) == 0:
      # `steal()` lost the CAS to a concurrent `pop()`; `trySteal` then does
      # `victims.excl(target)` and reports "no work" though work exists.
      return false
  when lossy != 0:
    # One CAS attempt, then give up on this victim - exactly what `trySteal`
    # does via `victims.excl(target)` after a nil `steal()`.
    s.work.compareExchange(cur, cur - 1, moAcquireRelease, moAcquire)
  else:
    while cur > 0:
      if s.work.compareExchange(cur, cur - 1, moAcquireRelease, moAcquire):
        return true
    false

proc consumerFn(s: ptr Shared) {.thread.} =
  # Structured exactly like `taskpools.eventLoop`.
  var rng = 0x9E3779B97F4A7C15'u64 * (seedCounter.fetchAdd(1, moRelaxed) + 1)
  while true:
    while s.tryTake(rng):
      discard s.consumed.fetchAdd(1, moRelease)

    let ticket = s.ec.sleepy()

    if s.tryTake(rng):
      s.ec.cancelSleep()
      s.ec.wake()
      discard s.consumed.fetchAdd(1, moRelease)
    elif s.quitting.load(moAcquire):
      s.ec.cancelSleep()
      break
    else:
      s.ec.sleep(ticket)

proc monitorFn(s: ptr Shared) {.thread.} =
  var
    stuckMs = 0
    lastWork = -1
    lastConsumed = -1

  while not s.monitorQuit.load(moAcquire):
    sleep(pollMs)

    let
      w = s.work.load(moAcquire)
      c = s.consumed.load(moAcquire)
      waiters = s.ec.getNumWaiters()
      wedged =
        w > 0 and
        not s.producerBusy.load(moAcquire) and
        waiters.preSleep == 0 and
        waiters.committedSleep == int32(s.numConsumers) and
        w == lastWork and c == lastConsumed

    if wedged:
      stuckMs += pollMs
      if stuckMs >= stuckMsThreshold:
        discard s.violations.fetchAdd(1, moRelease)
        discard c_printf(
          "[monitor] LOST WAKEUP: %d item(s) pending, all %d consumers " &
          "committed-asleep, 0 pre-waiters, no progress for %d ms\n",
          cint(w), cint(s.numConsumers), cint(stuckMs))
        flushFile(stdout)
        s.ec.wakeAll()   # unstick and keep measuring the rate
        stuckMs = 0
    else:
      stuckMs = 0

    lastWork = w
    lastConsumed = c

proc main() {.raises: [ResourceExhaustedError].} =
  let n = min(maxConsumers, numThreads())
  sh.numConsumers = n
  sh.ec.initialize()

  var wd: Watchdog
  wd.start("EventCount wake() invariant", 120_000)

  var
    consumers: array[maxConsumers, Thread[ptr Shared]]
    monitor: Thread[ptr Shared]
  for i in 0 ..< n:
    createThread(consumers[i], consumerFn, addr sh)
  createThread(monitor, monitorFn, addr sh)

  let budget = startBudget(budgetMs)
  var done = 0

  for i in 1 .. iters:
    # Publish one item and notify. `producerBusy` brackets the window in which
    # the invariant is legitimately false.
    sh.producerBusy.store(true, moRelease)
    discard sh.work.fetchAdd(1, moRelease)
    sh.ec.wake()
    sh.producerBusy.store(false, moRelease)

    # Wait for it to be consumed, so consumers are driven back to sleep between
    # every item: maximum pre-wait/wake overlap.
    while sh.consumed.load(moAcquire) < i:
      cpuRelax()

    done = i
    wd.beat()
    if (i and 0xFF) == 0 and budget.expired():
      break

  sh.quitting.store(true, moRelease)
  sh.ec.wakeAll()
  for i in 0 ..< n:
    joinThread(consumers[i])
  sh.monitorQuit.store(true, moRelease)
  joinThread(monitor)
  wd.stop()

  let v = sh.violations.load(moAcquire)
  echo "EventCount invariant: ", done, " items across ", n, " consumers in ",
       budget.elapsedMs(), " ms, lossy=", lossy, ", lossProb=", (if lossProb == 0: "none" else: "1/" & $lossProb), ", violations=", v
  doAssert v == 0, "lost wakeup detected: see [monitor] lines above"

main()

{.pop.}
