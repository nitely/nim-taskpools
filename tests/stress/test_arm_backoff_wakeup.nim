# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## Weak-memory stress test for the EventCount in `backoff.nim`.
## Drives the EventCount directly, with no taskpool in the way.
##
## `sleepy`/`wake` are the same store-buffer pattern as the flowvar handshake:
##
##   sleeper (sleepy)                     waker (wake / wakeAll)
##   ---------------------------------    ----------------------------------
##   waitset.fetchAdd(kPreWait, release)  publish work
##   epoch = events.load(acquire)         events.fetchAdd(1, release)
##   ...check condition...                w = waitset.load(acquire)
##   waitset.fetchAdd(commit, release)    if w has pre-waiters: return
##   while events.load(acquire) == epoch: elif w != 0: futex wake
##     events.wait(epoch)
##
## If the waker's load of `waitset` misses the sleeper AND the sleeper's load of
## `events` misses the epoch bump, the sleeper parks forever. Note `wake()`
## deliberately skips the syscall when it sees a *pre*-waiter, so on that path
## the epoch check is the only thing standing between the sleeper and a lost
## wakeup — there is no futex wake to fall back on.
##
## Unlike the flowvar handshake, this one is (release RMW, acquire load) on both
## sides, which on AArch64 compiles to `ldaddl` + `ldar`. ARMv8 acquire/release
## are RCsc: a store-release is never reordered past a following load-acquire,
## so the pattern happens to be safe on AArch64 — but only by accident of the
## ISA mapping, not by the language memory model (it would break on POWER, and
## it breaks the moment someone weakens one of these orderings). This test is
## the regression guard for that assumption.
##
## Two scenarios:
##  1. producer/consumer, mirroring `eventLoop`'s park protocol
##  2. all-park-then-wakeAll rounds, mirroring `Taskpool.shutdown`, which is the
##     most dangerous case: a lost wakeup there is permanent, nothing retries.
##
## Each scenario stops at whichever comes first, its iteration count or its
## time budget, so wall time does not explode on a many-core machine.
##
## Tunables: -d:tpStressIters:N -d:tpStressRounds:N
##           -d:tpStressBudgetMs:N (per scenario) -d:tpStressTimeoutMs:N

{.push raises: [], gcsafe.}

import
  std/[atomics, random],
  ../../taskpools/backoff,
  ../utils,
  ./watchdog

const
  iters {.intdefine: "tpStressIters".} = 300_000
  rounds {.intdefine: "tpStressRounds".} = 200_000
  budgetMs {.intdefine: "tpStressBudgetMs".} = 20_000
  timeoutMs {.intdefine: "tpStressTimeoutMs".} = 60_000
  maxConsumers = 64

# Scenario 1: producer / consumer
# ------------------------------------------------------------------------------

type
  Bag = object
    ec {.align: 64.}: EventCount
    work {.align: 64.}: Atomic[int]
    consumed {.align: 64.}: Atomic[int]
    quitting: Atomic[bool]

var bag: Bag

proc tryTake(b: ptr Bag): bool =
  var cur = b.work.load(moAcquire)
  while cur > 0:
    if b.work.compareExchange(cur, cur - 1, moAcquireRelease, moAcquire):
      return true
  false

proc consumerFn(b: ptr Bag) {.thread.} =
  # Structured exactly like `taskpools.eventLoop`.
  while true:
    while b.tryTake():
      discard b.consumed.fetchAdd(1, moRelease)

    let ticket = b.ec.sleepy()

    if b.tryTake():
      b.ec.cancelSleep()
      b.ec.wake()
      discard b.consumed.fetchAdd(1, moRelease)
    elif b.quitting.load(moAcquire):
      b.ec.cancelSleep()
      break
    else:
      b.ec.sleep(ticket)

proc stressProducerConsumer(numConsumers: int, wd: var Watchdog)
       {.raises: [ResourceExhaustedError].} =
  var threads: array[maxConsumers, Thread[ptr Bag]]
  for i in 0 ..< numConsumers:
    createThread(threads[i], consumerFn, addr bag)

  # One item at a time, waiting for it to be consumed, so the consumers are
  # forced back to sleep between every item: maximum park/wake churn.
  let budget = startBudget(budgetMs)
  var done = 0

  for i in 1 .. iters:
    discard bag.work.fetchAdd(1, moRelease)
    bag.ec.wake()
    while bag.consumed.load(moAcquire) < i:
      cpuRelax()
    done = i
    wd.beat()
    if (i and 0xFF) == 0 and budget.expired():
      break

  bag.quitting.store(true, moRelease)
  bag.ec.wakeAll()
  for i in 0 ..< numConsumers:
    joinThread(threads[i])

  doAssert bag.consumed.load(moAcquire) == done
  doAssert bag.ec.getNumWaiters() == (0'i32, 0'i32)
  echo "EventCount producer/consumer: ", done, " items across ", numConsumers,
       " consumers in ", budget.elapsedMs(), " ms - ok"

# Scenario 2: park everyone, then wakeAll  (the `shutdown` shape)
# ------------------------------------------------------------------------------

type
  Gate = object
    ec {.align: 64.}: EventCount
    round {.align: 64.}: Atomic[int]
    arrived {.align: 64.}: Atomic[int]

var gate: Gate

proc parkerFn(g: ptr Gate) {.thread.} =
  var seen = 0
  while true:
    while g.round.load(moAcquire) == seen:
      let ticket = g.ec.sleepy()
      if g.round.load(moAcquire) != seen:
        g.ec.cancelSleep()
        break
      g.ec.sleep(ticket)

    seen = g.round.load(moAcquire)
    if seen < 0:
      break
    discard g.arrived.fetchAdd(1, moRelease)

proc stressWakeAll(numParkers: int, wd: var Watchdog)
       {.raises: [ResourceExhaustedError].} =
  var threads: array[maxConsumers, Thread[ptr Gate]]
  var rng = initRand(0xC0FFEE)

  for i in 0 ..< numParkers:
    createThread(threads[i], parkerFn, addr gate)

  let budget = startBudget(budgetMs)
  var done = 0

  for r in 1 .. rounds:
    if (r and 1) == 0:
      # Everyone is committed to sleep: `wakeAll` will issue the syscall.
      # This is the `Taskpool.shutdown` path.
      while gate.ec.getNumWaiters().committedSleep != int32(numParkers):
        cpuRelax()
    else:
      # Bump the round while parkers are still in flight, so some of them are
      # in *pre*-sleep. `wakeAll` then skips the futex syscall entirely and the
      # epoch check in `sleep()` is the only thing preventing a lost wakeup.
      spinFor(rng.rand(256))

    gate.arrived.store(0, moRelease)
    gate.round.store(r, moRelease)
    gate.ec.wakeAll()

    while gate.arrived.load(moAcquire) != numParkers:
      cpuRelax()
    done = r
    wd.beat()
    if budget.expired():
      break

  gate.round.store(-1, moRelease)
  gate.ec.wakeAll()
  for i in 0 ..< numParkers:
    joinThread(threads[i])

  doAssert gate.ec.getNumWaiters() == (0'i32, 0'i32)
  echo "EventCount wakeAll: ", done, " rounds across ", numParkers,
       " parkers in ", budget.elapsedMs(), " ms - ok"

proc main() {.raises: [ResourceExhaustedError].} =
  let n = min(maxConsumers, numThreads())

  block:
    var wd: Watchdog
    wd.start("EventCount producer/consumer", timeoutMs)
    bag.ec.initialize()
    stressProducerConsumer(n, wd)
    wd.stop()

  block:
    var wd: Watchdog
    wd.start("EventCount wakeAll rounds", timeoutMs)
    gate.ec.initialize()
    stressWakeAll(n, wd)
    wd.stop()

main()

{.pop.}
