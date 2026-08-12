# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## Weak-memory stress test for the flowvar completion handshake.
## Drives `flowvars.nim` directly, with no taskpool in the way.
##
## The handshake in `flowvars.nim` is a Dekker/store-buffer pattern: each side
## stores to "its" word and then loads the other side's word.
##
##   runner (setCompleted)            waiter (sleepUntilComplete)
##   --------------------------       --------------------------------
##   completed.store(1, release)      synchro.fetchAdd(waiter, relaxed)
##   fence(seqCst)                    fence(acquire)             <-- too weak
##   waiter = synchro.load(relaxed)   while completed.load(relaxed) == 0:
##   if waiter != Sentinel:             completed.wait(0)
##     completed.wake()
##
## Correctness requires that the two store/load pairs cannot both miss each
## other. If the runner reads `synchro` before the waiter's registration lands
## AND the waiter reads `completed` before the runner's store lands, the runner
## skips `wake()` and the waiter blocks in `futex_wait` forever.
##
## The runner side is fine: `fence(moSequentiallyConsistent)` compiles to a full
## `dmb ish` on AArch64. The waiter side is not: a *relaxed* RMW followed by
## `fence(moAcquire)` compiles to
##
##     ldadd  w0, w8, [x8]      // no ordering at all
##     dmb    ishld             // load->load/store only, NOT store->load
##     ldr    w0, [completed]
##
## `dmb ishld` does not order the preceding store against the following load, so
## AArch64 is free to satisfy the load of `completed` before the store to
## `synchro` becomes visible. On x86 this is invisible because `lock xadd` is a
## full barrier, which is why this only ever shows up on ARM.
##
## Fix: `fence(moSequentiallyConsistent)` (or a seq-cst RMW) on the waiter side.
##
## This test does no work at all in the task: both threads are released from the
## same gate so that `setCompleted` and `sleepUntilComplete` land on top of each
## other, and a random jitter sweeps their relative arrival across the window.
##
## Stops at whichever comes first, the iteration count or the time budget.
##
## Tunables: -d:tpStressIters:N -d:tpStressBudgetMs:N -d:tpStressTimeoutMs:N

{.push raises: [], gcsafe.}

import
  std/[atomics, random],
  ../../taskpools/flowvars,
  ./watchdog

const
  iters {.intdefine: "tpStressIters".} = 2_000_000
  budgetMs {.intdefine: "tpStressBudgetMs".} = 20_000
  timeoutMs {.intdefine: "tpStressTimeoutMs".} = 60_000
  jitterRange = 96

type
  Shared = object
    node {.align: 64.}: Atomic[pointer]
    startRound {.align: 64.}: Atomic[int]
    runnerRound {.align: 64.}: Atomic[int]
    waiterRound {.align: 64.}: Atomic[int]
    runnerJitter: Atomic[int]
    waiterJitter: Atomic[int]
    waiterIdx: Atomic[int]
    parked: Atomic[int]     # how often the waiter actually had to park
    quitting: Atomic[bool]

var sh: Shared

proc noopTask(env: pointer) {.nimcall, gcsafe, raises: [].} =
  discard

proc runnerFn(s: ptr Shared) {.thread.} =
  var r = 1
  while true:
    while s.startRound.load(moAcquire) < r:
      cpuRelax()
    if s.quitting.load(moAcquire):
      break

    let tn = cast[TaskNode](s.node.load(moAcquire))
    spinFor(s.runnerJitter.load(moRelaxed))

    # Exactly what `taskpools.runTask` does for a task carrying a future.
    tn.setCompleted()
    tn.setGcReady()

    s.runnerRound.store(r, moRelease)
    inc r

proc waiterFn(s: ptr Shared) {.thread.} =
  var r = 1
  while true:
    while s.startRound.load(moAcquire) < r:
      cpuRelax()
    if s.quitting.load(moAcquire):
      break

    let tn = cast[TaskNode](s.node.load(moAcquire))
    let id = int32(s.waiterIdx.load(moRelaxed))
    spinFor(s.waiterJitter.load(moRelaxed))

    # Exactly what `taskpools.completeFuture` does once it runs out of work.
    if not tn.isCompleted():
      discard s.parked.fetchAdd(1, moRelaxed)
      while not tn.isCompleted():
        tn.sleepUntilComplete(id)

    s.waiterRound.store(r, moRelease)
    inc r

proc main() {.raises: [ResourceExhaustedError].} =
  var wd: Watchdog
  wd.start("flowvar completion handshake (direct API)", timeoutMs)

  var
    runner: Thread[ptr Shared]
    waiter: Thread[ptr Shared]
  createThread(runner, runnerFn, addr sh)
  createThread(waiter, waiterFn, addr sh)

  var rng = initRand(0x1337)
  let budget = startBudget(budgetMs)
  var done = 0

  for i in 1 .. iters:
    var tn = TaskNode.new(parent = nil, callback = noopTask, envSize = 0)
    sh.node.store(tn, moRelease)

    # Sweep the relative arrival of the two threads across the racy window.
    # waiterID 0 is the "external thread" case (`sync` from a non-worker),
    # non-zero is the "worker parked on a flowvar" case.
    sh.runnerJitter.store(rng.rand(jitterRange), moRelaxed)
    sh.waiterJitter.store(rng.rand(jitterRange), moRelaxed)
    sh.waiterIdx.store(i and 3, moRelaxed)

    sh.startRound.store(i, moRelease)

    while sh.runnerRound.load(moAcquire) < i or
          sh.waiterRound.load(moAcquire) < i:
      cpuRelax()

    while not tn.isGcReady():
      cpuRelax()
    tn.free()

    done = i
    wd.beat()
    if (i and 0x3FF) == 0 and budget.expired():
      break

  sh.quitting.store(true, moRelease)
  sh.startRound.store(done + 1, moRelease)
  joinThread(runner)
  joinThread(waiter)
  wd.stop()

  let parked = sh.parked.load(moAcquire)
  echo "flowvar handshake: ", done, " iterations in ", budget.elapsedMs(),
       " ms, waiter parked ", parked, " times (",
       (parked * 100) div max(1, done), "%)"
  doAssert parked > 0,
    "the waiter never parked: the test never exercised the racy window " &
    "(try raising -d:tpStressIters or widening the jitter)"

main()

{.pop.}
