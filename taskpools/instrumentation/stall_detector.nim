# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## Stall detector: turns a hung CI job into a diagnosis.
##
## Enabled with `-d:taskpoolsDebugStall`. Compiled out entirely otherwise -
## every symbol here is behind a `when`, so the default build is unchanged.
##
## Design constraint
## -----------------
## The bug being hunted is timing-sensitive and rare. Instrumentation that
## perturbs timings makes it stop reproducing, so the hot path must stay
## essentially free:
##
## * **No I/O, ever, until a stall is already detected.** No `echo`, no
##   `c_printf`, no logging on any scheduler path.
## * **No syscalls, no locks, no allocation** added anywhere.
## * **No contended atomics.** The only hot-path writes are relaxed stores into
##   the worker's own `Signal`, which is 64-byte aligned and written by that
##   worker alone - no cache line is shared between writers. On aarch64 a phase
##   update is a single `str`.
## * The detector thread is **passive**: it reads state the scheduler already
##   maintains (deque `peek`, `getNumWaiters`, injection queue) at 10 Hz from a
##   separate thread. The only cost it imposes is stealing a worker's exclusive
##   cache line ~10 times a second, which is unmeasurable.
##
## The two fields added to `Signal` (`phase`, `tasksRun`) exist because the
## scheduler has no other way to express "this worker is parked on a flowvar":
## such a worker is registered in *neither* `preSleep` nor `committedSleep`, so
## from the outside it is indistinguishable from one that is running.
##
## What it reports
## ---------------
## On a stall it prints, per worker, the phase and pending deque depth, plus the
## global backoff counts and whether the injection queue holds ownerless work,
## then `abort()`s so CI fails fast and leaves a core dump. Run
## `gdb -batch -ex "thread apply all bt" <exe> <core>` for the stacks.
##
## The three numbers that matter, and what they mean:
##
## * work pending in a **deque** + its owner parked  -> stranded behind a
##   sleeping owner (the `completeFuture` 2.2 class of bug)
## * work pending in the **injection queue** + all workers parked -> a lost
##   `submitTask` wakeup; that queue has no owner to fall back on
## * **no** work pending anywhere + everyone parked + `syncAll` still spinning
##   -> a task was silently dropped, or a completion was never signalled

const taskpoolsDebugStall* = defined(taskpoolsDebugStall)

when taskpoolsDebugStall:
  import std/atomics
  export atomics

  type
    WorkerPhase* = enum
      ## Where a worker is. Ordered so that "can still make progress" phases
      ## sort below the parked ones.
      phInit = 0          ## created, not yet in the event loop
      phRunning           ## executing a task, or back in caller code
      phSearching         ## between sleepy() and the park decision
      phSyncAll           ## root spinning in syncAll
      phSyncSteal         ## inside completeFuture, stealing/draining
      phParkedGlobal      ## parked in EventCount.sleep
      phParkedFlowvar     ## parked in sleepUntilComplete - invisible to getNumWaiters
      phTerminated        ## left the event loop

  func toPhase*(raw: uint32): WorkerPhase {.inline.} =
    ## Total conversion: the detector reads this concurrently and must never
    ## raise, whatever it happens to observe.
    if raw <= uint32(high(WorkerPhase)): WorkerPhase(raw) else: phInit

  func isParked*(p: WorkerPhase): bool {.inline.} =
    p in {phParkedGlobal, phParkedFlowvar, phTerminated}

  func phaseName*(p: WorkerPhase): cstring {.inline.} =
    case p
    of phInit: "init"
    of phRunning: "running"
    of phSearching: "searching"
    of phSyncAll: "syncAll-spin"
    of phSyncSteal: "sync-steal"
    of phParkedGlobal: "PARKED(backoff)"
    of phParkedFlowvar: "PARKED(flowvar)"
    of phTerminated: "terminated"
