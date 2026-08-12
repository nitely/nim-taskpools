# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## A progress watchdog for the stress tests.
##
## The failure mode these tests hunt for is a *lost wakeup*: a thread parks in a
## futex and nobody ever wakes it. Without a watchdog that shows up as a CI job
## that hangs until the 6h timeout, with no indication of which test stalled.
##
## Usage:
##
## ```Nim
## var wd: Watchdog
## wd.start("my stress test", timeoutMs = 60_000)
## for i in 0 ..< iters:
##   doSomething()
##   wd.beat()          # progress
## wd.stop()
## ```
##
## If `beat` is not called for `timeoutMs`, the watchdog prints the stall and
## `abort()`s the process, so the CI job fails fast and leaves a core dump with
## every thread's stack (including the one stuck in `futex_wait`).

{.push raises: [], gcsafe.}

import
  std/[atomics, monotimes, os, times],
  system/ansi_c

const pollMs = 25

type
  Watchdog* = object
    heartbeat {.align: 64.}: Atomic[int]
    quitting {.align: 64.}: Atomic[bool]
    name: cstring
    timeoutMs: int
    thr: Thread[ptr Watchdog]

proc watchdogLoop(wd: ptr Watchdog) {.thread.} =
  var
    last = wd.heartbeat.load(moAcquire)
    stalledMs = 0

  while not wd.quitting.load(moAcquire):
    sleep(pollMs)
    let now = wd.heartbeat.load(moAcquire)
    if now != last:
      last = now
      stalledMs = 0
      continue

    stalledMs += pollMs
    if stalledMs >= wd.timeoutMs:
      discard c_printf(
        "\n[watchdog] STALLED: '%s' made no progress for %d ms " &
        "(heartbeat stuck at %d).\n" &
        "[watchdog] This is the lost-wakeup/deadlock we are hunting. Aborting.\n",
        wd.name, cint(stalledMs), cint(now))
      flushFile(stdout)
      c_abort()

proc start*(wd: var Watchdog, name: cstring, timeoutMs: int)
       {.raises: [ResourceExhaustedError].} =
  ## Start monitoring. `name` must outlive the watchdog (use a string literal).
  wd.name = name
  wd.timeoutMs = timeoutMs
  wd.heartbeat.store(0, moRelease)
  wd.quitting.store(false, moRelease)
  createThread(wd.thr, watchdogLoop, addr wd)

proc beat*(wd: var Watchdog) {.inline.} =
  ## Signal progress.
  discard wd.heartbeat.fetchAdd(1, moRelease)

proc stop*(wd: var Watchdog) =
  wd.quitting.store(true, moRelease)
  joinThread(wd.thr)

proc spinFor*(n: int) {.inline.} =
  ## Short calibrated delay, used to sweep the relative arrival times of two
  ## threads across the racy window.
  for _ in 0 ..< n:
    cpuRelax()

# Time budget
# ------------------------------------------------------------------------------
#
# Iteration counts alone are not portable here: the cost of a round varies by
# orders of magnitude with the core count (a `Taskpool.new()` spawns one thread
# per core). Every scenario therefore runs until *either* its iteration count or
# its time budget is exhausted, so the suite takes about the same wall time on a
# 4-core laptop and a 128-core ARM box. Raise `-d:tpStressBudgetMs` for a soak.

type
  Budget* = object
    start: MonoTime
    limitMs: int

proc startBudget*(limitMs: int): Budget =
  Budget(start: getMonoTime(), limitMs: limitMs)

proc elapsedMs*(b: Budget): int =
  int inMilliseconds(getMonoTime() - b.start)

proc expired*(b: Budget): bool =
  b.elapsedMs() >= b.limitMs

{.pop.}
