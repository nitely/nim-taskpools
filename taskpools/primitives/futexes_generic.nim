# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

import std/[atomics, locks]
import ./futex_checks
export MemoryOrder

type
  Futex* = object
    value: Atomic[uint32]
    lock: Lock
    cond: Cond

static:
  # The kernel primitives compare a 32-bit word at the futex address; a padded
  # or resized atomic would silently make them compare the wrong bytes.
  doAssert sizeof(Atomic[uint32]) == 4, "the futex word must be exactly 32-bit"

proc initialize*(futex: var Futex) {.inline.} =
  # This backend has no kernel futex word, so alignment is not load-bearing
  # here. It is still checked so that a layout mistake is caught on every
  # platform rather than only on the ones that use a real futex.
  checkFutexAlignment(futex.value.addr)
  futex.value.store(0, moRelaxed)
  initLock(futex.lock)
  initCond(futex.cond)

proc teardown*(futex: var Futex) {.inline.} =
  futex.value.store(0, moRelaxed)
  deinitLock(futex.lock)
  deinitCond(futex.cond)

proc load*(futex: var Futex, order: MemoryOrder): uint32 {.inline.} =
  futex.value.load(order)

proc store*(futex: var Futex, value: uint32, order: MemoryOrder) {.inline.} =
  futex.value.store(value, order)

proc increment*(futex: var Futex, value: uint32, order: MemoryOrder): uint32 {.inline.} =
  futex.value.fetchAdd(value, order)

# `signal`/`broadcast` cannot report failure and `wait` cannot either, so
# unlike the OS-futex backends there is no return status to harden. The
# correctness argument here is instead structural: the waiter re-checks
# `value` while *holding* the lock before blocking, and a waker takes the same
# lock, so a wake issued after the check cannot be missed.
proc wait*(futex: var Futex, expected: uint32) {.inline.} =
  withLock(futex.lock):
    if futex.value.load(moAcquire) == expected:
      wait(futex.cond, futex.lock)

proc wake*(futex: var Futex) {.inline.} =
  withLock(futex.lock):
    signal(futex.cond)

proc wakeAll*(futex: var Futex) {.inline.} =
  withLock(futex.lock):
    broadcast(futex.cond)
