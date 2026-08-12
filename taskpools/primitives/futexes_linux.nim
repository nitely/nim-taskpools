# taskpools
# Copyright (c) 2019 Mamy André-Ratsimbazafy
# Copyright (c) 2021-2025 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

# A wrapper for linux futex.
# Condition variables do not always wake on signal which can deadlock the runtime
# so we need to roll up our sleeves and use the low-level futex API.

import std/[atomics, posix]
import ./futex_checks
export MemoryOrder

# OS primitives
# ------------------------------------------------------------------------

const
  FUTEX_WAIT_PRIVATE = 128
  FUTEX_WAKE_PRIVATE = 129

let NR_Futex {.importc: "SYS_futex", header: "<sys/syscall.h>".}: clong

proc syscall(sysno: clong): cint {.importc, header:"<unistd.h>", varargs.}

proc sysFutex(
       futexAddr: pointer, operation: uint32, expected: uint32 or int32,
       timeout: pointer = nil, val2: pointer = nil, val3: cint = 0): cint {.inline.} =
  ## See https://web.archive.org/web/20230208151430/http://locklessinc.com/articles/futex_cheat_sheet/
  ## and https://www.akkadia.org/drepper/futex.pdf
  syscall(NR_Futex, futexAddr, operation, expected, timeout, val2, val3)

# Futex API
# ------------------------------------------------------------------------

type
  Futex* = object
    value: Atomic[uint32]

static:
  # The kernel primitives compare a 32-bit word at the futex address; a padded
  # or resized atomic would silently make them compare the wrong bytes.
  doAssert sizeof(Atomic[uint32]) == 4, "the futex word must be exactly 32-bit"

proc initialize*(futex: var Futex) {.inline.} =
  checkFutexAlignment(futex.value.addr)
  futex.value.store(0, moRelaxed)

proc teardown*(futex: var Futex) {.inline.} =
  futex.value.store(0, moRelaxed)

proc load*(futex: var Futex, order: MemoryOrder): uint32 {.inline.} =
  futex.value.load(order)

proc store*(futex: var Futex, value: uint32, order: MemoryOrder) {.inline.} =
  futex.value.store(value, order)

proc increment*(futex: var Futex, value: uint32, order: MemoryOrder): uint32 {.inline.} =
  ## Increment a futex value, returns the previous one.
  futex.value.fetchAdd(value, order)

proc wait*(futex: var Futex, expected: uint32) {.inline.} =
  ## Suspend a thread if the value of the futex is the same as expected.

  # Returns 0 on a successful suspend + wake.
  # EAGAIN means the value already changed so we never parked - the caller
  # re-checks in its loop. EINTR is a signal. Both are normal and expected.
  # Any other failure means the wait was rejected outright: the caller spins on
  # its condition instead of parking. Not fatal, but it points at a broken futex
  # word (see checkFutexAlignment), so surface it rather than swallow it.
  let res {.used.} = sysFutex(futex.value.addr, FUTEX_WAIT_PRIVATE, expected)
  doAssert res == 0 or (res == -1 and (errno == EAGAIN or errno == EINTR)),
    "FUTEX_WAIT failed unexpectedly " & $res

proc wake*(futex: var Futex) {.inline.} =
  ## Wake one thread (from the same process)

  # Returns the number of threads actually woken - 0 is normal, it just means
  # nobody was parked - or -1 on failure. A failed wake is a *silent* lost
  # wakeup: the waiter stays parked forever and no caller here retries a wake.
  let res {.used.} = sysFutex(futex.value.addr, FUTEX_WAKE_PRIVATE, 1)
  doAssert res >= 0, "FUTEX_WAKE failed - a parked thread will never be woken " & $res

proc wakeAll*(futex: var Futex) {.inline.} =
  ## Wake all threads (from the same process)
  ##
  ## This only reaches threads already queued in the kernel. A thread that has
  ## committed to sleep but has not yet entered the syscall is not woken here;
  ## it is covered by the epoch bump the caller performs *before* this call,
  ## which makes its `wait` fail the kernel-side value comparison and return
  ## EAGAIN immediately.

  # Returns the number of threads actually woken, or -1 on failure.
  # `Taskpool.shutdown` issues exactly one wakeAll and then blocks on a barrier,
  # so a failure here hangs the process permanently.
  let res {.used.} = sysFutex(futex.value.addr, FUTEX_WAKE_PRIVATE, high(int32))
  doAssert res >= 0, "FUTEX_WAKE(all) failed - parked threads will never be woken " & $res
