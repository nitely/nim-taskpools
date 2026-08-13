# taskpools
# Copyright (c) 2019 Mamy André-Ratsimbazafy
# Copyright (c) 2021-2025 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

import std/atomics
export MemoryOrder

# OS primitives
# ------------------------------------------------------------------------

# Darwin futexes.
# https://github.com/odin-lang/Odin/blob/6983813b4ece0e48539dc1d3e7c7437569db1dc2/core/sync/futex_darwin.odin

const UL_COMPARE_AND_WAIT = 1

# operation bits [15, 8] contain the flags for __ulock_wake
#
const ULF_WAKE_ALL = 0x00000100

# operation bits [31, 24] contain the generic flags
const ULF_NO_ERRNO = 0x01000000

# proc ulock_wait2(operation: uint32, address: pointer, expected: uint64, timeout, value2: uint64): cint {.importc:"__ulock_wait2", noconv.}
proc ulock_wait(operation: uint32, address: pointer, expected: uint64, timeout: uint32): cint {.importc:"__ulock_wait", noconv.}
proc ulock_wake(operation: uint32, address: pointer, wake_value: uint64): cint {.importc:"__ulock_wake", noconv.}

# Futex API
# ------------------------------------------------------------------------

type
  Futex* = object
    value: Atomic[uint32]

proc initialize*(futex: var Futex) {.inline.} =
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
  discard ulock_wait(UL_COMPARE_AND_WAIT or ULF_NO_ERRNO, futex.value.addr, uint64 expected, 0)

proc wake*(futex: var Futex) {.inline.} =
  ## Wake one thread (from the same process)
  discard ulock_wake(UL_COMPARE_AND_WAIT or ULF_NO_ERRNO, futex.value.addr, 0)

proc wakeAll*(futex: var Futex) {.inline.} =
  ## Wake all threads (from the same process)
  discard ulock_wake(UL_COMPARE_AND_WAIT or ULF_WAKE_ALL or ULF_NO_ERRNO, futex.value.addr, 0)
