# taskpools
# Copyright (c) 2019 Mamy André-Ratsimbazafy
# Copyright (c) 2021-2025 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

# An implementation of futex using Windows primitives

# We don't import winlean directly because it pollutes the library with
# a global variable inet_ntop that stores a proc from "Ws2_32.dll"

import std/atomics
import ./futex_checks
export MemoryOrder

# OS primitives
# ------------------------------------------------------------------------

type
  WinBool* = int32
    ## WinBool uses opposite convention as posix, != 0 meaning success.

const INFINITE = -1'i32

# Contrary to the documentation, the futex related primitives are NOT in kernel32.dll
# but in API-MS-Win-Core-Synch-l1-2-0.dll ¯\_(ツ)_/¯
proc WaitOnAddress(
        Address: pointer, CompareAddress: pointer,
        AddressSize: csize_t, dwMilliseconds: int32
       ): WinBool {.importc, stdcall, dynlib: "API-MS-Win-Core-Synch-l1-2-0.dll".}
  # The Address should be volatile

proc WakeByAddressSingle(Address: pointer) {.importc, stdcall, dynlib: "API-MS-Win-Core-Synch-l1-2-0.dll".}
proc WakeByAddressAll(Address: pointer) {.importc, stdcall, dynlib: "API-MS-Win-Core-Synch-l1-2-0.dll".}

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

  # Returns TRUE if the wait succeeds or FALSE if not.
  # We pass INFINITE, so ERROR_TIMEOUT cannot occur: a FALSE here means the
  # wait was rejected outright and we never parked, leaving the caller to spin
  # on its condition. Not fatal, but it points at a broken futex word (see
  # checkFutexAlignment), so surface it rather than swallow it.
  #
  # NOTE: WaitOnAddress may also return TRUE spuriously; every caller re-checks
  # its condition in a loop, so that is handled.
  let ok {.used.} = WaitOnAddress(
    futex.value.addr, expected.addr, csize_t sizeof(expected), INFINITE)
  doAssert ok != 0, "WaitOnAddress failed unexpectedly " & $ok

proc wake*(futex: var Futex) {.inline.} =
  ## Wake one thread (from the same process)
  ##
  ## Unlike the Linux and Darwin backends there is nothing to harden: this
  ## returns void and reports neither failure nor the number of threads woken,
  ## so a rejected wake here is undetectable by design.
  WakeByAddressSingle(futex.value.addr)

proc wakeAll*(futex: var Futex) {.inline.} =
  ## Wake all threads (from the same process)
  ##
  ## Only reaches threads already parked inside WaitOnAddress. A thread that
  ## has committed to sleep but not yet entered the call is covered by the
  ## epoch bump the caller performs *before* this, which makes its WaitOnAddress
  ## fail the CompareAddress check and return immediately.
  ##
  ## Returns void: a rejected wake is undetectable, see `wake`.
  WakeByAddressAll(futex.value.addr)
