# taskpools
# Copyright (c) 2021-2026 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

## Shared hardening for the futex backends.
##
## Every backend can fail *silently*. The wake primitives return a status that
## the callers have no way to act on, so the historical shape was
## `discard sysFutex(...)`: if the kernel rejects the call, no thread is woken,
## nothing retries, and the parked worker stays parked forever. That is
## indistinguishable from the lost-wakeup deadlocks this subsystem exists to
## prevent, and it surfaces only as a CI job that hangs with no diagnostic.
##
## The checks here are `assert`s. They are removed by `--assertions:off`
## (implied by `-d:danger`) and are a single predictable branch next to a
## syscall otherwise, so they cost nothing that matters - but they turn a silent
## hang into a loud failure in every build that keeps assertions on.

{.push raises: [].}

template checkFutexAlignment*(p: pointer) =
  ## The futex word must be 4-byte aligned.
  ##
  ## Linux rejects a misaligned address with `EINVAL`; `WaitOnAddress` and
  ## `__ulock_wait` require natural alignment as well. Nothing in the type
  ## system enforces where a `Futex` lands inside an enclosing object, and the
  ## enclosing objects here (`EventCount`, `TaskState`) are hand-laid-out and
  ## heap-allocated, so a future field reordering could break this invisibly.
  ##
  ## Checked once per futex in `initialize`, never on the hot path.
  assert (cast[uint](p) and 3'u) == 0,
    "futex word must be 4-byte aligned - check the enclosing struct layout"

{.pop.}
