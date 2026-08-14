# taskpools
# Copyright (c) 2021-2025 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at http://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at http://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

# Taskpools
#
# This file implements a taskpool
#
# Implementation:
#
# It is a simple shared memory based work-stealing threadpool.
# The primary focus is:
# - Delegate compute intensive tasks to the threadpool.
# - Simple to audit by staying close to foundational papers
#   and using simple datastructures otherwise.
# - Low energy consumption:
#   threads should be put to sleep ASAP
#   instead of polling/spinning (energy vs latency tradeoff)
# - Decent performance:
#   Work-stealing has optimal asymptotic parallel speedup.
#   Work-stealing has significantly reduced contention
#   when many tasks are created,
#   for example by divide-and-conquer algorithms, compared to a global task queue
#
# Not a priority:
# - Handling trillions of very short tasks (less than 100µs).
# - Advanced task dependencies or events API.
# - Unbalanced parallel-for loops.
# - Handling services that should run for the lifetime of the program.
#
# Doing IO on a compute threadpool should be avoided
# In case a thread is blocked for IO, other threads can steal pending tasks in that thread.
# If all threads are pending for IO, the threadpool will not make any progress and be soft-locked.

{.push raises: [], gcsafe.} # Ensure no exceptions can happen

import
  std/[atomics, cpuinfo, isolation, macros, random, typetraits],
  ./[
    ast_utils, backoff, chase_lev_deques, flowvars,
    injection_queues, sparsesets,
  ],
  ./primitives/[barriers, allocs],
  ./instrumentation/[contracts, loggers, stall_detector]

export
  # flowvars
  Flowvar, isSpawned, isReady, isolation

const sharedHeap = defined(gcArc) or defined(gcOrc) or defined(gcAtomicArc)

type
  WorkerID = int32

  Signal = object
    # 64-byte aligned and written only by its owning worker, so the debug
    # fields below never share a cache line with another writer.
    terminate {.align: 64.}: Atomic[bool]
    when taskpoolsDebugStall:
      phase: Atomic[uint32]      ## WorkerPhase; relaxed store, owner-only
      tasksRun: Atomic[uint64]   ## relaxed increment, owner-only
      parkTicket: Atomic[uint32] ## epoch this worker last parked on

  WorkerContext = object
    ## Thread-local worker context

    # Params
    id: WorkerID
    taskpool: Taskpool

    # Tasks
    taskDeque: ptr ChaseLevDeque[TaskNode] # owned task deque
    currentTask: TaskNode

    # Synchronization
    signal: ptr Signal               # owned signal

    # Thefts
    rng: Rand                        # RNG state to select victims
    otherDeques: ptr UncheckedArray[ChaseLevDeque[TaskNode]]
    victims: SparseSet

  Taskpool* = ptr object
    ## A taskpool schedules procedures to be executed in parallel
    barrier {.align: 64.}: SyncBarrier
      ## Barrier for initialization and teardown
    # --- Align: 64
    globalBackoff {.align: 64.}: EventCount
      ## Multi-producer multi-consumer backoff
    # --- Align: 64
    numThreads* {.align: 64.}: int
    workerDeques: ptr UncheckedArray[ChaseLevDeque[TaskNode]]
      ## Direct access for task stealing
    workers: ptr UncheckedArray[Thread[(Taskpool, WorkerID)]]
    workerSignals: ptr UncheckedArray[Signal]
      ## Access signaledTerminate

    injectionQueue {.align: 64.}: InjectionQueue[TaskNode]
      ## Lock-free MPMC queue for tasks submitted by threads that are not
      ## currently running the scheduler (external threads or foreign pools).

template setPhase(ctx: WorkerContext, p: untyped) =
  ## One relaxed store to the worker's own cache line. Compiled out unless
  ## -d:taskpoolsDebugStall.
  when taskpoolsDebugStall:
    ctx.signal.phase.store(uint32(p), moRelaxed)

when not sharedHeap:
  proc supportsThreadMove*(T: type): bool {.compileTime.} =
    # Similar to `supportsCopyMem` but allows types with disabled `=copy`. Not
    # perfect - in particular, doesn't allow `proc (...) {.nimcall.}`
    when T is object | tuple:
      for f in fields(cast[ptr T](0)[]):
        if not supportsThreadMove(typeof(f)):
          return false
      true
    elif T is distinct:
      supportsThreadMove(distinctBase(T))
    elif T is array:
      supportsThreadMove(elementType(cast[ptr T](0)[]))
    elif T is
        SomeOrdinal | pointer | ptr | cstring | char | float | float32 | float64 | set:
      true
    else: # string | seq | ref | proc | iterator - last two are tricky
      false

# Thread-local config
# ---------------------------------------------

var workerContext {.threadvar.}: WorkerContext
  ## Thread-local Worker context

proc setupWorker() =
  ## Initialize the thread-local context of a worker
  ## Requires the ID and taskpool fields to be initialized
  template ctx: untyped = workerContext

  preCondition: not ctx.taskpool.isNil()
  preCondition: 0 <= ctx.id and ctx.id < ctx.taskpool.numThreads
  preCondition: not ctx.taskpool.workerDeques.isNil()
  preCondition: not ctx.taskpool.workerSignals.isNil()

  # Thefts
  ctx.rng = initRand(0xEFFACED + ctx.id)
  ctx.otherDeques = ctx.taskpool.workerDeques
  ctx.victims.allocate(ctx.taskpool.numThreads)

  # Synchronization
  ctx.signal = addr ctx.taskpool.workerSignals[ctx.id]
  ctx.signal.terminate.store(false, moRelaxed)
  when taskpoolsDebugStall:
    # The root worker never enters `eventLoop`, so nothing else would set these
    # before the detector's first poll.
    ctx.signal.phase.store(uint32(phInit), moRelaxed)
    ctx.signal.tasksRun.store(0, moRelaxed)

  # Tasks
  ctx.taskDeque = addr ctx.taskpool.workerDeques[ctx.id]
  ctx.currentTask = nil

  # Init
  ctx.taskDeque[].init(initialCapacity = 32)

proc teardownWorker() =
  ## Cleanup the thread-local context of a worker
  template ctx: untyped = workerContext
  ctx.taskDeque[].teardown()
  ctx.victims.delete()

proc eventLoop(ctx: var WorkerContext) {.raises:[].}

proc workerEntryFn(params: tuple[taskpool: Taskpool, id: WorkerID]) =
  ## On the start of the threadpool workers will execute this
  ## until they receive a termination signal
  # We assume that thread_local variables start all at their binary zero value
  preCondition: workerContext == default(WorkerContext)

  template ctx: untyped = workerContext

  # If the following crashes, you need --tlsEmulation:off
  ctx.id = params.id
  ctx.taskpool = params.taskpool

  setupWorker()

  # 1 matching barrier in Taskpool.new() for root thread
  discard params.taskpool.barrier.wait()

  {.gcsafe.}: # Not GC-safe when multi-threaded due to thread-local variables
    ctx.eventLoop()

  debugTermination:
    log(">>> Worker %2d shutting down <<<\n", ctx.id)

  # 1 matching barrier in taskpool.shutdown() for root thread
  discard params.taskpool.barrier.wait()

  teardownWorker()

# Tasks
# ---------------------------------------------

proc runTask(ctx: var WorkerContext, tn: var TaskNode) {.inline.} =
  ## Run a task and consume the task node.
  ##
  ## The task environment is intrusive to the node, so the callback receives a
  ## pointer to it. If the task carries a Flowvar (`hasFuture`), ownership is
  ## transferred to the awaiting thread: it reads the result and frees the node.
  ## Otherwise the node is freed here.
  let suspendedTask = ctx.currentTask
  ctx.currentTask = tn
  tn.callback(tn.env.addr)
  ctx.currentTask = suspendedTask
  when taskpoolsDebugStall:
    # Load+add+store, not fetchAdd: only the owning worker writes this, and the
    # detector tolerates a stale read. That avoids an atomic RMW (`lock xadd` /
    # `ldadd`) on the per-task path, leaving a plain load/add/store.
    ctx.signal.tasksRun.store(ctx.signal.tasksRun.load(moRelaxed) + 1, moRelaxed)
  if tn.hasFuture:
    # Sync with an awaiting thread that may have parked on the task,
    # then transfer ownership of the node to it.
    tn.setCompleted()
    tn.setGcReady()
  else:
    tn.free()

proc schedule(ctx: WorkerContext, tn: sink TaskNode, forceWake = false) {.inline.} =
  ## Schedule a task in the taskpool.
  ## This wakes another worker if our local queue is empty
  ## or forceWake is true.
  debug: log("Worker %2d: schedule task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, tn, tn.parent, ctx.currentTask)

  # Instead of notifying every time a task is scheduled, we notify
  # only when the worker queue is empty. This is a good approximation
  # of starvation in work-stealing.
  var wasEmpty = false
  ctx.taskDeque[].push(tn, wasEmpty)
  if forceWake or wasEmpty:
    ctx.taskpool.globalBackoff.wake()

proc submitTask(tp: Taskpool, tn: TaskNode) {.inline.} =
  ## Push a task onto the injection queue from any thread.
  ## Workers will drain the queue into their Chase-Lev deques, making tasks stealable.
  var wasEmpty = false
  tp.injectionQueue.push(tn, wasEmpty)
  if wasEmpty:
    # only one wake is needed; the worker will wake one after draining the queue,
    # the next worker will wake one after steal, and so on.
    tp.globalBackoff.wake()

proc drainInjectionQueue(ctx: var WorkerContext): bool {.inline, discardable.} =
  ## Atomically claim the entire injection queue and push all tasks into
  ## the calling worker's Chase-Lev deque, where they become stealable.
  ## Only one worker wins the exchange; the others drain nothing.
  ## Return whether the task queue was empty before the drain.
  result = false
  var wasEmpty = false
  for node in ctx.taskpool.injectionQueue.drain():
    ctx.taskDeque[].push(node, wasEmpty)
    result = result or wasEmpty

# Scheduler
# ---------------------------------------------

proc trySteal(ctx: var WorkerContext): TaskNode =
  ## Try to steal a task.

  ctx.victims.refill()
  ctx.victims.excl(ctx.id)

  while not ctx.victims.isEmpty():
    let target = ctx.victims.randomPick(ctx.rng)

    let stolenTask = ctx.otherDeques[target].steal()
    if not stolenTask.isNil:
      return stolenTask

    ctx.victims.excl(target)

  return nil

when taskpoolsDebugStall:
  proc recordParkState(ctx: var WorkerContext, ticket: ParkingTicket) =
    ## Records only the ticket epoch this worker is about to park on - a value
    ## already in a register, stored to the worker's own cache line.
    ##
    ## An earlier version also scanned every other deque here to record what the
    ## worker could see. That answered its question (workers park having
    ## correctly observed every deque empty) but the cross-thread cache traffic
    ## dropped the reproduction rate from 10/10 to roughly 1/3, so it is gone.
    ## Anything needing other threads' state belongs in the detector thread,
    ## which reads it off the hot path.
    ctx.signal.parkTicket.store(ticket.ticketEpoch(), moRelaxed)

proc eventLoop(ctx: var WorkerContext) =
  ## Each worker thread executes this loop over and over.
  while true:
    # 1. Pick from local deque
    debug: log("Worker %2d: eventLoop 1 - searching task from local deque\n", ctx.id)
    ctx.setPhase(phRunning)
    var processed = 0'u32
    while (var taskNode = ctx.taskDeque[].pop(); not taskNode.isNil):
      debug: log("Worker %2d: eventLoop 1 - running task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, taskNode, taskNode.parent, ctx.currentTask)
      ctx.runTask(taskNode)
      inc processed
      if processed >= tasksBetweenInjectionDrains:
        processed = 0
        if ctx.drainInjectionQueue():
          ctx.taskpool.globalBackoff.wake()

    ctx.setPhase(phSearching)
    let ticket = ctx.taskpool.globalBackoff.sleepy()

    # Drain the injection queue into our Chase-Lev deque so externally submitted
    # tasks become local work (and stealable by other workers).
    ctx.drainInjectionQueue()

    if (var taskNode = ctx.taskDeque[].pop(); not taskNode.isNil):
      # 2. Local queue contains injected tasks.
      debug: log("Worker %2d: eventLoop 2 - running injected task 0x%.08x\n", ctx.id, taskNode)
      ctx.setPhase(phRunning)
      ctx.taskpool.globalBackoff.cancelSleep()
      ctx.taskpool.globalBackoff.wake()
      ctx.runTask(taskNode)
    elif (var stolenTask = ctx.trySteal(); not stolenTask.isNil):
      # 3. Run stolen task
      debug: log("Worker %2d: eventLoop 3 - stole task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, stolenTask, stolenTask.parent, ctx.currentTask)
      # We managed to steal a task, cancel sleep
      ctx.setPhase(phRunning)
      ctx.taskpool.globalBackoff.cancelSleep()
      # Theft successful, there might be more work for idle threads, wake one
      # cancelSleep must be done before as wake has an optimization
      # to not notify when a thread is sleepy
      ctx.taskpool.globalBackoff.wake()
      ctx.runTask(stolenTask)
    elif ctx.signal.terminate.load(moAcquire):
      # 4. Taskpool has no more tasks and we were signaled to terminate
      ctx.taskpool.globalBackoff.cancelSleep()
      ctx.setPhase(phTerminated)
      debug: log("Worker %2d: eventLoop 4 - terminated\n", ctx.id)
      break
    else:
      # 5. Park the thread until a new task enters the taskpool
      debug: log("Worker %2d: eventLoop 5.a - sleeping\n", ctx.id)
      when taskpoolsDebugStall:
        ctx.recordParkState(ticket)
      ctx.setPhase(phParkedGlobal)
      ctx.taskpool.globalBackoff.sleep(ticket)
      ctx.setPhase(phSearching)
      debug: log("Worker %2d: eventLoop 5.b - waking\n", ctx.id)

# Tasking
# ---------------------------------------------

proc RootTask(env: pointer) =
  discard

template isRootTask(task: TaskNode): bool {.used.} =
  task.callback == RootTask

proc completeFuture[T](fv: Flowvar[T], parentResult: var T) =
  ## Eagerly complete an awaited Flowvar

  template ctx: untyped = workerContext

  template isFutReady(): untyped =
    fv.tryComplete(parentResult)

  if isFutReady():
    return

  # External thread: no Chase-Lev deque and no steal peers available.
  # There is nothing useful to do, so sleep until a worker completes the task.
  if ctx.taskpool.isNil:
    while not isFutReady():
      fv.sleepUntilReady(waiterID = 0)
    return

  ctx.setPhase(phSyncSteal)
  # Otherwise a worker that has returned to caller code still reads as
  # sync-steal, which misreports where a stalled thread actually is.
  defer: ctx.setPhase(phRunning)

  ## 1. Process all the children of the current tasks.
  ##    This ensures that we can give control back ASAP.
  debug: log("Worker %2d: sync 1 - searching task from local deque\n", ctx.id)
  while (var taskNode = ctx.taskDeque[].pop(); not taskNode.isNil):
    if taskNode.parent != ctx.currentTask:
      debug: log("Worker %2d: sync 1 - skipping non-direct descendant task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, taskNode, taskNode.parent, ctx.currentTask)
      ctx.schedule(taskNode, forceWake = true) # reschedule task and wake a sibling to take it over.
      break
    debug: log("Worker %2d: sync 1 - running task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, taskNode, taskNode.parent, ctx.currentTask)
    ctx.runTask(taskNode)
    if isFutReady():
      debug: log("Worker %2d: sync 1 - future ready, exiting\n", ctx.id)
      return

  ## 2. We run out-of-tasks or out-of-direct-child of our current awaited task
  ##    So the task is bottlenecked by dependencies in other threads,
  ##    hence we abandon our enqueued work and steal in the others' queues
  ##    in hope it advances our awaited task. This prioritizes latency over throughput.
  debug: log("Worker %2d: sync 2 - future not ready, becoming a thief (currentTask 0x%.08x)\n", ctx.id, ctx.currentTask)
  while not isFutReady():
    if (var taskNode = ctx.trySteal(); not taskNode.isNil):
      # Theft successful, there might be more work for idle threads, wake one
      ctx.taskpool.globalBackoff.wake()
      # We stole some task, we hope we advance our awaited task
      debug: log("Worker %2d: sync 2.1 - stole task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, taskNode, taskNode.parent, ctx.currentTask)
      ctx.runTask(taskNode)
    elif (var taskNode = ctx.taskDeque[].pop(); not taskNode.isNil):
      # We advance our own queue, this increases throughput but may impact latency on the awaited task.
      # It is also what makes parking below deadlock-free: a parked thread always
      # has an empty deque, so the tasks left in it, possibly the awaited one,
      # cannot be stranded behind a sleeping owner.
      debug: log("Worker %2d: sync 2.2 - couldn't steal, running own task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, taskNode, taskNode.parent, ctx.currentTask)
      ctx.runTask(taskNode)
    #elif ctx.drainInjectionQueue():
    #  # Drain injection queue, this increases throughput but may impact latency on the awaited task.
    #  # These tasks themselves cannot possibly help advance the awaited future,
    #  # but it could help the worker that would consume the queue if we don't.
    #  debug: log("Worker %2d: sync 2.3 - drained the injection queue instead of parking\n", ctx.id)
    #  ctx.taskpool.globalBackoff.wake()
    else:
      # Nothing to do, we park until the thread running our awaited task
      # completes it and wakes us up. We don't park on the global backoff as
      # it cannot wake a specific thread, so if more work is created in the
      # meantime we will miss it; that work is for a thread that can run it.
      debug: log("Worker %2d: sync 2.3.a - empty runtime, sleeping until the awaited task completes\n", ctx.id)
      # Invisible to getNumWaiters: a flowvar-parked worker is registered in
      # neither preSleep nor committedSleep. This phase is the only way the
      # detector can tell it apart from a running one.
      ctx.setPhase(phParkedFlowvar)
      fv.sleepUntilReady(ctx.id)
      ctx.setPhase(phSyncSteal)
      debug: log("Worker %2d: sync 2.3.b - awaited task completed, waking\n", ctx.id)

proc sync*[T](fv: sink Flowvar[T]): T {.inline, gcsafe.} =
  ## Blocks the current thread until the flowvar is available and returned.
  ## Worker threads help execute pending tasks while waiting, and park on the
  ## awaited task once they run out. Non-worker (external) threads have no task
  ## to run, so they park immediately.
  completeFuture(fv, result)
  cleanup(fv)

proc forceFuture*[T](fv: sink Flowvar[T], parentResult: var T) {.deprecated: "use sync".} =
  parentResult = sync(fv)

proc syncAll*(tp: Taskpool) =
  ## Blocks until all pending tasks are completed.
  ## This MUST only be called from the root scope that created the taskpool.
  ##
  ## All external threads MUST stop calling spawn before syncAll is called.
  ## There is an inherent race between the termination check and the injection
  ## queue: a task pushed after syncAll begins its final drain may not be
  ## waited for.
  template ctx: untyped = workerContext

  debugTermination:
    log(">>> Worker %2d enters barrier <<<\n", ctx.id)

  preCondition: ctx.id == 0
  preCondition: ctx.currentTask.isRootTask()
  ctx.setPhase(phSyncAll)

  # Empty all tasks
  while true:
    # 1. Empty local tasks
    debug: log("Worker %2d: syncAll 1 - searching task from local deque\n", ctx.id)
    while (var taskNode = ctx.taskDeque[].pop(); not taskNode.isNil):
      debug: log("Worker %2d: syncAll 1 - running task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, taskNode, taskNode.parent, ctx.currentTask)
      ctx.runTask(taskNode)

    # Drain injection queue into local deque so externally submitted tasks
    # are not left stranded while we wait for the pool to go idle.
    ctx.drainInjectionQueue()

    if (var taskNode = ctx.taskDeque[].pop(); not taskNode.isNil):
      # 2. Local queue contains injected tasks.
      debug: log("Worker %2d: syncAll 2 - running injected task 0x%.08x\n", ctx.id, taskNode)
      ctx.taskpool.globalBackoff.wake()
      ctx.runTask(taskNode)
    elif (var taskNode = ctx.trySteal(); not taskNode.isNil):
      # 3. We stole some task
      debug: log("Worker %2d: syncAll 3 - stole task 0x%.08x (parent 0x%.08x, current 0x%.08x)\n", ctx.id, taskNode, taskNode.parent, ctx.currentTask)
      # Theft successful, there might be more work for idle threads, wake one
      ctx.taskpool.globalBackoff.wake()
      ctx.runTask(taskNode)
    elif tp.globalBackoff.getNumWaiters() == (0'i32, int32(tp.numThreads - 1)):
      # 4. all threads besides the current are parked (and none are
      #    in pre-sleep, so none can still grab a task and create work)
      debugTermination:
        log("Worker %2d: syncAll 4 - termination, all other threads sleeping\n", ctx.id)
      break
    else:
      # 5. We don't park as there is no notif for task completion
      cpuRelax()

  ctx.setPhase(phRunning)
  debugTermination:
    log(">>> Worker %2d leaves barrier <<<\n", ctx.id)

# Stall detector
# ---------------------------------------------
# Everything below is compiled out unless -d:taskpoolsDebugStall.
# See taskpools/instrumentation/stall_detector.nim for the design constraints.

when taskpoolsDebugStall:
  import std/os
  import system/ansi_c

  const
    maxWatchedPools = 64
    stallSeconds {.intdefine: "taskpoolsStallSeconds".} = 30
      ## How long the whole pool must be bit-identical *and* have nobody
      ## running before we call it a stall. Raise it if a legitimate task in
      ## your workload runs longer than this.
    pollMs = 100

  var
    watchedPools: array[maxWatchedPools, Atomic[pointer]]
    watchedBusy: array[maxWatchedPools, Atomic[int]]
      ## Held by the detector while it reads a pool, so teardown cannot free
      ## the object out from under it.
    watchdogStarted: Atomic[bool]
    watchdogThread: Thread[void]

  proc dumpPool(tp: Taskpool, fingerprint: uint64) =
    ## Only ever called once, after a stall is already confirmed, so the cost
    ## of the I/O here is irrelevant.
    discard c_printf("\n=========== TASKPOOL STALL DETECTED ===========\n")
    discard c_printf("pool %p, %d threads, no progress for %ds\n",
      tp, cint(tp.numThreads), cint(stallSeconds))

    let waiters = tp.globalBackoff.getNumWaiters()
    discard c_printf("globalBackoff: preSleep=%d committedSleep=%d\n",
      cint(waiters.preSleep), cint(waiters.committedSleep))

    let injectionPending = not tp.injectionQueue.isEmpty()
    discard c_printf("injectionQueue: %s\n",
      if injectionPending: cstring"NON-EMPTY (ownerless work!)" else: cstring"empty")

    var totalPending = 0
    discard c_printf(
      "\n  id  phase              top  bottom   cap  pending  tasksRun  term\n")
    for i in 0 ..< tp.numThreads:
      let
        ph = toPhase(tp.workerSignals[i].phase.load(moRelaxed))
        st = tp.workerDeques[i].debugState()
        pending = max(0, st.bottom - st.top)
        ran = tp.workerSignals[i].tasksRun.load(moRelaxed)
        term = tp.workerSignals[i].terminate.load(moRelaxed)
      totalPending += pending
      discard c_printf("  %2d  %-16s %5d  %6d  %4d  %7d  %8llu  %s\n",
        cint(i), ph.phaseName(), cint(st.top), cint(st.bottom),
        cint(st.capacity), cint(pending), ran,
        if term: cstring"yes" else: cstring"no")

    # Each parked worker's ticket vs the live epoch. A ticket equal to the
    # current epoch means nothing bumped `events` since that worker parked; a
    # ticket behind it means the bump happened and the wake still did not land.
    discard c_printf("\nEventCount epoch now=%u\n",
      tp.globalBackoff.debugEpoch())
    discard c_printf("  id  parked-on-epoch\n")
    for i in 0 ..< tp.numThreads:
      let ph = toPhase(tp.workerSignals[i].phase.load(moRelaxed))
      if ph != phParkedGlobal:
        continue
      discard c_printf("  %2d  %15u\n",
        cint(i), tp.workerSignals[i].parkTicket.load(moRelaxed))

    discard c_printf("\n--- diagnosis ---\n")
    if totalPending > 0:
      discard c_printf(
        "%d task(s) pending in worker deques while nobody is running them.\n" &
        "  -> work stranded behind a parked/blocked owner; check which worker\n" &
        "     owns the non-empty deque and what phase it is in.\n", cint(totalPending))
    if injectionPending:
      discard c_printf(
        "Injection queue is NON-EMPTY. It has no owner to fall back on, so\n" &
        "  -> a submitTask() wakeup was lost.\n")
    if totalPending == 0 and not injectionPending:
      discard c_printf(
        "No work pending anywhere, yet nothing is progressing.\n" &
        "  -> a task was dropped, or a completion was never signalled\n" &
        "     (flowvar handshake / EventCount epoch).\n")
    discard c_printf("\nfingerprint=%llu\n", fingerprint)
    discard c_printf(
      "Aborting for a core dump. Recover the stacks with:\n" &
      "  gdb -batch -ex \"thread apply all bt\" <exe> <core>\n")
    discard c_printf("===============================================\n")
    flushFile(stdout)

  proc snapshot(tp: Taskpool, anyRunning: var bool): uint64 =
    ## Purely passive: reads state the scheduler already maintains.
    anyRunning = false
    result = 0'u64
    let waiters = tp.globalBackoff.getNumWaiters()
    result = result * 31 + uint64(waiters.preSleep)
    result = result * 31 + uint64(waiters.committedSleep)
    result = result * 31 + (if tp.injectionQueue.isEmpty(): 0'u64 else: 1'u64)
    for i in 0 ..< tp.numThreads:
      let ph = toPhase(tp.workerSignals[i].phase.load(moRelaxed))
      if ph == phRunning:
        anyRunning = true
      result = result * 31 + uint64(ph)
      result = result * 31 + tp.workerSignals[i].tasksRun.load(moRelaxed)
      #result = result * 31 + uint64(tp.workerDeques[i].peek())

  proc watchdogLoop() {.thread.} =
    var
      lastPrint: array[maxWatchedPools, uint64]
      stableMs: array[maxWatchedPools, int]
    while true:
      sleep(pollMs)
      for slot in 0 ..< maxWatchedPools:
        discard watchedBusy[slot].fetchAdd(1, moAcquire)
        let raw = watchedPools[slot].load(moAcquire)
        if not raw.isNil:
          let tp = cast[Taskpool](raw)
          var anyRunning = false
          let fp = tp.snapshot(anyRunning)
          # A long-running task looks identical to a stall except that someone
          # is in phRunning, so requiring "nobody running" removes that whole
          # class of false positive.
          if fp == lastPrint[slot] and not anyRunning:
            stableMs[slot] += pollMs
            if stableMs[slot] >= stallSeconds * 1000:
              tp.dumpPool(fp)
              c_abort()
          else:
            lastPrint[slot] = fp
            stableMs[slot] = 0
        else:
          stableMs[slot] = 0
        discard watchedBusy[slot].fetchSub(1, moRelease)

  proc registerPool(tp: Taskpool) {.raises: [ResourceExhaustedError].} =
    if not watchdogStarted.exchange(true, moAcquireRelease):
      createThread(watchdogThread, watchdogLoop)
    for slot in 0 ..< maxWatchedPools:
      var expected: pointer = nil
      if watchedPools[slot].compareExchange(expected, cast[pointer](tp), moAcquireRelease, moRelaxed):
        return

  proc unregisterPool(tp: Taskpool) =
    for slot in 0 ..< maxWatchedPools:
      var expected = cast[pointer](tp)
      if watchedPools[slot].compareExchange(expected, nil, moAcquireRelease, moRelaxed):
        # Wait for the detector to stop looking at this slot before the caller
        # frees the object.
        while watchedBusy[slot].load(moAcquire) != 0:
          cpuRelax()
        return

# Runtime
# ---------------------------------------------

proc new*(T: type Taskpool, numThreads = countProcessors()): T {.raises: [CatchableError].} =
  ## Initialize a threadpool that manages `numThreads` threads.
  ## Default to the number of logical processors available.

  type TpObj = typeof(default(Taskpool)[])
  # Event notifier requires an extra 64 bytes for alignment
  var tp = tp_allocAligned(TpObj, sizeof(TpObj) + 64, 64)

  tp.barrier.init(numThreads.int32)
  tp.globalBackoff.initialize()
  tp.numThreads = numThreads
  tp.injectionQueue.init()
  tp.workerDeques = tp_allocArrayAligned(ChaseLevDeque[TaskNode], numThreads, alignment = 64)
  tp.workers = tp_allocArrayAligned(Thread[(Taskpool, WorkerID)], numThreads, alignment = 64)
  tp.workerSignals = tp_allocArrayAligned(Signal, numThreads, alignment = 64)

  # Setup master thread
  workerContext.id = 0
  workerContext.taskpool = tp

  # Start worker threads
  for i in 1 ..< numThreads:
    createThread(tp.workers[i], workerEntryFn, (tp, WorkerID(i)))

  # Root worker
  setupWorker()

  # Root task, this is a sentinel task that is never called.
  workerContext.currentTask =
    TaskNode.new(parent = nil, callback = RootTask, envSize = 0)

  # Wait for the child threads
  discard tp.barrier.wait()
  when taskpoolsDebugStall:
    tp.registerPool()
  return tp

proc cleanup(tp: var Taskpool) =
  ## Cleanup all resources allocated by the taskpool
  preCondition: workerContext.currentTask.isRootTask()

  when taskpoolsDebugStall:
    tp.unregisterPool()

  for i in 1 ..< tp.numThreads:
    joinThread(tp.workers[i])

  postCondition: tp.injectionQueue.isEmpty()

  tp.workerSignals.tp_freeAligned()
  tp.workers.tp_freeAligned()
  tp.workerDeques.tp_freeAligned()
  `=destroy`(tp.globalBackoff)
  tp.barrier.delete()

  tp.tp_freeAligned()

proc shutdown*(tp: var Taskpool) =
  ## Wait until all tasks are processed and then shutdown the taskpool
  preCondition: workerContext.currentTask.isRootTask()
  tp.syncAll()

  # Signal termination to all threads
  for i in 0 ..< tp.numThreads:
    tp.workerSignals[i].terminate.store(true, moRelease)

  tp.globalBackoff.wakeAll()

  # 1 matching barrier in worker_entry_fn
  discard tp.barrier.wait()

  teardownWorker()
  tp.cleanup()

  # Dealloc dummy task
  workerContext.currentTask.free()

# Task parallelism
# ---------------------------------------------
{.pop.} # raises:[]

macro spawn*(tp: Taskpool, fnCall: typed): untyped =
  ## Spawns the input function call asynchronously, potentially on another thread of execution.
  ## Safe to call from any thread, including threads that are not part of the taskpool.
  ##
  ## If the function calls returns a result, spawn will wrap it in a Flowvar.
  ## You can use `sync` to block the current thread and extract the asynchronous result from the flowvar.
  ## You can use `isReady` to check if result is available and if subsequent
  ## `spawn` returns immediately.
  ##
  ## Tasks are processed approximately in Last-In-First-Out (LIFO) order
  fnCall.expectKind(nnkCall)

  let fn = fnCall[0]

  if hasClosure(fn):
    error("Closure calls cannot be spawned", fnCall)

  let
    envTup = nnkTupleConstr.newTree()
      # Tuple collecting the return value slot and the runtime arguments
    fwdCall = nnkCall.newTree(fn)
      # same as fnCall, but with parameters forwarded from the closure environment
    env = genSym(nskTemp, "env") # closure environment

  result = newStmtList()

  # A task is similar to a closure proc, but with the environment allocated
  # intrusively in the task node (a single allocation covering the node, the
  # arguments, and the return value slot).
  #
  # The environment is a tuple that holds, in order:
  #
  # * the return value slot, if the call returns a result (at index 0)
  # * runtime parameters, ie those that are not constants / literals / etc
  #
  # The result goes first so an awaiting Flowvar can read it via
  # `tryComplete` without knowing the argument layout.
  let
    retType = fn.getImpl().params()[0]
    hasFuture = retType.kind != nnkEmpty
    argBase = if hasFuture: 1 else: 0

  if hasFuture:
    # Reserve env[0] for the return value, default-initialized until the task runs.
    envTup.add quote do:
      default(typeof `retType`)

  # Continue with the runtime parameters:
  var j = argBase
  for i in 1 ..< fnCall.len:
    let p = fnCall[i]
    if isStatic(p):
      # Literals can be passed as-is to the callee
      fwdCall.add p
    else:
      # Non-literals must be copied to shared memory - add them to the env tuple
      # then extract them from the tuple on the calling side
      let jl = newLit(j)
      j += 1

      when sharedHeap:
        # In ORC, we can isolate values and move them between tasks
        envTup.add quote do:
          isolate(`p`)
        fwdCall.add quote do:
          extract(`env`[][`jl`])
      else:
        # `move` to support move-only types in refc
        envTup.add p
        let hasClosure = newLit(p.kind == nnkSym and hasClosure(p))

        # `refc` uses a thread-local heap - therefore, anything heap-allocated
        # cannot traverse thread boundaries, even if it's isolated - since
        # tasks are likely to end up on a different thread, block their
        # construction here.
        fwdCall.add quote do:
          when `hasClosure` or not supportsThreadMove(typeof(`p`)):
            {.
              error:
                "Garbage-collected types (seq, string, ref, closures) cannot be used as task arguments: " &
                $(typeof(`p`))
            .}

          move(`env`[][`jl`])
  let
    envp = genSym(nskParam, "envp")
      # closure environment, untyped pointer version in `fwdCall`
    envTy = genSym(nskType, "EnvType")
    taskFn = genSym(nskProc, $fn & "_task")
      # Function that calls `fn` within the taskpool thread

  if envTup.len > 0:
    result.add quote do:
      type `envTy` = typeof(`envTup`)

  let body =
    if hasFuture:
      quote do:
        let `env` = cast[ptr `envTy`](`envp`)
        `env`[][0] = `fwdCall`
    elif envTup.len > 0:
      quote do:
        let `env` = cast[ptr `envTy`](`envp`)
        `fwdCall`
    else:
      fwdCall

  result.add quote do:
    proc `taskFn`(`envp`: pointer) {.nimcall, gcsafe, raises: [].} =
      `body`

  # Allocate the single task node (node + intrusive env), write the environment
  # into it, then schedule it and, when needed, return a Flowvar over the node.
  let tn = genSym(nskTemp, "taskNode")

  let parent = quote do:
    if workerContext.taskpool != `tp`: nil else: workerContext.currentTask

  if envTup.len > 0:
    result.add quote do:
      let `tn` = TaskNode.new(`parent`, `taskFn`, sizeof(`envTy`))
      cast[ptr `envTy`](`tn`.env.addr)[] = `envTup`
  else:
    result.add quote do:
      let `tn` = TaskNode.new(`parent`, `taskFn`, 0)

  let fut = genSym(nskTemp, "fut")

  if hasFuture:
    # Must be done before the task node is scheduled.
    result.add quote do:
      let `fut` = newFlowVar(type `retType`, `tn`)
  
  result.add quote do:
    if workerContext.taskpool != `tp`:
      submitTask(`tp`, `tn`)
    else:
      schedule(workerContext, `tn`)
  
  if hasFuture:
    result.add quote do:
      `fut`

  # Wrap in a block for namespacing
  result = nnkBlockStmt.newTree(newEmptyNode(), result)
  # echo result.toStrLit()
