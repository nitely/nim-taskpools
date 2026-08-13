mode = ScriptMode.Verbose

packageName   = "taskpools"
version       = "0.2.1"
author        = "Status Research & Development GmbH"
description   = "lightweight, energy-efficient, easily auditable threadpool"
license       = "MIT"
skipDirs      = @["tests"]

requires "nim >= 2.0.14", "unittest2"

import strutils

let nimc = getEnv("NIMC", "nim") # Which nim compiler to use
let lang = getEnv("NIMLANG", "c") # Which backend (c/cpp/js)
let flags = getEnv("NIMFLAGS", "") # Extra flags for the compiler
let verbose = getEnv("V", "") notin ["", "0"]

let cfg =
  " --styleCheck:usages --styleCheck:error" &
  (if verbose: "" else: " --verbosity:0 --hints:off") &
  " --skipParentCfg --skipUserCfg --outdir:build --nimcache:build/nimcache -f" &
  " --stacktrace:on --linetrace:on" &
  " --threads:on"

proc build(args, path: string) =
  exec nimc & " " & lang & " " & cfg & " " & flags & " " & args & " " & path

proc run(args, path: string) =
  build args & " --mm:refc -r", path
  build args & " --mm:orc -r", path

  if (NimMajor, NimMinor) >= (2, 2) and defined(linux) and defined(amd64) and "danger" in args:
    build args & " --mm:arc -d:useMalloc --cc:clang --passc:-fsanitize=address --passl:-fsanitize=address --debugger:native -r", path
    build args & " --mm:orc -d:useMalloc --cc:clang --passc:-fsanitize=address --passl:-fsanitize=address --debugger:native -r", path
    build args & " --mm:orc -d:taskpoolsTsan -d:useMalloc --cc:clang --passc:-fsanitize=thread --passl:-fsanitize=thread --debugger:native -r", path
    build args & " --mm:refc -d:taskpoolsTsan --cc:clang --passc:-fsanitize=thread --passl:-fsanitize=thread --debugger:native -r", path

proc runTests(args: string) =
  # Internal data structures
  run args, "taskpools/sparsesets.nim"

  # Examples
  run args, "examples/e01_simple_tasks.nim"
  run args, "examples/e02_parallel_pi.nim"
  run args, "examples/e03_external_threads.nim"

  # Tests
  run args, "tests/test_all.nim"

  # Regression test for the schedule/push lost wakeup. Kept out of test_all
  # because it hangs rather than fails when the bug is back - run it under a
  # timeout in CI.
  run args, "tests/test_spawn_spin.nim"

task test, "Run tests":
  for mode in ["", "-d:release", "-d:danger"]:
    runTests(mode)

task test_generic_futex, "Run tests with generic futex":
  for mode in ["", "-d:release", "-d:danger"]:
    run mode & " -d:taskpoolsGenericFutex", "tests/test_all.nim"

proc runBenchs(args: string) =
  run args, "benchmarks/dfs/taskpool_dfs.nim"
  # run args, "benchmarks/fibonacci/taskpool_fib.nim"
  run args, "benchmarks/heat/taskpool_heat.nim"
  run args, "benchmarks/nqueens/taskpool_nqueens.nim"
  run args, "benchmarks/iqs_latency/taskpool_iqs_latency.nim"

  when not defined(windows):
    run args, "benchmarks/single_task_producer/taskpool_spc.nim"
    run args, "benchmarks/bouncing_producer_consumer/taskpool_bpc.nim"

  # TODO - generics in macro issue
  # run args, "benchmarks/matmul_cache_oblivious/taskpool_matmul_co.nim"

const stressTests = [
  "tests/stress/test_arm_flowvar_wakeup.nim",
  "tests/stress/test_arm_backoff_wakeup.nim",
  "tests/stress/test_arm_taskpool_wakeup.nim",
]

task test_stress, "Run the weak-memory (ARM) lost-wakeup stress tests":
  # These hunt for lost wakeups in the flowvar completion handshake and in the
  # EventCount. They only ever fail on weakly-ordered CPUs (aarch64): on x86 the
  # `lock`-prefixed RMWs are full barriers and hide the missing store-load
  # ordering. Each test is watchdogged, so a lost wakeup aborts the process
  # instead of hanging the CI job.
  #
  # Every scenario is capped by both an iteration count and a 20s time budget,
  # whichever comes first, so the wall time is ~7 scenarios x 20s regardless of
  # the core count (a `Taskpool.new()` spawns one thread per core, so a
  # count-only cap would take orders of magnitude longer on a big machine).
  #
  # When actually hunting on aarch64 hardware, raise the budget and loop the
  # binaries: -d:tpStressBudgetMs:600_000. The window is a handful of cycles
  # wide, so soak time is what finds it.
  # Tune with -d:tpStressBudgetMs:N -d:tpStressIters:N -d:tpStressRounds:N
  #           -d:tpStressTimeoutMs:N (watchdog stall threshold)
  for path in stressTests:
    run "-d:release -d:tpStressBudgetMs:60_000 -d:tpStressIters:50_000_000 -d:tpStressRounds:5_000_000", path

task test_stress2, "Run shutdown test":
  run "-d:release", "tests/stress/test_shutdown.nim"

task test_stall, "Run the suite under the stall detector (hang hunting)":
  # Builds everything with -d:taskpoolsDebugStall. A watchdog thread polls the
  # live pools at 10Hz; if a pool is bit-identical with nobody running for
  # taskpoolsStallSeconds it dumps per-worker phases + pending work and aborts,
  # so a hang fails the job in seconds with a diagnosis instead of timing out.
  #
  # The instrumentation is deliberately I/O-free on the scheduler paths (see
  # taskpools/instrumentation/stall_detector.nim): the whole point is to not
  # perturb the timings that make the bug reproduce.
  #
  # Tune the threshold with -d:taskpoolsStallSeconds:N (default 30).
  let stallFlags = " -d:taskpoolsDebugStall"
  for mode in ["-d:release", "-d:danger"]:
    run mode & stallFlags, "tests/test_all.nim"
    run mode & stallFlags & " -d:taskpoolsGenericFutex", "tests/test_all.nim"
  # run "-d:release" & stallFlags, "tests/stress/test_shutdown.nim"

task test_bench, "Run benchs":
  for mode in ["", "-d:release", "-d:danger"]:
    runBenchs(mode)

  # Avoid TSan; it's too slow
  run "-d:release", "benchmarks/fibonacci/taskpool_fib.nim"
