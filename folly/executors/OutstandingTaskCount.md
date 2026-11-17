# Optional outstanding task accounting

This branch adds an opt-in `CPUThreadPoolExecutor` counter. It does not add
load balancing, CPU capacity estimation, or request-level accounting.

```cpp
using Pool = folly::CPUThreadPoolExecutor;
Pool pool(16, Pool::Options().setTaskCounting(true));
auto n = pool.getOutstandingTaskCount();
if (n.has_value()) {
  // Export *n through an existing metrics path.
}
```

Counting defaults to disabled. The getter returns `folly::none` when disabled,
and an engaged optional containing zero when enabled and idle. Do not turn an
unavailable metric into zero when deciding where to submit work. The option is
fixed at construction; existing tasks cannot be accounted retroactively.

## Meaning and ownership

N counts ordinary tasks reserved for this pool and not yet completed or
discarded. `addImpl()` reserves before observers and queue publication. Taking
a task from the queue or starting its callback does not decrement N. A worker
releases the reservation after `runTask()` returns, including expiration
callbacks, exception handling, task observers, and its context cleanup.

A move-only ticket in `CPUTask` transfers the reservation through queue moves.
Ticket destruction releases a reservation if execution did not already do so.
This covers rejection before acceptance and destruction of unexecuted tasks.
If a custom queue accepts ownership and then throws, the reservation remains
with the accepted task. There is no unconditional rollback in an `add()` catch.
Move assignment releases the overwritten ticket before taking its replacement.

Internal stop markers have no ticket. `stop()` may leave tasks in the queue;
it does not force N to zero. `join()` and existing ownership protocols remain
the synchronization mechanisms. A relaxed observation of zero is not a safe
replacement for stopping producers or joining the executor.

The shared counter state can outlive the executor when a custom queue retains
a task. This does not make calling the getter on a destroyed executor valid.
The original KeepAlive logic is preserved, and accounting introduces no new
unprotected access to `this` after publishing a task.

N is neither the queue length nor the number of active/runnable threads. A
callback blocked on a lock or synchronous RPC is counted until it finishes.
An asynchronous callback that initiates RPC and returns releases its ticket;
a later submitted continuation is a new task. Graph nodes not yet submitted,
network waits between callbacks, and complete requests are not counted.

## Cost and compatibility

An enabled pool allocates one shared counter state. Each task performs a
relaxed count increment/decrement, shared-pointer ownership operations, and
ticket moves. There is no separately allocated ticket per task. The getter
performs one relaxed load; it does not synchronize task data. Multiple pool
snapshots and a subsequent submission are not a single atomic admission step.

Disabled accounting avoids counter updates and state allocation, but the
additional task member still changes object size and move/check costs. Do not
claim zero overhead without measurement.

The reference base is `v2026.10.26.00`, commit
`c8c7adb72c6d73e2fced5ff5cebc96aa3ff65b2c`. This is an experimental branch API,
not an assertion that other Folly releases provide it. `Options`, executor
layout, and public `CPUTask` layout change. Rebuild Folly and affected consumers,
including custom `BlockingQueue<CPUTask>` implementations. Never mix new headers
with old binaries.

For another Folly version, audit all submission overloads, constructor option
forwarding, task move constraints, queue rejection/ownership rules, expiration,
`runTask()` ownership, stop/join behavior, and post-enqueue KeepAlive protection.
Port by lifecycle responsibilities rather than line numbers. Do not change the
site's Folly version merely to apply this patch.

## Focused tests

`folly/executors/test/CPUThreadPoolExecutorOutstandingTest.cpp` tests disabled
and enabled states, running plus queued work, reservation before observers,
rejection, accepted ownership followed by an exception, task exceptions,
expiration, observer boundaries, moves, stop markers, retained tasks, and
multiple producers.

With an existing Folly test configuration, build the CMake target
`executors_cpu_thread_pool_executor_outstanding_test` and run the corresponding
test only. The test is also registered in BUCK. A separate handoff harness in
the accompanying `executor_demo` branch can compile it against an isolated
Folly installation without enabling the complete Folly test build.

This document describes functionality. It does not claim ARM overhead,
production integration, all-tests success, or sanitizer validation.
