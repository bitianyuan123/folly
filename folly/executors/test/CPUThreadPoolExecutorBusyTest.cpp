/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/executors/CPUThreadPoolExecutor.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include <folly/executors/QueueObserver.h>
#include <folly/executors/task_queue/LifoSemMPMCQueue.h>
#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>

namespace folly {
namespace {

using Pool = CPUThreadPoolExecutor;
using namespace std::chrono_literals;

Pool::Options counting() {
  return Pool::Options().setBusyCounting();
}

size_t count(const Pool& pool) {
  auto value = pool.getBusyTaskCount();
  EXPECT_TRUE(value.has_value());
  return value.value_or(0);
}

template <typename Predicate>
bool waitUntil(Predicate&& predicate) {
  const auto end = std::chrono::steady_clock::now() + 5s;
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= end) {
      return false;
    }
    std::this_thread::yield();
  }
  return true;
}

// Each blocked callback also has a deadline. Test failures must not leave an
// executor destructor waiting forever for a gate that will never be released.
struct Gate {
  Baton<> entered;
  Baton<> release;

  void block() {
    entered.post();
    EXPECT_TRUE(release.try_wait_for(5s));
  }
  void waitForEntry() { EXPECT_TRUE(entered.try_wait_for(5s)); }
};

// Unlike Baton (single waiter), this gate supports several blocked workers.
class MultiWaiterGate {
 public:
  bool wait() {
    std::unique_lock lock(mutex_);
    return condition_.wait_for(lock, 5s, [&] { return released_; });
  }
  void post() {
    {
      std::lock_guard lock(mutex_);
      released_ = true;
    }
    condition_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable condition_;
  bool released_ = false;
};

#if FOLLY_HAVE_WEAK_SYMBOLS
struct QueueBoundaryState {
  Pool* pool = nullptr;
  Gate gate;
};
std::atomic<QueueBoundaryState*> queueBoundaryState{nullptr};

class BoundaryQueueObserver final : public QueueObserver {
 public:
  explicit BoundaryQueueObserver(QueueBoundaryState& state) : state_(state) {}
  intptr_t onEnqueued(const RequestContext*) override {
    EXPECT_EQ(count(*state_.pool), 0);
    return 0;
  }
  void onDequeued(intptr_t) override {
    EXPECT_EQ(count(*state_.pool), 0);
    state_.gate.block();
  }

 private:
  QueueBoundaryState& state_;
};

class BoundaryQueueObserverFactory final : public QueueObserverFactory {
 public:
  explicit BoundaryQueueObserverFactory(QueueBoundaryState& state)
      : state_(state) {}
  std::unique_ptr<QueueObserver> create(int8_t) override {
    return std::make_unique<BoundaryQueueObserver>(state_);
  }

 private:
  QueueBoundaryState& state_;
};
#endif

} // namespace

#if FOLLY_HAVE_WEAK_SYMBOLS
// Only the explicitly named test pool gets this observer; unrelated executor
// regression tests linked into the same binary keep the default behavior.
std::unique_ptr<QueueObserverFactory> make_queue_observer_factory(
    const std::string& context, size_t, WorkerProvider*) {
  auto* state = queueBoundaryState.load(std::memory_order_acquire);
  if (state && context == "cpu.BusyQueueBoundary") {
    return std::make_unique<BoundaryQueueObserverFactory>(*state);
  }
  return nullptr;
}
#endif

namespace {

TEST(CPUThreadPoolExecutorBusyTest, DisabledIsUnavailable) {
  Pool pool(1);
  EXPECT_FALSE(pool.getBusyTaskCount().has_value());
  pool.add([&] { EXPECT_FALSE(pool.getBusyTaskCount().has_value()); });
  pool.join();
  EXPECT_FALSE(pool.getBusyTaskCount().has_value());
}

TEST(CPUThreadPoolExecutorBusyTest, ExplicitDisableAndIdleZero) {
  Pool disabled(1, counting().setBusyCounting(false));
  EXPECT_FALSE(disabled.getBusyTaskCount().has_value());
  disabled.join();
  Pool enabled(2, counting());
  EXPECT_EQ(count(enabled), 0);
  enabled.join();
  EXPECT_EQ(count(enabled), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, QueuedTasksExcluded) {
  Gate running;
  Pool pool(1, counting());
  pool.add([&] {
    EXPECT_EQ(count(pool), 1);
    running.block();
  });
  running.waitForEntry();
  pool.add([] {});
  pool.addWithPriority([] {}, Executor::HI_PRI);
  EXPECT_EQ(pool.getTaskQueueSize(), 2);
  EXPECT_EQ(count(pool), 1);
  running.release.post();
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, BlockedCallbackRemainsBusy) {
  Gate blocked;
  Pool pool(1, counting());
  pool.add([&] { blocked.block(); });
  blocked.waitForEntry();
  // The callback waits on a synchronization primitive rather than consuming
  // CPU, but worker occupancy is still one. B is not an OS runnable count.
  EXPECT_EQ(pool.getTaskQueueSize(), 0);
  EXPECT_EQ(count(pool), 1);
  blocked.release.post();
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, RejectedAndQueuedNeverBusy) {
  Pool pool(
      std::make_pair(size_t{0}, size_t{0}),
      std::make_unique<LifoSemMPMCQueue<Pool::CPUTask>>(1),
      std::make_shared<NamedThreadFactory>("BusyTest"),
      counting());
  pool.add([] {});
  EXPECT_EQ(count(pool), 0);
  EXPECT_THROW(pool.add([] {}), QueueFullException);
  EXPECT_EQ(count(pool), 0);
  pool.stop();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, UserExceptionReleasesBusy) {
  Pool pool(1, counting());
  pool.add([&] {
    EXPECT_EQ(count(pool), 1);
    throw std::runtime_error("expected task exception");
  });
  pool.add([&] { EXPECT_EQ(count(pool), 1); });
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, ExpirationCallbackRemainsBusy) {
  Gate expired;
  Pool pool(
      std::make_pair(size_t{0}, size_t{0}),
      std::make_shared<NamedThreadFactory>("BusyTest"),
      counting());
  std::atomic<bool> ran{false};
  pool.add(
      [&] { ran = true; },
      1ms,
      [&] {
        EXPECT_EQ(count(pool), 1);
        expired.block();
        throw std::runtime_error("expected expiration callback exception");
      });
  EXPECT_EQ(count(pool), 0);
  std::this_thread::sleep_for(5ms);
  pool.setNumThreads(1);
  expired.waitForEntry();
  EXPECT_EQ(count(pool), 1);
  EXPECT_FALSE(ran.load());
  expired.release.post();
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, ExpirationWithoutCallbackIsBalanced) {
  struct Observer final : ThreadPoolExecutor::TaskObserver {
    explicit Observer(Pool& pool) : pool(pool) {}
    void taskProcessed(
        const ThreadPoolExecutor::ProcessedTaskInfo& info) noexcept override {
      EXPECT_TRUE(info.expired);
      EXPECT_EQ(count(pool), 1);
      ++expired;
    }
    Pool& pool;
    size_t expired = 0;
  };
  Pool pool(
      std::make_pair(size_t{0}, size_t{0}),
      std::make_shared<NamedThreadFactory>("BusyTest"),
      counting());
  auto observer = std::make_unique<Observer>(pool);
  auto* observed = observer.get();
  pool.addTaskObserver(std::move(observer));
  std::atomic<bool> ran{false};
  pool.add([&] { ran = true; }, 1ms);
  EXPECT_EQ(count(pool), 0);
  std::this_thread::sleep_for(5ms);
  pool.setNumThreads(1);
  pool.join();
  EXPECT_FALSE(ran.load());
  EXPECT_EQ(observed->expired, 1);
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, TaskObserversInsideRunTaskAreBusy) {
  struct Observer final : ThreadPoolExecutor::TaskObserver {
    Observer(Pool& pool, Gate& processed) : pool(pool), processed(processed) {}
    void taskEnqueued(const ThreadPoolExecutor::TaskInfo&) noexcept override {
      EXPECT_EQ(count(pool), 0);
    }
    void taskDequeued(
        const ThreadPoolExecutor::DequeuedTaskInfo&) noexcept override {
      EXPECT_EQ(count(pool), 1);
    }
    void taskProcessed(
        const ThreadPoolExecutor::ProcessedTaskInfo&) noexcept override {
      EXPECT_EQ(count(pool), 1);
      processed.block();
    }
    Pool& pool;
    Gate& processed;
  };
  Gate processed;
  Pool pool(
      std::make_pair(size_t{0}, size_t{0}),
      std::make_shared<NamedThreadFactory>("BusyTest"),
      counting());
  pool.addTaskObserver(std::make_unique<Observer>(pool, processed));
  pool.add([] {});
  pool.setNumThreads(1);
  processed.waitForEntry();
  EXPECT_EQ(count(pool), 1);
  processed.release.post();
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, QueueObserverBeforeRunTaskIsExcluded) {
#if FOLLY_HAVE_WEAK_SYMBOLS
  QueueBoundaryState state;
  queueBoundaryState.store(&state, std::memory_order_release);
  Pool pool(
      std::make_pair(size_t{0}, size_t{0}),
      std::make_shared<NamedThreadFactory>("BusyQueueBoundary"),
      counting());
  state.pool = &pool;
  pool.add([&] { EXPECT_EQ(count(pool), 1); });
  pool.setNumThreads(1);
  state.gate.waitForEntry();
  EXPECT_EQ(count(pool), 0);
  state.gate.release.post();
  pool.join();
  EXPECT_EQ(count(pool), 0);
  queueBoundaryState.store(nullptr, std::memory_order_release);
#else
  GTEST_SKIP() << "QueueObserver factory injection requires weak symbols";
#endif
}

TEST(CPUThreadPoolExecutorBusyTest, EverySubmissionOverloadUsesSameBoundary) {
  Pool pool(0, 3, std::make_shared<NamedThreadFactory>("BusyTest"), counting());
  std::atomic<size_t> completed{0};
  auto work = [&] {
    EXPECT_EQ(count(pool), 1);
    ++completed;
  };
  pool.add(work);
  pool.addWithPriority(work, Executor::HI_PRI);
  pool.add(work, std::chrono::hours(1), [] { ADD_FAILURE(); });
  pool.add(work, Executor::LO_PRI, std::chrono::hours(1), [] {
    ADD_FAILURE();
  });
  EXPECT_EQ(count(pool), 0);
  pool.setNumThreads(1);
  pool.join();
  EXPECT_EQ(completed.load(), 4);
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, StopRetainsBusyUntilCallbackReturns) {
  Gate running;
  Pool pool(
      std::make_pair(size_t{1}, size_t{1}),
      std::make_shared<NamedThreadFactory>("BusyTest"),
      counting());
  std::atomic<size_t> ranQueued{0};
  pool.add([&] { running.block(); });
  running.waitForEntry();
  pool.add([&] { ++ranQueued; });
  pool.add([&] { ++ranQueued; });
  std::thread stopper([&] { pool.stop(); });
  EXPECT_TRUE(waitUntil([&] { return pool.getTaskQueueSize() >= 3; }));
  // stop() has enqueued poison, but the active callback is still occupied.
  EXPECT_EQ(count(pool), 1);
  running.release.post();
  stopper.join();
  EXPECT_EQ(count(pool), 0);
  EXPECT_EQ(ranQueued.load(), 0);
  EXPECT_GE(pool.getTaskQueueSize(), 2);
}

TEST(CPUThreadPoolExecutorBusyTest, ConcurrentProducersRespectWorkerCapacity) {
  constexpr size_t workers = 4;
  MultiWaiterGate release;
  Pool pool(
      std::make_pair(workers, workers),
      std::make_shared<NamedThreadFactory>("BusyTest"),
      counting());
  std::atomic<size_t> entered{0};
  for (size_t i = 0; i < workers; ++i) {
    pool.add([&] {
      ++entered;
      EXPECT_TRUE(release.wait());
    });
  }
  EXPECT_TRUE(waitUntil([&] { return entered.load() == workers; }));
  EXPECT_EQ(count(pool), workers);
  std::atomic<size_t> completed{0};
  std::vector<std::thread> producers;
  for (size_t i = 0; i < 4; ++i) {
    producers.emplace_back([&] {
      for (size_t j = 0; j < 100; ++j) {
        pool.add([&] {
          const auto busy = count(pool);
          EXPECT_GE(busy, 1);
          EXPECT_LE(busy, workers);
          ++completed;
        });
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }
  EXPECT_EQ(count(pool), workers);
  release.post();
  pool.join();
  EXPECT_EQ(completed.load(), 400);
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, DynamicallyCreatedWorkersAreCounted) {
  MultiWaiterGate release;
  Pool pool(
      std::make_pair(size_t{4}, size_t{0}),
      std::make_shared<NamedThreadFactory>("BusyTest"),
      counting());
  EXPECT_EQ(count(pool), 0);
  std::atomic<size_t> entered{0};
  for (size_t i = 0; i < 4; ++i) {
    pool.add([&] {
      ++entered;
      EXPECT_TRUE(release.wait());
    });
    // Ensure earlier workers are occupied before asking the dynamic pool to
    // grow again; do not depend on racing wakeup/worker-creation timing.
    EXPECT_TRUE(waitUntil([&] { return entered.load() == i + 1; }));
  }
  EXPECT_EQ(count(pool), 4);
  release.post();
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorBusyTest, AllConstructorsPreserveOption) {
  auto factory = std::make_shared<NamedThreadFactory>("BusyTest");
  std::vector<std::unique_ptr<Pool>> pools;
  pools.push_back(std::make_unique<Pool>(1, counting()));
  pools.push_back(std::make_unique<Pool>(1, factory, counting()));
  pools.push_back(std::make_unique<Pool>(
      std::make_pair(size_t{1}, size_t{1}), factory, counting()));
  pools.push_back(
      std::make_unique<Pool>(1, Pool::makeDefaultQueue(), factory, counting()));
  pools.push_back(std::make_unique<Pool>(
      std::make_pair(size_t{1}, size_t{1}),
      Pool::makeDefaultQueue(),
      factory,
      counting()));
  pools.push_back(std::make_unique<Pool>(1, 3, factory, counting()));
  pools.push_back(std::make_unique<Pool>(1, 3, 8, factory, counting()));
  for (auto& pool : pools) {
    EXPECT_EQ(count(*pool), 0);
    auto* executor = pool.get();
    pool->add([executor] { EXPECT_EQ(count(*executor), 1); });
    pool->join();
    EXPECT_EQ(count(*pool), 0);
  }
}

} // namespace
} // namespace folly
