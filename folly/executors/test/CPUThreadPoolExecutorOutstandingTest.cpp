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
#include <deque>
#include <memory>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <vector>

#include <folly/executors/task_queue/LifoSemMPMCQueue.h>
#include <folly/portability/GTest.h>
#include <folly/synchronization/Baton.h>

namespace folly {
namespace {

using Pool = CPUThreadPoolExecutor;
using Task = Pool::CPUTask;
using namespace std::chrono_literals;

static_assert(std::is_nothrow_move_constructible_v<Task>);
static_assert(std::is_nothrow_move_assignable_v<Task>);
static_assert(!std::is_copy_constructible_v<Task>);

Pool::Options counting() {
  return Pool::Options().setTaskCounting();
}

size_t count(const Pool& pool) {
  auto value = pool.getOutstandingTaskCount();
  EXPECT_TRUE(value.has_value());
  return value.value_or(0);
}

// Used only with zero worker threads. The shared queue storage deliberately
// outlives the executor in one test, as an external/custom queue can do.
struct ManualQueueState {
  std::deque<Task> tasks;
  bool reject = false;
  bool throwAfterAccept = false;
};

class ManualQueue final : public BlockingQueue<Task> {
 public:
  explicit ManualQueue(std::shared_ptr<ManualQueueState> state)
      : state_(std::move(state)) {}

  BlockingQueueAddResult add(Task&& task) override {
    if (state_->reject) {
      throw QueueFullException("deliberate rejection before acceptance");
    }
    state_->tasks.push_back(std::move(task));
    if (state_->throwAfterAccept) {
      throw std::runtime_error("deliberate failure after ownership transfer");
    }
    return {};
  }
  Task take() override {
    throw std::logic_error("ManualQueue requires zero workers");
  }
  Optional<Task> try_take_for(std::chrono::milliseconds) override {
    throw std::logic_error("ManualQueue requires zero workers");
  }
  size_t size() override { return state_->tasks.size(); }

 private:
  std::shared_ptr<ManualQueueState> state_;
};

std::unique_ptr<Pool> manualPool(
    const std::shared_ptr<ManualQueueState>& state) {
  return std::make_unique<Pool>(
      std::make_pair(size_t{0}, size_t{0}),
      std::make_unique<ManualQueue>(state),
      std::make_shared<NamedThreadFactory>("OutstandingTest"),
      counting());
}

TEST(CPUThreadPoolExecutorOutstandingTest, DisabledIsNotZero) {
  Pool pool(1);
  EXPECT_FALSE(pool.getOutstandingTaskCount().has_value());
  pool.add([&] { EXPECT_FALSE(pool.getOutstandingTaskCount().has_value()); });
  pool.join();
  EXPECT_FALSE(pool.getOutstandingTaskCount().has_value());
}

TEST(CPUThreadPoolExecutorOutstandingTest, EnabledIdleAndJoin) {
  Pool pool(2, counting());
  EXPECT_EQ(count(pool), 0);
  std::atomic<size_t> completed{0};
  for (size_t i = 0; i < 100; ++i) {
    pool.add([&] { ++completed; });
  }
  pool.join();
  EXPECT_EQ(completed.load(), 100);
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, IncludesRunningAndQueuedTasks) {
  Pool pool(1, counting());
  Baton<> entered;
  Baton<> release;
  pool.add([&] {
    EXPECT_GE(count(pool), 1);
    entered.post();
    release.wait();
  });
  EXPECT_TRUE(entered.try_wait_for(5s));
  pool.add([] {});
  pool.addWithPriority([] {}, Executor::HI_PRI);
  EXPECT_EQ(pool.getTaskQueueSize(), 2);
  EXPECT_EQ(count(pool), 3);
  release.post();
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, CountedBeforeEnqueueObserver) {
  struct Observer final : ThreadPoolExecutor::TaskObserver {
    explicit Observer(Pool& pool) : pool(pool) {}
    void taskEnqueued(const ThreadPoolExecutor::TaskInfo&) noexcept override {
      EXPECT_EQ(count(pool), 1);
      ++calls;
    }
    Pool& pool;
    size_t calls = 0;
  };
  auto state = std::make_shared<ManualQueueState>();
  state->reject = true;
  auto pool = manualPool(state);
  auto observer = std::make_unique<Observer>(*pool);
  auto* observed = observer.get();
  pool->addTaskObserver(std::move(observer));
  EXPECT_THROW(pool->add([] {}), QueueFullException);
  EXPECT_EQ(observed->calls, 1);
  EXPECT_EQ(count(*pool), 0);
  EXPECT_TRUE(state->tasks.empty());
}

TEST(CPUThreadPoolExecutorOutstandingTest, AcceptedOwnershipSurvivesThrow) {
  auto state = std::make_shared<ManualQueueState>();
  state->throwAfterAccept = true;
  auto pool = manualPool(state);
  EXPECT_THROW(pool->add([] {}), std::runtime_error);
  // The queue owns this task. A catch-all N-- in add() would undercount it.
  EXPECT_EQ(count(*pool), 1);
  EXPECT_EQ(state->tasks.size(), 1);
  state->tasks.clear();
  EXPECT_EQ(count(*pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, BoundedQueueRejectionRollsBack) {
  Pool pool(
      std::make_pair(size_t{0}, size_t{0}),
      std::make_unique<LifoSemMPMCQueue<Task>>(1),
      std::make_shared<NamedThreadFactory>("OutstandingTest"),
      counting());
  pool.add([] {});
  EXPECT_EQ(count(pool), 1);
  EXPECT_THROW(pool.add([] {}), QueueFullException);
  EXPECT_EQ(count(pool), 1);
  pool.stop();
  EXPECT_EQ(count(pool), 1); // Destruction, rather than stop(), discards it.
}

TEST(CPUThreadPoolExecutorOutstandingTest, UserExceptionReleasesReservation) {
  Pool pool(1, counting());
  pool.add([] { throw std::runtime_error("expected task exception"); });
  pool.add([] {});
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, ExpirationCallbackRemainsCounted) {
  Pool pool(
      std::make_pair(size_t{0}, size_t{0}),
      std::make_shared<NamedThreadFactory>("OutstandingTest"),
      counting());
  std::atomic<bool> ran{false};
  Baton<> expired;
  Baton<> release;
  pool.add(
      [&] { ran = true; },
      1ms,
      [&] {
        EXPECT_EQ(count(pool), 1);
        expired.post();
        release.wait();
        throw std::runtime_error("expected expiration callback exception");
      });
  EXPECT_EQ(count(pool), 1);
  std::this_thread::sleep_for(5ms);
  pool.setNumThreads(1);
  EXPECT_TRUE(expired.try_wait_for(5s));
  EXPECT_EQ(count(pool), 1);
  EXPECT_FALSE(ran.load());
  release.post();
  pool.join();
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, ProcessedObserverStillCounted) {
  struct Observer final : ThreadPoolExecutor::TaskObserver {
    explicit Observer(Pool& pool) : pool(pool) {}
    void taskProcessed(const ThreadPoolExecutor::ProcessedTaskInfo&)
        noexcept override {
      EXPECT_EQ(count(pool), 1);
      ++calls;
    }
    Pool& pool;
    size_t calls = 0;
  };
  Pool pool(1, counting());
  auto observer = std::make_unique<Observer>(pool);
  auto* observed = observer.get();
  pool.addTaskObserver(std::move(observer));
  pool.add([] {});
  pool.join();
  EXPECT_EQ(observed->calls, 1);
  EXPECT_EQ(count(pool), 0);
}

TEST(
    CPUThreadPoolExecutorOutstandingTest,
    ExpirationWithoutCallbackReleasesReservation) {
  Pool pool(
      0,
      int8_t{3},
      std::make_shared<NamedThreadFactory>("OutstandingTest"),
      counting());
  std::atomic<bool> ran{false};
  pool.add([&] { ran = true; }, Executor::HI_PRI, 1ms);
  EXPECT_EQ(count(pool), 1);
  std::this_thread::sleep_for(5ms);
  pool.setNumThreads(1);
  pool.join();
  EXPECT_FALSE(ran.load());
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, MoveAndOverwriteReleaseExactlyOnce) {
  auto state = std::make_shared<ManualQueueState>();
  auto pool = manualPool(state);
  pool->add([] {});
  pool->addWithPriority([] {}, Executor::HI_PRI);
  EXPECT_EQ(count(*pool), 2);
  {
    Task first(std::move(state->tasks.front()));
    state->tasks.pop_front();
    EXPECT_EQ(count(*pool), 2);
    first = std::move(state->tasks.front()); // Discards the previous first task.
    state->tasks.pop_front();
    EXPECT_EQ(count(*pool), 1);
    first = Task(); // Replacing a counted task with poison releases its ticket.
    EXPECT_EQ(count(*pool), 0);
  }
  EXPECT_EQ(count(*pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, StopRetainsUntilActuallyDiscarded) {
  auto state = std::make_shared<ManualQueueState>();
  auto pool = manualPool(state);
  pool->add([] {});
  pool->add([] {});
  pool->stop();
  EXPECT_EQ(state->tasks.size(), 2);
  EXPECT_EQ(count(*pool), 2);
  state->tasks.clear();
  EXPECT_EQ(count(*pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, StopPoisonIsNotOrdinaryWork) {
  class InspectablePool : public Pool {
   public:
    using Pool::Pool;
    Task takePending() { return getTaskQueue()->take(); }
  };
  InspectablePool pool(1, counting());
  Baton<> entered;
  Baton<> release;
  pool.add([&] {
    entered.post();
    release.wait();
  });
  EXPECT_TRUE(entered.try_wait_for(5s));
  pool.add([] { ADD_FAILURE() << "pending task should be discarded"; });
  pool.add([] { ADD_FAILURE() << "pending task should be discarded"; });
  std::thread stopper([&] { pool.stop(); });
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (pool.getTaskQueueSize() < 3 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  const bool poisonQueued = pool.getTaskQueueSize() == 3;
  EXPECT_TRUE(poisonQueued);
  EXPECT_EQ(count(pool), 3); // Running + two pending, not the poison.
  release.post();
  stopper.join();
  if (!poisonQueued) {
    return;
  }
  EXPECT_EQ(count(pool), 2);
  pool.takePending(); // Destroy first unexecuted task.
  EXPECT_EQ(count(pool), 1);
  pool.takePending(); // Destroy second unexecuted task.
  EXPECT_EQ(count(pool), 0);
  pool.takePending(); // Poison cleanup has no effect.
  EXPECT_EQ(count(pool), 0);
}

TEST(CPUThreadPoolExecutorOutstandingTest, RetainedTaskMayOutliveExecutor) {
  auto state = std::make_shared<ManualQueueState>();
  {
    auto pool = manualPool(state);
    pool->add([] {});
    EXPECT_EQ(count(*pool), 1);
  }
  ASSERT_EQ(state->tasks.size(), 1);
  // ASan/UBSan can verify that ticket cleanup does not access a dead executor.
  Task held(std::move(state->tasks.front()));
  state->tasks.clear();
  held = Task();
}

TEST(CPUThreadPoolExecutorOutstandingTest, MultipleProducersDrainToZero) {
  Pool pool(4, counting());
  std::atomic<size_t> completed{0};
  std::vector<std::thread> producers;
  for (size_t p = 0; p < 4; ++p) {
    producers.emplace_back([&] {
      for (size_t i = 0; i < 250; ++i) {
        pool.addWithPriority([&] { ++completed; }, Executor::MID_PRI);
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }
  pool.join();
  EXPECT_EQ(completed.load(), 1000);
  EXPECT_EQ(count(pool), 0);
}

} // namespace
} // namespace folly
