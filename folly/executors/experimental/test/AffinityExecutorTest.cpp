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

#include <folly/executors/experimental/AffinityExecutor.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <limits>
#include <mutex>
#include <thread>

#include <folly/io/async/Request.h>
#include <folly/portability/GTest.h>

namespace folly::experimental {
namespace {

using Pool = AffinityExecutor;
using namespace std::chrono_literals;

Pool::Config config(Pool::Policy policy = Pool::Policy::Borrow) {
  Pool::Config cfg;
  cfg.policy = policy;
  cfg.groups.resize(2);
  cfg.groups[0].numaNode = 0;
  cfg.groups[1].numaNode = 1;
  return cfg;
}

class Gate {
 public:
  void wait() {
    std::unique_lock lock(mutex_);
    cv_.wait(lock, [&] { return open_; });
  }
  void open() {
    std::lock_guard lock(mutex_);
    open_ = true;
    cv_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool open_{false};
};

std::future<void> block(
    Pool& pool, Gate& gate, uint64_t estimate = 10000, size_t home = 0) {
  auto started = std::make_shared<std::promise<void>>();
  auto future = started->get_future();
  Pool::Hint hint;
  hint.home = home;
  hint.estimatedCpuUs = estimate;
  if (!pool.add(hint, [started, &gate](const auto&) {
        started->set_value();
        gate.wait();
      })) {
    throw std::runtime_error("blocking test task rejected");
  }
  return future;
}

std::future<Pool::Decision> submit(Pool& pool, Pool::Hint hint = {}) {
  auto result = std::make_shared<std::promise<Pool::Decision>>();
  auto future = result->get_future();
  if (!pool.add(hint, [result](const auto& d) { result->set_value(d); })) {
    throw std::runtime_error("test task rejected");
  }
  return future;
}

TEST(AffinityExecutor, HomeKeepsBusyGroup) {
  Gate gate;
  Pool pool(config(Pool::Policy::Home));
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
  auto result = submit(pool);
  EXPECT_EQ(pool.snapshots()[0].pending, 1);
  EXPECT_EQ(pool.snapshots()[0].inflight, 1);
  gate.open();
  ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(result.get().chosen, 0);
}

TEST(AffinityExecutor, LoadReadSeparatesRunningAndQueuedWorkWithoutWorkerTids) {
  for (auto queue : {Pool::QueueKind::Default,
                     Pool::QueueKind::Lifo,
                     Pool::QueueKind::Throttled}) {
    Gate gate;
    auto cfg = config(Pool::Policy::Home);
    cfg.queueKind = queue;
    cfg.groups[0].capacity = 2.5;
    Pool pool(cfg);
    auto release = folly::makeGuard([&] { gate.open(); });
    ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
    EXPECT_EQ(pool.readQueueDepth(0), 0);
    EXPECT_EQ(pool.readLoad(0).inflight, 1);
    auto result = submit(pool);
    const auto load = pool.readLoad(0);
    EXPECT_EQ(load.group, 0);
    EXPECT_EQ(load.pending, 1);
    EXPECT_EQ(load.inflight, 1);
    EXPECT_EQ(load.outstanding, 2);
    EXPECT_EQ(load.estimatedOutstandingCpuUs, 11000);
    EXPECT_EQ(load.submitted, 2);
    EXPECT_EQ(load.completed, 0);
    EXPECT_EQ(load.rejected, 0);
    EXPECT_EQ(load.migratedIn, 0);
    EXPECT_EQ(load.failed, 0);
    EXPECT_EQ(load.queueDepth, 1);
    EXPECT_EQ(pool.readQueueDepth(0), load.queueDepth);
    EXPECT_DOUBLE_EQ(load.capacity, 2.5);
    EXPECT_TRUE(load.workerTids.empty());
    EXPECT_EQ(pool.snapshots()[0].outstanding, load.outstanding);
#ifdef __linux__
    EXPECT_FALSE(pool.snapshots()[0].workerTids.empty());
#endif
    EXPECT_EQ(pool.readLoad(1).outstanding, 0);
    EXPECT_EQ(pool.readQueueDepth(1), 0);
    gate.open();
    ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
    pool.join();
    const auto drained = pool.readLoad(0);
    EXPECT_EQ(drained.pending, 0);
    EXPECT_EQ(drained.inflight, 0);
    EXPECT_EQ(drained.outstanding, 0);
    EXPECT_EQ(drained.estimatedOutstandingCpuUs, 0);
    EXPECT_EQ(drained.completed, 2);
    EXPECT_EQ(pool.readQueueDepth(0), 0);
    EXPECT_TRUE(drained.workerTids.empty());
  }
}

TEST(AffinityExecutor, LoadReadsRejectInvalidGroup) {
  Pool pool(config());
  for (auto group : {pool.config().groups.size(),
                     std::numeric_limits<size_t>::max()}) {
    EXPECT_THROW(pool.readLoad(group), std::out_of_range);
    EXPECT_THROW(pool.readQueueDepth(group), std::out_of_range);
  }
}

TEST(AffinityExecutor, BorrowAvoidsBusyHomeAndReportsReturnMigration) {
  Gate gate;
  Pool pool(config());
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
  auto result = submit(pool);
  ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
  const auto first = result.get();
  EXPECT_EQ(first.chosen, 1);
  EXPECT_TRUE(first.migrated);
  EXPECT_TRUE(first.rerouted);
  EXPECT_TRUE(first.crossNuma);
  gate.open();
  Pool::Hint returnHint;
  returnHint.hasParent = true;
  returnHint.parent = 1;
  // A promise set inside the callback precedes its final accounting; wait for
  // the accounting itself before testing the return-home decision.
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (pool.snapshots()[0].outstanding != 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }
  ASSERT_EQ(pool.snapshots()[0].outstanding, 0);
  auto back = submit(pool, returnHint);
  ASSERT_EQ(back.wait_for(5s), std::future_status::ready);
  const auto returned = back.get();
  EXPECT_EQ(returned.chosen, 0);
  EXPECT_EQ(returned.source, 1);
  EXPECT_TRUE(returned.migrated);
  EXPECT_TRUE(returned.crossNuma);
  EXPECT_FALSE(returned.rerouted);
}

TEST(AffinityExecutor, StickyUsesParentAsPreferred) {
  Pool pool(config(Pool::Policy::Sticky));
  Pool::Hint hint;
  hint.hasParent = true;
  hint.parent = 1;
  const auto d = submit(pool, hint).get();
  EXPECT_EQ(d.preferred, 1);
  EXPECT_EQ(d.chosen, 1);
  EXPECT_FALSE(d.migrated);
}

TEST(AffinityExecutor, FanoutBalancesOnlyFlaggedSubmissions) {
  Gate gate;
  Pool pool(config(Pool::Policy::Fanout));
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
  Pool::Hint fanout;
  fanout.isFanout = true;
  EXPECT_EQ(submit(pool, fanout).get().chosen, 1);
  Pool::Hint continuation;
  continuation.hasParent = true;
  continuation.parent = 0;
  auto stays = submit(pool, continuation);
  EXPECT_EQ(pool.snapshots()[0].pending, 1);
  gate.open();
  EXPECT_EQ(stays.get().chosen, 0);
  continuation.parent = 1;
  EXPECT_EQ(submit(pool, continuation).get().chosen, 1);
}

TEST(AffinityExecutor, QueueComparatorCannotSeeRunningTask) {
  for (auto metric : {Pool::LoadMetric::Queue, Pool::LoadMetric::Pending}) {
    Gate gate;
    auto cfg = config();
    cfg.metric = metric;
    Pool pool(cfg);
    auto release = folly::makeGuard([&] { gate.open(); });
    ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
    auto result = submit(pool);
    EXPECT_EQ(pool.snapshots()[0].pending, 1);
    gate.open();
    const auto d = result.get();
    EXPECT_EQ(d.chosen, 0);
    EXPECT_EQ(d.chosenInflight, 1);
    EXPECT_EQ(d.chosenPending, 0);
    EXPECT_EQ(d.chosenOutstanding, 1);
    EXPECT_EQ(d.chosenOutstandingCpuUs, 10000);
  }
}

TEST(AffinityExecutor, CrossNumaPenaltyAndHysteresisPreventSmallGains) {
  for (int scenario = 0; scenario != 3; ++scenario) {
    Gate gate;
    auto cfg = config();
    cfg.crossNumaPenaltyUs = scenario == 0 ? 20000 : 0;
    cfg.minGainUs = scenario == 1 ? 20000 : 0;
    cfg.relativeGain = scenario == 2 ? 0.95 : 0;
    Pool pool(cfg);
    auto release = folly::makeGuard([&] { gate.open(); });
    ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
    auto result = submit(pool);
    EXPECT_EQ(pool.snapshots()[0].pending, 1);
    gate.open();
    EXPECT_EQ(result.get().chosen, 0);
  }
}

TEST(AffinityExecutor, LeastLoadIgnoresAffinityPenaltyAndHysteresis) {
  Gate gate;
  auto cfg = config(Pool::Policy::LeastLoad);
  cfg.crossNumaPenaltyUs = 1000000;
  cfg.minGainUs = 1000000;
  Pool pool(cfg);
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
  EXPECT_EQ(submit(pool).get().chosen, 1);
}

TEST(AffinityExecutor, PreferredNumaBorrowsLocallyDespiteLowerRemoteLoad) {
  Gate gate;
  auto cfg = config();
  cfg.groups.resize(3);
  cfg.groups[1].numaNode = 0;
  cfg.groups[2].numaNode = 1;
  cfg.candidateScope = Pool::CandidateScope::PreferredNuma;
  Pool pool(cfg);
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(block(pool, gate, 10000, 0).wait_for(5s), std::future_status::ready);
  ASSERT_EQ(block(pool, gate, 1000, 1).wait_for(5s), std::future_status::ready);
  // Local group 1 has less work than home but more than the idle remote group.
  auto result = submit(pool);
  EXPECT_EQ(pool.snapshots()[1].pending, 1);
  EXPECT_EQ(pool.snapshots()[2].outstanding, 0);
  gate.open();
  ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
  const auto d = result.get();
  EXPECT_EQ(d.preferred, 0);
  EXPECT_EQ(d.chosen, 1);
  EXPECT_TRUE(d.migrated);
  EXPECT_FALSE(d.crossNuma);
}

TEST(AffinityExecutor, PreferredNumaStickyAnchorsActualParentDomain) {
  Gate gate;
  auto cfg = config(Pool::Policy::Sticky);
  cfg.groups.resize(3);
  cfg.groups[2].numaNode = 1;
  cfg.candidateScope = Pool::CandidateScope::PreferredNuma;
  Pool pool(cfg);
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(block(pool, gate, 10000, 1).wait_for(5s), std::future_status::ready);
  Pool::Hint hint;
  hint.home = 0;
  hint.hasParent = true;
  hint.parent = 1;
  auto result = submit(pool, hint);
  ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
  const auto d = result.get();
  EXPECT_EQ(d.source, 1);
  EXPECT_EQ(d.preferred, 1);
  EXPECT_EQ(d.chosen, 2);
  EXPECT_FALSE(d.crossNuma);
}

TEST(AffinityExecutor, PreferredNumaAlsoConstrainsLeastLoad) {
  Gate gate;
  auto cfg = config(Pool::Policy::LeastLoad);
  cfg.candidateScope = Pool::CandidateScope::PreferredNuma;
  Pool pool(cfg);
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
  auto result = submit(pool);
  EXPECT_EQ(pool.snapshots()[0].pending, 1);
  EXPECT_EQ(pool.snapshots()[1].outstanding, 0);
  gate.open();
  ASSERT_EQ(result.wait_for(5s), std::future_status::ready);
  EXPECT_EQ(result.get().chosen, 0);
}

TEST(AffinityExecutor, PreferredNumaBorrowCanReturnFromExternalRemoteParent) {
  auto cfg = config();
  cfg.candidateScope = Pool::CandidateScope::PreferredNuma;
  Pool pool(cfg);
  Pool::Hint hint;
  hint.home = 0;
  hint.hasParent = true;
  hint.parent = 1;
  const auto d = submit(pool, hint).get();
  EXPECT_EQ(d.source, 1);
  EXPECT_EQ(d.preferred, 0);
  EXPECT_EQ(d.chosen, 0);
  // Scope constrains the preferred domain, not the externally supplied source.
  EXPECT_TRUE(d.migrated);
  EXPECT_TRUE(d.crossNuma);
  EXPECT_FALSE(d.rerouted);
}

TEST(AffinityExecutor, PreferredNumaRequiresKnownLabelsButAllKeepsCompatibility) {
  auto cfg = config();
  cfg.groups[1].numaNode = -1;
  EXPECT_NO_THROW(Pool{cfg});
  cfg.candidateScope = Pool::CandidateScope::PreferredNuma;
  EXPECT_THROW(Pool{cfg}, std::invalid_argument);
}

TEST(AffinityExecutor, BorrowChargesReturnToHomeAgainstActualSource) {
  auto cfg = config();
  cfg.crossNumaPenaltyUs = 20000;
  Pool pool(cfg);
  Pool::Hint hint;
  hint.hasParent = true;
  hint.parent = 1;
  const auto d = submit(pool, hint).get();
  EXPECT_EQ(d.preferred, 0);
  EXPECT_EQ(d.source, 1);
  EXPECT_EQ(d.chosen, 1);
  EXPECT_TRUE(d.rerouted);
  EXPECT_FALSE(d.migrated);
  EXPECT_FALSE(d.crossNuma);
  EXPECT_EQ(d.preferredScoreUs, d.preferredLoadUs + 20000);
}

TEST(AffinityExecutor, NonblockingRejectionPreservesAccounting) {
  Gate gate;
  auto cfg = config(Pool::Policy::Home);
  cfg.maxOutstanding = 1;
  Pool pool(cfg);
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(block(pool, gate).wait_for(5s), std::future_status::ready);
  EXPECT_FALSE(pool.add({}, [](const auto&) {
    FAIL() << "rejected callback ran";
  }));
  auto during = pool.snapshots()[0];
  EXPECT_EQ(during.outstanding, 1);
  EXPECT_EQ(during.submitted, 1);
  EXPECT_EQ(during.rejected, 1);
  gate.open();
  pool.join();
  auto final = pool.snapshots()[0];
  EXPECT_EQ(final.pending, 0);
  EXPECT_EQ(final.inflight, 0);
  EXPECT_EQ(final.outstanding, 0);
  EXPECT_EQ(final.estimatedOutstandingCpuUs, 0);
  EXPECT_EQ(final.submitted, final.completed);
  EXPECT_FALSE(pool.add({}, [](const auto&) {}));
  pool.join();
}

TEST(AffinityExecutor, EstimatedWorkOverflowRollsBackReservation) {
  Gate gate;
  auto cfg = config(Pool::Policy::Home);
  Pool pool(cfg);
  auto release = folly::makeGuard([&] { gate.open(); });
  ASSERT_EQ(
      block(pool, gate, std::numeric_limits<uint64_t>::max()).wait_for(5s),
      std::future_status::ready);
  EXPECT_THROW(pool.add({}, [](const auto&) {}), std::overflow_error);
  const auto during = pool.snapshots()[0];
  EXPECT_EQ(during.outstanding, 1);
  EXPECT_EQ(during.submitted, 1);
  EXPECT_EQ(during.rejected, 1);
  gate.open();
  pool.join();
  EXPECT_EQ(pool.snapshots()[0].estimatedOutstandingCpuUs, 0);
}

TEST(AffinityExecutor, ThrowingCallbackReleasesEveryCounter) {
  Pool pool(config());
  ASSERT_TRUE(pool.add({}, [](const auto&) {
    throw std::runtime_error("test failure");
  }));
  pool.join();
  const auto s = pool.snapshots()[0];
  EXPECT_EQ(s.failed, 1);
  EXPECT_EQ(s.completed, 1);
  EXPECT_EQ(s.outstanding, 0);
  EXPECT_EQ(s.pending, 0);
  EXPECT_EQ(s.inflight, 0);
  EXPECT_EQ(s.estimatedOutstandingCpuUs, 0);
}

TEST(AffinityExecutor, JoinDrainsRecursivelySubmittedCrossGroupChildren) {
  Pool pool(config(Pool::Policy::Home));
  std::atomic<unsigned> callbacks{0};
  std::atomic<unsigned> rejected{0};
  std::function<void(unsigned)> recurse;
  recurse = [&](unsigned depth) {
    Pool::Hint hint;
    hint.home = depth % 2;
    if (!pool.add(hint, [&, depth](const auto&) {
          ++callbacks;
          if (depth != 0) {
            recurse(depth - 1);
          }
        })) {
      ++rejected;
    }
  };
  recurse(1000);
  pool.join();
  EXPECT_EQ(callbacks, 1001);
  EXPECT_EQ(rejected, 0);
  for (const auto& group : pool.snapshots()) {
    EXPECT_EQ(group.submitted, group.completed);
    EXPECT_EQ(group.outstanding, 0);
    EXPECT_TRUE(group.workerTids.empty());
  }
}

TEST(AffinityExecutor, ConcurrentProducersHaveExactQuiescentCounters) {
  auto cfg = config();
  cfg.groups[0].threads = cfg.groups[1].threads = 2;
  Pool pool(cfg);
  std::atomic<unsigned> callbacks{0};
  std::vector<std::thread> producers;
  for (unsigned i = 0; i != 4; ++i) {
    producers.emplace_back([&, i] {
      Pool::Hint hint;
      hint.home = i % 2;
      for (unsigned j = 0; j != 1000; ++j) {
        EXPECT_TRUE(pool.add(hint, [&](const auto&) { ++callbacks; }));
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }
  pool.join();
  EXPECT_EQ(callbacks, 4000);
  uint64_t completed = 0;
  for (const auto& group : pool.snapshots()) {
    completed += group.completed;
    EXPECT_EQ(group.submitted, group.completed);
    EXPECT_EQ(group.pending, 0);
    EXPECT_EQ(group.inflight, 0);
    EXPECT_EQ(group.outstanding, 0);
    EXPECT_EQ(group.estimatedOutstandingCpuUs, 0);
  }
  EXPECT_EQ(completed, 4000);
}

TEST(
    AffinityExecutor, ConcurrentJoinClosesAdmissionWithoutLosingAcceptedTasks) {
  // Keep the executor alive until producers have stopped: concurrent
  // destruction would be a caller lifetime bug, whereas concurrent add() and
  // join() are safe.
  for (unsigned round = 0; round != 20; ++round) {
    Pool pool(config());
    std::atomic<unsigned> accepted{0};
    std::atomic<unsigned> completed{0};
    std::atomic<unsigned> rejected{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> producers;
    for (unsigned producer = 0; producer != 4; ++producer) {
      producers.emplace_back([&, producer] {
        while (!go.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        Pool::Hint hint;
        hint.home = producer % 2;
        for (unsigned i = 0; i != 200; ++i) {
          if (pool.add(hint, [&](const auto&) { ++completed; })) {
            ++accepted;
          } else {
            ++rejected;
          }
        }
      });
    }
    go.store(true, std::memory_order_release);
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (accepted.load() < 4 && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    pool.join();
    for (auto& producer : producers) {
      producer.join();
    }
    EXPECT_EQ(accepted.load(), completed.load());
    EXPECT_EQ(accepted.load() + rejected.load(), 800);
    for (const auto& group : pool.snapshots()) {
      EXPECT_EQ(group.submitted, group.completed);
      EXPECT_EQ(group.outstanding, 0);
    }
  }
}

TEST(AffinityExecutor, RequestContextIsCapturedByUnderlyingPool) {
  Pool pool(config());
  folly::RequestContextScopeGuard context;
  auto expected = folly::RequestContext::saveContext();
  ASSERT_NE(expected, nullptr);
  auto result = std::make_shared<std::promise<bool>>();
  auto future = result->get_future();
  ASSERT_TRUE(pool.add({}, [expected, result](const auto&) {
    result->set_value(folly::RequestContext::saveContext() == expected);
  }));
  EXPECT_TRUE(future.get());
}

TEST(AffinityExecutor, InvalidConfigurationAndHintsFailClearly) {
  EXPECT_THROW(Pool(Pool::Config{}), std::invalid_argument);
  auto cfg = config();
  cfg.groups[0].threads = 0;
  EXPECT_THROW(Pool{cfg}, std::invalid_argument);
  cfg = config();
  cfg.groups[0].cpus = {-1};
  EXPECT_THROW(Pool{cfg}, std::invalid_argument);
  cfg = config();
  cfg.groups[0].capacity = std::numeric_limits<double>::quiet_NaN();
  EXPECT_THROW(Pool{cfg}, std::invalid_argument);
  Pool pool(config());
  Pool::Hint hint;
  hint.home = 2;
  EXPECT_THROW(pool.add(hint, [](const auto&) {}), std::invalid_argument);
  hint.home = 0;
  hint.hasParent = true;
  hint.parent = 2;
  EXPECT_THROW(pool.add(hint, [](const auto&) {}), std::invalid_argument);
  hint.hasParent = false;
  hint.estimatedCpuUs = 0;
  EXPECT_THROW(pool.add(hint, [](const auto&) {}), std::invalid_argument);
  EXPECT_THROW(pool.add({}, {}), std::invalid_argument);
}

#ifdef __linux__
TEST(AffinityExecutor, BindingIsAppliedBeforeFirstTaskAndTidsAreAvailable) {
  const auto cpu = ::sched_getcpu();
  ASSERT_GE(cpu, 0);
  auto cfg = config(Pool::Policy::Home);
  cfg.groups.resize(1);
  cfg.groups[0].cpus = {cpu};
  cfg.groups[0].threads = 2;
  Pool pool(cfg);
  EXPECT_EQ(pool.snapshots()[0].workerTids.size(), 2);
  auto checked = std::make_shared<std::promise<bool>>();
  auto result = checked->get_future();
  ASSERT_TRUE(pool.add({}, [cpu, checked](const auto&) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    const auto ok = ::sched_getaffinity(0, sizeof(mask), &mask) == 0 &&
        CPU_COUNT(&mask) == 1 && CPU_ISSET(cpu, &mask) &&
        ::sched_getcpu() == cpu;
    checked->set_value(ok);
  }));
  EXPECT_TRUE(result.get());
  pool.join();
  EXPECT_TRUE(pool.snapshots()[0].workerTids.empty());
}
#endif

} // namespace
} // namespace folly::experimental
