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

#pragma once

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include <folly/Function.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/thread_factory/ThreadFactory.h>
#include <folly/system/ThreadName.h>

namespace folly::experimental {

/**
 * Experimental placement/admission layer over independent, unchanged Folly
 * CPU pools. See README.md for model limitations and lifecycle requirements.
 *
 * This is intentionally not an Executor: add() can reject without blocking,
 * and explicit graph placement hints are required. Each accepted callback is
 * passed its immutable destination before it can enqueue children. Folly's
 * normal RequestContext capture/restore applies to the underlying add().
 */
class AffinityExecutor {
 public:
  enum class Policy { Home, Borrow, Sticky, Fanout, LeastLoad };
  enum class LoadMetric { Work, Queue, Pending };
  enum class QueueKind { Default, Lifo, Throttled };
  enum class CandidateScope { All, PreferredNuma };

  struct GroupConfig {
    std::vector<int> cpus; // Empty retains the creating thread's allowed mask.
    int numaNode{-1}; // Label only: this does not bind memory allocation.
    size_t threads{1};
    double capacity{1.0}; // Measured CPU-equivalent capacity, NOT thread count.
    std::string name;
  };

  struct Config {
    std::vector<GroupConfig> groups;
    Policy policy{Policy::Borrow};
    LoadMetric metric{LoadMetric::Work};
    QueueKind queueKind{QueueKind::Default};
    // Restricts candidates by NUMA label, not memory placement. PreferredNuma
    // is anchored to the policy's preferred group (home or actual parent).
    CandidateScope candidateScope{CandidateScope::All};
    std::chrono::nanoseconds wakeUpInterval{0};
    size_t maxOutstanding{0}; // Per group; 0 means unlimited.
    double crossNumaPenaltyUs{0};
    double minGainUs{0};
    double relativeGain{0}; // Fraction in [0, 1); both gain tests must pass.
  };

  struct Hint {
    size_t home{0};
    size_t parent{0};
    bool hasParent{false};
    bool isFanout{false};
    uint64_t estimatedCpuUs{1000};
  };

  struct Decision {
    size_t home{0};
    size_t source{0};
    size_t preferred{0};
    size_t chosen{0};
    bool rerouted{false}; // Different from policy's preferred group.
    bool migrated{false};
    bool crossNuma{false};
    double preferredLoadUs{0};
    double preferredScoreUs{0};
    double chosenLoadUs{0};
    double chosenScoreUs{0}; // Includes locality penalty, if applicable.
    uint64_t chosenPending{0};
    uint64_t chosenInflight{0};
    uint64_t chosenOutstanding{0};
    uint64_t chosenOutstandingCpuUs{0};
    size_t chosenQueueDepth{
        0}; // Folly semaphore guess, not physical occupancy.
  };

  struct Snapshot {
    size_t group{0};
    uint64_t pending{0}; // Accepted/reserved, callback has not started.
    uint64_t inflight{
        0}; // Callback started; may be running, sleeping, preempted.
    uint64_t outstanding{0};
    uint64_t estimatedOutstandingCpuUs{0}; // Full estimates until completion.
    uint64_t submitted{0};
    uint64_t completed{0}; // Includes callbacks that threw.
    uint64_t rejected{0}; // Selected-group rejection or submit failure.
    uint64_t migratedIn{0};
    uint64_t failed{0}; // Callback exceptions.
    size_t queueDepth{0};
    double capacity{1};
    std::vector<int> workerTids; // Linux TIDs; empty after join.
  };

  explicit AffinityExecutor(Config config) : config_(std::move(config)) {
    validateConfig();
    groups_.reserve(config_.groups.size());
    for (size_t index = 0; index < config_.groups.size(); ++index) {
      const auto& spec = config_.groups[index];
      auto group = std::make_unique<Group>();
      group->threadState = std::make_shared<ThreadState>();
      auto name = spec.name.empty()
          ? "affinity-" + std::to_string(index) + "-"
          : spec.name;
      auto factory = std::make_shared<AffinityThreadFactory>(
          spec.cpus, std::move(name), group->threadState);
      std::unique_ptr<BlockingQueue<CPUThreadPoolExecutor::CPUTask>> queue;
      switch (config_.queueKind) {
        case QueueKind::Lifo:
          queue = CPUThreadPoolExecutor::makeLifoSemQueue();
          break;
        case QueueKind::Throttled:
          queue = CPUThreadPoolExecutor::makeThrottledLifoSemQueue(
              config_.wakeUpInterval);
          break;
        default:
          queue = CPUThreadPoolExecutor::makeDefaultQueue();
          break;
      }
      // Explicit (max,min) bypasses the dynamic-pool gflag; all threads exist
      // before construction returns, even when the pool is initially idle.
      group->pool = std::make_unique<CPUThreadPoolExecutor>(
          std::make_pair(spec.threads, spec.threads),
          std::move(queue),
          std::move(factory));
      groups_.push_back(std::move(group));
    }
  }

  AffinityExecutor(const AffinityExecutor&) = delete;
  AffinityExecutor& operator=(const AffinityExecutor&) = delete;

  ~AffinityExecutor() {
    // Destruction from a callback is invalid, just as self-joining a thread is.
    // The owner must outlive all users of add()/snapshots().
    join();
  }

  /**
   * false: chosen group is full, or external admission is closed.
   * Throws for invalid input, allocation/enqueue failures (counters rolled
   * back). No queue-capacity blocking and no synchronous/inline callback
   * execution. A rejection is final; caller must propagate
   * cancellation/rejection in a DAG.
   */
  bool add(Hint hint, folly::Function<void(const Decision&)> task) {
    validateHint(hint);
    if (!task) {
      throw std::invalid_argument("AffinityExecutor: empty callback");
    }

    // Sequential consistency here is deliberate: join's closed-admission /
    // zero-submitters handshake must not miss a concurrently admitted submit.
    submitters_.fetch_add(1);
    auto leave = folly::makeGuard([this] { leaveSubmitter(); });
    auto state = state_.load();
    if (state == State::Joined ||
        (state == State::Draining && currentExecutor_ != this)) {
      groups_[hint.home]->rejected.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    const auto decision = choose(hint);
    auto& group = *groups_[decision.chosen];
    if (!reserveOutstanding(group)) {
      group.rejected.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    totalOutstanding_.fetch_add(1, std::memory_order_relaxed);

    bool workReserved = false;
    bool countersReserved = false;
    auto rollback = folly::makeGuard([&] {
      if (countersReserved) {
        group.pending.fetch_sub(1, std::memory_order_relaxed);
        group.submitted.fetch_sub(1, std::memory_order_relaxed);
        if (decision.migrated) {
          group.migratedIn.fetch_sub(1, std::memory_order_relaxed);
        }
      }
      if (workReserved) {
        group.work.fetch_sub(hint.estimatedCpuUs, std::memory_order_relaxed);
      }
      group.outstanding.fetch_sub(1, std::memory_order_release);
      totalOutstanding_.fetch_sub(1, std::memory_order_release);
      group.rejected.fetch_add(1, std::memory_order_relaxed);
      notifyDrain();
    });

    reserveWork(group, hint.estimatedCpuUs);
    workReserved = true;
    group.pending.fetch_add(1, std::memory_order_relaxed);
    group.submitted.fetch_add(1, std::memory_order_relaxed);
    if (decision.migrated) {
      group.migratedIn.fetch_add(1, std::memory_order_relaxed);
    }
    countersReserved = true;
    group.pool->add(
        [this,
         groupPtr = &group,
         decision,
         estimate = hint.estimatedCpuUs,
         task = std::move(task)]() mutable {
          auto& runningGroup = *groupPtr;
          runningGroup.inflight.fetch_add(1, std::memory_order_relaxed);
          runningGroup.pending.fetch_sub(1, std::memory_order_relaxed);
          auto previous = currentExecutor_;
          currentExecutor_ = this;
          auto finish = folly::makeGuard([&] {
            currentExecutor_ = previous;
            runningGroup.inflight.fetch_sub(1, std::memory_order_relaxed);
            runningGroup.work.fetch_sub(estimate, std::memory_order_relaxed);
            runningGroup.completed.fetch_add(1, std::memory_order_relaxed);
            runningGroup.outstanding.fetch_sub(1, std::memory_order_release);
            totalOutstanding_.fetch_sub(1, std::memory_order_release);
            notifyDrain();
          });
          try {
            task(decision);
          } catch (...) {
            runningGroup.failed.fetch_add(1, std::memory_order_relaxed);
            throw; // Folly runTask catches/logs it; finish still releases
                   // accounting.
          }
        });
    rollback.dismiss();
    return true;
  }

  /** Relaxed, non-transactional samples; exact conservation only at quiescence.
   */
  std::vector<Snapshot> snapshots() const {
    std::vector<Snapshot> result;
    result.reserve(groups_.size());
    for (size_t i = 0; i < groups_.size(); ++i) {
      const auto& g = *groups_[i];
      Snapshot s;
      s.group = i;
      s.pending = g.pending.load(std::memory_order_relaxed);
      s.inflight = g.inflight.load(std::memory_order_relaxed);
      s.outstanding = g.outstanding.load(std::memory_order_relaxed);
      s.estimatedOutstandingCpuUs = g.work.load(std::memory_order_relaxed);
      s.submitted = g.submitted.load(std::memory_order_relaxed);
      s.completed = g.completed.load(std::memory_order_relaxed);
      s.rejected = g.rejected.load(std::memory_order_relaxed);
      s.migratedIn = g.migratedIn.load(std::memory_order_relaxed);
      s.failed = g.failed.load(std::memory_order_relaxed);
      s.queueDepth = g.pool->getTaskQueueSize();
      s.capacity = config_.groups[i].capacity;
      {
        std::lock_guard lock(g.threadState->mutex);
        s.workerTids = g.threadState->tids;
      }
      result.push_back(std::move(s));
    }
    return result;
  }

  const Config& config() const noexcept { return config_; }

  /**
   * Stop external producers first. join closes external admission but permits
   * callbacks already on this executor to submit children, drains all groups
   * together, then joins their workers. External AIO callbacks are not internal
   * children: drain the entire DAG (including RPCs) before join in that case.
   * Idempotent. Throws on self-join. Never use add()/snapshots() after
   * destruction.
   */
  void join() {
    if (currentExecutor_ == this) {
      throw std::logic_error("AffinityExecutor: cannot join from own callback");
    }
    std::lock_guard joining(joinMutex_);
    if (state_.load() == State::Joined) {
      return;
    }
    state_.store(State::Draining);
    {
      std::unique_lock lock(drainMutex_);
      drained_.wait(lock, [this] {
        if (submitters_.load() != 0) {
          return false;
        }
        // A global count is required: scanning groups can miss a child moved
        // into an already-scanned group as its parent finishes in a later one.
        return totalOutstanding_.load(std::memory_order_acquire) == 0;
      });
    }
    state_.store(State::Joined);
    for (auto& group : groups_) {
      group->pool->join();
    }
  }

 private:
  enum class State { Open, Draining, Joined };
  inline static thread_local AffinityExecutor* currentExecutor_{nullptr};

  struct ThreadState {
    std::mutex mutex;
    std::vector<int> tids;
  };

  class AffinityThreadFactory : public ThreadFactory {
   public:
    AffinityThreadFactory(
        std::vector<int> cpus,
        std::string name,
        std::shared_ptr<ThreadState> state)
        : cpus_(std::move(cpus)),
          name_(std::move(name)),
          state_(std::move(state)) {}

    const std::string& getNamePrefix() const override { return name_; }

    std::thread newThread(Func&& func) override {
      auto ready = std::make_shared<std::promise<std::exception_ptr>>();
      auto started = ready->get_future();
      auto name = name_ + std::to_string(sequence_.fetch_add(1));
      std::thread thread(
          [cpus = cpus_,
           name = std::move(name),
           state = state_,
           ready,
           func = std::move(func)]() mutable {
            int tid = 0;
            try {
              bind(cpus);
              folly::setThreadName(name);
#ifdef __linux__
              tid = static_cast<int>(::syscall(SYS_gettid));
              {
                std::lock_guard lock(state->mutex);
                state->tids.push_back(tid);
              }
#endif
            } catch (...) {
              ready->set_value(std::current_exception());
              return;
            }
            ready->set_value(nullptr);
            auto cleanup = folly::makeGuard([&] {
              std::lock_guard lock(state->mutex);
              auto& ids = state->tids;
              ids.erase(std::remove(ids.begin(), ids.end(), tid), ids.end());
            });
            func();
          });
      if (auto error = started.get()) {
        thread.join();
        std::rethrow_exception(error);
      }
      return thread;
    }

   private:
    static void bind(const std::vector<int>& cpus) {
#ifdef __linux__
      if (cpus.empty()) {
        return;
      }
      const size_t count =
          static_cast<size_t>(*std::max_element(cpus.begin(), cpus.end())) + 1;
      auto* mask = CPU_ALLOC(count);
      if (!mask) {
        throw std::bad_alloc();
      }
      auto cleanup = folly::makeGuard([&] { CPU_FREE(mask); });
      const auto bytes = CPU_ALLOC_SIZE(count);
      CPU_ZERO_S(bytes, mask);
      for (auto cpu : cpus) {
        CPU_SET_S(cpu, bytes, mask);
      }
      const auto error =
          ::pthread_setaffinity_np(::pthread_self(), bytes, mask);
      if (error != 0) {
        throw std::system_error(
            error, std::generic_category(), "set worker CPU affinity");
      }
#else
      if (!cpus.empty()) {
        throw std::invalid_argument(
            "AffinityExecutor CPU binding requires Linux");
      }
#endif
    }

    std::vector<int> cpus_;
    std::string name_;
    std::shared_ptr<ThreadState> state_;
    std::atomic<size_t> sequence_{0};
  };

  struct alignas(64) Group {
    std::atomic<uint64_t> pending{0}, inflight{0}, outstanding{0}, work{0};
    std::atomic<uint64_t> submitted{0}, completed{0}, rejected{0};
    std::atomic<uint64_t> migratedIn{0}, failed{0};
    std::shared_ptr<ThreadState> threadState;
    // Destroy the actual executor before the accounting it may still use.
    std::unique_ptr<CPUThreadPoolExecutor> pool;
  };

  static std::vector<int> allowedCpus() {
#ifdef __linux__
    // sched_getaffinity requires a mask large enough for the kernel's CPU set,
    // not merely for the CPUs present in the caller's cgroup.
    for (size_t count = 1024; count <= (1U << 20); count *= 2) {
      auto* mask = CPU_ALLOC(count);
      if (!mask) {
        throw std::bad_alloc();
      }
      auto cleanup = folly::makeGuard([&] { CPU_FREE(mask); });
      const auto bytes = CPU_ALLOC_SIZE(count);
      CPU_ZERO_S(bytes, mask);
      if (::sched_getaffinity(0, bytes, mask) == 0) {
        std::vector<int> result;
        for (size_t i = 0; i < count; ++i) {
          if (CPU_ISSET_S(i, bytes, mask)) {
            result.push_back(static_cast<int>(i));
          }
        }
        return result;
      }
      if (errno != EINVAL) {
        throw std::system_error(
            errno, std::generic_category(), "read CPU affinity");
      }
    }
    throw std::runtime_error("AffinityExecutor: kernel CPU mask is too large");
#else
    return {};
#endif
  }

  void validateConfig() const {
    if (config_.groups.empty()) {
      throw std::invalid_argument(
          "AffinityExecutor: at least one group is required");
    }
    if (!std::isfinite(config_.crossNumaPenaltyUs) ||
        config_.crossNumaPenaltyUs < 0 || !std::isfinite(config_.minGainUs) ||
        config_.minGainUs < 0 || !std::isfinite(config_.relativeGain) ||
        config_.relativeGain < 0 || config_.relativeGain >= 1 ||
        config_.wakeUpInterval.count() < 0) {
      throw std::invalid_argument(
          "AffinityExecutor: invalid migration/queue parameter");
    }
    const auto allowed = allowedCpus();
    for (const auto& group : config_.groups) {
      if (config_.candidateScope == CandidateScope::PreferredNuma &&
          group.numaNode < 0) {
        throw std::invalid_argument(
            "AffinityExecutor: PreferredNuma requires known NUMA labels");
      }
      if (!group.threads || !std::isfinite(group.capacity) ||
          group.capacity <= 0) {
        throw std::invalid_argument(
            "AffinityExecutor: threads/capacity must be positive");
      }
      auto cpus = group.cpus;
      std::sort(cpus.begin(), cpus.end());
      if (std::adjacent_find(cpus.begin(), cpus.end()) != cpus.end()) {
        throw std::invalid_argument("AffinityExecutor: duplicate CPU in group");
      }
      for (auto cpu : cpus) {
        if (cpu < 0 ||
            !std::binary_search(allowed.begin(), allowed.end(), cpu)) {
          throw std::invalid_argument(
              "AffinityExecutor: CPU outside current allowed mask");
        }
      }
    }
  }

  void validateHint(const Hint& hint) const {
    if (hint.home >= groups_.size() ||
        (hint.hasParent && hint.parent >= groups_.size()) ||
        !hint.estimatedCpuUs) {
      throw std::invalid_argument("AffinityExecutor: invalid placement hint");
    }
  }

  double load(size_t index, uint64_t estimate) const {
    const auto& group = *groups_[index];
    double work;
    if (config_.metric == LoadMetric::Work) {
      work = static_cast<double>(group.work.load(std::memory_order_relaxed)) +
          estimate;
    } else {
      const auto queueSize = config_.metric == LoadMetric::Queue
          ? group.pool->getTaskQueueSize()
          : group.pending.load(std::memory_order_relaxed);
      work = static_cast<double>(queueSize) * estimate;
    }
    return work / config_.groups[index].capacity;
  }

  bool crossNuma(size_t from, size_t to) const {
    const auto a = config_.groups[from].numaNode;
    const auto b = config_.groups[to].numaNode;
    return a >= 0 && b >= 0 && a != b;
  }

  Decision choose(const Hint& hint) const {
    Decision d;
    d.home = hint.home;
    d.source = hint.hasParent ? hint.parent : hint.home;
    const bool followsParent =
        config_.policy == Policy::Sticky || config_.policy == Policy::Fanout;
    d.preferred = followsParent && hint.hasParent ? hint.parent : hint.home;
    d.chosen = d.preferred;
    d.preferredLoadUs = load(d.preferred, hint.estimatedCpuUs);
    const auto penalty = [&](size_t target) {
      return config_.policy != Policy::LeastLoad && crossNuma(d.source, target)
          ? config_.crossNumaPenaltyUs
          : 0.0;
    };
    d.preferredScoreUs = d.preferredLoadUs + penalty(d.preferred);
    d.chosenLoadUs = d.preferredLoadUs;
    d.chosenScoreUs = d.preferredScoreUs;
    const bool balance = config_.policy != Policy::Home &&
        (config_.policy != Policy::Fanout || hint.isFanout);
    if (balance) {
      for (size_t i = 0; i < groups_.size(); ++i) {
        if (i == d.preferred) {
          continue;
        }
        // This is a candidate constraint, including for LeastLoad. It does not
        // prohibit Borrow returning to home from an externally supplied parent
        // in another NUMA domain: source and preferred have distinct meanings.
        if (config_.candidateScope == CandidateScope::PreferredNuma &&
            config_.groups[i].numaNode !=
                config_.groups[d.preferred].numaNode) {
          continue;
        }
        const auto candidateLoad = load(i, hint.estimatedCpuUs);
        const auto score = candidateLoad + penalty(i);
        if (score < d.chosenScoreUs) {
          d.chosen = i;
          d.chosenLoadUs = candidateLoad;
          d.chosenScoreUs = score;
        }
      }
      const auto gain = d.preferredScoreUs - d.chosenScoreUs;
      if (config_.policy != Policy::LeastLoad &&
          (gain < config_.minGainUs ||
           gain < d.preferredScoreUs * config_.relativeGain)) {
        d.chosen = d.preferred;
        d.chosenLoadUs = d.preferredLoadUs;
        d.chosenScoreUs = d.preferredScoreUs;
      }
    }
    d.rerouted = d.chosen != d.preferred;
    d.migrated = d.chosen != d.source;
    d.crossNuma = crossNuma(d.source, d.chosen);
    const auto& g = *groups_[d.chosen];
    d.chosenPending = g.pending.load(std::memory_order_relaxed);
    d.chosenInflight = g.inflight.load(std::memory_order_relaxed);
    d.chosenOutstanding = g.outstanding.load(std::memory_order_relaxed);
    d.chosenOutstandingCpuUs = g.work.load(std::memory_order_relaxed);
    d.chosenQueueDepth = g.pool->getTaskQueueSize();
    return d;
  }

  bool reserveOutstanding(Group& group) const {
    auto value = group.outstanding.load(std::memory_order_relaxed);
    do {
      if (value == std::numeric_limits<uint64_t>::max() ||
          (config_.maxOutstanding && value >= config_.maxOutstanding)) {
        return false;
      }
    } while (!group.outstanding.compare_exchange_weak(
        value, value + 1, std::memory_order_relaxed));
    return true;
  }

  static void reserveWork(Group& group, uint64_t estimate) {
    auto value = group.work.load(std::memory_order_relaxed);
    do {
      if (estimate > std::numeric_limits<uint64_t>::max() - value) {
        throw std::overflow_error("AffinityExecutor: estimated work overflow");
      }
    } while (!group.work.compare_exchange_weak(
        value, value + estimate, std::memory_order_relaxed));
  }

  void notifyDrain() {
    if (state_.load() == State::Draining) {
      // Lock only during shutdown. Pairing with wait's mutex prevents lost
      // wakes.
      std::lock_guard lock(drainMutex_);
      drained_.notify_all();
    }
  }

  void leaveSubmitter() {
    submitters_.fetch_sub(1);
    notifyDrain();
  }

  const Config config_;
  std::vector<std::unique_ptr<Group>> groups_;
  std::atomic<State> state_{State::Open};
  std::atomic<uint64_t> submitters_{0};
  std::atomic<uint64_t> totalOutstanding_{0};
  std::mutex joinMutex_;
  std::mutex drainMutex_;
  std::condition_variable drained_;
};

} // namespace folly::experimental
