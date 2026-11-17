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

#include <limits.h>

#include <array>
#include <atomic>
#include <memory>

#include <folly/Optional.h>

#include <folly/executors/QueueObserver.h>
#include <folly/executors/ThreadPoolExecutor.h>

FOLLY_GFLAGS_DECLARE_bool(dynamic_cputhreadpoolexecutor);

namespace folly {

/**
 * A Thread pool for CPU bound tasks.
 *
 * @note A single queue backed by:
 * - An efficient semaphore, such as:
 *   - folly::LifoSem
 *   - folly::ThrottledlifoSem
 * * An efficient unbounded concurrent queue, such as:
 *   - folly::UMPMCQueue
 * Therefore, this thread pool scales to very high levels of concurrent access.
 *
 * @note If a bounded queue (folly::QueueBehaviorIfFull::BLOCK) is used, and
 * tasks executing on a given thread pool schedule more tasks, deadlock is
 * possible if the queue becomes full. Deadlock is also possible if there is
 * a circular dependency among multiple thread pools with blocking queues.
 * To avoid this situation, either use non-blocking queue(s) only (default and
 * recommended), or schedule tasks only from threads not belonging to the given
 * thread pool(s).
 *
 * @note LifoSem and ThrottledLifoSem wake up threads in LIFO order - i.e. there
 * are only ever as few threads as necessary actually running, and we always try
 * to reuse the same few threads for better cache locality. The other threads
 * are be suspended continuously until they are needed to handle spikes in work,
 * and their stacks would be madvised away while the threads are suspended.
 *
 * @note Supports priorities - priorities are implemented as multiple queues -
 * each worker thread checks the highest priority queue first. Threads
 * themselves don't have priorities set, so a series of long running low
 * priority tasks could still hog all the threads. (at last check pthreads
 * thread priorities didn't work very well).
 */
class CPUThreadPoolExecutor
    : public ThreadPoolExecutor,
      public GetThreadIdCollector {
 private:
  struct OutstandingTaskState {
    std::atomic<size_t> count{0};
  };

  // The reservation follows the task through queue moves. Its shared state may
  // outlive the executor, including when a custom queue retains discarded tasks.
  // There is one allocation per enabled executor, not one allocation per task.
  class OutstandingTaskTicket {
   public:
    OutstandingTaskTicket() = default;
    explicit OutstandingTaskTicket(std::shared_ptr<OutstandingTaskState> state)
        : state_(std::move(state)) {
      if (state_) {
        state_->count.fetch_add(1, std::memory_order_relaxed);
      }
    }
    OutstandingTaskTicket(OutstandingTaskTicket&&) noexcept = default;
    OutstandingTaskTicket& operator=(OutstandingTaskTicket&& other) noexcept {
      if (this != &other) {
        release();
        state_ = std::move(other.state_);
      }
      return *this;
    }
    OutstandingTaskTicket(const OutstandingTaskTicket&) = delete;
    OutstandingTaskTicket& operator=(const OutstandingTaskTicket&) = delete;
    ~OutstandingTaskTicket() { release(); }

    void release() noexcept {
      if (state_) {
        state_->count.fetch_sub(1, std::memory_order_relaxed);
        state_.reset();
      }
    }

   private:
    std::shared_ptr<OutstandingTaskState> state_;
  };

 public:
  struct CPUTask;
  struct Options {
    enum class Blocking {
      prohibit,
      allow,
    };

    constexpr Options() noexcept
        : blocking{Blocking::allow}, taskCounting{false} {}

    Options& setBlocking(Blocking b) {
      blocking = b;
      return *this;
    }

    Options& setTaskCounting(bool enabled = true) {
      taskCounting = enabled;
      return *this;
    }

    Blocking blocking;
    bool taskCounting;
  };

  // These function return unbounded blocking queues with the default semaphore.
  static std::unique_ptr<BlockingQueue<CPUTask>> makeDefaultQueue();
  static std::unique_ptr<BlockingQueue<CPUTask>> makeDefaultPriorityQueue(
      int8_t numPriorities);

  // These function return unbounded blocking queues with LifoSem.
  static std::unique_ptr<BlockingQueue<CPUTask>> makeLifoSemQueue();
  static std::unique_ptr<BlockingQueue<CPUTask>> makeLifoSemPriorityQueue(
      int8_t numPriorities);

  // These function return unbounded blocking queues with ThrottledLifoSem.
  static std::unique_ptr<BlockingQueue<CPUTask>> makeThrottledLifoSemQueue(
      std::chrono::nanoseconds wakeUpInterval = {});
  static std::unique_ptr<BlockingQueue<CPUTask>>
  makeThrottledLifoSemPriorityQueue(
      int8_t numPriorities, std::chrono::nanoseconds wakeUpInterval = {});

  CPUThreadPoolExecutor(
      size_t numThreads,
      std::unique_ptr<BlockingQueue<CPUTask>> taskQueue,
      std::shared_ptr<ThreadFactory> threadFactory =
          std::make_shared<NamedThreadFactory>("CPUThreadPool"),
      Options opt = {});

  CPUThreadPoolExecutor(
      std::pair<size_t, size_t> numThreads,
      std::unique_ptr<BlockingQueue<CPUTask>> taskQueue,
      std::shared_ptr<ThreadFactory> threadFactory =
          std::make_shared<NamedThreadFactory>("CPUThreadPool"),
      Options opt = {});

  explicit CPUThreadPoolExecutor(size_t numThreads, Options opt = {});

  CPUThreadPoolExecutor(
      size_t numThreads,
      std::shared_ptr<ThreadFactory> threadFactory,
      Options opt = {});

  explicit CPUThreadPoolExecutor(
      std::pair<size_t, size_t> numThreads,
      std::shared_ptr<ThreadFactory> threadFactory =
          std::make_shared<NamedThreadFactory>("CPUThreadPool"),
      Options opt = {});

  CPUThreadPoolExecutor(
      size_t numThreads,
      int8_t numPriorities,
      std::shared_ptr<ThreadFactory> threadFactory =
          std::make_shared<NamedThreadFactory>("CPUThreadPool"),
      Options opt = {});

  CPUThreadPoolExecutor(
      size_t numThreads,
      int8_t numPriorities,
      size_t maxQueueSize,
      std::shared_ptr<ThreadFactory> threadFactory =
          std::make_shared<NamedThreadFactory>("CPUThreadPool"),
      Options opt = {});

  ~CPUThreadPoolExecutor() override;

  void add(Func func) override;
  void add(
      Func func,
      std::chrono::milliseconds expiration,
      Func expireCallback = nullptr) override;

  void addWithPriority(Func func, int8_t priority) override;
  virtual void add(
      Func func,
      int8_t priority,
      std::chrono::milliseconds expiration,
      Func expireCallback = nullptr);

  size_t getTaskQueueSize() const;

  /**
   * Number of reserved ordinary tasks not yet finished or discarded. Includes
   * the submission interval, queue wait, and the complete runTask() interval
   * (including expiration callbacks and task observers). Dequeue/start does
   * not decrement it. Internal stop tasks are excluded. stop() may retain
   * queued tasks, so it does not force the count to zero.
   *
   * Returns none unless Options::setTaskCounting() was enabled at construction.
   * This relaxed snapshot is not a synchronization or admission-control API;
   * it counts callbacks, not CPU usage, runnable threads, or logical requests.
   */
  folly::Optional<size_t> getOutstandingTaskCount() const noexcept;

  uint8_t getNumPriorities() const override;

  /// Implements the GetThreadIdCollector interface
  WorkerProvider* FOLLY_NULLABLE getThreadIdCollector() override;

  struct CPUTask : public ThreadPoolExecutor::Task {
    CPUTask(); // Poison.
    CPUTask(
        Func&& f,
        std::shared_ptr<folly::RequestContext> context,
        std::chrono::milliseconds expiration,
        Func&& expireCallback,
        int8_t pri);

    CPUTask(CPUTask&&) noexcept = default;
    CPUTask& operator=(CPUTask&&) noexcept = default;
    CPUTask(const CPUTask&) = delete;
    CPUTask& operator=(const CPUTask&) = delete;

   private:
    friend class CPUThreadPoolExecutor;

    intptr_t queueObserverPayload_{0};
    OutstandingTaskTicket outstandingTicket_;
  };

  static const size_t kDefaultMaxQueueSize;

 protected:
  BlockingQueue<CPUTask>* FOLLY_NONNULL getTaskQueue();
  template <typename EnqueueTask>
  void addImpl(EnqueueTask&& enqueueTask, CPUTask&& task);

  std::unique_ptr<ThreadIdWorkerProvider> threadIdCollector_{
      std::make_unique<ThreadIdWorkerProvider>()};

 private:
  void threadRun(ThreadPtr thread) override;
  void stopThreads(size_t n) override;
  size_t getPendingTaskCountImpl() const override final;

  bool shouldStopThread(bool isPoison);
  void stopThread(const ThreadPtr& thread);

  std::unique_ptr<folly::QueueObserverFactory> createQueueObserverFactory();
  QueueObserver* FOLLY_NULLABLE getQueueObserver(int8_t pri);

  const std::shared_ptr<OutstandingTaskState> outstandingTaskState_;
  std::unique_ptr<BlockingQueue<CPUTask>> taskQueue_;
  // It is possible to have as many detectors as there are priorities,
  std::array<std::atomic<folly::QueueObserver*>, UCHAR_MAX + 1> queueObservers_;
  std::unique_ptr<folly::QueueObserverFactory> queueObserverFactory_{
      createQueueObserverFactory()};
  std::atomic<size_t> threadsToStop_{0};
  Options::Blocking prohibitBlockingOnThreadPools_ = Options::Blocking::allow;
};

template <typename EnqueueTask>
void CPUThreadPoolExecutor::addImpl(EnqueueTask&& enqueueTask, CPUTask&& task) {
  if (!task.func_) {
    // Reserve empty funcs as poison by logging the error inline.
    invokeCatchingExns("ThreadPoolExecutor: func", std::move(task.func_));
    return;
  }

  // Reserve before observers and queue publication. Exceptions before the
  // queue accepts the task release the reservation through task destruction.
  // After publication, the ticket belongs to the task, not the submitter.
  task.outstandingTicket_ = OutstandingTaskTicket(outstandingTaskState_);

  if (auto queueObserver = getQueueObserver(task.priority())) {
    task.queueObserverPayload_ = queueObserver->onEnqueued(task.context_.get());
  }
  registerTaskEnqueue(task);

  // It's not safe to expect that the executor is alive after a task is added to
  // the queue (this task could be holding the last KeepAlive and when finished
  // - it may unblock the executor shutdown).
  // If we need executor to be alive after adding into the queue, we have to
  // acquire a KeepAlive.
  bool mayNeedToAddThreads = minThreads_.load(std::memory_order_relaxed) == 0 ||
      activeThreads_.load(std::memory_order_relaxed) <
          maxThreads_.load(std::memory_order_relaxed);
  folly::Executor::KeepAlive<> ka = mayNeedToAddThreads
      ? getKeepAliveToken(this)
      : folly::Executor::KeepAlive<>{};

  auto result = enqueueTask(std::move(task));

  if (mayNeedToAddThreads && !result.reusedThread) {
    ensureActiveThreads();
  }
}

} // namespace folly
