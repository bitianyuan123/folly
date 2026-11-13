# AffinityExecutor：分组 CPU 调度实验

`AffinityExecutor.h` 是一个 **header-only 实验包装器**。每个组拥有一个未改动的
`CPUThreadPoolExecutor`，使用显式 `(maxThreads, minThreads) = (N, N)` 固定线程数。
不改变 Folly 默认线程池、队列、信号量或 Linux 调度器的行为，不宣称已经达到生产可用状态。
基础版本为 `v2026.10.26.00`。

## 适用边界

该包装器用于比较“请求亲和 + 跨组借用”的不同策略、记录排队与在途任务，并给业务 DAG
调度层提供一个可复用实现。它不是 `folly::Executor` 子类：显式拒绝提交和 DAG 提示是接口的
一部分，不能隐藏在返回 `void` 的通用 `Executor::add()` 里。

建议先在业务调度层使用此包装器验证收益。只有当多个业务需要相同的分组、统计、生命周期
机制时，才考虑形成稳定的 Folly 公共 API。请求 home、父算子位置、扇出边界、估计 CPU 成本、
RPC 完成、取消和重试仍应由业务层掌握；Folly 无法从一个匿名函数可靠推断这些信息。

## 接口与配置

```cpp
#include <folly/executors/experimental/AffinityExecutor.h>

using Executor = folly::experimental::AffinityExecutor;
Executor::Config config;
config.policy = Executor::Policy::Borrow;
config.metric = Executor::LoadMetric::Work;
config.maxOutstanding = 4096; // 每组，含已经开始的任务；0 为无限制
config.crossNumaPenaltyUs = 200; // 实验校准值，不是库提供的硬件常数
config.minGainUs = 100;
config.relativeGain = 0.10;

// 示例 CPU 编号必须替换成运行环境允许的 CPU；线程数不代表硬件容量。
Executor::GroupConfig group;
group.cpus = {0, 1};
group.numaNode = 0;
group.threads = 4;
group.capacity = 2.0; // CPU 等效吞吐能力，SMT 不应机械按逻辑核数翻倍
config.groups.push_back(group);
Executor pool(std::move(config));

Executor::Hint hint;
hint.home = 0;
hint.isFanout = true;
hint.estimatedCpuUs = 1000;
if (!pool.add(hint, [&pool](const Executor::Decision& d) {
      // d 已在入队前确定。后继任务可直接使用 d.chosen，无需等 add() 返回。
      Executor::Hint child;
      child.home = d.home;
      child.hasParent = true;
      child.parent = d.chosen;
      child.estimatedCpuUs = 500;
      bool accepted = pool.add(child, [](const auto&) { /* 运行后继算子 */ });
      // 实际 DAG 必须处理 accepted == false，完成失败/取消计数，避免永久等 join。
      (void)accepted;
    })) {
  // 请求入口的过载/关闭处理。
}
pool.join();
```

每组 `cpus` 可为空，表示继承创建线程的允许 CPU mask。`numaNode` 是策略标签；不做内存绑定、
页迁移或 NUMA 拓扑发现。可以配置任意组数，也允许 CPU 集合重叠，但重叠组的容量不能独立相加。
非空 CPU 集必须属于构造时调用线程的允许集合，重复或非法编号会抛异常。

自定义 `ThreadFactory` 在线程开始 Folly worker 循环前调用 `pthread_setaffinity_np()`，
通过启动握手将错误传回构造线程。禁止静默忽略亲和性错误。所有固定 worker 创建成功后构造才
返回；`snapshots()` 可以立即取得 Linux TID。CPU 热插拔/cpuset 运行期变更仍须外部监控。

`QueueKind::{Default,Lifo,Throttled}` 分别使用 Folly 默认队列、显式 LifoSem 队列、
显式 ThrottledLifoSem 队列。后者的 `wakeUpInterval` 由用户配置；零值不能解释为人为添加了
固定唤醒间隔。包装器没有实现新的任务传递通道。

## 调度策略与迁移语义

| 策略 | 首选组 | 哪些提交可以比较其他组 |
| --- | --- | --- |
| `Home` | request home | 不比较 |
| `Borrow` | request home | 每次提交 |
| `Sticky` | 父算子的实际组；没有父算子则 home | 每次提交 |
| `Fanout` | 父算子的实际组；没有父算子则 home | 仅 `isFanout=true` |
| `LeastLoad` | home 作为平局选择 | 每次提交，不使用亲和惩罚或迟滞 |

`isFanout` 是调用者的语义标记。如果要测试“仅初始扇出均衡”，就仅给初始扇出的边设置它；
库不能自行识别 DAG 的初始层。

`source = hasParent ? parent : home` 表示当前数据/计算链来源；`preferred` 表示策略首选。
两者不可混淆：Borrow 从远端父算子回到 home 是一次迁移，即使它没有偏离首选组。
`Decision::migrated` 为 `chosen != source`，`rerouted` 为 `chosen != preferred`，
`crossNuma` 比较 source 与 chosen 的有效 NUMA 标签。未指定 NUMA（`-1`）不计跨 NUMA。

候选分数：

```
Work:    load_us = (组内尚未完成任务的完整 estimatedCpuUs 之和 + 新任务估计) / capacity
Queue:   load_us = Folly getTaskQueueSize() * 新任务 estimatedCpuUs / capacity
Pending: load_us = 包装器 pending * 新任务 estimatedCpuUs / capacity
score_us = load_us + (source 到候选组跨 NUMA ? crossNumaPenaltyUs : 0)
```

`Queue` 是用户希望对照的原始队列读数；在这个 Folly 版本里它可能是信号量的近似值，不能
视为物理队列中精确的任务数。`Pending` 则包括“已接纳/保留、尚未开始回调”的任务。两者均
看不到已经开始执行后被内核抢占、阻塞或仍在运行的任务，故仅作为比较基线。

`Work` 使用完整估计而不是剩余 CPU 时间：没有对长任务已经消耗的 CPU 成本做递减。它避免
每个任务读取线程 CPU 时钟，但会高估临近完成的长任务。RPC 等待不能作为 CPU 成本直接累加。
`capacity` 应通过校准估算；SMT、共享缓存、内存带宽、同组的其他线程池和 cgroup 配额都会改变
有效容量。重叠 CPU 组没有建模资源竞争。

选择最低 score 后，还要求相对首选组同时满足绝对增益 `minGainUs` 和相对增益
`relativeGain * preferredScoreUs`。平局保持首选组。惩罚同样应用于首选组，因 Borrow 返回 home
也可能跨 NUMA。`LeastLoad` 忽略所有惩罚和迟滞，作为无亲和对照。

本实现在线性时间内扫描组，在 6 组场景中简单且可解释。多个提交者可能同时选择同一组；
近似统计不能保证严格的最优平衡或消除同步迁移。本版没有随机二选一、迁移冷却计时器、
任务窃取、公平队列、请求配额、优先级或关键路径优先。

## 指标、并发和拒绝

`Snapshot` 包括：

* `pending`：已保留/接纳但回调尚未开始，包括尚未完成底层入队的短窗口。
* `inflight`：回调已经开始但未结束。它不等于正在 CPU 上执行的线程数。
* `outstanding`：已保留但未完成的任务数，是容量上限的依据。
* `estimatedOutstandingCpuUs`：上述完整 CPU 估计之和。
* `submitted/completed/rejected/migratedIn/failed`：接纳、结束、拒绝、迁入和回调异常累计数。
* `queueDepth`：底层 Folly 队列读数；`workerTids`：可用于 `/proc/self/task/TID` 采样的线程 ID。

`Decision` 直接携带所选组入队前的 pending、inflight、outstanding、估计工作量和 queueDepth，
避免每次提交为采样创建 vector 或复制全组 TID。`snapshots()` 适合按秒采样，会分配 vector、
并为读取 TID 获取每组的互斥锁。

指标读取使用 `memory_order_relaxed` 原子操作；它们是非事务性的近似观测，多字段暂时不满足
代数恒等式是允许的。不能把普通变量的并发读写当成“可容忍误差”，那会引入 C++ 数据竞争。
停稳后应满足 `submitted == completed`、pending/inflight/outstanding/work 均为零；completed
包括抛异常的回调。failed 是 completed 的子集。

容量通过每组 `outstanding` 的 CAS 保留，满时立即返回 false，不等待队列空位。即使所选组
因并发保留刚好已满，当前调用也不会重试其他组；这保证行为可解释，但不是最充分的资源利用。
底层队列始终无界，容量约束由包装器覆盖“排队 + 在途”的总量。闭合 admission 后的拒绝记到
hint.home；正常满容量拒绝记到 chosen。调用者必须统计请求拒绝和子算子拒绝，不能只报告成功
请求时延，否则会产生幸存者偏差。

任务函数抛异常时，会释放所有任务/工作量计数，增加 failed，再交给 Folly 原有异常捕获记录。
提交失败（如估计总和溢出或分配失败）会回滚预留计数并重新抛出异常；普通过载返回 false。
空函数、无效 home/parent 或零 CPU 估计属于参数错误。

本实现不声称无开销或完整 lock-free：每任务有多个原子 RMW；关闭协议还有全局 submitters 和
totalOutstanding 计数，会产生缓存行竞争。全局总数用于防止逐组扫描时漏掉从后面组新迁到前面
组的后继任务。仅凭理论说“采样读不用原子所以更快”是不成立的；必须用相同 DAG 对照测试成本。

## 生命周期与异步 RPC

先停止外部生产者。`join()` 关闭外部 admission，但允许当前仍在此包装器回调中运行的任务
提交子任务；它等待所有组一起清空，再逐组调用底层 join，避免一个组先关闭导致其他组的后继
入队失败。`join()` 可重复调用，不允许从自身回调里调用。析构会 join，因此对象必须由外部
拥有并活得比所有使用者更久；不能在自己的最后一个回调中销毁包装器。

TLS 只识别此包装器正在执行的回调。**来自 AIO/RPC 的外部回调不属于内部递归提交**，一旦
join 开始它们会被拒绝。因此真实业务必须先用 DAG/request 的完成计数等待全部外部异步阶段
完成，再调用 join。类似地，join 返回后可以读最终统计，但销毁与 add/snapshots 并发使用是
调用者生命周期错误。本包装器没有实现请求取消或 RPC 超时管理。

## 编译与测试

头文件依赖正常构建的 Folly 库和 C++20。配套 `executor_demo` 工程负责构建 GoogleTest 测试
`test/AffinityExecutorTest.cpp` 并链接同一版本的 Folly；参见该仓库的构建脚本和 CTest 指南。
使用已安装 Folly 时，可把此源码树作为额外 include 路径，优先于安装目录。

也可以从 Folly 仓库根目录单独构建本实验测试，无需 `executor_demo`：

```sh
cmake -S folly/executors/experimental -B _build-affinity -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_PREFIX_PATH=/path/to/folly/install
cmake --build _build-affinity -j2
ctest --test-dir _build-affinity --output-on-failure
```

需要安装 GoogleTest 开发包。已在本次 x86 开发环境通过 Folly 实库链接和全部测试；该结果仅
证明编译与功能契约，不代替 ARM 性能测试。

测试覆盖 Home/Borrow/Sticky/Fanout/LeastLoad、原始 Queue 与 Pending 的盲区、跨 NUMA 惩罚、
绝对/相对迟滞、过载拒绝、工作量溢出的预留回滚、回调异常清理、跨组递归入队、并发生产者、
RequestContext 传播、参数校验、启动前绑核和线程 ID。运行异常测试时出现 Folly 的预期异常日志
不代表测试失败。测试只验证实现契约，不验证 ARM NUMA 上的性能收益。

正式上线前仍须人工复查：真实 CPU/NUMA/SMT 拓扑与容量、缓存/内存和 RPC 成本、关键路径及
大任务混合、公平性、有限容量拒绝策略、长运行下原子更新开销、ARM 实机稳定性，以及异常和
取消路径对整个 DAG 完成计数的影响。
