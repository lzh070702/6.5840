# Lab 1: MapReduce 完整学习笔记

## 第一部分：这个 Lab 到底在干什么

### 1.1 用一句话说

你要从零实现一个 MapReduce 分布式计算框架：一个 Coordinator 进程负责调度，多个 Worker 进程负责执行用户定义的 Map 和 Reduce 函数，最终把一堆输入文本处理成汇总结果。

### 1.2 为什么需要这个东西

假设你有 8 本大书（`pg-*.txt`），要统计每本书里每个单词出现了多少次。单机做很简单：一个循环读完所有文件，建哈希表累加。但如果数据是 8TB 呢？单机内存放不下，速度也慢。

MapReduce 的思路：把大任务拆成小任务，分给多台机器并行做，最后合并。你只写两个函数（Map 和 Reduce），框架负责切分、调度、容错、机器间传数据。

### 1.3 你要实现的系统长什么样

```text
┌─────────────────────────────────────────────┐
│              Coordinator (1个)                │
│  - 知道有 M 个 Map 任务、R 个 Reduce 任务      │
│  - 维护每个任务的状态（未派/进行中/已完成）      │
│  - 通过 RPC 接受 Worker 的"领任务"和"报完成"    │
└──────────────┬──────────────────────────────┘
               │ RPC (Unix socket)
    ┌──────────┼──────────┐
    ▼          ▼          ▼
┌────────┐ ┌────────┐ ┌────────┐
│Worker 1│ │Worker 2│ │Worker 3│  ...
│ 领任务  │ │ 领任务  │ │ 领任务  │
│ 执行    │ │ 执行    │ │ 执行    │
│ 报完成  │ │ 报完成  │ │ 报完成  │
└────────┘ └────────┘ └────────┘
```

### 1.4 数据流：从输入文件到输出文件

以 word count 为例，假设 8 个输入文件、nReduce=10：

**Map 阶段（8 个 Map 任务并行）：**

```text
输入：pg-being_ernest.txt（第 0 个文件）
  ↓ Map 函数（用户写的）
输出：一堆 (单词, "1") 键值对
  ↓ 按 hash(单词) % 10 分成 10 个桶
写磁盘：
  mr-0-0  （第0个Map任务，第0个Reduce分区）
  mr-0-1
  ...
  mr-0-9
```

每个 Map 任务都产出 10 个中间文件。8 个 Map 任务全部完成后，磁盘上有 8×10 = 80 个中间文件。

**Reduce 阶段（10 个 Reduce 任务并行）：**

```text
Reduce 任务 0：
  读 mr-0-0, mr-1-0, mr-2-0, ..., mr-7-0  （所有 Map 的第0分区）
  ↓ 按 key 排序、分组
  ↓ Reduce 函数（用户写的）
写磁盘：mr-out-0

Reduce 任务 1：
  读 mr-0-1, mr-1-1, ..., mr-7-1
  ↓ ...
写磁盘：mr-out-1

... 一直到 mr-out-9
```

最终结果：10 个 `mr-out-*` 文件合起来就是完整的 word count 结果。

### 1.5 关键设计约束（这些是 Lab 要考察的）

| 约束 | 为什么 |
|---|---|
| Map 任务数 = 输入文件数 | 每个文件一个 Map 任务，测试会检查 |
| Reduce 任务数 = nReduce 参数 | 由 MakeCoordinator 的参数决定 |
| Map 输出按 hash(key) % nReduce 分区 | 保证同一个 key 的所有 value 落到同一个 Reduce |
| 所有 Map 完成后才能开始 Reduce | Reduce 要读所有 Map 的中间输出 |
| Worker 随时可能崩溃 | Coordinator 必须超时重派 |
| 同一个任务可能被多个 Worker 重复执行 | 必须保证重复执行不破坏结果 |

### 1.6 容错：Lab 1 的灵魂

分布式系统里 Worker 崩溃是常态。你的 Coordinator 必须处理：

- **Worker 领了任务后挂了，任务永远不完成** → 超时重派：记录任务派发时间，超过 10 秒没汇报就重新标为可派发
- **重派后原来的 Worker 可能又活了，两个 Worker 同时执行同一个任务** → 幂等写：先写临时文件，再原子 rename 到最终文件名，保证重复执行不破坏结果

---

## 第二部分：具体要改哪些文件，怎么测试

### 2.1 你要改的三个文件（都在 `src/mr/` 下）

#### `rpc.go` —— 定义通信协议

这个文件原本只有示例结构体，你需要添加：

- 任务类型枚举（Map / Reduce / 等待 / 退出）
- 任务结构体：包含任务类型、任务 ID、输入文件名、nReduce、nMap 等字段
- “领任务” RPC 的 Args 和 Reply 结构体
- “报完成” RPC 的 Args 和 Reply 结构体

#### `coordinator.go` —— 实现调度逻辑

这个文件原本只有空壳，你需要填充：

- Coordinator 结构体：保存输入文件列表、nReduce、每个任务的状态和开始时间、当前阶段
- GetTask RPC handler：根据阶段和任务状态，决定派什么任务给 Worker
- ReportDone RPC handler：标记任务完成，检查是否该切换阶段
- Done() 方法：返回整个 job 是否完成
- MakeCoordinator() 函数：初始化 Coordinator 并启动 RPC 服务器

#### `worker.go` —— 实现执行逻辑

这个文件原本只有空壳，你需要填充：

- Worker() 主循环：反复领任务 → 执行 → 报完成
- Map 任务执行：读输入文件、调用 mapf、按 hash 分区写中间文件
- Reduce 任务执行：读所有 Map 的对应分区中间文件、排序分组、调用 reducef、写输出文件

### 2.2 不要改的文件

| 文件 | 为什么不能改 |
|---|---|
| `main/mrcoordinator.go` | 入口程序，负责解析参数、调用 MakeCoordinator、轮询 Done() |
| `main/mrworker.go` | 入口程序，负责加载插件、调用 Worker() |
| `mrapps/wc.go` | 用户提供的 word count 函数，你的框架要对它通用 |
| `mrapps/indexer.go` | 用户提供的 indexer 函数，测试会换用它验证通用性 |
| `mr/mr_test.go` | 测试文件，评分标准 |
| `mr/util.go` | 工具函数 |

### 2.4 推荐的开发节奏

1. 先实现 `rpc.go` 的消息结构 → `go build ./...` 确认编译通过
2. 实现 `coordinator.go` 的骨架（结构体 + MakeCoordinator + Done）→ 编译
3. 实现 `worker.go` 的主循环 → 编译
4. 跑 `go test -run TestWc`，看报什么错，逐个修
5. 通过 `TestWc` 后，跑 `go test -run TestIndexer`，验证通用性
6. 逐个通过并行、任务数、提前退出、崩溃恢复测试
7. 最后跑 `go test` 全套

不要三个文件全写完再跑测试。每写完一个文件就编译，每实现一个功能就跑对应的单个测试。

---

## 第三部分：前置知识 —— MapReduce 论文详解

论文：**MapReduce: Simplified Data Processing on Large Clusters**（Jeffrey Dean & Sanjay Ghemawat, Google, 2004）

下载：<https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf>

以下按论文结构讲解，只讲 Lab 1 需要的部分。

### 3.1 Section 2: Programming Model（编程模型）

#### 论文原文的核心定义

**Map**：由用户编写，接收一个输入键值对，产生一组中间键值对。

```text
map(k1, v1) → list(k2, v2)
```

在 word count 里：

- 输入：`("pg-being_ernest.txt", "It was the best of times...")`
- 输出：`[("It", "1"), ("was", "1"), ("the", "1"), ("best", "1"), ...]`

**Reduce**：由用户编写，接收一个中间 key 和它对应的所有 value，合并成一个或多个最终值。

```text
reduce(k2, list(v2)) → list(v2)
```

在 word count 里：

- 输入：`("the", ["1", "1", "1", "1", ...])`
- 输出：`("the", "12345")`

#### 论文强调的三个关键性质

**性质 1：Map 之间完全独立。** 每个 Map 任务只读自己那份输入，不知道其他 Map 在做什么。这意味着 Map 可以任意并行、任意重排、任意重试，结果不变。

**性质 2：Reduce 按 key 分组。** 同一个 key 的所有 value 必须在同一次 Reduce 调用里处理。如果 “the” 的 value 被分到两个不同的 Reduce 任务里，各自统计一半，最终结果就错了。这直接决定了：Map 输出必须按 key 分区，保证同一 key 落到同一 Reduce。

**性质 3：用户不写分布式代码。** 用户只写两个纯函数，框架负责切分、调度、传输、容错。这是 MapReduce 的核心价值——把分布式复杂性封装起来。

#### Lab 1 对应

`mrapps/wc.go` 里的 Map 和 Reduce 就是这两个函数：

```go
func Map(filename string, contents string) []mr.KeyValue
func Reduce(key string, values []string) string
```

你的框架必须对任意符合这个签名的函数通用。测试会用 `wc.go` 和 `indexer.go` 两个不同的用户函数来验证你没有硬编码。

### 3.2 Section 3.1: Execution Overview（执行流程）

这是论文最核心的一节，配合 Figure 1 理解。

论文描述的 7 个步骤：

#### Step 1：切分输入

输入数据被切成 M 个分片（split），每片通常 16–64MB（由 `mapreduce.input.fileinputformat.split.minsize` 控制）。每个分片对应一个 Map 任务。

论文说：切分的目的是让 Map 任务数量远大于机器数量，这样便于负载均衡（快的机器多干活）和故障恢复（挂了只需重跑几个小任务）。

**Lab 1 对应**：每个输入文件就是一个 Map 任务，不再进一步切分。M = 输入文件数。测试 `TestJobCount` 会验证这一点。

#### Step 2：启动进程

一个 Master（Lab 里叫 Coordinator）+ 多个 Worker。Master 负责分配任务，Worker 负责执行。

#### Step 3：Master 派发任务

Master 把 M 个 Map 任务和 R 个 Reduce 任务放进队列，挑空闲的 Worker 派发。

**Lab 1 对应**：Worker 通过 RPC 主动向 Coordinator 请求任务（pull 模式），而不是 Coordinator 主动 push。这是 Lab 的简化——论文里 Master 是主动分配的。

#### Step 4：Map Worker 执行

Worker 读输入分片，解析出键值对，调用用户 Map 函数，得到中间 `(k2, v2)`。

论文的关键设计：中间结果不直接给 Reduce，而是：

- 用分区函数 `hash(k2) mod R` 把中间结果分成 R 个区域，缓存在内存里，周期性刷到本地磁盘。

论文用 `hash(key) mod R` 而不是其他分区方式，是因为它简单、确定、均匀。确定性是容错的前提：同一个 key 无论被哪个 Map 处理，分区结果都一样。

刷盘时，论文说 Map Worker 把这 R 个区域的文件位置汇报给 Master。Master 再转发给 Reduce Worker。

**Lab 1 对应**：Lab 简化了位置汇报——不需要 Master 保存文件位置表，而是用固定命名规则 `mr-<MapTaskID>-<ReducePartitionID>`。Reduce Worker 直接按命名规则拼出文件名去读。这消除了 Master 维护位置表的复杂度，但保留了核心的分区语义。

#### Step 5：Reduce Worker 读取中间结果

Reduce Worker 通过 RPC 向 Master 询问“我的分区在哪些 Map 机器的哪些文件里”，然后远程读取（HTTP 或 RPC）这些中间文件。

读完后，Reduce Worker 按 key 排序，把相同 key 的 value 聚在一起。论文说：如果中间数据太大内存放不下，需要外部排序（external sort）。

**Lab 1 对应**：单机跑，不需要远程读取，直接用 `os.Open` 读本地文件。数据量小，内存排序足够（用 Go 的 `sort.Strings` 即可）。

#### Step 6：Reduce Worker 执行

对排序后的每个唯一 key，调用用户 Reduce 函数，把结果追加到该 Reduce 分区的最终输出文件。

论文说：Reduce 输出直接写到全局文件系统（GFS），所以 Reduce 完成后不需要重跑（输出已持久化）。

#### Step 7：完成

所有 Map 和 Reduce 完成后，Master 唤醒用户程序，MapReduce 调用返回。

#### Lab 1 与论文的差异总结

| 论文 | Lab 1 |
|---|---|
| 输入按 16–64MB 切分 | 每个文件一个 Map 任务 |
| Master 保存中间文件位置表 | 固定文件名代替位置表 |
| Map 结果缓存在内存、周期性刷盘 | 每个 Map 任务直接写 R 个文件 |
| Reduce 远程读取中间文件 | 本地读取 |
| 可能外部排序 | 内存排序 |
| Master 主动 push 任务 | Worker 主动 pull 任务 |

但有四个东西不能简化：

1. Map 输出必须按 `hash(key) mod R` 分区
2. Reduce 必须读所有 Map 的同一分区
3. 中间文件必须能唯一标识 `(MapID, ReduceID)`
4. 所有 Map 完成后才能开始 Reduce

### 3.3 Section 3.2: Master Data Structures（Master 的数据结构）

论文原文：

> The master keeps the state for each map task and reduce task, and stores the state (idle, in-progress, or completed) for each task.

Master 为每个 Map 任务和每个 Reduce 任务保存三种状态之一：

- **idle**：未派发
- **in-progress**：已派给某个 Worker，正在执行
- **completed**：已完成

此外 Master 还保存：

- 每个 Map 任务产出的 R 个中间文件的位置（在哪个 Worker 的本地磁盘）
- 每个已完成 Map 任务的输出大小（用于 Reduce 拉取时预估）

论文还说：Master 保存“每个 in-progress 任务被派给了哪个 Worker”，用于故障检测。

#### Lab 1 对应：你的 Coordinator 需要什么

```text
Coordinator {
    files       []string      // 输入文件列表，决定 M
    nReduce     int           // R
    mapTasks    []TaskState   // 每个 Map 任务的状态
    reduceTasks []TaskState   // 每个 Reduce 任务的状态
    phase       Phase         // MapPhase / ReducePhase / AllDone
    mu          sync.Mutex    // 保护以上所有字段
}

TaskState {
    Status    TaskStatus      // Idle / InProgress / Done
    StartTime time.Time       // 派发时间，用于超时判断
}
```

为什么需要 `StartTime`：论文 3.3 节的故障检测依赖它。Coordinator 需要知道任务派出去多久了，超过阈值（Lab 通常用 10 秒）就认为执行它的 Worker 挂了，重新派发。

为什么需要 `phase`：Map 阶段只派 Map 任务，Reduce 阶段只派 Reduce 任务。阶段切换的时机是 Lab 的核心考点（见下文 3.4）。

为什么需要 `mu`：多个 Worker 会并发调用 `GetTask` 和 `ReportDone`，Coordinator 的字段是共享内存，必须加锁保护。

### 3.4 Section 3.3: Fault Tolerance（容错）

这是 Lab 1 最核心、最容易出 bug 的部分。

#### 3.4.1 Worker Failure（Worker 故障）

论文机制：

> The master pings every worker periodically. If no response is received from a worker within a certain amount of time, the master marks the worker as failed.

- Master 周期性 ping 每个 Worker
- 一段时间没响应，标记该 Worker 为 failed
- 该 Worker 上已完成的 Map 任务要重置为 idle，重新派发
- 该 Worker 上已完成的 Reduce 任务不需要重跑

为什么 Map 要重跑而 Reduce 不用？论文解释：

> Completed map tasks are re-executed on a failure because their output is stored on the local disk(s) of the failed machine and is therefore inaccessible. Completed reduce tasks do not need to be re-executed since their output is stored in a global file system.

Map 的输出在失败机器的本地磁盘，机器挂了就访问不到了，所以必须重跑。Reduce 的输出在全局文件系统（GFS），已经持久化，不需要重跑。

**Lab 1 的简化**：Lab 不要求 Worker 主动 ping，而是采用**惰性超时检测**——Coordinator 不主动检查，而是在 Worker 来领任务时，顺便检查是否有 in-progress 任务超时。超时就把它重置为可派发。

这个简化的效果类似，但实现更简单：不需要后台 goroutine 定期检查，只需要在 `GetTask` 里加一个判断。

**Lab 1 对应**：`TaskTimeout = 10 * time.Second`（官方 handout 建议的值）。`GetTask` 里检查：

```text
for 每个 Idle 或 (InProgress 且超时) 的任务:
    标记为 InProgress，记录 StartTime = now
    返回该任务
```

#### 3.4.2 Master Failure（Master 故障）

论文坦白：

> It is easy to make the master write periodic checkpoints of the master data structures... However, given that there is only a single master, its failure is unlikely; therefore our current implementation aborts the MapReduce computation if the master fails.

Master 挂了，整个 MapReduce 就挂了。论文的解法是 checkpoint，但论文明确说这不是重点。

**Lab 1 对应**：Coordinator 挂了就挂了，测试不会模拟 Coordinator 崩溃。你不需要做 Coordinator 容错。

#### 3.4.3 重复执行的正确性（论文 3.3 隐含 + Lab 必须显式处理）

这是 Lab 1 最容易出错的地方。

论文的前提：Map 和 Reduce 函数必须是确定性的（deterministic）——同样输入，输出相同。

但仅有确定性不够。当超时重派发生后：

- 原来的 Worker 可能没死，只是慢
- 新 Worker 也在执行同一个任务
- 两个 Worker 可能同时往同一个输出文件写

如果直接边写边刷最终文件，两个 Worker 的写入会交错，文件损坏。

论文的保证：最终结果和顺序执行一次相同。因为：

- Map 任务重复执行，产生的是同名中间文件，后完成的覆盖先完成的。只要 Map 函数确定性，内容一样。
- Reduce 任务重复执行，产生的是同名输出文件，同理。

Lab 1 的工程实现：论文没有明说具体做法，但标准做法是：

1. Worker 先把结果写到临时文件（`os.CreateTemp`）
2. 写完、关闭后，用 `os.Rename` 原子地重命名到最终文件名

为什么必须是同目录下的 rename：`os.Rename` 在同一文件系统内是原子操作。如果临时文件在 `/tmp`，目标文件在当前目录，可能跨文件系统，rename 会失败或不是原子替换。所以临时文件必须创建在目标目录下。

原子性的含义：其他进程看到的最终文件，要么是旧的完整版本，要么是新的完整版本，绝不会是写了一半的中间状态。两个 Worker 同时 rename，后完成的胜出，内容一致（因为确定性），所以安全。

**Lab 1 对应**：Map 写 `mr-X-Y` 和 Reduce 写 `mr-out-Y` 时，都必须用这个模式。

#### 3.4.4 阶段切换的正确性

论文：Reduce 必须等所有 Map 完成，因为 Reduce 要读所有 Map 的中间输出。

Lab 1 的实现约束：

- 不是“所有 Map 任务都派发过”就切换
- 而是“所有 Map 任务状态都是 Done”才切换

同理，所有 Reduce 任务 Done 后才进入 AllDone。

常见 bug：Coordinator 在还有 Map 任务 InProgress 时就切换到 ReducePhase，导致 Reduce 读不到某些中间文件，结果错误或 panic。

#### 3.4.5 Worker 提前退出的问题

论文：Master 主动分配任务，Worker 不会“没任务可做”。

Lab 1：Worker 主动 pull 任务。如果 Coordinator 没有可派的任务（所有任务都在 InProgress），但整体没完成，Worker 不能退出——否则最后几个任务如果执行它们的 Worker 挂了，就没人来领重派的任务了。

**Lab 1 对应**：`GetTask` 的回复必须有等待状态。Worker 收到等待后 sleep 一会儿再问，不能退出。只有收到退出状态（所有任务 Done）才退出。

测试 `TestEarlyExit` 专门验证这一点。

### 3.5 Section 3.5: Task Granularity（任务粒度）

论文原文：

> We stipulate that M and R should be much larger than the number of worker machines. Having each worker perform many different tasks improves dynamic load balancing, and also speeds up recovery when a worker fails.

- M 和 R 应该远大于 Worker 数量
- 好处：负载均衡（快的机器多干活）、故障恢复快（挂了只需重跑几个小任务）
- 代价：Master 要为每个任务保存状态，调度开销随任务数增长

论文建议 M 和 R 是机器数量的几倍，但也不要太大。

**Lab 1 对应**：M = 输入文件数（测试给 8 个左右），R = nReduce（测试通常传 10）。你不需要自己调参，但需要理解：一个 Worker 会执行多个任务，Worker 的主循环必须反复领任务，不能只领一次就退出。

### 3.6 论文中 Lab 1 不需要管的部分

明确跳过，避免浪费时间：

| 论文章节 | 内容 | 为什么不需要 |
|---|---|---|
| Section 3.4 | Locality（数据本地性） | 单机跑，不涉及跨机器调度 |
| Section 3.6 | Backup Tasks（慢节点备份） | 只要求超时重派，不要求预测慢节点 |
| Section 4.1 | 可定制分区函数 | 用固定 hash 分区 |
| Section 4.2 | 顺序保证 | 测试只检查内容正确 |
| Section 4.3 | Combiner | 不要求 |
| Section 4.4 | 输入输出类型 | 固定用文件 |
| Section 4.5 | 副作用 | 不涉及 |
| Section 4.6 | 跳过坏记录 | 不要求 |
| Section 5 | 性能 | 与实现无关 |
| Section 6 | 经验 | 与实现无关 |

### 3.7 论文浓缩：Lab 1 的九条不变量

写代码时守住这些，测试就能过：

1. M = 输入文件数，每个文件一个 Map 任务；R = nReduce，每个分区一个 Reduce 任务。
2. Map 输出按 `hash(key) % R` 分成 R 个中间文件，命名为 `mr-<MapID>-<ReduceID>`。
3. 所有 Map 任务 Done 后，才能派发 Reduce 任务。
4. Reduce 任务读所有 Map 产出的、属于自己分区的中间文件。
5. Coordinator 为每个任务维护 Idle / InProgress / Done 状态，InProgress 记录开始时间。
6. 超时的 InProgress 任务重置为可派发，允许重复执行。
7. 输出文件先写临时文件，再原子 rename，保证重复执行不破坏结果。
8. 没有可派任务但整体未完成时，Worker 收到等待，不能退出；全部完成才退出。
9. Coordinator 的共享状态加锁保护，锁内不做文件 IO。
