# Lab 3: Raft 学习笔记

## 第一部分：这个 Lab 到底在干什么

### 1.1 一句话概括

**Raft 共识算法**：让一组服务器对“日志里有哪些命令、顺序是什么”达成一致。即使少数服务器崩溃、网络丢包、消息乱序，只要多数服务器活着，整个系统就能继续工作，并且**所有服务器最终执行完全相同的命令序列**。

### 1.2 为什么需要共识

Lab 2 的单机 KV Server 有个致命弱点：**机器挂了，数据就没了**。自然的想法是复制到多台机器。但多台机器带来新问题：

- 客户端把 `Put(x, 1)` 发给 Leader，Leader 复制给 Follower 时自己挂了
- 新 Leader 上任，它怎么知道 `Put(x, 1)` 到底该不该生效？
- 如果两个 Follower 对“谁是新 Leader”意见不一致，会不会出现两个 Leader 同时接受写？

**共识算法**就是回答这些问题：让一组机器在“部分故障”下，仍然对**同一个日志序列**达成一致。Raft 是 Paxos 之后最著名的共识算法，设计目标就是**可理解性**。

### 1.3 Raft 的三个子问题

论文把 Raft 拆成三个相对独立的问题：

1. **Leader Election（领导者选举）**：集群中必须有一个 Leader 接受客户端请求。Leader 挂了，要能选出新的。
2. **Log Replication（日志复制）**：Leader 把客户端命令追加到自己的日志，然后复制到所有 Follower。多数 Follower 确认后，命令“提交”，可以应用到状态机。
3. **Safety（安全性）**：保证不会出现“两个 Leader 同时提交不同命令”或“已提交的命令丢失”的情况。

Lab 3 的四个子任务对应：

| 子任务 | 内容 | 对应论文 |
|---|---|---|
| **3A** | Leader election | Section 5.1, 5.2, 5.5, 5.6 |
| **3B** | Log replication | Section 5.3, 5.4 |
| **3C** | Persistence（崩溃恢复） | Section 5.3, 5.4 + Figure 2 的持久化要求 |
| **3D** | Log compaction（快照） | Section 7 |

### 1.4 Raft 的核心机制：Term（任期）

Raft 用**单调递增的整数 term** 作为逻辑时钟。每次选举开始，term +1。Term 的作用：

- 区分“过期的 Leader”和“当前的 Leader”：term 小的消息一律拒绝
- 每个服务器记住自己见过的最大 term
- 一次 term 内最多选出一个 Leader（因为每个服务器一个 term 只投一票）

**这是 Raft 所有安全性的基础**：没有 term，就无法区分“旧 Leader 的过期日志复制请求”和“新 Leader 的合法请求”。

### 1.5 三种角色与状态转换

```text
        超时, 没收到心跳
Follower ─────────────→ Candidate
   ↑                      │
   │ 收到新 Leader 的      │ 获得多数票
   │ AppendEntries        ↓
   └────────────────── Leader
```

- **Follower**：被动接收 Leader 的 `AppendEntries` 或 Candidate 的 `RequestVote`
- **Candidate**：超时后自增 term、投票给自己、向所有节点发 `RequestVote`
- **Leader**：赢得选举后，周期性向所有 Follower 发 `AppendEntries`（心跳 + 日志复制）

### 1.6 两个核心 RPC

**RequestVote**（Candidate → 所有节点）：

- 参数：`term`、`candidateId`、`lastLogIndex`、`lastLogTerm`
- 回复：`term`、`voteGranted`
- 投票规则：如果 `candidate.term >= 我的 term` 且 **候选人的日志至少和我一样新** 且我这个 term 还没投过票 → 投给它

**AppendEntries**（Leader → 所有 Follower，也用作心跳）：

- 参数：`term`、`leaderId`、`prevLogIndex`、`prevLogTerm`、`entries[]`、`leaderCommit`
- 回复：`term`、`success`
- 一致性检查：如果 Follower 在 `prevLogIndex` 处的日志 term 不等于 `prevLogTerm`，拒绝。Leader 收到拒绝后递减 `nextIndex` 重试。

### 1.7 安全性：Raft 最微妙的部分

论文 Section 5.4 讲了几条安全规则，**Lab 3B 的测试会专门验证它们**：

1. **选举限制**：候选人日志必须“至少和投票者一样新”，否则不能当选。这保证新 Leader 一定包含所有已提交的日志。
2. **提交规则**：Leader 只能提交**当前 term** 的日志条目（通过多数确认），不能通过数副本数来提交旧 term 的条目。这是 Figure 8 那个经典反例的解法。
3. **日志匹配性质**：如果两个日志在某个 index 的 term 相同，那么它们之前的所有日志都相同。这是 `AppendEntries` 一致性检查的理论基础。

### 1.8 持久化（3C）

服务器崩溃重启后，必须恢复：

- `currentTerm`
- `votedFor`（这个 term 投给了谁）
- `log[]`（日志条目）

**不持久化这三样，Raft 的安全性会被破坏**：比如重启后忘了自己投过票，可能同一 term 投给两个候选人，导致两个 Leader。论文 Figure 2 明确标注了哪些字段必须持久化。

### 1.9 快照（3D）

日志无限增长会撑爆内存/磁盘。**快照**把“截至某个 index 的状态机状态”存下来，然后丢弃该 index 之前的日志。Follower 落后太多时，Leader 用 `InstallSnapshot` RPC 把快照发给它。

---

## 第二部分：具体要改哪些文件，怎么测试

### 2.1 文件清单

所有文件位于 `src/raft1/` 下：

| 文件 | 是否修改 | 职责 |
|---|---|---|
| `raft.go` | 修改 | 核心：`Raft` 结构体、`Make()`、`RequestVote` / `AppendEntries` handler、`ticker()`、`Start()`、`GetState()`、`persist()` / `readPersist()`、`Snapshot()`；RPC Args/Reply 也在这里 |
| `server.go` | 不改 | tester 与 Raft 之间的胶水（`rfsrv`） |
| `proxy.go` | 不改 | 测试用的 RPC 代理 |
| `util.go` | 不改 | `DPrintf` 调试函数 |
| `test.go` / `raft_test.go` | 不改 | 测试与评分标准 |

本仓库没有 `rpc.go` 和 `persister.go`：RPC 结构体在 `raft.go` 里，Persister 是 `tester.Persister`（`src/tester1/persister.go`）。

### 2.2 测试方式

**按子任务递进跑**，不要一上来跑全套：

```bash
cd ~/6.5840/src
make raft1-build          # 先编译 main/raft1d，否则测试找不到守护进程

cd raft1

# 3A: Leader election
go test -run 3A -v

# 3B: Log replication
go test -run 3B -v

# 3C: Persistence
go test -run 3C -v

# 3D: Snapshot
go test -run 3D -v

# 全部 + 竞态检测（最终验收）
go test -v -race
```

也可以直接 `make raft1`（编译 + 跑全部测试），或 `make raft1 RUN="-run 3A"` 只跑 3A。

**测试文件结构**（`raft_test.go` 等）里，每个测试函数名以 `3A` / `3B` / `3C` / `3D` 结尾（例如 `TestInitialElection3A`）；`-run 3A` 会匹配所有 3A 测试。

**3A 的测试会反复模拟**：网络分区、节点崩溃重启、心跳丢失。你会在日志里看到大量 `warning: term changed` 之类的输出，**只要测试 `PASS` 就是正常的**。

### 2.3 开发节奏建议

1. **先只写 3A**：`ticker()` + `RequestVote` handler + 状态转换。跑通 `go test -run 3A`。
2. **再加 3B**：`AppendEntries` handler + `Start()` + `applyCh` 推送。跑通 `go test -run 3B`。
3. **再加 3C**：`persist()` / `readPersist()`，在关键状态变更后调用。跑通 `go test -run 3C`。
4. **最后 3D**：`Snapshot()` / `InstallSnapshot`。跑通 `go test -run 3D`。
5. **全套 `go test -v -race`** 收尾。

**不要跳步**。3B 的测试依赖 3A 的选举正确，3C 依赖 3B 的日志正确。3A 没过就写 3B，debug 时你会分不清是选举问题还是复制问题。

---

## 第三部分：前置知识详解

论文：*In Search of an Understandable Consensus Algorithm (Extended Version)*

下载：`https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf`

**重点读 Section 5 全部 + Section 7（仅 3D 需要）+ Figure 2**。

### 3.1 Section 5.1: Raft Basics（基础）

**服务器状态**：每个服务器在任意时刻处于三种状态之一——Follower、Candidate、Leader。

**Term**：

- 单调递增的整数，逻辑时钟
- 每次选举开始，term +1
- 服务器之间通信时携带 term；收到比自己大的 term，立即更新自己的 term 并转为 Follower；收到比自己小的 term，拒绝
- **一个 term 内最多一个 Leader**（选举安全性）

**RPC 规则**：

- RequestVote 和 AppendEntries 都是幂等的
- 收到 RPC 的回复如果 term 更大，说明自己过期了，转为 Follower

**Lab 3 对应**：`Raft` 结构体里必须有 `currentTerm`、`state`（Follower/Candidate/Leader）、`votedFor`。

### 3.2 Section 5.2: Leader Election（领导者选举）

**触发**：Follower 在 `electionTimeout`（通常 150–300ms 随机）内没收到 Leader 的心跳，转为 Candidate。

**选举过程**：

1. term +1，投票给自己
2. 并行向所有其他服务器发 `RequestVote`
3. 如果获得**多数票**（`> n/2`），成为 Leader
4. 如果收到新 Leader 的 `AppendEntries`，退回 Follower
5. 如果选举超时，term +1，重新选举

**随机化超时**：Raft 用**随机化的 election timeout** 避免“所有 Follower 同时超时、同时竞选、选票分散、谁也赢不了”的活锁。论文 Section 5.6 专门讲这个。

**Lab 3A 对应**：

- `ticker()` 里用一个 goroutine 周期性检查：如果现在是 Follower/Candidate 且距上次收到心跳超过随机超时，发起选举
- `RequestVote` handler 实现投票规则：
  - `args.Term < currentTerm` → 拒绝
  - `args.Term > currentTerm` → 更新 term，转 Follower
  - `votedFor == -1 || votedFor == args.CandidateId` 且**候选人日志至少和我一样新** → 投票
- 日志新旧比较：先比 `lastLogTerm`，大的更新；term 相同比 `lastLogIndex`，大的更新

**3A 测试会验证**：无 Leader 时能选出 Leader；Leader 挂了能重新选；网络分区后少数派不能选出 Leader；term 正确递增。

### 3.3 Section 5.3: Log Replication（日志复制）

**日志结构**：每个条目是 `{Term, Command}`，从 index 1 开始（index 0 是哨兵，term=0）。

**复制流程**：

1. 客户端命令到达 Leader，Leader 追加到本地日志
2. Leader 并行向所有 Follower 发 `AppendEntries`，携带 `prevLogIndex`、`prevLogTerm`、新条目
3. Follower 检查：如果本地在 `prevLogIndex` 处的 term 不等于 `prevLogTerm`，拒绝
4. Follower 接受：删除冲突的后续条目，追加新条目
5. Leader 收到多数确认后，**提交**该条目，应用到状态机，并通过 `applyCh` 通知上层
6. Leader 在下一次 `AppendEntries` 里告诉 Follower `leaderCommit`，Follower 也提交

**一致性检查的作用**：`prevLogIndex + prevLogTerm` 构成“日志匹配性质”的归纳基础。如果 Follower 在 `prevLogIndex` 处的 term 匹配，那么它之前的所有日志都和 Leader 一致。这保证 Leader 只需从后往前找分歧点，不需要全量比对。

**Lab 3B 对应**：

- `Start(command)`：Leader 追加日志，立即返回 index（不等待提交），然后异步触发复制
- `AppendEntries` handler：实现一致性检查、冲突截断、追加、更新 `commitIndex`
- 后台 goroutine：Leader 周期性向每个 Follower 发 `AppendEntries`（心跳 + 复制），维护每个 Follower 的 `nextIndex` 和 `matchIndex`
- `applyCh`：已提交的条目按顺序推给上层（`applyLoop` goroutine）

**3B 测试会验证**：日志能正确复制到多数节点；Leader 挂了新 Leader 能保留已提交日志；日志冲突能被正确截断；提交索引正确推进。

### 3.4 Section 5.4: Safety（安全性）—— 最容易写错的部分

#### 3.4.1 选举限制（Election Restriction）

**规则**：候选人必须包含所有已提交的日志，才有资格当选。

**实现**：`RequestVote` 的投票规则里，比较候选人日志和投票者日志的“新旧”。如果候选人日志更旧，拒绝投票。

**为什么重要**：如果没有这条规则，一个日志落后的节点可能当选 Leader，然后覆盖掉已提交的日志——**已提交的命令丢失**。

**Lab 3B 对应**：`RequestVote` handler 里必须实现日志新旧比较，不能只比 term。

#### 3.4.2 提交规则（Commitment Rule）—— Figure 8 的坑

**规则**：Leader 只能通过“**当前 term** 的日志条目获得多数确认”来提交。不能因为“某个旧 term 的条目在多数节点上有副本”就提交它。

**Figure 8 的反例**：论文用一张图展示了，如果 Leader 提交旧 term 的条目，会出现“已提交的条目被覆盖”的矛盾。

**实现**：Leader 计算 `commitIndex` 时，从后往前找**当前 term** 的条目，且该条目的 `matchIndex` 多数 ≥ 该 index，才提交。找到后，**之前的所有条目（包括旧 term 的）一并提交**。

**Lab 3B 对应**：这是 3B 测试里最容易挂的地方。测试会构造“旧 term 条目在多数节点上但未提交，新 Leader 上任”的场景，验证你不会错误提交。

#### 3.4.3 日志匹配性质（Log Matching Property）

**性质**：如果两个日志在某个 index 的 term 相同，那么它们从开头到该 index 的所有条目都相同。

**保证方式**：

- Leader 同一 term 同一 index 只创建一个条目
- `AppendEntries` 的一致性检查：只有 `prevLogIndex` 处 term 匹配才接受

**Lab 3B 对应**：这是 `AppendEntries` handler 的核心逻辑，写对了这条性质自然成立。

### 3.5 Section 5.5–5.6: 故障处理与时序

**Follower/Candidate 崩溃**：Raft 的 RPC 是幂等的，重启后重新加入即可。

**Timing 要求**：论文给出一个不等式：

```text
broadcastTime ≪ electionTimeout ≪ MTBF
```

- `broadcastTime`：发一轮 RPC 并收到多数回复的时间
- `electionTimeout`：选举超时
- `MTBF`：平均故障间隔

**Lab 3 对应**：`electionTimeout` 通常设 150–300ms 随机，心跳间隔 100ms 左右。**心跳间隔必须显著小于 electionTimeout**，否则 Follower 会误以为 Leader 挂了，频繁发起选举。3A 的测试如果频繁出现“term changed”，先检查心跳频率。

### 3.6 Section 7: Log Compaction（仅 3D 需要）

**问题**：日志无限增长，内存/磁盘撑不住。

**快照**：把“截至 index X 的状态机状态”序列化存下来，然后丢弃 X 之前的日志。

**Raft 要求**：

- 每个服务器独立做快照（通常是上层应用触发）
- Leader 发现某个 Follower 的 `nextIndex` 已经小于自己的快照起始 index，改用 `InstallSnapshot` RPC 发快照
- 快照也要持久化

**Lab 3D 对应**：

- `Snapshot(index, snapshot)`：上层调用，告诉 Raft“index 之前的状态已快照，可以丢弃”
- `InstallSnapshot` RPC handler：自己定义 `InstallSnapshotArgs/Reply`，Leader 把快照发给落后太多的 Follower
- Follower 安装快照后，通过 `applyCh` 发送 `ApplyMsg{SnapshotValid: true, Snapshot: ..., SnapshotTerm: ..., SnapshotIndex: ...}`，让上层更新状态机
- 持久化时，`persister.Save(raftState, snapshot)` 两个参数都要传

**3D 的测试会验证**：日志被正确截断；落后太多的 Follower 能通过快照追上；快照后崩溃重启能恢复。

### 3.7 Figure 2：必须背下来的表

论文 Figure 2 是 Raft 的“规范总结”，列出了：

- **每个服务器必须持久化的状态**：`currentTerm`、`votedFor`、`log[]`
- **易失状态**：`commitIndex`、`lastApplied`、`nextIndex[]`、`matchIndex[]`
- **RequestVote 和 AppendEntries 的完整参数/回复**
- **各角色的规则**（什么时候做什么）

**Lab 3 的实现应该严格对照 Figure 2**。写代码时把 Figure 2 打印出来贴在旁边，每个字段、每条规则都对应到代码里。3C 的持久化测试会直接验证你有没有漏掉 `votedFor` 或 `currentTerm`。

### 3.8 论文中 Lab 3 不需要管的部分

| 论文章节 | 内容 | 为什么不需要 |
|---|---|---|
| Section 6 | 集群成员变更 | Lab 3 不要求动态增删节点 |
| Section 8 | 客户端交互（线性一致读） | Lab 3 只实现 Raft 层，不实现上层 KV |
| Section 9 | 实现与评估 | 与 Lab 无关 |
| Section 10 | 相关工作 | 与 Lab 无关 |

### 3.9 论文浓缩：Lab 3 的核心不变量

写代码时守住这些，测试就能过：

1. **一个 term 内最多一个 Leader**：每个服务器一个 term 只投一票（`votedFor` 持久化）。
2. **选举限制**：候选人日志必须至少和投票者一样新，否则不能当选。
3. **日志匹配性质**：`AppendEntries` 必须做 `prevLogIndex + prevLogTerm` 一致性检查。
4. **提交规则**：只能通过当前 term 的多数确认来提交，不能提交旧 term 的条目。
5. **持久化三件套**：`currentTerm`、`votedFor`、`log[]` 在**任何**影响它们的变更后立即持久化。
6. **心跳频率 ≪ 选举超时**：否则频繁误选举。
7. **随机化选举超时**：避免选票分裂。
8. **`applyCh` 按顺序推送**：已提交的条目必须按 index 顺序应用到状态机。
