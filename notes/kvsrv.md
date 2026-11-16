# Lab 2: Key/Value Server 学习笔记

## 第一部分：这个 Lab 到底在干什么

### 1.1 一句话概括

实现一个带 RPC 接口的单机 Key/Value 存储，并配套一个客户端库（Clerk）。核心挑战不在“存数据”，而在于网络不可靠时，客户端的安全重试不能让服务端把同一个操作执行两次。

### 1.2 与 Lab 1 的本质区别：从“调度”转向“语义”

Lab 1（MapReduce）的核心是任务调度和容错重派，Worker 是消耗性的，重复执行靠幂等写盘兜底。

Lab 2 的核心是操作语义的精确性。客户端发出的 `Put` 一旦在服务端生效，就必须保证不会因为客户端重试而被执行第二次。`Put` 不是天然幂等的：它既写值，又推进版本号；如果服务端不检查版本、把重试的 `Put` 再执行一次，版本号就会多加一次，后续基于版本的 CAS 判断全部错位。Lab 1 的“重复执行 + 覆盖”思路在这里不再安全。

### 1.3 系统架构：Clerk ↔ Server

Server 维护一个内存中的 `map[string]kvEntry`，每个 entry 包含 `Value` 和 `Version`。Clerk 是客户端库，封装 RPC 调用和重试逻辑。客户端只通过 Clerk 的 `Put` / `Get` 方法与服务器交互。**本仓库的 `kvsrv1` 没有 `Append`，只有这两个 RPC。**

暴露的 RPC 操作（以 2026 年官方页面 `kvsrv1` 为例）：

| RPC | 语义 |
|---|---|
| `Get(key)` | 返回当前值和版本号；key 不存在返回 `ErrNoKey` |
| `Put(key, value, version)` | 条件写入：version 匹配才更新并把版本号加 1；`version == 0` 表示创建 |

`Put` 携带 version 是这个 Lab 的关键设计。它把“去重”的责任从服务端跟踪“哪个 RPC 执行过”转移到了客户端维护乐观版本号上：客户端重试时携带同一个 version；如果第一次已经执行，服务端第二次收到时版本已经不匹配，返回 `ErrVersion`，从而防止重复修改；如果第一次没到达，第二次则会正常执行。服务端不会返回上次的结果；客户端需要根据 `ErrVersion` 出现的位置，区分“肯定没执行”和“可能执行过”（`ErrMaybe`）。

### 1.4 网络故障下的核心困境

客户端发出 `Put` 后没有收到回复，它无法区分两种情况：

1. 请求没到达服务器。
2. 请求到达并执行了，但回复丢了。

如果是第一种，客户端应该重试；如果是第二种，客户端不能让服务端再执行一次。Lab 2 用版本号（`version`）让服务端判断请求携带的版本是否仍然匹配：匹配就执行，不匹配就拒绝；客户端再根据 `ErrVersion` 出现的位置，决定返回 `ErrVersion` 还是 `ErrMaybe`。

---

## 第二部分：涉及的文件与测试

### 2.1 文件清单（2026 年版本 `kvsrv1`）

所有文件位于 `src/kvsrv1/` 下：

| 文件 | 是否修改 | 职责 |
|---|---|---|
| `client.go` | 修改 | Clerk 的 `Put` / `Get` 实现：构造 RPC 参数、发送、处理重试 |
| `server.go` | 修改 | 实现 `Put` / `Get` 的 RPC handler：维护 map + version，加锁，条件更新 |
| `rpc/rpc.go` | 不修改 | RPC Args/Reply 结构，version 字段和错误值的定义 |
| 测试文件 | 不修改 | 评分标准 |

注意：本仓库是 2026 年的 `kvsrv1`，**没有 `Append`**，只有 `Get` 和 `Put`。更早的 `kvsrv/` 骨架才有 `Put` / `Append` / `Get`，去重靠客户端 ID + 序列号，与这里的版本号机制不同；不要照搬。

### 2.2 测试命令

测试会启动 `src/main/kvsrv1d` 守护进程，所以先编译再跑测试：

```bash
cd ~/6.5840/src
make kvsrv1-build          # 先编译 main/kvsrv1d，否则测试找不到守护进程
cd kvsrv1
go test -v -race
```

也可以直接 `make kvsrv1`（编译 + 跑全部测试），或 `make kvsrv1 RUN="-run TestReliablePut"` 只跑一个；锁测试用 `make lock1`。

`-race` 是必须的：Server 的 map 和 version 会被多个 Clerk 并发访问，不加锁或锁范围不对，这里会直接报出数据竞争。

### 2.3 测试的递进结构

| 阶段 | 测试内容 | 考察点 |
|---|---|---|
| 1 | 无丢包的基本 Put/Get（`TestReliablePut`） | RPC 通路、map 读写、锁、`version == 0` 创建、`ErrNoKey` / `ErrVersion` |
| 2 | 无丢包的并发客户端（`TestPutConcurrentReliable`、`TestMemPutManyClientsReliable`） | 线性一致性、并发下的原子性；服务端内存不随客户端数增长 |
| 3 | 有丢包的 Put（`TestUnreliableNet`） | 客户端重试、服务端去重（版本号 + `ErrMaybe`） |
| 4 | 基于 KV Server 实现分布式锁（`make lock1`） | 综合运用，验证 CAS 语义 |

调试时先跑单步，不要一上来就跑全套。如果 `TestReliablePut` 卡住，说明基础 RPC 通路有问题；如果它通过但 `TestUnreliableNet` 失败，说明去重或 `ErrMaybe` 逻辑没写对。

---

## 第三部分：前置知识

### 3.1 线性一致性（Linearizability）

定义（操作层面）：一组并发操作的执行结果，必须等价于某个顺序执行的结果，且这个顺序尊重操作在真实时间上的先后——如果一个操作在另一个操作开始之前就已经完成，那么在等价顺序中它必须排在前面。

对 KV Server 的直接要求：

- 如果一个 `Put(k, v)` 已经成功返回给客户端，那么之后任何 `Get(k)` 都必须看到 `v`（或更新的值）。
- 并发 `Put` 和 `Get` 可以以任意顺序线性化，但不能出现 `Get` 看到了一个“中间状态”或“从未存在过的状态”。

与 Lab 1 的对比：MapReduce 的中间文件是“最终一致性”的——Reduce 读到的输入可能来自某次 Map 的任意一次成功执行，只要内容正确即可。Lab 2 要求每一次读写都要有明确的、全局一致的先后顺序。

### 3.2 At-Most-Once 语义与重复检测

**At-most-once**：一个操作最多被执行一次。如果客户端重试了，服务端要么不执行（如果之前没收到），要么返回之前执行的结果（如果之前已经执行过），但绝不第二次修改状态。本 Lab 的服务端不缓存上次的结果；它通过 `ErrVersion` 拒绝重复写入，客户端把“重试时收到的 `ErrVersion`”转成 `ErrMaybe`，告诉上层“可能执行了”。

为什么 `Put` 是试金石：`Get` 天然幂等，重复执行无害；`Put` 会写值并推进版本号，如果服务端不检查版本、把重试的 `Put` 再执行一次，版本号就会多加一次，后续基于版本的 CAS 全部错位。测试用 `TestUnreliableNet` 专门验证：如果 `Clerk.Put` 返回 `ErrMaybe`，单客户端场景下这次 Put 必然已经生效；重试时再收到 `ErrVersion`，不能再改一次。

#### 本版本的机制：版本号 + 条件写入

- 每个 key 维护一个 `version`（写入次数）；不存在的 key 视为 `version == 0`。
- `Put(key, value, version)` 只有在客户端提供的 `version` 与服务器当前 `version` 相等时才执行，执行后 `version++`。
- 特殊规则：`version == 0` 表示“创建”——key 不存在则插入，新版本为 1；key 已存在则返回 `ErrVersion`。key 不存在但 `version != 0` 时返回 `ErrNoKey`。
- 客户端第一次发 `Put` 时携带它认为的当前版本；如果超时重试，携带同一个版本。

如果第一次已经执行，服务端第二次收到时版本已经改变，条件不满足，返回 `ErrVersion`；如果第一次没到达，第二次则会正常执行。注意：服务端**不会**返回上次的结果，客户端也无法仅凭 `ErrVersion` 区分“第一次没到达”和“第一次成功但回复丢了”。

#### ErrVersion 与 ErrMaybe

- **第一次 RPC 就返回 `ErrVersion`**：这次 Put 肯定没有执行（版本一开始就不匹配），`Clerk.Put` 直接返回 `ErrVersion`。
- **重试时返回 `ErrVersion`**：第一次 RPC 可能已经成功并推进了版本，也可能被其他客户端抢先改了版本；客户端无法确定，必须返回 `ErrMaybe`。
- **重试时返回 `OK`**：这次重试执行成功；之前那次肯定没有生效，否则版本会对不上。

更早的 `kvsrv` 骨架用“客户端 ID + 序列号”去重，那个版本才有 `Append`；本仓库不使用这套机制，也不需要为每个客户端保存会话状态。

### 3.3 并发正确性：锁的范围

Server 的 map 和每个 key 的 version 是共享可变状态，必须用 `sync.Mutex` 保护。

锁的纪律（比 Lab 1 更敏感）：

- `Put` handler：持锁检查版本（包括 `version == 0` 的创建分支）→ 决定是否更新 → 更新 map 和 version → 释放锁。这三个步骤必须是一个原子临界区，否则两个携带相同版本的 `Put` 可能同时通过检查，都执行更新。
- `Get` handler：持锁读 map 和 version，构造回复后释放。
- 不在持锁期间做任何 RPC 或耗时 IO。Lab 2 的 handler 本身很短，但如果在里面调用 `time.Sleep` 或网络操作，所有并发的 `Get`/`Put` 都会被阻塞。

一个常见错误：只在 map 访问时加锁，但版本检查与 map 更新分开加锁，导致“检查-执行”之间存在竞态窗口。

### 3.4 网络重试与超时（Clerk 侧）

Clerk 的 RPC 调用在丢包时会返回失败（或超时）。Clerk 必须重试，直到收到明确回复。

重试的约束：

- 重试时必须发送完全相同的请求参数（相同的 `version`）。如果每次重试都生成新的版本号，去重机制就失效了。
- 重试应该带有退避（backoff）或至少一个短暂的 sleep，避免在服务器短暂过载时打爆它。但 Lab 2 的测试通常容忍立即重试，只要去重正确即可。

为什么不能“超时就放弃”：如果客户端超时后直接返回错误给上层，上层无法知道操作到底有没有生效。线性一致性要求：一旦客户端收到成功，效果必须持久可见；如果收到失败，效果必须没有发生。在不可靠网络下，只有持续重试直到获得明确答复，才能满足这个二值确定性。

### 3.5 论文与阅读材料

Lab 2（Key/Value Server）没有一篇单独对应的“经典论文”。它的思想来源分散在：

- Lab 1 MapReduce 论文的 3.3 节：重复执行正确性的思想（幂等 + 确定性），但 Lab 2 将其推进到了非幂等操作的去重。
- Lab 2 官方 FAQ 与讲义：关于线性一致性和重复检测（duplicate detection）的说明。2024 版页面直接链接了 `l-raft-QA.txt` 中关于重复检测的讨论；这些笔记虽然服务于后续的 Raft Lab，但其中“客户端会话/序列号”的思路是后续 Raft Lab 的基础，本 Lab 的 `kvsrv1` 用的是按 key 的版本号。
- 后续 Raft Lab 的铺垫：Lab 2 实现的 KV Server 将在后续 Lab 中被复制到多台机器上（Lab 3/4）。Lab 2 把去重逻辑打磨好，是为了在 Raft 之上构建复制状态机时，不会因为客户端重试而导致状态机多次 Apply 同一条命令。

阅读顺序：

1. 先读 Lab 2 官方实验说明（`nil.csail.mit.edu/6.5840/labs/lab-kvsrv1.html` 或 `pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html`），重点是 Introduction 和 Duplicate detection 部分。
2. 读线性一致性 FAQ（实验页面会链接），能用自己的话解释“为什么并发 Put/Get 必须等价于某个顺序执行”。
3. 不需要读 Paxos/Raft 论文——那些是 Lab 3 的内容。Lab 2 的论文前置知识几乎是零，核心是工程上的去重设计。
