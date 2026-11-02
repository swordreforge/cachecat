# Stream 消费组支持与阻塞唤醒设计

本文档总结本轮为 cache-cat 补齐 Redis Stream 消费组命令、并重做阻塞唤醒链路所做的全部工作。
实现已落在 `master` 分支（详见文末提交列表），全部行为都以真实节点和单元/集成测试验证过。

## 1. 背景与范围

改造前，Stream 只暴露了 `XADD`、`XGROUP`、`XREAD`，消费组读取（`XREADGROUP`）、
待处理列表（`XPENDING`、`XACK`）和认领（`XCLAIM`）都缺少协议层入口。
另一方面，阻塞读取的唤醒只由 `XADD` 触发，且所有等待者共用一份不区分类型的上下文，
导致组读在唤醒后仍要提交无用的 Raft 写、group 被销毁时客户端会被挂起。

本轮工作分两部分：

1. 补齐 `XLEN`、`XRANGE`、`XACK`、`XREADGROUP`、`XPENDING`、`XCLAIM` 六个命令；
2. 把阻塞唤醒从"只有追加才有事件"重构成"任意 stream 写入产生通用变更事件 + 等待者自检"。

之后又完成了四批增量：第一批补齐只读的 `XREVRANGE` 与 `XINFO`（见 2.2），
第二批补齐删除/裁剪的 `XDEL` 与 `XTRIM`（见 2.3），第三批补齐自动认领的
`XAUTOCLAIM`（见 2.4），第四批补齐 `XSETID`、`XACKDEL` 与 `XDELEX`（见 2.5）。

## 2. 新增命令

| 命令 | trait | 复制载荷 | 说明 |
| --- | --- | --- | --- |
| `XLEN` | `ReadCommand` | `ReadOperation::XLen` | 单键读，返回条目数 |
| `XRANGE` | `ReadCommand` | `ReadOperation::XRange` | 单键读，区间 + `COUNT` |
| `XPENDING` | `ReadCommand` | `ReadOperation::XPending` | summary / extended 两种形式 |
| `XACK` | `ComputeCommand` | `BaseOperation::XAck` | 单键读改写，清 PEL |
| `XCLAIM` | `ComputeCommand` | `BaseOperation::XClaim` | 单键读改写，转移归属 |
| `XREADGROUP` | 多键读写 | `RedisOperation::RedisXReadGroup` + 每键 `BaseOperation::XReadGroup` | 原子多键，快照按单键记录 |
| `XREVRANGE` | `ReadCommand` | `ReadOperation::XRevRange` | 单键读，反向区间 + `COUNT` |
| `XINFO` | `ReadCommand` | `ReadOperation::XInfo` | `STREAM` / `GROUPS` / `CONSUMERS` / `HELP` |
| `XDEL` | `ComputeCommand` | `BaseOperation::XDel` | 单键读改写，删除条目 |
| `XTRIM` | `ComputeCommand` | `BaseOperation::XTrim` | 单键读改写，裁剪 + 引用策略 |
| `XAUTOCLAIM` | `ComputeCommand` | `BaseOperation::XAutoClaim` | 单键读改写，扫描 PEL 自动认领 |
| `XSETID` | `ComputeCommand` | `BaseOperation::XSetId` | 单键读改写，更新内部 ID 元数据 |
| `XACKDEL` | `ComputeCommand` | `BaseOperation::XAckDel` | 单键读改写，确认并删除 |
| `XDELEX` | `ComputeCommand` | `BaseOperation::XDelex` | 单键读改写，按引用策略删除 |

十四个命令同时注册到服务端工厂 `CommandFactory` 和 Lua 工厂 `RaftCommandFactory`，
因此 `MULTI/EXEC` 与 `EVAL` 中同样可用。`XREVRANGE` / `XINFO` 属于第一批只读扩展，
`XDEL` / `XTRIM` 属于第二批删除/裁剪扩展，`XAUTOCLAIM` 属于第三批自动认领扩展，
`XSETID` / `XACKDEL` / `XDELEX` 属于第四批元数据与 Redis 8.2 删除扩展。

### 2.1 与 Redis 语义对齐的细节

实现前对照了 `redis/8.0` 的 `src/t_stream.c`，以下几个容易写错的点已逐条对齐：

- **XRANGE 区间**：支持 `-`、`+`、`(id` 独占、裸毫秒（下界补 `-0`、上界补 `-MAX`）；
  独占边界向内平移，溢出时报 `ERR invalid start/end ID for the interval`；
  作为独占边界的 `(-` / `(+` 属于非法 ID；`COUNT 0`（含负数）在 **key 存在时**返回
  null array，而 key 不存在时返回空数组；重复 `COUNT` 以最后一个为准。
- **XACK**：先查 key → 类型 → group，**最后**才解析 ID。因此缺 key 或缺 group 时即使 ID
  非法也返回 `0`；group 存在但 ID 非法才报 `ERR Invalid stream ID ...`。没有实际确认时
  返回 `MochaOperation::Abort`，不产生快照增量。
- **XREADGROUP**：`>` 表示新条目，显式 ID 表示消费者历史；`$`、`+` 报 Redis 原文错误；
  `COUNT` / `BLOCK` / `NOACK` 均支持；`NOGROUP` 文案带 `in XREADGROUP with GROUP option`。
- **XPENDING**：summary 返回 `[total, min, max, [[consumer, count], ...]]`；
  extended 返回 `[id, consumer, idle, deliveries]`，支持 `IDLE`、区间、`count`、`consumer`；
  `IDLE` 为负按 0 处理，`count` 为负按 0 处理。
- **XCLAIM**：`min-idle-time`、`IDLE`、`TIME`、`RETRYCOUNT`、`FORCE`、`JUSTID`、`LASTID`
  全部支持，ID 与选项可交错出现；`JUSTID` 只返回 ID。

### 2.2 只读扩展（第一批）

- **XREVRANGE**：线上参数顺序是 `end start`，解析时先反转为与 `XRANGE` 相同的
  闭区间 `[start, end]` 再复制；`COUNT` 规则与 `XRANGE` 完全一致（重复取最后一个、
  负数按 0、key 存在时 `COUNT 0` 返回 null array、缺 key 返回空数组）。
  `COUNT` 解析已抽成 `parse_count_option` 供两个命令共用。
- **XINFO**：`STREAM [FULL [COUNT n]]`、`GROUPS`、`CONSUMERS`、`HELP` 全部支持。
  缺 key 统一返回 `ERR no such key`（与 Redis 一致，而不是 NOGROUP）；
  `CONSUMERS` 缺 group 返回
  `NOGROUP No such consumer group '<group>' for key name '<key>'`。
  RESP2 使用 Redis 的扁平 field/value 数组，RESP3 使用 map。
- **确定性时间**：`XINFO CONSUMERS` 的 `idle`/`inactive` 和 `STREAM FULL` 里的
  `seen-time`/`active-time` 都使用本次读取的逻辑时钟（新增
  `xinfo_consumers_at`、`xinfo_stream_full_at`），因此各副本结果一致。
  `FULL` 默认 `COUNT 10`、`COUNT 0` 为不限、负数额按默认 10，与 Redis 相同。
- **字段取舍**：cache-cat 用 B 树而不是 radix tree 存储条目，所以不输出 Redis 内部的
  `radix-tree-keys` / `radix-tree-nodes`，其余字段（长度、首尾条目、组、PEL、
  消费者时间等）均按 Redis 语义输出。

### 2.3 删除与裁剪（第二批）

- **XDEL**：Redis 先查 key、再查类型，**最后**才解析 ID，因此缺 key 时即使 ID 非法也
  返回 `0`。复制的请求保留原始 ID 字节，在 `mutate` 里类型检查通过后再解析，保证错误
  顺序一致。删除只移除 payload：PEL、group 游标和 `last-generated-id` 保留，
  只有真正删掉的 ID 才推进 `max-deleted-entry-id`；重复 ID 只计一次。
- **XTRIM**：支持 `MAXLEN` / `MINID`、可选 `~` / `=`、`LIMIT n`，以及 Redis 8.2 的
  `KEEPREF` / `DELREF` / `ACKED` 引用策略。选项在查 key **之前**解析，所以缺 key 时
  非法参数仍然报错；缺 key 本身返回 `0` 且不创建。
  - `LIMIT` 只能与 `~` 同用，`0` 表示不限；`MAXLEN` / `LIMIT` 为负分别报
    Redis 原文错误；`MAXLEN` 与 `MINID` 不能同时出现。未给 `LIMIT` 的 `~` 使用核心的
    10,000 条检查上限。
  - `KEEPREF`（默认）保留 PEL；`DELREF` 同时清理被裁剪条目的 PEL 引用；
    `ACKED` 只裁剪已不被任何 group 引用的条目。
  - 与 XDEL 不同，XTRIM 不推进 `max-deleted-entry-id`。
- **快照与唤醒**：`TrimOptions` 已可序列化，快照增量与 Raft 日志能原样重放
  `~`/`LIMIT`/引用策略；两者都不追加条目，因此 `changed_streams` 的 append 标志为
  false，阻塞中的 XREAD / XREADGROUP 不会被删除或裁剪唤醒，直到下一次 XADD。

### 2.4 自动认领（第三批）

- **XAUTOCLAIM**：支持 `min-idle-time`、`start`、`COUNT`（默认 100，必须为正）和
  `JUSTID`。`start` 接受 `-`、`+`、裸毫秒和 `(id` 独占形式；独占 MAX 溢出时报
  `ERR invalid start ID for the interval`。
- **错误优先级**：Redis 先解析 idle/start/选项，再查 key 和 group；因此缺 key 时非法
  参数仍然先报参数错误，只有参数合法才返回
  `NOGROUP No such key '<key>' or consumer group '<group>'`。
- **扫描语义**：从 `start` 起扫描 PEL，最多检查 `10 * COUNT` 条，已认领与已清理的
  删除条目都计入 `COUNT`；游标 `next-start` 指向下一个未扫描的 PEL ID，扫描到末尾为
  `0-0`。payload 已删除的 PEL 记录会被清理并放入回复第三项。
- **确定性时间**：新增 `xautoclaim_at(..., now)`，认领后的 `delivery_ms`、消费者
  `seen-time`/`active-time` 全部来自复制的写时钟；`JUSTID` 不增加投递次数。
- 回复为 `[next-start, claimed, deleted-ids]`；`claimed` 非 `JUSTID` 时是
  `[id, fields]`，`JUSTID` 时是 ID 数组。成功调用总会创建/触碰 consumer，因此会记录
  一次 stream 快照增量。

### 2.5 元数据与 Redis 8.2 删除（第四批）

- **XSETID**：`XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]`。Last ID 与选项
  在查 key 之前解析；`ENTRIESADDED` 为负报 `ERR entries_added must be positive`，
  `MAXDELETEDID` 大于目标 ID 报 Redis 原文错误。执行时依次检查当前 tombstone、
  stream 顶部条目和 `entries_added >= length`，错误文案与 Redis 一致；缺 key 返回
  `ERR no such key`。`MAXDELETEDID 0-0` 表示保留现有 tombstone。
- **XACKDEL**：`XACKDEL key group [KEEPREF|DELREF|ACKED] IDS numids id ...`。
  先查类型、再解析选项、再查 group，最后才解析 ID，避免部分确认后才发现非法 ID。
  缺 key 或缺 group 时按 `numids` 返回全 `-1`，不解析 ID。`ACKED` 只删除已不被任何
  group 引用的条目，`DELREF` 会同时清理所有 group 的 PEL 引用，`KEEPREF` 保留 PEL。
- **XDELEX**：`XDELEX key [KEEPREF|DELREF|ACKED] IDS numids id ...`。不要求 ID 在 PEL
  中；`ACKED` 对仍被引用的条目返回 `2` 且不删除，`DELREF` 先清引用再删 payload，
  `KEEPREF` 直接删 payload 并保留 PEL。缺 key 时返回全 `-1`。
- 两者共用同一套 `IDS numids` / 引用策略解析与状态编码；状态值固定为
  `-1`（不存在）、`1`（已删除）、`2`（仍被引用）。

## 3. 复制与快照

### 3.1 读写路径

- **读**：`ReadOperation` → `read_request` → lease read + 读锁 → `ReadCommand`。
- **单键写**：`BaseOperation` → `base_request` → `execute_compute`（快照 CAS 复用）。
- **多键原子写**：`XREADGROUP` 整体作为 `RedisOperation::RedisXReadGroup` 进一条日志；
  执行时逐 key 调用 `execute_compute(XReadGroupStreamReq)`，因此快照增量仍按**单键**
  记录，恢复时是独立的 CAS 操作。

### 3.2 逻辑时钟（确定性）

消费组状态（PEL 的 `delivery_ms`、consumer 活跃时间、XPENDING 的 idle）都属于会被复制和
快照的状态，不能使用各节点自己的物理时钟。核心层因此新增带时钟参数的方法：

- `RedisStream::xreadgroup_at(...)`、`xclaim_at(...)`、`xpending_range_at(...)`、
  `xinfo_consumers_at(...)`、`xinfo_stream_full_at(...)`、`xautoclaim_at(...)`
- 协议层传入 `update.write_clock`；EXEC/Lua 内的读走 `execute_with_clock`，拿到同一个写时钟

快照回放测试验证了：同一批操作在主节点与恢复节点上得到逐字节一致的 PEL。

### 3.3 消费组 generation（stream 快照版本 4）

仅靠内存里的 `Arc<()>` identity 无法跨节点比较，也无法防住"阻塞期间销毁并重建同名 group"
后客户端静默接入新 group。为此新增**复制化的 group 世代号**：

- `Group::generation` 由流内单调计数器 `next_group_generation` 在每次 `XGROUP CREATE` 时分配；
- 随 stream 快照持久化（`SNAPSHOT_VERSION` 3 → 4，含恢复时范围校验与计数器续接）；
- 阻塞的 `XREADGROUP` 在第一次提交前捕获每个 key 的 generation 写入 `expected_generations`；
  apply 时逐 key 校验，不一致直接 `NOGROUP`。

这条校验是**权威判据**：即使"销毁 + 重建"恰好发生在客户端本地检查与提交之间，也不会串读新 group。

## 4. 阻塞唤醒

### 4.1 等待器类型化

原先 `WriteState::blocking_keys` 的上下文类型固定为 `XReadParams`，组读只能塞一个"令牌"，
每次唤醒仍要跑一遍 XREAD 计算。现在统一为：

```rust
enum StreamWaiter {
    Read(XReadParams),                                   // XREAD：载荷就是回复
    Group { group: Bytes, identities: Vec<((u16, Bytes), Arc<()>)> },
}
```

一个注册表服务两种等待者；唤醒时按类型分派，组等待者只做组状态判断。

### 4.2 通用变更事件 + 追加标志

`ApplyEffects::changed_streams` 从 `HashSet` 变为 `HashMap<(db, key), bool>`：

- 事件在 **`execute_compute` 唯一一处**发出：任意成功的 stream 写入（XADD、XGROUP、XACK、
  XCLAIM、XREADGROUP）都会登记该 key，新命令无需自己补唤醒代码；
- `bool` 表示本批次是否**追加了条目**：XADD 是唯一增长 `entries_added` 的命令，
  所以通过 mutation 前后比较 `entries_added` 得到，默认正确、不会因新命令漏标记；
- 分派时 XREAD 等待者只在追加事件上被评估（XACK/XCLAIM/组读不再触发无意义的 XREAD 扫描），
  组等待者仍评估所有事件（链式投递依赖游标推进这类非追加事件）。

### 4.3 每 (key, group) 只唤醒一个 + 链式投递

- 新条目事件只唤醒该 group 中**注册最早**的等待者；
- 被唤醒者的组读本身是一条复制写，游标推进后又会发出通用事件；若仍有新条目，
  下一个等待者被唤醒——写入次数与"非空读取次数"一致，投递保持恰好一次；
- group 已销毁或已重建：**所有**该 group 的等待者都被唤醒，并且 `NOGROUP`
  直接作为唤醒载荷返回，客户端不再提交注定失败的写；
- 不同 group 互不影响（每个 group 各自唤醒一个）。

### 4.4 时序与代价

- 注册与"是否就绪"的检查都在全局写锁内完成；apply 也需要同一把锁，因此不会丢唤醒。
- 同一 key 的组状态按 `(key, group)` 缓存一次（identity、`has_new`、"是否已唤醒"），
  同一 group 的多个等待者只做一次 `Arc::ptr_eq`。
- `wake_ready` 改为把该 key 的等待者队列整体取出、按注册顺序逐个 `pop_first`，
  只把仍阻塞的 ID 用原节点放回；不再每次事件克隆整棵 `BTreeSet`。
- 唤醒路径使用 `execute_multi_read_ref`，不再为每个阻塞的 XREAD 读者克隆
  `XReadParams`（keys/ids 向量）。

### 4.5 DEL 删除 stream

- Redis 只为 XREADGROUP 的等待者设置 `unblock_on_nokey`，因此删除 stream key 时：
  - 阻塞中的 `XREADGROUP` 立即收到 `NOGROUP`；
  - 普通 `XREAD` 保持阻塞，直到下一次 `XADD`。
- 实现上，`DEL`（单键 `BaseOperation::Del` 与多键 `RedisDel` 都会逐 key 调用 `del`）
  在真正移除 stream 后，若该 key 上有等待者，就往 `changed_streams` 写入
  `(key, false)` 的非追加事件。分派器原有的“非追加事件只评估 group 等待者”规则直接
  复用，无需新增唤醒分支。
- 没有等待者时不记录事件；快照回放 `effects` 为 `None`，不受影响。
- 局限：`UNLINK` 以及 `SET` 覆盖 stream 的路径尚未统一接入；Redis 8 的 `UNLINK`
  同样会唤醒阻塞组读。

## 5. 本轮顺带修复的两个既有问题

### 5.1 快照文件版本号没有跟上格式

`docs/snapshot_zh.md` 写明动态元数据格式是版本 2，但 `snapshot_handler.rs` 的
`VERSION` 常量一直是 1。结果是：新写的快照与旧的"内联 296 字节元数据"格式共用同一个
版本字节，版本门形同虚设；测试 `snapshot_readers_reject_invalid_metadata_consistently`
把版本 1 当作"旧版本"，因此一直失败。

修复：`VERSION` 1 → 2，并把头部断言改为 `MCDC\x02`。现在旧格式文件会被明确拒绝。

### 5.2 `#[serde(skip)]` 变体之后的枚举下标错位

`RedisOperation` 的 `RedisEvalSha` 变体带 `#[serde(skip)]`。serde 派生在编码时按包含该变体的
序号计数、解码时只认未跳过的变体，因此**任何声明在其后的变体都会写出越界下标**，表现为
Raft 日志 apply 时报 `bincode2 decode: invalid value: integer ...`。

`RedisXReadGroup` 因此声明在 `RedisEvalSha` 之前，并新增 bincode 往返回归测试。
后续若还要新增变体，应继续遵守这一约束（更彻底的做法是去掉该变体上的 `skip`）。

## 6. 测试与验证

- 单元 + 集成测试：`cargo test -p cache_cat --lib -- --skip test::tests::test_add`
  → **620 passed / 0 failed**（`test_add` 是需要本地 5001 节点的手工测试）。
- 覆盖点：十四个命令的参数/边界/错误文案、快照回放后的 PEL 一致性、generation 快照往返与
  越界拒绝、过期 generation 被拒、单 group 单唤醒、多 group 各自唤醒、销毁后全部收到
  `NOGROUP`、非追加事件不扫描 XREAD、`XREVRANGE` 的 end/start 反转、`XINFO` 的
  RESP2/RESP3 形状与逻辑时钟时间、`XDEL`/`XTRIM` 的快照增量重放与引用策略、
  `XAUTOCLAIM` 的游标/COUNT/JUSTID/删除清理与确定性写时钟、`XSETID` 的元数据校验、
  `XACKDEL`/`XDELEX` 的状态数组与引用策略、DEL 对 group/普通 XREAD 的差异化唤醒。
- 真实单节点（debug 构建）验证：
  - `BLOCK 0` + `XGROUP DESTROY` → 立即收到 `NOGROUP`（不再永久挂起）；
  - 3 个阻塞消费者 + 一次 XADD → 只有 1 个被服务；下一次 XADD → 下一个；
  - 销毁其它 group 不影响当前 group 的等待者；
  - 阻塞期间销毁并重建 + XADD → 返回 `NOGROUP`，新 group 的 PEL 保持为空；
  - `EVAL` 内连续两次 XADD（单条日志）→ 两个消费者通过链式唤醒各拿一条；
  - `XACK` 不唤醒阻塞的 XREAD，`XADD` 才唤醒；
  - `XREVRANGE` 反向返回、`COUNT` 与缺 key 行为正确；`XINFO` 的 RESP2 扁平数组与
    RESP3 map、`HELP`、`NOGROUP` 文案、`MULTI/EXEC` 入队与 `EVAL` 调用均正确；
  - `XDEL` / `XTRIM` 的删除、裁剪、`LIMIT`、`KEEPREF`/`DELREF`/`ACKED`、`MULTI/EXEC`
    与 `EVAL` 均正确；阻塞中的 XREAD / XREADGROUP 不会被删除或裁剪唤醒，下一次 XADD
    才唤醒；
  - `XAUTOCLAIM` 的游标翻页、`COUNT`、`JUSTID`、`min-idle-time`、删除条目清理、
    错误优先级、`MULTI/EXEC` 与 `EVAL` 均正确；
  - `XSETID` 的元数据更新与全部错误分支、`XACKDEL`/`XDELEX` 的状态数组与
    `KEEPREF`/`DELREF`/`ACKED`、`MULTI/EXEC` 与 `EVAL` 均正确；
  - `DEL` 删除 stream 后，阻塞的 XREADGROUP 立即收到 `NOGROUP`；普通 XREAD 保持阻塞，
    下一次 XADD 才唤醒；单键/多键 DEL 与 `MULTI` 内的 DEL 行为一致；
  - 快照写入头部为 `MCDC\x02`，重启后数据、消费者组与 PEL 正常恢复。

## 7. 已知限制与后续方向

- **Redis Stream 命令面已补齐**：`XLEN`、`XRANGE`、`XREVRANGE`、`XINFO`、`XACK`、
  `XACKDEL`、`XCLAIM`、`XAUTOCLAIM`、`XDEL`、`XDELEX`、`XTRIM`、`XSETID`、`XGROUP`、
  `XREAD`、`XREADGROUP`、`XADD` 均已暴露。
- `DEL` 删除 stream 现在会立即唤醒阻塞中的 `XREADGROUP`（返回 `NOGROUP`），普通
  `XREAD` 仍保持阻塞；`UNLINK` 与 `SET` 覆盖 stream 尚未接入同一路径。
- 阻塞的首次提交仍是一次 Raft 写（与 Redis 一致：空读也要创建/更新 consumer）。
- 若等待期间 group 被销毁，客户端在收到 `NOGROUP` 时不会重试；这符合 Redis 语义。
- 服务端仍使用 `Arc<RwLock<RedisStream>>`，`SharedStream` 自带的 Notify 阻塞栈未被使用；
  将来若要统一，需要先解决"本地阻塞不能绕过 Raft"这一前提。

## 8. 提交列表

```
3c6666f wake blocked consumer groups when DEL removes a stream
230c4bf add XSETID, XACKDEL and XDELEX stream commands
36d809c add XAUTOCLAIM stream command
e4c159d add XDEL and XTRIM stream commands
91e8886 add XREVRANGE and XINFO stream commands
db95a9f trim allocations on the stream wake path
c4ecf53 skip useless XREAD scans on non-append stream changes
efce599 avoid needless Raft writes on stream wakeups
70608ad fix stream blocking wakeups
1efeea2 add stream consumer group commands
c4f6d2e fix snapshot format version
```
