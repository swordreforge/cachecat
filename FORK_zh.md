# Fork 说明

本仓库是 [nasuiyile/cache-cat](https://github.com/nasuiyile/cache-cat) 的 fork。
上游历史完整保留，本 fork 基于上游最后一个提交 `5ed250c`（"Removed some useless code"），
其后的提交为本 fork 的改动：

- 对比链接：<https://github.com/swordreforge/cachecat/compare/5ed250c...master>
- 本地查看：`git log --oneline 5ed250c..HEAD`

## 改了什么

### Redis Stream 命令面

补齐了缺失的消费组、元数据和 Redis 8.2 命令：

- `XLEN`、`XRANGE`、`XREVRANGE`、`XINFO`
- `XPENDING`、`XACK`、`XACKDEL`、`XCLAIM`、`XAUTOCLAIM`
- `XREADGROUP`（含阻塞读）
- `XDEL`、`XDELEX`、`XTRIM`、`XSETID`

全部注册到服务端与 Lua 工厂，因此 `MULTI/EXEC`、`EVAL` 中可用。

### 阻塞读与唤醒

- 用统一的 `StreamWaiter` 注册表替代原先无类型的阻塞上下文，同时服务 XREAD 和
  XREADGROUP。
- stream 写入统一发布 `changed_streams` 事件；XREAD 只在追加事件上评估，group 等待者
  重新检查组状态。
- 新条目每个 `(key, group)` 最多唤醒一个等待者，其余通过链式投递继续。
- group 被销毁/重建时直接返回 `NOGROUP`，不再提交注定失败的 Raft 写。
- `DEL`/`UNLINK` stream、`SET`/`MSET` 覆盖 stream 现在会按 Redis 的
  `unblock_on_nokey` 语义唤醒阻塞组读（`NOGROUP`/`WRONGTYPE`），普通 XREAD 保持阻塞。
- 空阻塞 XREADGROUP 只有在新 consumer 时才立即写；已存在 consumer 延迟到首次交付。
- 删除未使用的本地 `SharedStream`/`Notify` 阻塞栈，统一由 Raft-aware 注册表负责。

### 复制与确定性

- 核心层新增带时钟参数的 `xreadgroup_at`、`xclaim_at`、`xpending_range_at`、
  `xinfo_consumers_at`、`xinfo_stream_full_at`、`xautoclaim_at`，PEL 时间与 consumer
  时间全部来自复制写时钟。
- 新增复制化的消费组 generation，stream 快照版本升到 4，关闭“阻塞期间销毁并重建”
  的串读窗口。
- 新操作均补充快照回放与 bincode 往返测试。

### 修复的既有问题

- 快照文件 `VERSION` 长期为 1，但文档格式已是 2。
- `RedisOperation::RedisEvalSha` 带 `#[serde(skip)]`，声明在其后的变体会写出越界
  serde 下标，导致 Raft apply 失败。

### 性能

- RESP 编码的长度和整数回复改为栈上格式化，不再为每个 bulk/frame 分配临时
  `String`。
- HGETALL 直接返回 string-map 回复，避免为每个 field/value 构造 `Value` 包装和
  对 `Value` 排序。

### 文档与测试

- 新增中英文 Stream 设计文档、容量规划与官方 YCSB 测试说明（`docs/`）。
- 当前测试：`cargo test -p cache_cat --lib -- --skip test::tests::test_add`
  （608 passed）。

## 兼容性

这是 alpha 分支，不保证与上游的状态/日志兼容：

- Stream 快照版本已升到 4；
- Raft 日志/快照载荷包含新的 stream 操作与时钟字段；
- 上游旧快照文件默认视为不兼容，需要迁移。

## 许可证

上游与本案均使用 Apache License 2.0，见 `LICENSE`。上游还声明包含来自
[lichuang/coredb](https://github.com/lichuang/coredb) 与
[lichuang/rockraft](https://github.com/lichuang/rockraft) 的代码。
