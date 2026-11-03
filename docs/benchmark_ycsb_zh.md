# 官方 YCSB Redis 基准测试

本文记录使用官方 [YCSB](https://github.com/brianfrankcooper/YCSB) Redis binding 对单节点
cache-cat 的一次测试。它和仓库内自定义的 `benchmark/` 程序是两套不同的测试。

## 环境

- YCSB `0.17.0`，Redis binding `0.17.0`（Jedis `2.9.0`）
- cache-cat release 二进制，单节点
- Raft：`127.0.0.1:5001`；Redis 协议：`127.0.0.1:6380`
- 主机：22 vCPU，30 GiB RAM
- 数据：1,000,000 条，每条约 1 KB（10 个 field，每个 100 B）
- 请求分布：zipfian
- 线程数：64
- Workload：A（50/50 读写）、B（95/5）、C（100% 读）

注意：YCSB 的 Redis binding 会把每条记录存成一个 Redis hash，使用
`HMSET`/`HGETALL`，不是字符串 `SET`/`GET`。因此这里测的是官方 binding 产生的
hash 密集负载。

YCSB 自带的 `bin/ycsb` 是 Python 2 脚本，本机没有 Python 2，因此使用等价的
`bin/ycsb.sh` 启动器。

## 复现方式

启动服务端：

```bash
./target/release/cache_cat \
  --conf cache_cat/conf/node1.toml \
  --log-path /tmp/cc-ycsb/log \
  --redis-port 6380 \
  --address 127.0.0.1:5001
```

加载数据并运行 workload：

```bash
export YCSB_HOME=/path/to/ycsb-0.17.0

"$YCSB_HOME/bin/ycsb.sh" load redis -s -P "$YCSB_HOME/workloads/workloada" \
  -p redis.host=127.0.0.1 -p redis.port=6380 \
  -p recordcount=1000000 -p operationcount=0 -p threadcount=64

for w in a b c; do
  "$YCSB_HOME/bin/ycsb.sh" run redis -s -P "$YCSB_HOME/workloads/workload$w" \
    -p redis.host=127.0.0.1 -p redis.port=6380 \
    -p recordcount=1000000 -p operationcount=1000000 -p threadcount=64
done
```

## 结果

加载阶段：

| 操作 | 数量 | 耗时 | 吞吐量 | 平均 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| INSERT | 1,000,000 | 21.105 s | 47,382 ops/s | 1,342 µs | 2,433 µs | 3,309 µs |

运行阶段：

| Workload | 操作数 | 耗时 | 吞吐量 | Read 平均 / P99 | Update 平均 / P99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| A（50/50） | 1,000,000 | 8.345 s | **119,832 ops/s** | 191 µs / 691 µs | 866 µs / 1,986 µs |
| B（95/5） | 1,000,000 | 2.937 s | **340,483 ops/s** | 176 µs / 569 µs | 324 µs / 1,183 µs |
| C（100% 读） | 1,000,000 | 2.502 s | **399,680 ops/s** | 156 µs / 522 µs | — |

所有操作都返回 `OK`，没有错误。

## 解读

- 更新是写瓶颈：Raft 的 append/apply 使 workload A 的 update 延迟约为 read 的 4-5 倍。
- 读密集型 workload 在这台 64 线程主机上约 340k-400k ops/s。
- 加载 1M 条记录是冷启动、纯写阶段，明显慢于读吞吐，因为每条 INSERT 都是复制写。
- 这些数据来自单节点、本机回环、无网络/TLS、1 KB value、1M key space 的环境，不是
  集群基准，也不应直接当作通用容量数据。

## Profiling 观察

使用 `perf record -e task-clock -F 199 -g --call-graph dwarf` 分别在 workload A
（50/50）和 workload C（100% 读）期间采样，结果显示：

- allocator 是主要的用户态开销之一（`mi_free`、`_mi_page_malloc_zero`、页回收）；
- 读路径上 `Bytes` clone/drop、RESP/hash 编码明显；
- Tokio 调度与 blocking-pool task poll 可见；
- 写路径能看到 `raft_engine::Engine::write`，但 `memcpy`、`sendfile`、
  `copy_file_range` 没有出现在热点前列；
- 约 40% 的样本是未解析的内核栈，因为本机 `/proc/kallsyms` 被隐藏，无法进一步区分
  网络/syscall/fsync 的具体占比。

结论：对这类 1 KB hash 负载，zero-copy 不会命中主要用户态热点，io_uring 也不是
第一优先级。更值得先做的是降低 HGETALL/编码路径的分配与 clone、批量 Raft 日志写、
快照流式化；在拿到可解析的内核符号之前，不应把 io_uring 当成确定收益。

### RESP 编码分配 A/B

后续把每个 RESP bulk/frame 长度（以及整数回复）的临时 `String` 改为栈上十进制格式化。
在同一台主机上，每个二进制各跑 5 次 workload C（每次 1M 读），中位吞吐从
290.0k ops/s 提升到 297.6k ops/s，平均读延迟从 213.4 µs 降到 208.3 µs。差异约 2%，
接近运行间噪声，但新路径确实消除了每个 bulk 和每个数组/map 长度的一次分配。

内存影响与主机规划见 `capacity_planning_zh.md`。
