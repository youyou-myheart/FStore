# Space、全局 PG 与 Chunk ID

FMaster 管理全局 PG 池、Space 规格、PG 的独占归属以及副本路由。**全集群固定使用最多 10,000,000 个逻辑 PG**，全局编号为 `0..9,999,999`。未绑定的编号是空闲 PG，无需预先建立实体记录。

## Chunk ID

Chunk ID 的逻辑宽度为 **120 位 / 15 字节**，高位到低位依次是 48 位 Blob 序号、8 位 Space ID、64 位 Blob 内 Chunk 索引。

```text
[ seq_id:48 ][ space_id:8 ][ index:64 ]
chunk_id = (uint120(seq_id) << 72) | (uint120(space_id) << 64) | index
pg_id    = hash_index_v1(index) % 10,000,000
```

`seq_id` 是非零的全局 Blob 序号，上限 `2^48 - 1`。同一 Blob 的所有 Chunk 共用该序号和 Space ID。Space ID 使用 `0..255`，最多创建 256 个 Space。`index` 使用完整的 `uint64` 范围，可以跳号。

PG 路由只取决于 `index`，与 `seq_id`、`space_id` 无关。同一 Blob 的不同 Chunk 可以位于不同 PG；两个不同 Blob 使用相同 index 时，会计算出相同的全局 PG。路由还必须校验该 PG 确实属于 ID 中的 Space。

RPC 字符串与文件名使用固定 **30 位小写十六进制**，保留前导零，相当于大端排列的 15 字节。C++ `ChunkId` 是字段视图，序列化需使用显式编码方法，不能直接复制结构体内存，因为原生结构体可能有对齐填充。

`hash_index_v1` 使用 SplitMix64 的固定整数混合步骤，所有运算按无符号 64 位溢出：

```cpp
x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
return x ^ (x >> 31);
```

算法与常量属于持久化路由协议，不能用 `std::hash` 替代或在升级时随意修改。

## 全局 PG 的独占绑定

一个 PG 只能属于一个 Space。**创建 Space 会原子绑定 200 个全局空闲 PG**，优先选择较小的空闲编号；空闲编号碎片化时会领取多个区间，合计仍为 200 个。剩余不足 200 个、节点不足或放置策略不能满足时，整个创建失败。

后续绑定关系由运维显式调整，可以绑定任意空闲的全局 PG 区间。Space 保存的是实际绑定区间与数量，并不要求 PG 编号连续，也不要求从 0 开始。

```sh
master=127.0.0.1:8090
./build/fmaster_cli "$master" space-create logs 3 67108864 --placement=single-dc
./build/fmaster_cli "$master" spaces
./build/fmaster_cli "$master" pg-pool 0 500
./build/fmaster_cli "$master" pgs logs 0 100

# 为 logs 额外绑定全局 PG [7000000, 7000200)。
./build/fmaster_cli "$master" pg-bind logs 7000000 200
./build/fmaster_cli "$master" pg logs 7000000
```

`pg-pool START LIMIT` 查看全局归属，包括空闲 PG，同时返回全局总量和已绑定数量。`pgs NAME START LIMIT` 按全局编号升序遍历这个 Space 的绑定，自动跳过不属于它的区间，返回 `next_start` 以便继续。指定 `--include-inactive` 时按连续全局编号查看，其中 `active=false` 表示这个 PG 未绑定到所查询的 Space。

`pg-bind NAME START COUNT` 采用半开区间 `[START, START + COUNT)`。已经绑定到同一 Space 的部分保持原样，新空闲部分获得副本映射；只要区间内有任何 PG 属于其他 Space，就整批失败。相同绑定重复执行为无操作，不增加版本。

```sh
# 尚未发放 Blob 序号的 Space 可释放不需要的 PG。
./build/fmaster_cli "$master" pg-unbind unused 200 100
```

`pg-unbind` 支持裁剪或拆分区间。Space 一旦领取过 Blob 序号段，就可能已有数据，本版禁止解绑其 PG；有数据时的解除绑定、转移归属和副本迁移需要后续迁移协议。这个检查与 Blob 序号分配在同一事务锁内执行，避免解绑和分配并发造成路由失效。

`space-expand` 已被显式 `pg-bind/pg-unbind` 取代。PG 总数始终保持全局 1,000 万，绑定变化不会修改哈希模数，也不会重新映射已有 Chunk。

## Space 规格和放置策略

Space 规格创建后固定，包含 `replicas`（1..16）、`chunk_size`（1..INT64_MAX 字节）和独立的 `placement_policy`。

| 策略参数 | 含义 |
|---|---|
| `mode = SINGLE_DATACENTER` | 一个 PG 的副本位于同一机房的不同 FServer |
| `mode = BALANCED_DATACENTERS` | 按机房尽量均衡，默认至少跨两个机房 |
| `mode = DISTINCT_DATACENTERS` | 每个副本位于不同机房 |
| `min_datacenters` | 最少机房数；独立机房策略必须等于副本数 |
| `datacenters` | 机房白名单，空列表允许所有满足条件的机房 |

节点机房由 `server-set --datacenter=NAME` 配置，省略此参数时保留原值。跨机房策略要求节点有机房标签。

```sh
./build/fmaster_cli "$master" server-set fs1 10.0.0.1:8080 --datacenter=dc-a
./build/fmaster_cli "$master" server-set fs2 10.0.0.2:8080 --datacenter=dc-a
./build/fmaster_cli "$master" server-set fs3 10.0.1.1:8080 --datacenter=dc-b
./build/fmaster_cli "$master" space-create logs 3 67108864 \
  --placement=balanced-dc --min-datacenters=2 --datacenters=dc-a,dc-b
```

建立新 PG 映射时，只选择已启用、近期探测成功且至少有一个健康启用盘报告的 FServer。每段绑定保存不可变的候选节点集合与放置算法版本，从全局 PG ID 确定副本集合。已有 PG 的映射不受后续绑定和新增节点影响。相邻且候选集合相同的区间会合并，因此绑定一千万个 PG 也无需写入一千万条元数据。

被候选集合引用的节点不能直接删除或修改地址、机房。节点仍可禁用，相关 PG 的新 Chunk 分配会报告不可用。离线节点的旧映射保留，用于读取和后续修复。

当前映射粒度是 FServer。Chunk 实际使用 FServer 启动参数指定的数据目录；盘信息用于健康选择，这一版尚未增加按 PG 选择盘的数据通路。

## Blob 序号与高效索引分配

```sh
./build/fmaster_cli "$master" blob-allocate logs 1
# 使用返回的 seq_id 与 start_cursor，保存它们以便重试。
./build/fmaster_cli "$master" allocate logs SEQ_ID START_CURSOR 100
# 也可按当前绑定自动选择起点。
./build/fmaster_cli "$master" allocate logs SEQ_ID auto 100
./build/fmaster_cli "$master" locate logs CHUNK_ID
```

`blob-allocate` 返回全局唯一的 48 位 Blob 序号、Space ID 和首批分配的 `start_cursor`。FMaster 以每段至多 4,096 个序号持久化预留，并记录预留段所属 Space。持久化完成后才返回序号；重启跳过未使用的预留序号，避免重复发放。达到 48 位上限时明确失败，不发生回绕。

默认起点用 `hash(seq_id) % bound_pgs` 选中已绑定 PG 的一个序号，再转换为实际全局 PG 编号。这样只有一个 Chunk 的小 Blob 也会分散到所有绑定 PG，非连续区间不会集中到区间起点。这个步骤只选择 index；Chunk ID 的路由公式仍然是 `hash(index) % 10,000,000`。

省略 CURSOR 或指定 `auto` 时，按当前绑定重新计算起点；显式传入 `0` 仍表示从哈希空间零开始。需要跨绑定变更保持首个 Chunk 稳定时，传入并保存 `blob-allocate` 返回的 `start_cursor`。续分配必须使用上一批返回的 `next_cursor`。

`allocate NAME SEQ_ID CURSOR COUNT` 使用 **hash(index) 空间中的 uint64 游标**，并不把游标当成 index：

1. 找到不小于 CURSOR、且对 1,000 万取余后属于该 Space 绑定集合的哈希值 `h`。
2. 用固定哈希的逆函数计算 `index = inverse_hash_index_v1(h)`。
3. 组装 120 位 ID，返回对应 PG 和副本节点。
4. 返回 `next_cursor`，作为下一批的起点。

这避免了只绑定 200 个 PG 时平均约五万次试算才能找到一个合法 index。分配成本取决于请求数量和绑定区间数，无需扫描一千万个 PG。单次最多 1,024 个 Chunk。同一批内重复访问同一 PG 时，只计算和检查一次副本路由。

index 本身不保证递增。调用方保存 Blob 序号、返回的完整 ID 和 `next_cursor`，业务块顺序由业务记录关联。同一 Blob 的并发调用需要分配互不重叠的游标区间。追加 PG 绑定后游标仍单调前进，因此后续分配不会重复先前的 index。没有 `next_cursor` 表示已经耗尽 uint64 哈希空间；不足以满足请求数量时整批失败。

相同序号、游标和绑定集合可以重复规划同一批索引，但绑定集合改变后结果可能不同。实际创建或写入重试应使用已保存的完整 Chunk ID。FMaster 不保存逐 Blob/Chunk 的存在性记录，路由查询成功不代表文件已创建，文件存在性由 FServer 判断。

## C++ SDK

```cpp
#include "space_client.hpp"

chunkstore::SpaceClient client("127.0.0.1:8090", "logs");
auto blob = client.allocate_blob();
auto batch = client.allocate_chunks(blob.seq_id(), blob.start_cursor(), 2);
auto& chunk = batch.chunks.front();
// 持久保存 Blob 序号、start_cursor、完整 Chunk ID 与 next_cursor。
chunk.create();
chunk.append(0, std::string_view("hello"), 1000);
chunk.freeze(5);

if (batch.next_cursor) {
    auto next = client.allocate_chunks(blob.seq_id(), *batch.next_cursor, 2);
}
auto reopened = client.open_chunk(chunk.id());
auto bytes = reopened.read(0, 5);
```

句柄持有可复用的副本客户端，后续追加、读取、冻结直接访问 FServer。`allocate_chunk()` 和 `allocate_chunks(count)` 是分配新 Blob 并获取首批 Chunk 的便捷接口，自动使用返回的分散起点。`allocate_chunks(seq_id, count)` 按当前绑定自动选择首批起点；三参数版本指定明确的游标。

每个 `SpaceClient` 按有序副本地址列表缓存最多 256 套 `ReplicatedChunkClient`，不同 Chunk 和 PG 可以复用相同的客户端。缓存按插入顺序淘汰，不改变读请求优先访问的副本顺序。缓存锁只用于创建句柄和查看统计；句柄的 create/append/read/freeze 不访问缓存，也不获取这把锁。并发未命中允许重复构造，但缓存只保留一份。活跃句柄持有共享引用，在缓存淘汰或 `SpaceClient` 销毁后仍可继续使用。

SDK 会先校验整批回复的 ID、游标、Space ID 与规格一致性，再填充客户端缓存；错误批次整批拒绝。

`replica_pool_stats()` 返回 `hits`、`misses` 和 `cached_clients`；前两者统计句柄构造时的缓存查询，不统计数据 I/O。缓存命中意味着复用客户端和 Channel，bRPC 底层连接管理仍遵循自身规则。

## 持久化、兼容性和验证

`spaces.pb` 的 `FSPACE04` 格式保存 Space 规格、全局 PG 独占绑定、候选节点集合、Blob 序号预留段，以及 PG 恢复状态和路由覆盖，带 CRC32C 校验，采用临时文件写入、文件 fsync、rename、目录 fsync。现有 `FSPACE03` 快照校验通过后自动升级，旧 FMaster 会拒绝新格式，避免忽略恢复后的路由。`topology.pb` 继续保存节点与盘，备份应同时保留两者。元数据持久化失败后拒绝后续服务，损坏快照拒绝启动。

Space 目录有独立 revision。创建、绑定、解绑支持末尾 `--expected-revision=N`，从 `spaces/space/pg-pool` 查询该版本。序号段预留不改变配置版本。

旧版空 Space 目录可以升级；包含 Space 的旧目录需要显式迁移，避免把原先的局部 PG 或旧 ID 误解释成新全局路由。FServer 底层继续支持已有 24 位和 32 位十六进制文件名，包括读取、修复、删除恢复和过期清理。

测试覆盖 48/8/64 位边界、哈希与逆函数、独占冲突、默认 200 PG、非连续绑定、碎片回收、全局一千万 PG 上限、256 Space 上限、游标耗尽、并发分配及真实 SIGKILL 后恢复。真实集群测试覆盖 SDK 多副本读写、冻结、修复与回收站恢复。

## 分配性能测量

```sh
cmake --build build --target space_bench -j
./build/space_bench 127.0.0.1:8090 logs 1000 1
./build/space_bench 127.0.0.1:8090 logs 100 1024
```

`space_bench` 使用真实 bRPC 请求申请 Blob 序号、规划 Chunk 路由并构造 SDK 句柄，输出每秒 Blob/路由数、每 Blob 的 P50/P99 延迟、PG 分布与客户端缓存统计。它会消耗 Blob 序号，不创建或写入 Chunk 文件，因此结果是分配链路性能，不能用作磁盘写入吞吐。已有数据路径测试和 `chunk_bench` 继续覆盖存储 I/O。

本轮本机对比（2026-11-27）：Release 构建，loopback bRPC，1 个 FMaster、3 个 FServer、3 副本、200 个绑定 PG，串行客户端。每种配置运行 3 次，表中速率与延迟取三次的中位数；短时测量仅用于观察分配路径变化。

| 工作负载 | 修改前 | 修改后 |
|---|---:|---:|
| 1,000 个 Blob，每个 1 Chunk：路由/秒 | 8,961 | 11,520 |
| 同上：每 Blob P50 | 92.2 μs | 65.4 μs |
| 同上：覆盖 PG 数 | 1 | 196～200 |
| 同上：最繁忙 PG 的 Chunk 数 | 1,000 | 12～13 |
| 100 个 Blob，每个 1,024 Chunk：路由/秒 | 176,640 | 306,673 |
| 同上：每 Blob P50 | 4.76 ms | 2.85 ms |

批量场景每次生成 102,400 个路由，优化后缓存统计为 6 次未命中、102,394 次命中，保留 6 套客户端。三副本的地址顺序有六种排列，因此保留六套并保持原有首读副本顺序。FMaster 本批内每个 PG 只做一次副本选择。这里没有 Chunk 数据写入或 fsync 的性能数据，不代表存储吞吐提升相同比例。
