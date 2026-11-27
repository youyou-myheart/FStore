# FStore

Linux C++20 存储服务，包含 FMaster 集群配置与状态管理，以及 FServer Chunk 服务，使用 Apache bRPC 的二进制 `baidu_std` 协议。每个 Chunk 对应一个普通文件，文件中存放原始字节，`.open` 和 `.frozen` 后缀表示可写/只读；只读文件通过扩展属性区分用户 Freeze 与后台 Seal。数据目录由服务进程独占。

## 编译与启动

需要 C++20 编译器、CMake 3.20+、pkg-config、Protobuf（包括 `protoc`）和已安装的 Apache bRPC 共享库。若 bRPC 安装在自定义目录，将其 `brpc.pc` 所在目录加入 `PKG_CONFIG_PATH`，并让动态链接器能找到 bRPC 及其依赖库。

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
mkdir -p data
./build/chunk_server data 127.0.0.1:8080 64
```

服务位置参数为 `DATA_DIR [ADDRESS:PORT [MAX_CONCURRENCY [RATE_MIB_S]]]`，默认 `127.0.0.1:8080` 、64 个并发在线 RPC 和 256 MiB/s 的节点总额度。磁盘操作在独立工作线程池执行。Chunk 接口只接受 `baidu_std`，bRPC 内置服务已关闭。

服务端按 Chunk 调度写请求。每次最多合并 32 个偏移连续的追加请求，合计最多 8 MiB，共用一次落盘同步。创建、封存、删除和恢复作为批次边界。每个请求独立检查剩余容量并返回各自的偏移。

## FMaster 配置与状态管理

`fmaster` 使用 bRPC `baidu_std` 管理 FServer 清单和每台 FServer 的盘配置。每台服务有稳定 ID、RPC 地址和启用状态，每块盘有 ID、绝对路径和启用状态。盘路径位于对应 FServer 主机上。当前版本运行一个 FMaster，配置保存在独占的数据目录中，变更成功响应前完成持久化。

首次启动可通过 protobuf 文本配置导入服务和盘，例如 `cluster.pbtxt`。

```text
servers {
  id: "fs1"
  endpoint: "10.0.0.1:8080"
  disks { id: "disk1" path: "/data/fstore" }
  disks { id: "disk2" path: "/data/fstore2" disabled: true }
}
```

```sh
mkdir -p master-data
./build/fmaster --data-dir=master-data --listen=10.0.0.10:8090 --config=cluster.pbtxt
./build/fmaster_cli 10.0.0.10:8090 list
./build/fmaster_cli 10.0.0.10:8090 server-set fs2 10.0.0.2:8080 enabled
./build/fmaster_cli 10.0.0.10:8090 disk-set fs2 disk1 /data/fstore enabled
./build/fmaster_cli 10.0.0.10:8090 disk-set fs2 disk1 /data/fstore disabled
./build/fmaster_cli 10.0.0.10:8090 disk-remove fs2 disk1
./build/fmaster_cli 10.0.0.10:8090 server-remove fs2
```

管理命令支持集群概览、节点和盘详情、容量与异常列表、持续查看和配置导出。查看命令默认输出表格，追加 `--json` 可供脚本读取；`list` 保持完整 JSON 输出。完整参数和输出说明见 [FMaster 命令手册](docs/fmaster_commands.md)。

```sh
./build/fmaster_cli --help
./build/fmaster_cli 10.0.0.10:8090 status
./build/fmaster_cli 10.0.0.10:8090 servers --offline
./build/fmaster_cli 10.0.0.10:8090 disk fs1 disk1
./build/fmaster_cli 10.0.0.10:8090 capacity fs1
./build/fmaster_cli 10.0.0.10:8090 errors
./build/fmaster_cli 10.0.0.10:8090 watch --interval-ms=1000
./build/fmaster_cli 10.0.0.10:8090 server-disable fs1
./build/fmaster_cli 10.0.0.10:8090 server-enable fs1
./build/fmaster_cli 10.0.0.10:8090 disk-disable fs1 disk1
./build/fmaster_cli 10.0.0.10:8090 disk-enable fs1 disk1
./build/fmaster_cli 10.0.0.10:8090 probe fs1
./build/fmaster_cli 10.0.0.10:8090 config > cluster-backup.pbtxt
./build/fmaster_cli 10.0.0.10:8090 validate cluster-backup.pbtxt
```

`server-enable/disable` 和 `disk-enable/disable` 原子更新启用状态并保留其他配置，支持 `--expected-revision=N`。`probe` 请求立即探测并返回接受的节点数，结果通过查看命令获取。`validate` 返回规范化后的配置，当前配置保持不变。容量逐盘显示，包含观测时间和健康状态，同一文件系统下的多个目录不汇总计算总容量。

`server-set` 保留该服务已有的盘配置，`disk-set` 创建或替换指定盘配置。移除服务时同时移除它的盘登记。启用和禁用记录管理员配置意图，FMaster 通过主动心跳获取运行状态。Chunk 数据目录由启动参数 `DATA_DIR` 指定，盘配置用于清单与状态管理。移除登记仅修改 FMaster 配置。

启动的 `--config` 仅在数据目录首次初始化时使用。之后重启会加载持久化配置，命令行修改继续生效。显式调用 `fmaster_cli ENDPOINT apply FILE` 可原子替换整份配置。修改命令返回新的配置版本号，追加 `--expected-revision=N` 可以只在当前版本为 N 时更新，版本冲突时返回错误。配置校验覆盖重复服务 ID、地址、盘 ID 和同一服务内的盘路径，盘路径必须为绝对路径。

FServer 在 Chunk RPC 端口提供心跳接口，通过稳定的 ID 和服务地址校验探测对象。

```sh
./build/chunk_server /data/fstore 10.0.0.1:8080 --server-id=fs1
# 监听全部网卡时，指定已登记的服务地址。
./build/chunk_server /data/fstore 0.0.0.0:8080 --server-id=fs1 --advertise=10.0.0.1:8080
```

FMaster 按盘配置向每个 FServer 发起心跳，FServer 采集目录所在文件系统的总空间、空闲空间和普通用户可用空间，返回每块盘的可访问状态。缺失、无法访问或类型错误的目录附带错误信息。两个目录位于同一文件系统时，空间统计反映同一个文件系统。

FMaster 默认每 2 秒探测一次，单次 RPC 超时 1 秒，10 秒未成功时将节点标记为离线。分别使用 `--heartbeat-interval-ms`、`--probe-timeout-ms` 和 `--heartbeat-timeout-ms` 调整。各节点独立调度，一台节点超时期间其他节点继续被探测。`list` 输出 JSON，包含配置、在线状态、最近探测与成功时间、错误，以及每个配置盘的状态和空间信息。

盘状态为 `DISK_UNKNOWN`、`DISK_HEALTHY`、`DISK_ERROR` 或 `DISK_STALE`。未完成探测时为 UNKNOWN，路径访问失败为 ERROR，节点离线后历史容量附带 STALE 标记与原观测时间。配置启用状态与运行状态分别记录，禁用的节点也会被探测。FMaster 重启后重新探测所有节点。

## Space 与 PG

FMaster 为全集群管理固定的 10,000,000 个逻辑 PG。每个 PG 独占绑定一个 Space，Space 创建时自动领取 200 个全局空闲 PG，后续由运维通过绑定与解绑命令调整。Space 独立配置副本数、Chunk 大小和 `placement_policy`。

Chunk ID 为 `seq_id:48 | space_id:8 | index:64`，共 120 位。PG 由固定的 `hash(index) % 10,000,000` 决定，副本路由必须与 Space 的 PG 绑定一致。

```sh
./build/fmaster_cli 10.0.0.10:8090 space-create logs 3 67108864 --placement=single-dc
./build/fmaster_cli 10.0.0.10:8090 pg-pool 0 500
./build/fmaster_cli 10.0.0.10:8090 pg-bind logs 7000000 200
./build/fmaster_cli 10.0.0.10:8090 blob-allocate logs 1
./build/fmaster_cli 10.0.0.10:8090 allocate logs SEQ_ID START_CURSOR 10
```

索引通过可逆哈希直接生成，使用 `next_cursor` 分批分配；index 本身不保证递增。默认起点按 Blob 分散到已绑定 PG，避免小 Blob 集中在第一个 PG。`SpaceClient` 根据路由执行多副本读写，并缓存最多 256 套副本客户端供不同 Chunk 复用。元数据、独占绑定、分配游标与 SDK 用法见 [Space 与 PG 说明](docs/spaces.md)。

FServer 启动时扫描活动 Chunk 文件，重建内存中的 `PG → Chunk` 清单；创建、冻结、删除、恢复和修复安装同步更新清单。FMaster 连续探测失败满 10 分钟后，选择符合 Space 放置策略的替代节点，执行 `repair → 封存全部 Chunk → 源节点推送数据 → 核对清单 → normal`。进度、路由版本和节点上的隔离状态均持久化，支持中途重启继续。存活副本数据不一致时停止自动推进并记录错误。

```sh
./build/fmaster_cli 127.0.0.1:8090 recoveries [SPACE [AFTER_ID [LIMIT]]]
./build/chunk_cli 127.0.0.1:8080 pgs [START [LIMIT]]
./build/chunk_cli 127.0.0.1:8080 pg-chunks PG_ID [AFTER_CHUNK [LIMIT]]
```

恢复状态、故障处理、SDK 路由刷新及当前限制见 [PG 故障恢复](docs/pg-recovery.md)。

用户 `FreezeChunk` 产生 `Frozen / USER_FREEZE`，后台 `SealChunk`、`SealPG` 产生 `Sealed / PG_REPAIR`，首次关闭原因随文件持久化、复制和回收站恢复保留。两者都永久禁止追加；`FencePG` 是独立的 PG 临时写隔离。`chunk_cli ENDPOINT state ID [--pg-epoch=N]` 可以查看细分状态，PG 清单也返回 `close_reason`。协议旧 `frozen` 字段对两种只读状态都为 true，以兼容旧 SDK。

## Onebox 与测试

本机 onebox 默认启动 1 个 FMaster、3 个 FServer，每个 FServer 登记 2 个盘目录。各进程有独立的数据目录和日志，端口自动分配。Chunk 数据使用每台 FServer 的第一个目录，全部盘目录参加状态探测。

```sh
python3 tools/onebox.py up --build-dir build --root /tmp/fstore-onebox
python3 tools/onebox.py status --root /tmp/fstore-onebox
python3 tools/onebox.py check --root /tmp/fstore-onebox
python3 tools/onebox.py down --root /tmp/fstore-onebox
```

`down` 停止该集群的进程并保留数据、配置与日志，再次 `up` 使用已保存的配置恢复。`check` 检查节点和盘状态，并执行多副本读写、冻结和修复。

```sh
python3 tools/run_tests.py --build-dir build
python3 tools/run_tests.py --build-dir build --skip-build --suite unit
python3 tools/run_tests.py --build-dir build --skip-build --suite integration
python3 tools/run_tests.py --build-dir build --skip-build --suite e2e
```

测试入口配置并编译项目后运行 CTest，日志写入 `build/test-results`，支持 JUnit 的 CTest 还会生成 XML 报告。测试分类、故障场景和运行环境见 [测试说明](docs/testing.md)。

## 客户端

```sh
server=127.0.0.1:8080
id=0123456789abcdef0123456789abcdef
./build/chunk_cli "$server" create "$id" 1048576
printf 'hello' | ./build/chunk_cli "$server" append "$id" 0 1000 -
./build/chunk_cli "$server" read "$id" 0 5 > result.bin
./build/chunk_cli "$server" freeze "$id"
```

追加也接受文件路径。命令行 `append` 输出偏移和长度，`read` 将原始字节写到标准输出。失败时错误写到标准错误，退出码为非零。

应用通过 CMake 链接 `chunk_client`，包含 `chunk_client.hpp`。同一个客户端实例可以跨线程复用，默认连接超时 1 秒、调用超时 5 秒，两者可用 `ClientOptions` 配置。

```cpp
#include "chunk_client.hpp"

chunkstore::ChunkClient client("127.0.0.1:8080");
const std::string id = "0123456789abcdef0123456789abcdef";
client.create_chunk(id, 1048576);
const auto [offset, length] = client.append(id, 0, "hello", 1000);
const auto data = client.read_chunk(id, offset, length);  // butil::IOBuf
client.freeze_chunk(id);
```

### 多副本提交

用逗号指定同一 Chunk 的副本地址。客户端并发发出请求，每个地址复用一个 bRPC 通道，所有副本都确认后才返回成功。可将每台 server 部署在不同主机上。

```sh
replicas=10.0.0.1:8080,10.0.0.2:8080,10.0.0.3:8080
id=0123456789abcdef0123456789abcdef
./build/chunk_cli "$replicas" create "$id" 1048576
printf 'hello' | ./build/chunk_cli "$replicas" append "$id" 0 1000 -
./build/chunk_cli "$replicas" freeze "$id" 5
./build/chunk_cli 10.0.0.2:8080 read "$id" 0 5
```

C++ 应用包含 `chunk_client.hpp`，链接 `chunk_client`。

```cpp
chunkstore::ReplicatedChunkClient client({"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"});
client.create_chunk(id, 1048576);
const auto [offset, length] = client.append(id, 0, "hello", 1000);
client.freeze_chunk(id, offset + length);
```

- **创建**会检查每个副本的容量和返回状态。容量相同、状态一致的重复创建可成功。客户端拒绝解析到同一地址和端口的重复副本。调用方负责把地址映射到独立的存储节点。
- **追加**向所有副本发送相同的偏移、数据和乱序等待时间。正常写入每个节点只需一次 RPC，多个 attachment 共享客户端的 `IOBuf` 数据块。多个线程可共用一个客户端，由调用方统一分配各 Chunk 的字节范围。
- **部分失败**时，已落盘的字节继续保留且可读。CLI 返回非零并列出每台节点的结果，SDK 抛出 `ReplicaCommitError`，其 `replies()` 按地址输入顺序记录确认值或异常。传输错误表示结果未知。重试时使用相同的 Chunk ID、副本列表、偏移和原始数据。遇到已占用的偏移或已封存的副本，客户端会读回该范围，逐字节一致才计为确认。整个重试期间应保留同一代 Chunk，并保证每个偏移只分配给一份数据。
- **封存**时传入已提交的最终长度。每个节点在写锁内核对自己的长度，相符才封存。落后的副本可补写后重试，已按该长度封存的副本会重复确认。此操作使用新增的 `FreezeChunkAtLength` RPC，多副本封存前须升级对应节点。

多副本 SDK 提供两种读取语义，都按 `offset` 和 `length` 指定范围。

整体读接口 `client.read_chunk(id, offset, length)` 返回一份 Chunk 数据，CLI 支持 `chunk_cli "$replicas" read "$id" OFFSET LENGTH`。先读取配置中的第一个副本，成功响应带 frozen 时直接返回。首个副本未冻结或读取失败时，并发读取其余副本。任意成功响应带 frozen 即可返回该数据，否则请求范围内完全相同的数据达到多数派 `floor(N/2) + 1` 即可返回，N 是配置中的副本总数。未达多数派时抛出 `ReplicaCommitError`，错误信息包括各副本的失败原因或相同内容的票数。旧服务端缺少冻结状态字段时按未冻结处理。

已收到的只读响应优先于未关闭数据的票数。这里的协议 `frozen=true` 同时涵盖用户 Frozen 和后台 Sealed。满足只读或 quorum 条件后会取消其余 RPC 并返回，多数派结果无需等待慢副本。该规则信任单个副本的不可追加状态，即使其他副本尚未完成关闭也可返回。

逐副本读接口 `client.read_replicas(id, offset, length)` 并发读取所有副本，等待各自成功、失败或超时后，按配置地址顺序返回 `std::vector<ReplicaReadResult>`。每项包含 `endpoint`、`data`、`frozen`、`state` 和 `error`，不同副本的数据和状态逐条保留。成功时 `data` 是原始 `butil::IOBuf`，`frozen` 是兼容的只读标志，`state` 是 `std::optional<State>`，区分 Open、Frozen 和 Sealed；旧服务端缺少状态字段时为空。失败时 `error` 保存该副本的异常，数据和状态为空，其他副本的成功结果仍然返回。可用 `std::rethrow_exception(reply.error)` 获取 `RpcError` 或 `RemoteChunkError`。单节点 `read_with_state()` 也返回数据和细分状态。超时使用 `ClientOptions`，超过 8 MiB 的读取长度会直接抛出 `std::length_error`。

```cpp
const auto data = client.read_chunk(id, offset, length);
const auto replicas = client.read_replicas(id, offset, length);
// replicas[i].endpoint、data、state、frozen、error 对应第 i 个配置副本。
```

运行 `python3 tests/replicated_client.py build` 可验证三个独立 server 的并发写入、节点掉线与恢复、重复提交、冲突检测、条件封存、任意副本冻结读取、多数派数据比较、慢副本取消及逐副本数据与错误返回。

## 协议语义

| RPC | 请求 | 成功结果 |
|---|---|---|
| `CreateChunk` | Chunk ID、容量（字节） | 当前封存状态和容量，同 ID 同容量可重复调用 |
| `Append` | Chunk ID、预期字节偏移、字节数、等待超时（毫秒）、请求 attachment | 起始偏移和追加长度 |
| `ReadChunk` | Chunk ID、偏移、长度 | 响应 attachment 中的原始字节 |
| `FreezeChunk` | Chunk ID | 最终字节长度，重复封存返回相同长度 |
| `FreezeChunkAtLength` | Chunk ID、预期最终字节长度 | 长度匹配时封存并返回长度，长度有差异时返回 `LENGTH_MISMATCH` |
| `SealChunk` | Chunk ID、恢复 token、可选预期长度 | 后台封存并返回长度，要求匹配的 SOURCE PG fence |
| `DeleteChunk` | Chunk ID | 本次删除的 Unix 纳秒时间戳，用于恢复 |
| `RestoreChunk` | Chunk ID、删除时间戳 | 恢复后的 Open、Frozen 或 Sealed 状态 |

协议定义见 `chunk.proto`。Chunk ID 为 120 位（`seq_id:48 | space_id:8 | index:64`），使用 30 位小写十六进制字符串。仅 index 的固定哈希决定全局 PG，同一 Blob 共用序号；底层接口兼容已有的 24/32 位十六进制 ID。单次追加、读取各限 8 MiB。小记录可以用 `butil::IOBuf` 拼成一个请求，共用一次落盘同步。追加在同步成功后才返回。客户端用 `RpcError` 表示 RPC 调用失败，用 `RemoteChunkError` 表示服务端业务错误。

容量必须为正数，创建后固定。容量保存在 Chunk 文件的 Linux 扩展属性中，存储文件系统必须支持 `user.*` 扩展属性。文件长度随写入增长。恰好写满时追加成功，超过剩余容量的请求整批返回 `CHUNK_FULL`。同 ID 使用不同容量创建会返回 `CAPACITY_MISMATCH`。

旧文件可以继续读取。旧 Chunk 首次追加前，用原 ID 调用 `CreateChunk` 补登记容量，容量至少要等于当前文件长度。升级后，旧客户端需要传入容量才能调用创建接口。

### 乱序追加

每次 `Append` 必须带 `offset` 和 `wait_timeout_ms`，且数据不能为空。调用方按字节分配偏移。例如先发送偏移 5，再并发发送偏移 0 的 5 字节数据。服务端只写从当前长度开始的连续范围，会把两段放在同一批中（受批次上限约束）。C++ 调用是同步的，可以在多个线程共享同一个客户端发送。CLI 用法为 `append ID OFFSET WAIT_MS FILE|-`。

- 等待超时范围为 0 到 60,000 ms，从请求进入服务端追加处理开始计时。0 表示有缺口时立即返回。请求在进入写入批次前到期会被丢弃，并返回 `GAP_TIMEOUT`。已经进入批次的请求等待落盘完成。
- 已覆盖的偏移返回 `OFFSET_CONFLICT`，超过 Chunk 容量返回 `CHUNK_FULL`。成功封存会结束该 Chunk 未补齐的追加，返回 `FROZEN`。
- 服务端最多将约一半的并发额度用于等待缺口，留出额度接收补齐请求。等待队列满时返回 `WRITE_UNAVAILABLE`。
- C++ 客户端把调用超时设为默认 RPC 超时加本次等待时间，并关闭自动重试。收到 `GAP_TIMEOUT` 可确认该请求未写入。传输超时或断连时结果仍未知，可按原偏移核对数据。重启后按文件长度恢复下一可写偏移，未落盘的等待请求随进程退出而丢失。

## 节点流量调度

每个节点统一限制在线 I/O、回收站物理删除和后台复制的计费字节速率。启动时位置参数 `RATE_MIB_S` 是正整数 MiB/s。例如总额度设为 100 MiB/s，三类队列都持续积压时，长期份额分别是 **80、10、10 MiB/s**。空队列释放的份额由其他活跃队列按权重分配。

| 队列 | 权重 | 计费 |
|---|---:|---|
| 在线 | 8 | 读、追加按请求字节数，创建、封存、移入回收站、恢复等元数据请求按 4 KiB |
| 回收站 | 1 | 24 小时保留期结束后，每次物理删除按文件逻辑大小 |
| 复制 | 1 | 发送节点按读取发送的块大小，接收节点按接收写入的块大小，各自计入本节点额度 |

每个操作最低计费 4 KiB。调度器每次最多发放 64 KiB 额度，整项操作额度凑齐后放行。额度包括已发放给排队大文件的部分。文件系统缓存、文件系统元数据、RPC 往返和磁盘同步会影响实际设备带宽与完成时间。回收站积压时，过期文件排队等待物理删除。乱序追加等待时间从服务收到请求开始计算，包含流量队列中的等待。

复制的 10% 份额由高、中、低三级按 **4:2:1** 分享，每级内部按 FIFO 排队。未传优先级的请求使用中优先级。源和目标分别执行限流。源节点根据双方速率选择后续块大小，最大 8 MiB。

```sh
./build/chunk_server data 127.0.0.1:8080 64 100
./build/chunk_cli 127.0.0.1:8080 repair "$id" 10.0.0.2:8080 high
./build/chunk_cli 127.0.0.1:8080 stats
```

`stats` 返回节点配置速率，以及在线、回收站和三个复制子队列的累计发放字节数与当前等待项数。取两次采样的 `granted_bytes` 差值即可计算该时间窗内的份额。实际部署可将总额度设为节点可持续处理的带宽。每个优先级的复制接收请求有独立的并发额度，在线请求共用 `MAX_CONCURRENCY`。达到对应并发上限时返回 `WRITE_UNAVAILABLE`。

## 并发执行与压测

在线请求的入队使用多生产者、单消费者队列。生产者用原子比较交换发布任务，消费者批量取出并恢复 FIFO 顺序。工作线程默认在空闲时睡眠，到达新请求、乱序等待期限或停止信号时唤醒。可通过启动参数启用轮询。

| 路径 | 执行方式 |
|---|---|
| 限流 | 没有积压且令牌充足时，至多 64 KiB 的请求可在调用线程直接取得额度。需要排队的请求由一个调度线程独占五条 FIFO。两条路径共用同一个原子令牌桶。统计值逐项原子采样。 |
| 在线存储 | 写请求按 Chunk ID 选择工作线程。同一个哈希桶有待处理请求时维持归属，包括等待缺口的追加。桶空闲后可选择负载较低的线程，让其他 Chunk 绕开正在同步磁盘的线程。读取独立分配。 |
| 读取 | 每个线程缓存最近使用的文件版本弱引用。命中时固定该版本，在已发布的长度内执行 `pread`。删除、恢复和修复替换使旧版本失效，已开始的读可以从原文件读完。首次访问或版本失效时经过分片索引和 Chunk 锁。 |
| 多副本追加 | 客户端直接发出多个异步 bRPC 调用，共享请求数据块，收齐结果后再返回。正常提交省去为每个副本启动和回收 bthread 的开销。 |
| 修复 | 优先级之间可并行校验、写块和同步文件。接收端用会话锁登记和清理传输，在分块写入和文件同步前释放该锁。 |

同一 Chunk 的修改保留写锁，追加仍在 `fdatasync` 成功后发布长度并确认。文件系统、bRPC 内部和空闲等待也有各自的同步机制。

120 位 Chunk 的数据操作还持有 PG 级共享隔离锁，使恢复切换能等待已经进入存储层的操作完成。每线程缓存最近访问的 PG，命中时避开全局 PG 索引锁；心跳中的 Chunk 总数通过原子计数读取。PG 成员清单只在成员或冻结状态改变时更新，普通追加和读取不修改清单。本实现不是全链路无锁。

仓库附带使用长连接 C++ SDK 的压测工具。使用临时数据目录启动 server，每轮追加测量都会创建新 Chunk。下面依次测量 16 并发热点读、16 并发单副本追加和 8 并发三副本追加。参数为操作、并发线程数、每线程操作数、每次请求字节数和服务地址。

```sh
cmake --build build --target chunk_bench -j
./build/chunk_bench read   16 1500 4096 127.0.0.1:8080
./build/chunk_bench append 16  300 4096 127.0.0.1:8080
./build/chunk_bench append  8  250 4096 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082
```

工具输出逻辑操作数每秒和单次调用的 P50、P99 延迟。多副本追加的延迟涵盖所有副本确认。可用相同的节点数、并发上限、限流额度和文件系统比较不同版本。

本次与 `2b10e72` 的对比使用 Release 构建、同机 TCP 回环、overlay 文件系统，容器 CPU 配额为 4 核。每节点并发上限 128，限流 4096 MiB/s。新旧版本交替运行七轮，下表分别取各轮吞吐和 P99 的中位数。读取命中已预热的单个 Chunk，追加线程各写自己的 Chunk。

| 4 KiB 请求 | 原版 ops/s | 优化后 ops/s | 原版 P99 | 优化后 P99 |
|---|---:|---:|---:|---:|
| 单并发读 | 15,013 | 18,818 | 173 µs | 136 µs |
| 16 并发读 | 50,915 | 53,878 | 668 µs | 715 µs |
| 16 并发单副本追加 | 7,919 | 11,848 | 4.11 ms | 2.95 ms |
| 8 并发三副本追加 | 3,894 | 4,086 | 4.19 ms | 4.11 ms |

### 存储工作线程 polling

在位置参数后加 `--worker-poll=50us --poll-workers=1`，让一个存储工作线程在队列清空后继续检查新请求，最多 50 微秒。适合关注小请求尾延迟、能为突发流量留出 CPU 的节点。

```sh
./build/chunk_server data 127.0.0.1:8080 128 4096 --worker-poll=50us --poll-workers=1
```

| 启动参数 | 空闲时的行为 |
|---|---|
| `--worker-poll=sleep`（默认） | 工作线程通过信号量休眠。 |
| `--worker-poll=<N>us` | 轮询 N 微秒，期间收到请求即处理，预算耗尽后休眠。`0us` 等同于 `sleep`。 |
| `--worker-poll=busy` | 持续轮询。一条空闲轮询线程约占一个逻辑核，多节点部署时须按节点计算 CPU。 |

启用轮询后，`--poll-workers=N` 从现有存储线程池中选择 N 个线程，默认 1 个。可分配的新任务优先交给空闲轮询线程，轮询线程忙时由整个线程池分担。已有待写请求的 Chunk 继续使用原工作线程。各模式都按最早的乱序追加截止时间检查超时，并响应进程停止信号。

下表为同一 Release 构建、4 核 CPU 配额、TCP 回环、4 KiB 请求下的六轮中位数。三种模式轮换顺序，每节点使用 128 并发上限和 4096 MiB/s 限额，启用轮询时每节点选 1 个线程。读测试单独启动一个节点，追加测试在同机启动三个节点并等待全部确认。空闲 CPU 按单节点进程采样 1 秒，100% 代表一个逻辑核。

| 模式 | 16 并发热点读 ops/s | 读 P99 | 8 并发三副本追加 ops/s | 追加 P99 | 空闲 CPU |
|---|---:|---:|---:|---:|---:|
| `sleep` | 44,555 | 1.013 ms | 3,975 | 5.13 ms | ≈0% |
| `50us` | 48,720 | 0.869 ms | 4,192 | 4.81 ms | ≈0% |
| `busy` | 49,204 | 0.975 ms | 2,858 | 8.17 ms | ≈100% |

混合轮询也消耗负载期间的 CPU。这组 16 并发读中，服务进程的 CPU 中位数从 `sleep` 的 170% 升至 `50us` 的 184%。可用上面的 `chunk_bench` 命令在实际节点对比两种模式。

## 节点间修复

向源节点 A 调用 `RepairChunk(chunk_id, destination)`。A 从已封存的 Chunk 读取数据，复用一条 bRPC 连接向 B 发送 attachment，每块最多 8 MiB。成功响应中的 `length` 为 B 已持久化的快照字节数。

```sh
./build/chunk_cli 10.0.0.1:8080 repair 0123456789abcdef0123456789abcdef 10.0.0.2:8080
```

C++ 客户端用连接到 A 的实例调用 `source.repair_chunk(id, "10.0.0.2:8080")`。可用第三个参数设置整次 RPC 的超时，默认 300,000 ms。指定高优先级时调用 `source.repair_chunk(id, destination, chunkstore::rpc::v1::HIGH)`，也可在第四个参数设置超时。A 以自己的 Frozen 或 Sealed 只读副本为数据源。源为 Open 时，A 在发送数据前返回 `REPAIR_CONFLICT`。

| B 上的副本 | 发布规则 |
|---|---|
| 缺失 | 创建只读副本，保留 A 的容量、数据及关闭原因 |
| OPEN | 返回 `REPAIR_CONFLICT`，即使已有字节与源完全一致 |
| Frozen / Sealed | 使用 A 的只读快照替换整个副本，数据与关闭原因一起原子安装 |

B 验证每块的顺序、长度和 CRC32C，在分片目录所在文件系统的匿名临时文件中暂存，通过最终文件同步、重命名和目录同步完成发布。需要本地文件系统支持 `O_TMPFILE`、用户扩展属性和通过 `/proc/self/fd` 发布匿名文件。B 会保留旧副本供读取，暂存空间需能容纳完整的新快照。

每个节点的每个复制优先级可同时发送一个 Chunk、接收一个 Chunk。每级发送队列最多额外等待四个修复任务。同一目标 Chunk 的不同接收会话互斥。在块传输中断时，A 尝试通知 B 放弃临时文件，B 也会在空闲 60 秒后清理。再次请求会从偏移 0 重传。如果最后的响应丢失，B 可能已经发布完整副本。命令行和发送端均通过 `baidu_std` 传输。

运行 `python3 tests/repair.py build` 可验证两个独立节点之间的多块复制、重复调用、已损坏副本替换、活动副本冲突和重启恢复。

## 删除与回收站

```sh
deleted_at=$(./build/chunk_cli "$server" delete "$id")
./build/chunk_cli "$server" restore "$id" "$deleted_at"
```

删除只作用于当前节点，Open、Frozen、Sealed 均可删除。服务将整个文件原子移入同一分片的 `.trash/<id>.<删除时间戳>.open|frozen`，同步目录后返回。后续读取、追加、关闭和重复删除返回 `NOT_FOUND`。保留期内可用删除返回的时间戳恢复指定版本，数据、容量和关闭原因随文件保留。可以重新创建同 ID 的 Chunk。若该 ID 已在线，恢复会返回 `RESTORE_CONFLICT`。

保留期从删除时刻起满 24 小时，以文件名中的时间戳跨重启计算。过期后立即失去恢复资格。服务启动时扫描回收站，此后每分钟后台扫描一次，清除已过期的条目。节点停机期间到期的条目在下次启动后清理。
