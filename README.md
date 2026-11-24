# Chunk Server

Linux C++20 单机 Chunk 服务，使用 Apache bRPC 的二进制 `baidu_std` 协议。每个 Chunk 对应一个普通文件，文件中存放原始字节，`.open` 和 `.frozen` 后缀表示状态。数据目录由服务进程独占。

## 编译与启动

需要 C++20 编译器、CMake 3.20+、pkg-config、Protobuf（包括 `protoc`）和已安装的 Apache bRPC 共享库。若 bRPC 安装在自定义目录，将其 `brpc.pc` 所在目录加入 `PKG_CONFIG_PATH`，并让动态链接器能找到 bRPC 及其依赖库。

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
mkdir -p data
./build/chunk_server data 127.0.0.1:8080 64
```

服务参数为 `DATA_DIR [ADDRESS:PORT [MAX_CONCURRENCY [RATE_MIB_S]]]`，默认 `127.0.0.1:8080` 、64 个并发在线 RPC 和 256 MiB/s 的节点总额度。磁盘操作在独立工作线程池执行。Chunk 接口只接受 `baidu_std`，bRPC 内置服务已关闭。

服务端按 Chunk 调度写请求。每次最多合并 32 个偏移连续的追加请求，合计最多 8 MiB，共用一次落盘同步。创建、封存、删除和恢复作为批次边界。每个请求独立检查剩余容量并返回各自的偏移。

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

## 协议语义

| RPC | 请求 | 成功结果 |
|---|---|---|
| `CreateChunk` | Chunk ID、容量（字节） | 当前封存状态和容量，同 ID 同容量可重复调用 |
| `Append` | Chunk ID、预期字节偏移、字节数、等待超时（毫秒）、请求 attachment | 起始偏移和追加长度 |
| `ReadChunk` | Chunk ID、偏移、长度 | 响应 attachment 中的原始字节 |
| `FreezeChunk` | Chunk ID | 最终字节长度，重复封存返回相同长度 |
| `DeleteChunk` | Chunk ID | 本次删除的 Unix 纳秒时间戳，用于恢复 |
| `RestoreChunk` | Chunk ID、删除时间戳 | 恢复后的 OPEN 或 FROZEN 状态 |

协议定义见 `chunk.proto`。Chunk ID 为 32 位小写十六进制字符。单次追加、读取各限 8 MiB。小记录可以用 `butil::IOBuf` 拼成一个请求，共用一次落盘同步。追加在同步成功后才返回。客户端用 `RpcError` 表示 RPC 调用失败，用 `RemoteChunkError` 表示服务端业务错误。

容量必须为正数，创建后固定。容量保存在 Chunk 文件的 Linux 扩展属性中，存储文件系统必须支持 `user.*` 扩展属性。文件长度随写入增长。恰好写满时追加成功，超过剩余容量的请求整批返回 `CHUNK_FULL`。同 ID 使用不同容量创建会返回 `CAPACITY_MISMATCH`。

旧文件可以继续读取。旧 Chunk 首次追加前，用原 ID 调用 `CreateChunk` 补登记容量，容量至少要等于当前文件长度。升级后，旧客户端需要传入容量才能调用创建接口。

### 乱序追加

每次 `Append` 必须带 `offset` 和 `wait_timeout_ms`，且数据不能为空。调用方按字节分配偏移。例如先发送偏移 5，再并发发送偏移 0 的 5 字节数据。服务端只写从当前长度开始的连续范围，会把两段放在同一批中（受批次上限约束）。C++ 调用是同步的，可以在多个线程共享同一个客户端发送。CLI 用法为 `append ID OFFSET WAIT_MS FILE|-`。

- 等待超时范围为 0 到 60,000 ms，从请求进入服务端追加处理开始计时。0 表示有缺口时立即返回。请求在进入写入批次前到期会被丢弃，并返回 `GAP_TIMEOUT`。已经进入批次的请求等待落盘完成。
- 已覆盖的偏移返回 `OFFSET_CONFLICT`，超过 Chunk 容量返回 `CHUNK_FULL`。成功封存会结束该 Chunk 未补齐的追加，返回 `FROZEN`。
- 服务端最多将约一半的并发额度用于等待缺口，留出额度接收补齐请求。等待队列满时返回 `WRITE_UNAVAILABLE`。
- C++ 客户端把调用超时设为默认 RPC 超时加本次等待时间，并关闭自动重试。收到 `GAP_TIMEOUT` 可确认该请求未写入。传输超时或断连时结果仍未知，可按原偏移核对数据。重启后按文件长度恢复下一可写偏移，未落盘的等待请求随进程退出而丢失。

## 节点流量调度

每个节点统一限制在线 I/O、回收站物理删除和后台复制的计费字节速率。启动时最后一个参数是正整数 MiB/s。例如总额度设为 100 MiB/s，三类队列都持续积压时，长期份额分别是 **80、10、10 MiB/s**。空队列释放的份额由其他活跃队列按权重分配。

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

## 节点间修复

向源节点 A 调用 `RepairChunk(chunk_id, destination)`。A 从已封存的 Chunk 读取数据，复用一条 bRPC 连接向 B 发送 attachment，每块最多 8 MiB。成功响应中的 `length` 为 B 已持久化的快照字节数。

```sh
./build/chunk_cli 10.0.0.1:8080 repair 0123456789abcdef0123456789abcdef 10.0.0.2:8080
```

C++ 客户端用连接到 A 的实例调用 `source.repair_chunk(id, "10.0.0.2:8080")`。可用第三个参数设置整次 RPC 的超时，默认 300,000 ms。指定高优先级时调用 `source.repair_chunk(id, destination, chunkstore::rpc::v1::HIGH)`，也可在第四个参数设置超时。A 以自己的 FROZEN 副本为权威数据源。源为 OPEN 时，A 在发送数据前返回 `REPAIR_CONFLICT`。

| B 上的副本 | 发布规则 |
|---|---|
| 缺失 | 创建 FROZEN 副本，保留 A 的容量和数据 |
| OPEN | 返回 `REPAIR_CONFLICT`，即使已有字节与源完全一致 |
| FROZEN | 使用 A 的 FROZEN 快照替换整个副本，可修复丢失或损坏的字节 |

B 验证每块的顺序、长度和 CRC32C，在分片目录所在文件系统的匿名临时文件中暂存，通过最终文件同步、重命名和目录同步完成发布。需要本地文件系统支持 `O_TMPFILE`、用户扩展属性和通过 `/proc/self/fd` 发布匿名文件。B 会保留旧副本供读取，暂存空间需能容纳完整的新快照。

每个节点的每个复制优先级可同时发送一个 Chunk、接收一个 Chunk。每级发送队列最多额外等待四个修复任务。同一目标 Chunk 的不同接收会话互斥。在块传输中断时，A 尝试通知 B 放弃临时文件，B 也会在空闲 60 秒后清理。再次请求会从偏移 0 重传。如果最后的响应丢失，B 可能已经发布完整副本。命令行和发送端均通过 `baidu_std` 传输。

运行 `python3 tests/repair.py build` 可验证两个独立节点之间的多块复制、重复调用、已损坏副本替换、活动副本冲突和重启恢复。

## 删除与回收站

```sh
deleted_at=$(./build/chunk_cli "$server" delete "$id")
./build/chunk_cli "$server" restore "$id" "$deleted_at"
```

删除只作用于当前节点，OPEN 和 FROZEN 均可删除。服务将整个文件原子移入同一分片的 `.trash/<id>.<删除时间戳>.open|frozen`，同步目录后返回。后续读取、追加、封存和重复删除返回 `NOT_FOUND`。保留期内可用删除返回的时间戳恢复指定版本，数据、容量和封存状态随文件保留。可以重新创建同 ID 的 Chunk。若该 ID 已在线，恢复会返回 `RESTORE_CONFLICT`。

保留期从删除时刻起满 24 小时，以文件名中的时间戳跨重启计算。过期后立即失去恢复资格。服务启动时扫描回收站，此后每分钟后台扫描一次，清除已过期的条目。节点停机期间到期的条目在下次启动后清理。
