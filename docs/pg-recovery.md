# PG 故障恢复

本版针对 FServer 故障后自动补齐 PG 副本。正常状态对应路由的 `recovering=false`；修复状态对应 `recovering=true`。修复期间停止该 PG 的新建、追加、删除和恢复，允许读取已有数据。全部 Chunk 关闭后才开始复制，完成后发布新副本列表并恢复正常状态。原 Chunk 保持 frozen 或 sealed，业务继续写入需要分配新 Chunk。

## Freeze、Seal 与 Fence

| 操作 | Chunk 状态 / 关闭原因 | 生命周期 |
|---|---|---|
| 用户 `FreezeChunk` / `FreezeChunkAtLength` | `State::Frozen` / `USER_FREEZE` | 用户主动结束写入，永久只读 |
| 后台 `SealChunk` / `SealPG` | `State::Sealed` / `PG_REPAIR` | 为恢复固定内容与长度，永久只读 |
| `FencePG` | PG 的 SOURCE/TARGET/ACTIVE/RETIRED 模式 | 临时阻写与拓扑隔离，不是 Chunk 关闭原因 |

首次成功关闭决定该文件版本的原因；对已 Frozen 的 Chunk 执行 Seal 不覆盖 USER_FREEZE，对已 Sealed 的 Chunk 执行 Freeze 也不覆盖 PG_REPAIR。两种操作都可重复调用，并核对可选的预期长度。修复安装会整体替换文件版本，新目标继承选定源的关闭原因。

`SealChunk` 是恢复控制 RPC，必须携带匹配本地 SOURCE fence 的非零 `recovery_token`。`SealPG` 在完成数据核对后逐个调用同一存储操作。解除 PG Fence 后，原有 Sealed Chunk 仍拒绝追加，错误码为 `SEALED`；用户 Frozen Chunk 返回 `FROZEN`。旧的 `SetPGFence` RPC 保留为 `FencePG` 的兼容入口。

仍然一个 Chunk 对应一个文件。`.open` 表示可追加，`.frozen` 是兼容的只读文件后缀，两种关闭原因通过同一文件的 `user.chunkstore.close_reason` 扩展属性区分，值为 `USER_FREEZE` 或 `PG_REPAIR`。关闭时先写属性并 fsync 文件，再 rename 和同步目录；rename 前遗留在 `.open` 文件上的属性不生效。修复先将数据和原因写入临时文件再一起原子安装，移入回收站和恢复时属性随 inode 保留。

旧 `.frozen` 文件没有该属性时解释为 USER_FREEZE；历史版本未记录关闭来源，无法追溯区分当时是否由后台关闭。未知或损坏的原因不会被默认为可写状态。备份和外部文件迁移需要保留扩展属性，与已有容量属性的要求相同。

## 故障检测与放置

FMaster 主动探测 FServer，维护配置拓扑、节点可用状态和各盘容量。第一次失败的时间持久化；后续失败不重置时间，成功探测清除本次故障计时。默认连续失败 600,000 ms 后进入调度候选。重启 FMaster 保留原时间，但必须再次探测失败才会使用该故障记录。

调度只扫描已绑定 PG，每轮最多 256 个，最多保留 16 个正在执行的恢复任务。替代节点须为启用、最近心跳成功、支持 PG 恢复的 FServer，不能与原 PG 成员重复，且有健康、启用、可用容量至少为一个 Chunk 的登记盘；副本数、机房白名单和 Space 放置策略仍须满足。多个故障副本都超过宽限期后可一起补齐，至少需要一个健康存活副本。

没有数据源、没有合适目标或存活成员尚未满足健康条件时不修改路由，后续调度继续检查。节点故障时间可通过 `recoveries` 查看；禁用节点不自动作为故障迁出，这是管理配置与故障检测的区别。

```sh
./build/fmaster --data-dir=/data/master --listen=127.0.0.1:8090 \
  --heartbeat-interval-ms=2000 --heartbeat-timeout-ms=10000 --probe-timeout-ms=1000 \
  --recovery-grace-ms=600000 --recovery-interval-ms=1000 --recovery-rpc-timeout-ms=300000
```

`recovery-interval-ms` 控制恢复调度轮次，`recovery-rpc-timeout-ms` 是单次封存检查或批量复制 RPC 的超时。实际完成时间取决于数据量、调度队列和限流；10 分钟是开始调度的故障宽限期。

## 持久化状态机

| 阶段 | 行为 | 重试与崩溃恢复 |
|---|---|---|
| `PREPARING` | 先保存计划并标记 PG 为 repair，再对存活源发送 SOURCE 隔离，对替代节点发送 TARGET 隔离 | RPC 幂等，重启后补发未确认的隔离 |
| `SEALING` | 每批最多 16 个 Chunk，核对所有存活源的清单、长度、容量和 CRC32C，再 Seal 本批；所有批次完成后才能复制 | 关闭原因和数据先同步，再 rename 和同步目录；已关闭 Chunk 保留原原因 |
| `COPYING` | 给一个存活源发 `RepairPG`，源主动向目标推送本批 Frozen/Sealed Chunk | 目标原子安装数据和关闭原因才确认；批次确认后保存游标，未确认批次可重复复制 |
| `VERIFYING` | 分页比较完整清单和只读状态，核对新目标与选定源的关闭原因，数量、ID 和分页边界必须一致 | 目标有多余文件也不能通过；传输内容由分块 CRC32C 校验 |
| `ACTIVATING` | 将所有新成员切换为 ACTIVE 和新的 PG epoch，然后持久化发布新路由 | 发布前重启继续发幂等激活；不会重新从头执行封存 |
| `RECOVERED` | 新路由恢复 normal，旧故障成员通过后续心跳接收 RETIRED 指令 | 指令和节点本地隔离均可重放，旧文件保留，不自动物理删除 |

隔离操作持有 PG 独占锁，等待已进入存储层的操作结束。新的创建、追加、删除和恢复在 SOURCE/TARGET 状态被拒绝；已入队但尚未执行的请求也必须通过隔离检查。源的集合与内容因此保持稳定。排队请求可能在关闭 PG 前已经完成部分副本提交，封存前的跨副本核对负责发现这些差异。

发现成员清单、长度、容量或内容校验和不同，任务停留在 SEALING 并记录 `error`，后续继续重试。本版不会自动选择较长文件、截断不同尾部或以少数副本覆盖其他副本。它没有已提交偏移日志来证明哪一份是最终权威数据；需要先诊断和处理分歧。

封存检查、数据发送与接收使用现有后台复制 NORMAL 队列，遵守在线、回收站删除、后台复制的 8:1:1 满载带宽份额。每个复制块按目标建议自适应大小，上限 8 MiB，默认从 64 KiB 协商；进程间通过 bRPC 传输，数据不经过 FMaster。

## FServer 内存索引

启动获得数据目录独占锁后，扫描两位十六进制分片目录中的 `完整ID.open` 和 `完整ID.frozen`，读取只读文件的关闭原因属性。按 120 位 ID 中 index 的固定哈希计算 PG，建立有序 Chunk 清单，记录 Open/Frozen/Sealed 状态、总数、未关闭数和内存 generation。完成扫描之后才启动网络服务，不扫描文件内容、不一次打开所有文件描述符。普通数据读写使用内存状态，不逐请求读取扩展属性。

`.trash`、创建/安装中的临时文件和未命名修复临时文件不属于活动清单。24/32 位旧 ID 继续支持原存储操作，但没有本版 PG 归属，不参与自动 PG 恢复。冲突的 open/frozen 文件、非普通活动文件或损坏的控制元数据使启动失败，避免携带不完整清单参与恢复。

创建、冻结、删除、回收站恢复、修复安装在文件名变更后更新索引；崩溃后以活动文件重新扫描重建。索引不是独立持久化数据库。`.pg_control/PG_ID` 仅保存恢复 token、epoch 和模式，有校验和，按临时文件写入、fsync、rename、目录 fsync 发布。它必须跨重启保留，不能在修复过程中手动删除。

```sh
./build/chunk_cli HOST:PORT pgs 0 100
./build/chunk_cli HOST:PORT pg-chunks 37 '' 100
# 后续页面使用上次返回的 next_chunk。
./build/fmaster_cli MASTER:PORT recoveries logs 0 100
./build/fmaster_cli MASTER:PORT pg logs 37
./build/chunk_cli HOST:PORT state CHUNK_ID --pg-epoch=EPOCH
# 恢复控制调用，必须已有对应的 SOURCE fence。
./build/chunk_cli HOST:PORT seal CHUNK_ID RECOVERY_TOKEN EXPECTED_LENGTH
```

## 客户端路由

PG 初始 epoch 为 0，每次完成恢复使用新的持久化恢复序号。所有数据请求携带 `pg_epoch`；FServer 拒绝与当前 epoch 不符的请求。`SpaceClient` 在构造句柄时把路由 epoch 写入副本客户端，缓存键也包含 epoch。

已持有的 `SpaceChunk` 不自动刷新路由。收到隔离/旧版本错误后，通过 `SpaceClient::open_chunk(id)` 获取新句柄；重试写入前仍需处理“原请求是否部分成功”的既有语义。直接使用 `ChunkClient` 时设置 `ClientOptions::pg_epoch`，命令行在数据命令末尾加 `--pg-epoch=N`。

协议的旧 `frozen` 字段继续表示“只读”，对 Frozen 和 Sealed 都为 true，旧 SDK 因此保留原有单个只读副本即可返回的行为。新回复的 `close_reason` 区分 USER_FREEZE / PG_REPAIR / NOT_CLOSED；单节点 SDK 的 `read_with_state()` 和逐副本读取的 `ReplicaReadResult::state` 返回 `std::optional<State>`。旧服务器只有 frozen=true 时解释为 Frozen，两种状态字段都缺失时保留 unknown。未知枚举或相互矛盾的状态字段按协议错误处理。Frozen 和 Sealed 都可走整体读取的只读快速路径；Open 仍要求 quorum。

旧故障节点重新出现后，FMaster 在心跳中要求它持久化 RETIRED，再接纳其成功报告。这个机制不是心跳租约：网络隔离的旧节点，以及重启后尚未收到新心跳的旧节点，不能即时获知新路由；不要把它理解成完整的网络分区强一致协议。旧客户端刷新路由后使用新成员和 epoch。

## 当前边界

- 这是单 FMaster 持久化恢复流程，没有 Master 选主或多 Master fencing。
- 恢复中源或新目标再次故障时，任务保留原计划重试；尚未自动重新选择本次任务的目标。
- 容量检查是登记盘的初步准入，没有整个 PG 的字节预留。FServer 当前仍向启动指定的数据目录写入，尚未实现每盘放置调度；空间不足会使复制失败并保留 repair 状态。
- 已完成任务用于旧成员退役指令，当前保留在 `spaces.pb`；该快照仍有 16 MiB 上限。全局 1,000 万 PG 的紧凑初始放置并不意味着可以无限累积逐 PG 恢复历史，生产规模需要后续分片元数据和历史压缩。
- PG 配置拓扑仍保护被放置范围引用的节点，自动恢复不等于自动删除历史节点配置。退役副本的数据也不自动进入回收站。
