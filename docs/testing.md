# 测试与本地联调

在 Linux 环境中安装 C++20 编译器、CMake、Protobuf、Apache bRPC 和 Python 3。bRPC 使用自定义安装目录时，将 `brpc.pc` 加入 `PKG_CONFIG_PATH`，并配置动态库搜索路径。

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

测试创建独立的临时数据目录，并为网络测试分配本机回环端口。网络测试需要允许创建 TCP socket。各脚本负责结束自己启动的服务进程。

## 分层执行

| 层级 | CTest 标签 | 测试内容 |
|---|---|---|
| 单元测试 | `unit` | ID 编码、PG 游标与批量分配、SDK 回复校验与客户端缓存、统一限流器、读取版本及 FMaster 状态持久化 |
| 集成测试 | `integration` | 多副本 SDK、polling、FMaster 管理命令与主动探测、修复、回收站和流量调度 |
| 端到端测试 | `e2e` | onebox 启动真实 FMaster/FServer 集群，进行配置、状态和 Chunk 读写验证 |
| 慢测试 | `slow` | 回收站后台回收、真实在线与后台流量竞争 |

```sh
ctest --test-dir build -L unit --output-on-failure
ctest --test-dir build -L integration -LE slow --output-on-failure
ctest --test-dir build -L slow --output-on-failure
ctest --test-dir build -L e2e --output-on-failure
```

回收站测试会等待后台清理周期。流量竞争测试单独执行，以便稳定验证配额分配。失败输出可通过 `ctest --test-dir build --rerun-failed --output-on-failure` 复查，上一轮详细结果保存在 `build/Testing/Temporary/LastTest.log`。

## FMaster 与 FServer

```sh
ctest --test-dir build -R '^fmaster' --output-on-failure
python3 -B tests/fmaster.py build
```

`fmaster_store` 验证配置原子更新、版本冲突、持久化恢复和心跳报告校验。`fmaster` 集成测试启动真实进程，验证启动配置导入、命令修改、目录独占、SIGKILL 后重启、损坏快照拒绝加载和节点重新发现。

`fmaster_commands` 通过真实 bRPC 进程验证管理命令，包括节点与盘筛选、容量和异常输出、原子启停与并发版本冲突、配置导出再导入、只读校验、立即探测和有限次数的持续查看。立即探测测试使用 5 秒周期，要求手动请求在 2 秒内刷新观测。

FMaster 主动调用每台 FServer 的状态接口。集成测试同时运行两台 FServer，再暂停其中一台，验证健康节点仍能持续被探测。测试还覆盖节点身份和地址校验、动态盘配置、目录不存在与普通文件路径，以及 `DISK_UNKNOWN`、`DISK_HEALTHY`、`DISK_ERROR`、`DISK_STALE` 的转换。容量使用目录所在文件系统的 `statvfs` 结果。

`fmaster_probe_test` 由 Python 集成测试调用，直接检查真实 FServer 的 bRPC 状态接口，包括错误身份、重复盘 ID、重复路径、相对路径和空盘列表。

`chunk_id` 验证 120 位 ID（48 位 Blob 序号、8 位 Space、64 位索引）、固定 index 哈希和逆函数。`fmaster_spaces` 验证全局 1,000 万 PG 的紧凑存储、默认绑定 200 PG、独占与非连续绑定、运维解绑、256 Space 上限、48 位序号与 64 位游标边界。`space_integration` 运行真实多节点集群，验证管理命令、SDK 路由读写、大小限制、修复/恢复及 SIGKILL 后重启。回收站测试覆盖新旧长度 ID 的过期清理。

## 存储与多副本

```sh
ctest --test-dir build -R 'replicated_client|read_version|worker_polling' --output-on-failure
ctest --test-dir build -R 'chunk_repair|chunk_trash|traffic_integration' --output-on-failure
```

多副本测试覆盖创建、追加、乱序等待、重复提交、条件冻结，以及两种读语义。整体读验证 frozen 优先、quorum 比较和慢副本取消，逐副本读验证数据分歧、部分失败、冻结状态、结果顺序和并发调用。修复与回收站测试使用实际 Chunk 文件和节点进程，流量测试通过 bRPC 产生在线、复制和回收流量。

## onebox 联调

onebox 工具在指定目录中管理本机 FMaster 和多台 FServer，并保存各进程的数据、日志及清单。启动后可以用同一目录执行检查、查看状态或停止服务。

```sh
python3 tools/onebox.py up --root /tmp/fstore-onebox-demo --build-dir build --servers 3 --disks-per-server 2
python3 tools/onebox.py status --root /tmp/fstore-onebox-demo
python3 tools/onebox.py check --root /tmp/fstore-onebox-demo
python3 tools/onebox.py down --root /tmp/fstore-onebox-demo
```

onebox 的服务使用独立回环端口。盘目录用于验证配置和文件系统状态，Chunk 数据目录由各 FServer 的启动参数指定。需要定位问题时，可在 `--root` 指定目录中检查服务日志和配置清单。

`fmaster_spaces` 还验证 20,000 个单 Chunk Blob 在 400 个非连续 PG 上的起点分布、显式零游标语义和绑定变化后保留起点。`space_integration` 验证 SDK 客户端复用、并发读取和句柄独立生命周期；`space_pool` 使用受控路由服务验证 256 项缓存上限、淘汰与并发未命中，不向端点样例发送数据 I/O。可选的 `space_bench` 用于测量真实 bRPC 分配链路，见 [Space 文档](spaces.md)。


## 新增分配与缓存 UT

以下四组测试直接调用生产逻辑，不启动 FMaster/FServer、不监听 TCP 端口；Store 测试只使用独立临时目录。

| CTest 名称 | 核心覆盖 |
|---|---|
| `fmaster_placement` | 空/单 PG、不同长度的非连续区间、固定起点样例、间隙与取余回绕、uint64 最后一个可达值、自动游标与显式零值的 Protobuf presence |
| `fmaster_batch` | 单 PG 连续分配 2,048 个不重复 ID、确定性重试、请求间重新检查健康状态、后部 PG 不可用时整批失败、游标耗尽和超量请求不暴露部分结果 |
| `replica_client_pool` | 端点顺序和键区分、256 项 FIFO 淘汰、构造失败/空结果重试、活跃引用生命周期、并发未命中只发布一份、慢构造不阻塞命中、并发淘汰及计数 |
| `space_client_validation` | ID/PG/Space 一致性、副本数量及重复地址、Blob/容量/游标边界、回复数量和序列顺序、重复 ID、错误续游标、同批 Space ID 与规格不一致、错误 Locate 回复 |

```sh
python3 tools/run_tests.py --build-dir build --suite unit
# 已构建时只运行本轮新增 UT：
ctest --test-dir build -R '^(fmaster_placement|fmaster_batch|replica_client_pool|space_client_validation)$' --output-on-failure
```

四组包含 24 个具名场景，失败时输出场景名与断言原因。缓存测试使用可控构造工厂和 barrier/promise 协调并发，不依靠 sleep 制造竞态；真实 bRPC、进程恢复与数据 I/O 仍由原集成测试覆盖。新 UT 发现并固定了 SDK 接受同批混合 Space ID/规格的缺口，SDK 在整批校验通过后才填充缓存和建立句柄。
