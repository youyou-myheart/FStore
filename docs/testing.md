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
| 单元测试 | `unit` | 统一限流器、读取版本生命周期、FMaster 配置持久化与状态合并 |
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
