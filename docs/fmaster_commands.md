# FMaster 命令参考

使用 `fmaster_cli ENDPOINT COMMAND` 管理 FMaster。下例通过 `master` 指定控制服务地址。

```sh
master=127.0.0.1:8090
./build/fmaster_cli "$master" status
```

## 状态与列表

| 命令 | 输出内容 |
|---|---|
| `status` | 配置版本、在线和离线服务数量、启用和禁用数量、各盘健康状态数量 |
| `list` | 完整配置和运行状态的 JSON 快照 |
| `servers` | 所有服务的地址、管理员状态与运行状态 |
| `server SERVER_ID` | 指定服务及其盘的详细配置和状态 |
| `disks [SERVER_ID]` | 全部盘或指定服务下的盘 |
| `disk SERVER_ID DISK_ID` | 指定盘的配置、容量、健康状态及观测时间 |
| `capacity [SERVER_ID]` | 各盘的总量、已用、空闲和可用空间 |
| `errors [SERVER_ID]` | 离线服务、最近探测失败的服务及处于 unknown、error、stale 状态的盘 |

视图命令可添加 `--json` 供脚本解析。`list` 固定输出 JSON。服务列表支持 `--online`、`--offline`、`--enabled` 和 `--disabled`，盘列表支持 `--health=unknown|healthy|error|stale`。同一类互斥筛选项重复或同时出现时会返回参数错误。

```sh
./build/fmaster_cli "$master" servers --online
./build/fmaster_cli "$master" servers --disabled --json
./build/fmaster_cli "$master" disks fs1 --health=error
./build/fmaster_cli "$master" disk fs1 disk1 --json
./build/fmaster_cli "$master" errors --json
./build/fmaster_cli "$master" capacity fs1 --json
```

`servers`、`server`、`disks`、`disk` 和 `errors` 的 JSON 使用 `ListResponse` 结构，保留 `topology.revision`，配置项和运行状态按相同筛选条件输出。`status --json` 包含 `servers` 与 `disks` 计数。64 位版本号和字节数使用 JSON 字符串表示。

容量按配置盘逐行展示。同一文件系统的多个目录可能报告相同容量，容量视图保留各目录的观测结果。`capacity --json` 的顶层包含 `revision` 和 `disks` 数组，每项包括服务 ID、盘 ID、路径、健康状态、容量字段和 `observed_unix_ms` 观测时间。当前样本没有有效容量观测时，容量字段显示空值。`stale` 保留之前的有效容量，可通过观测时间判断样本的新旧。

| 盘状态 | 含义 |
|---|---|
| `unknown` | 尚未取得该盘的有效探测结果 |
| `healthy` | 最近的探测成功访问目录并读取文件系统容量 |
| `error` | 最近的探测遇到路径或文件系统错误 |
| `stale` | 最近成功响应已超过状态有效期，保留历史观测时间和容量 |

服务在最近一次探测失败后，可能因先前成功响应仍在有效期内而保持在线。`errors` 会保留这类服务的探测错误，便于及时查看连接或调用失败。

## 修改服务和盘

```sh
./build/fmaster_cli "$master" server-set fs1 10.0.0.1:8080 enabled
./build/fmaster_cli "$master" disk-set fs1 disk1 /data/fstore enabled
./build/fmaster_cli "$master" server-disable fs1
./build/fmaster_cli "$master" server-enable fs1
./build/fmaster_cli "$master" disk-disable fs1 disk1
./build/fmaster_cli "$master" disk-enable fs1 disk1
./build/fmaster_cli "$master" disk-remove fs1 disk1
./build/fmaster_cli "$master" server-remove fs1
```

`server-set ID ENDPOINT [enabled|disabled]` 创建或修改服务的地址和管理员状态，并保留已有盘配置。`disk-set SERVER_ID DISK_ID PATH [enabled|disabled]` 创建或修改指定盘的路径和管理员状态。省略状态时使用 enabled。

`server-enable`、`server-disable`、`disk-enable` 和 `disk-disable` 原子修改对应的管理员状态，并保留地址、路径及其他盘配置。移除服务时一并移除它的盘登记。启用和禁用表示配置与状态管理中的管理员意图，Chunk 数据目录由 FServer 启动参数指定。

修改命令成功后输出当前配置版本号，重复应用相同配置可返回原版本。需要防止覆盖他人的修改时，在命令末尾添加 `--expected-revision=N`。

```sh
./build/fmaster_cli "$master" server-disable fs1 --expected-revision=12
./build/fmaster_cli "$master" disk-enable fs1 disk1 --expected-revision=13
```

版本不匹配时命令失败并返回 `CONFLICT`。服务和盘的启停命令要求目标已存在。

## 配置导出、校验与导入

```sh
./build/fmaster_cli "$master" config > cluster.pbtxt
./build/fmaster_cli "$master" validate cluster.pbtxt
./build/fmaster_cli "$master" apply cluster.pbtxt --expected-revision=14
```

`config` 导出 protobuf 文本格式的 `Topology`，可直接用于 FMaster 首次启动的 `--config` 或 `apply`。`validate FILE` 检查 ID、地址、盘路径及重复项，输出规范化配置的 JSON，版本字段为 0。校验仅处理待导入内容。

`apply FILE` 校验整份配置并原子替换当前配置。导入文件中的版本字段不决定新版本，FMaster 根据当前版本和实际变更生成版本号。导出后立即导入同一配置可保持原版本。

## 立即探测与持续观察

```sh
./build/fmaster_cli "$master" probe fs1
./build/fmaster_cli "$master" probe
./build/fmaster_cli "$master" watch --interval-ms=1000 --count=5
./build/fmaster_cli "$master" watch --json --interval-ms=1000 --count=5
```

`probe SERVER_ID` 请求立即探测指定服务，省略 ID 时请求探测全部已登记服务。JSON 返回 `scheduled_servers`，表示已接受的调度数量。探测异步执行，可继续通过详情命令检查新的观测时间、状态和错误。

`watch` 周期性输出状态汇总。`--interval-ms` 设置正数毫秒间隔，`--count` 设置正数采样次数，省略次数时持续执行，可用 Ctrl+C 结束。JSON 模式每行输出一个完整汇总对象，便于逐行处理。

## 返回值

命令成功时退出码为 0，参数错误、RPC 失败和 FMaster 业务错误时返回非零，错误说明写入标准错误。常见业务错误包括 `INVALID_ARGUMENT`、`NOT_FOUND`、`CONFLICT` 和 `STORAGE_ERROR`。
