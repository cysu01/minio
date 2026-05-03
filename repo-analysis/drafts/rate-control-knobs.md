# MinIO 限流操作参数手册

> **配套文档**：`06-module-rate-control.md`（机制原理）。本手册按 4 层结构枚举每层的可调参数 —— 环境变量、`mc admin config` 配置键、`mc admin` 命令 —— 每条都给出源码引用 (`path:line`)。
> **代码版本**：`/home/vscode/repo-analyses/minio-20260503/repo`（约 2026-05 截面）。
>
> **使用约定**：
> - "类型" 列：`env` = 进程启动环境变量；`mc-config` = 通过 `mc admin config set` 持久化到集群配置；`mc-cmd` = 专用 `mc admin` 子命令。
> - 大多数 API/Scanner/Heal 类参数同时支持 env 与 mc-config 两种入口；env 在 `LookupConfig` 里作为 override（典型模式：`env.Get(EnvX, kvs.GetWithDefault(X, DefaultKVS))`，见 `internal/config/api/api.go:239`）。
> - "(no env override, config-only)" 表示只能通过 `mc admin config set` 设置，没有对应 env。

---

## L1 入站 API 限流 (Incoming Request Limit)

操作员在此层主要回答两个问题：(1) 单节点最多接多少并发 S3 请求？(2) 复制/transition/cleanup 等"前台触发的后台动作"分多少 worker？这层全部归在 `api` 子系统下，热更新友好（改完即生效，正在执行的请求按旧配置完成）。

### 配置项汇总

| 控制项 | 类型 | 名称 | 默认值 | 取值范围 | 作用 | 源码引用 |
|---|---|---|---|---|---|---|
| 最大并发请求数（每节点） | env | `MINIO_API_REQUESTS_MAX` | `0`（自动按 RAM 算） | `≥0` 整数 | 显式设值时是**集群总数**，运行时再除以节点数 | `internal/config/api/api.go:56`、`cmd/handler-api.go:127-150` |
| 最大并发请求数 | mc-config | `api requests_max` | `0` | 同上 | 同上 | `internal/config/api/api.go:36`、`:93-95` |
| 集群健康超时 | env | `MINIO_API_CLUSTER_DEADLINE` | `10s` | duration | 节点间健康检查/分布式调用最长等待 | `internal/config/api/api.go:58`、`:97-99` |
| 集群健康超时 | mc-config | `api cluster_deadline` | `10s` | duration | 同上 | `internal/config/api/api.go:37`、`cmd/handler-api.go:115-119` |
| CORS 允许的源 | env | `MINIO_API_CORS_ALLOW_ORIGIN` | `*` | 逗号分隔 | 浏览器跨域白名单 | `internal/config/api/api.go:59`、`:101-103` |
| CORS 允许的源 | mc-config | `api cors_allow_origin` | `*` | 同上 | 同上 | `internal/config/api/api.go:38` |
| 远程传输超时 | env | `MINIO_API_REMOTE_TRANSPORT_DEADLINE` | `2h` | duration | 联邦/代理 transport 上限 | `internal/config/api/api.go:60`、`:105-107` |
| 远程传输超时 | mc-config | `api remote_transport_deadline` | `2h` | duration | 同上 | `internal/config/api/api.go:39` |
| LIST quorum 策略 | env | `MINIO_API_LIST_QUORUM` | `strict` | `strict`/`optimal`/`reduced`/`disk`/`auto` | LIST 时的 quorum 策略 | `internal/config/api/api.go:62`、`:262-266` |
| LIST quorum 策略 | mc-config | `api list_quorum` | `strict` | 同上 | 同上 | `internal/config/api/api.go:40` |
| 复制优先级 | env | `MINIO_API_REPLICATION_PRIORITY` | `auto` | `slow`/`fast`/`auto` | 决定 worker 数预设档位 | `internal/config/api/api.go:64`、`:269-275` |
| 复制优先级 | mc-config | `api replication_priority` | `auto` | 同上 | `slow`→50/`auto`→100/`fast`→500 worker | `internal/config/api/api.go:41`、`cmd/bucket-replication.go:1905-1915` |
| 复制 worker 上限 | env | `MINIO_API_REPLICATION_MAX_WORKERS` | `500` | 1–500 | priority=fast 时的硬上限 | `internal/config/api/api.go:65`、`:280-282` |
| 复制 worker 上限 | mc-config | `api replication_max_workers` | `500` | 1–500 | 同上 | `internal/config/api/api.go:42`、`:117-119` |
| 大对象复制 worker 上限 | env | `MINIO_API_REPLICATION_MAX_LRG_WORKERS` | `10` | 1–10 | 处理 ≥128MiB 对象的独立池 | `internal/config/api/api.go:66`、`:289-291` |
| 大对象复制 worker 上限 | mc-config | `api replication_max_lrg_workers` | `10` | 1–10 | 同上 | `internal/config/api/api.go:43`、`:121-123` |
| Transition worker 数 | env | `MINIO_API_TRANSITION_WORKERS` | `100` | 整数 | 生命周期 transition 到冷层的 worker 数 | `internal/config/api/api.go:61`、`:295-298` |
| Transition worker 数 | mc-config | `api transition_workers` | `100` | 整数 | 同上 | `internal/config/api/api.go:45`、`:125-127` |
| 过期 multipart 清理周期 | env | `MINIO_API_STALE_UPLOADS_CLEANUP_INTERVAL` | `6h` | duration | 多久触发一次清理扫描 | `internal/config/api/api.go:68`、`:312-315` |
| 过期 multipart 清理周期 | mc-config | `api stale_uploads_cleanup_interval` | `6h` | duration | 同上 | `internal/config/api/api.go:46`、`:129-131` |
| 过期 multipart 阈值 | env | `MINIO_API_STALE_UPLOADS_EXPIRY` | `24h` | duration | 多久未完成的 multipart 视为过期 | `internal/config/api/api.go:69`、`:318-321` |
| 过期 multipart 阈值 | mc-config | `api stale_uploads_expiry` | `24h` | duration | 同上 | `internal/config/api/api.go:47`、`:133-135` |
| 删除清理周期 | env | `MINIO_API_DELETE_CLEANUP_INTERVAL`（兼容旧 `MINIO_DELETE_CLEANUP_INTERVAL`） | `5m` | duration | 永久删除 trash 中文件的周期 | `internal/config/api/api.go:70-71`、`:301-309` |
| 删除清理周期 | mc-config | `api delete_cleanup_interval` | `5m` | duration | 同上 | `internal/config/api/api.go:48`、`:137-139` |
| O_DIRECT 写 | env | `MINIO_API_ODIRECT` | `on` | `on`/`off` | 是否对大对象写启用 O_DIRECT | `internal/config/api/api.go:72`、`:212` |
| O_DIRECT 写 | mc-config | `api odirect` | `on` | `on`/`off` | 同上 | `internal/config/api/api.go:50`、`:146-148` |
| 服务端 gzip | env | `MINIO_API_GZIP_OBJECTS` | `off` | `on`/`off` | 是否对响应做 gzip | `internal/config/api/api.go:74`、`:213` |
| 服务端 gzip | mc-config | `api gzip_objects` | `off` | `on`/`off` | 同上 | `internal/config/api/api.go:51`、`:150-152` |
| Root 凭据访问 | env | `MINIO_API_ROOT_ACCESS` | `on` | `on`/`off` | 是否允许 root 凭据走 S3 接口 | `internal/config/api/api.go:75`、`:214` |
| Root 凭据访问 | mc-config | `api root_access` | `on` | `on`/`off` | 同上 | `internal/config/api/api.go:52`、`:154-156` |
| Bucket 通知同步发送 | env | `MINIO_API_SYNC_EVENTS` | `off` | `on`/`off` | 通知同步发送（吞吐降低，丢失风险降低） | `internal/config/api/api.go:76`、`:324` |
| Bucket 通知同步发送 | mc-config | `api sync_events` | `off` | `on`/`off` | 同上 | `internal/config/api/api.go:53`、`:158-160` |
| 单对象最大版本数 | env | `MINIO_API_OBJECT_MAX_VERSIONS`（兼容旧 `_MINIO_OBJECT_MAX_VERSIONS`） | `MaxInt64`（`9223372036854775807`） | 正整数 | 同 key 累计版本超过即拒 PUT | `internal/config/api/api.go:77-78`、`:326-340` |
| 单对象最大版本数 | mc-config | `api object_max_versions` | `9223372036854775807` | 正整数 | 同上 | `internal/config/api/api.go:54`、`:162-164` |
| Root drive 阈值 | env | `MINIO_ROOTDRIVE_THRESHOLD_SIZE`（旧 `MINIO_ROOTDISK_THRESHOLD_SIZE`） | 未设 | 容量 | 小于此阈值的盘视为系统盘并跳过 | `internal/config/constants.go:67-68`、`cmd/common-main.go:750-752` |
| 服务冻结 | mc-cmd | `mc admin service freeze ALIAS` / `unfreeze ALIAS` | 关 | — | `globalServiceFreeze` 原子位，`maxClients` 中间件入口处阻塞所有请求 | `cmd/admin-handlers.go:480-518`、`cmd/handler-api.go:315` |

### 已废弃（仍可识别但会被忽略）
- `requests_deadline` / `MINIO_API_REQUESTS_DEADLINE`（早期"等待 X 秒拿不到信号量就 503"，现在改为非阻塞 `default` 立即拒）：`internal/config/api/api.go:84`。
- `replication_workers` / `replication_failed_workers` / `expiry_workers`：被 `replication_priority` + `replication_max_workers` 取代：`internal/config/api/api.go:85-86`、`:202-208`。

### 常用 mc 命令示例
```bash
mc admin config get ALIAS api                                     # 查看当前 api 配置（含 env override 标记）
mc admin config set ALIAS api requests_max=8000                   # 调大并发上限（值为集群总数，自动除以节点数）
mc admin config set ALIAS api replication_priority=fast           # 切换复制为 fast（500 worker / 8 MRF）
mc admin service freeze ALIAS                                     # 临时冻结整个服务
mc admin service unfreeze ALIAS                                   # 解除冻结
```

---

## L2 后台任务节流 (Background Task Throttling)

这一层调的是 scanner、healing、delete-cleanup 等后台子系统的"占多少时间片"。Scanner 和 healing 各有独立的配置子系统 (`scanner` / `heal`)。Sleeper 实例的 `factor`/`maxWait` 由 scanner 子系统的预设档位 (`speed`) 一次性配齐；healing 用一组独立的 IO/sleep 上限。

### 配置项汇总

| 控制项 | 类型 | 名称 | 默认值 | 取值范围 | 作用 | 源码引用 |
|---|---|---|---|---|---|---|
| Scanner 速度档位 | env | `MINIO_SCANNER_SPEED` | `default` | `fastest`/`fast`/`default`/`slow`/`slowest` | 一次性设定 `Delay`/`MaxWait`/`Cycle`：`fastest` 0/0/1s、`fast` 1/100ms/1m、`default` 2/1s/1m、`slow` 10/15s/1m、`slowest` 100/15s/30m | `internal/config/scanner/scanner.go:32`、`:158-170` |
| Scanner 速度档位 | mc-config | `scanner speed` | `default` | 同上 | 同上 | `internal/config/scanner/scanner.go:31`、`:78-80` |
| Scanner 空闲行为 | env | `MINIO_SCANNER_IDLE_SPEED` | `on`（继续 sleep） | `on`/`off` | `off` 时绕过 `dynamicSleeper`，scanner 永远全速跑 | `internal/config/scanner/scanner.go:35`、`:139-146` |
| Scanner 空闲行为 | mc-config | `scanner idle_speed` | `""`（按 on 处理） | `on`/`off` | 同上 | `internal/config/scanner/scanner.go:34`、`:82-85` |
| 版本数告警阈值 | env | `MINIO_SCANNER_ALERT_EXCESS_VERSIONS` | `100` | 整数 | 单对象版本超过此数会进 audit 日志 | `internal/config/scanner/scanner.go:38`、`:127-130` |
| 版本数告警阈值 | mc-config | `scanner alert_excess_versions` | `100` | 整数 | 同上 | `internal/config/scanner/scanner.go:37`、`:87-89` |
| 子目录数告警阈值 | env | `MINIO_SCANNER_ALERT_EXCESS_FOLDERS` | `50000` | 整数 | 单 erasure set 单文件夹下子目录超过此数告警 | `internal/config/scanner/scanner.go:41`、`:133-136` |
| 子目录数告警阈值 | mc-config | `scanner alert_excess_folders` | `50000` | 整数 | 同上 | `internal/config/scanner/scanner.go:40`、`:90-92` |
| Bitrot 扫描周期 | env | `MINIO_HEAL_BITROTSCAN` | `off` | `on`（连续）/`off`（关）/`Nm`（N≥1 月） | scanner 抽样时是否做 bitrot 校验 | `internal/config/heal/heal.go:39`、`:159-164` |
| Bitrot 扫描周期 | mc-config | `heal bitrotscan` | `off` | 同上 | 同上 | `internal/config/heal/heal.go:34`、`:104-107` |
| Healing 单对象 sleep 上限 | env | `MINIO_HEAL_MAX_SLEEP` | `250ms` | duration | healSleeper 的 `maxWait` | `internal/config/heal/heal.go:40`、`:166-168` |
| Healing 单对象 sleep 上限 | mc-config | `heal max_sleep` | `250ms` | duration | 同上 | `internal/config/heal/heal.go:35`、`:108-111` |
| Healing 每秒 IO 上限 | env | `MINIO_HEAL_MAX_IO` | `100` | 正整数 | dynamicSleeper 速率换算上限 | `internal/config/heal/heal.go:41`、`:170-172` |
| Healing 每秒 IO 上限 | mc-config | `heal max_io` | `100` | 正整数 | 同上 | `internal/config/heal/heal.go:36`、`:112-115` |
| Healing 单盘 worker 数 | env | `MINIO_HEAL_DRIVE_WORKERS` | 自动（按盘数） | `≥1` 整数 | 每磁盘并发 healer 数，覆盖默认 | `internal/config/heal/heal.go:42`、`:174-185` |
| Healing 单盘 worker 数 | mc-config | `heal drive_workers` | `""`（自动） | `≥1` 整数 | 同上 | `internal/config/heal/heal.go:37`、`:116-119` |
| Healing 全局 worker 数 | env | `_MINIO_HEAL_WORKERS`（内部，下划线前缀） | `GOMAXPROCS/2` | 正整数 | 覆盖 `newHealRoutine` 默认值 | `cmd/background-heal-ops.go:157-165` |
| Healing 全局 worker 数 | (no env override, config-only via 内部 env) | — | `GOMAXPROCS/2` | — | 无 mc-config 入口；非生产推荐 | 同上 |
| MRF healing factor | (硬编码常量) | — | `factor=5, maxWait=1s` | — | mrf.go 的 `healSleeper` | `cmd/mrf.go:213` 附近、实现见 `cmd/data-scanner.go:1365-1468` |
| Trash 清理 sleeper | (硬编码常量) | — | `factor=5, maxWait=25ms` | — | `deleteCleanupSleeper` | `cmd/globals.go:441` |
| 过期 multipart 清理 sleeper | (硬编码常量) | — | `factor=5, maxWait=25ms` | — | `deleteMultipartCleanupSleeper` | `cmd/globals.go:444` |
| 抽样 heal 概率 | (硬编码常量) | `healObjectSelectProb` | `1024`（即 1/1024） | — | scanner 每对象触发"shouldHeal"的概率 | `cmd/data-scanner.go:59` |
| Scanner sleeper 默认实例 | (硬编码) | `scannerSleeper` | `factor=2, maxWait=1s` | — | 启动默认；运行时被 `scanner speed` 覆盖 | `cmd/data-scanner.go:66` |
| 主动 heal | mc-cmd | `mc admin heal ALIAS[/BUCKET[/PREFIX]] --recursive` | — | — | 即时 healing；走 admin `/heal/{bucket}` 路由 | `cmd/admin-router.go`（healHandler 路由） |

### 已废弃（仍可解析但被 `speed` 档位覆盖）
- `MINIO_SCANNER_DELAY` / `MINIO_CRAWLER_DELAY`：`internal/config/scanner/scanner.go:48,50`、`:176-184`。
- `MINIO_SCANNER_MAX_WAIT` / `MINIO_CRAWLER_MAX_WAIT`：`:51-52`、`:185-192`。
- `MINIO_SCANNER_CYCLE`：`:49`、`:193-201`。

### 常用 mc 命令示例
```bash
mc admin config set ALIAS scanner speed=slowest                              # 让 99% 时间给前台
mc admin config set ALIAS scanner speed=fastest idle_speed=off               # 冷数据集群全速扫
mc admin config set ALIAS heal bitrotscan=1m                                 # 每月一次 bitrot
mc admin config set ALIAS heal max_io=50 max_sleep=500ms                     # 抑制 healing IO
mc admin heal ALIAS/mybucket --recursive                                     # 主动修某 bucket
```

---

## L3 跨集群带宽 (Replication Bandwidth)

这层是真正"上限式"的速率限制，基于 `golang.org/x/time/rate.Limiter` 令牌桶，per-`(bucket, ARN)` 一个独立桶。**没有 env / mc-config 直接调它** —— 限速值随 bucket-target 一起配置（`mc admin bucket remote add/edit --bandwidth=...`），或站点复制场景由 `mc admin replicate update --default-bandwidth=...` 下发。

### 配置项汇总

| 控制项 | 类型 | 名称 | 默认值 | 取值范围 | 作用 | 源码引用 |
|---|---|---|---|---|---|---|
| 单 bucket-target 带宽限速 | mc-cmd | `mc admin bucket remote add ALIAS/BUCKET URL --bandwidth=N[MGT]` | 无（不限速） | `≥100MB/s`（最小硬下限） | 设置 `target.BandwidthLimit`，集群总带宽，运行时除以节点数 | `cmd/admin-bucket-handlers.go:236-249`（`BandwidthLimitUpdateType` + 100MB/s 校验）、`cmd/bucket-targets.go:402-413`、`internal/bucket/bandwidth/monitor.go:196-207` |
| 更新已存在 target 的带宽限速 | mc-cmd | `mc admin bucket remote edit ALIAS/BUCKET --arn=ARN --bandwidth=N` | 同上 | 同上 | 同 handler，传 `update=true` | `cmd/admin-bucket-handlers.go:147,186-241`、路由 `cmd/admin-router.go:334-336` |
| 删除 target（同时清掉限速） | mc-cmd | `mc admin bucket remote rm ALIAS/BUCKET --arn=ARN` | — | — | `RemoveTarget` 调用 `updateBandwidthLimit(..., 0)` | `cmd/bucket-targets.go:465`、`cmd/admin-router.go:337-339` |
| Site-replication 默认带宽 | mc-cmd | `mc admin replicate update SITE --default-bandwidth=N` | 不限速 | 整数（`bandwidth.IsSet` 决定是否启用） | 设到 `peer.DefaultBandwidth.Limit`，对每个 target 应用 | `cmd/site-replication.go:999-1019,4042,4171`，handler `cmd/admin-handlers-site-replication.go:407` |
| 监控带宽（不是控制项） | mc-cmd | `mc admin bucket bandwidth ALIAS [BUCKETS...]` | — | — | 读 EWMA 报告 | `cmd/peer-rest-server.go:80,1034-1037`、`cmd/admin-handlers.go:1564,1581` |
| 限速在 reader 上的应用 | (无运维入口) | `bandwidth.NewMonitoredReader` | — | — | replication 读取时挂上令牌桶，`WaitN` 阻塞 | `internal/bucket/bandwidth/reader.go:49-93`、`cmd/bucket-replication.go:1318-1331,1604-1617` |
| 复制队列容量 | (硬编码常量) | worker 池 channel | `100000`（按 worker 数分摊） | — | 上层入站缓冲；溢出转 MRF | `cmd/bucket-replication.go:1855-1864`、`:1929-1932` |
| 大对象阈值 | (硬编码常量) | `minLargeObjSize` | `128 * humanize.MiByte`（128MiB） | — | 超过此大小走独立 large worker 池 | `cmd/bucket-replication.go:2176`、`:2197` |
| 小对象 throttle 超时 | (硬编码常量) | `throttleDeadline` | `1 * time.Hour` | — | 小对象在限速队列等令牌的最长时间 | `cmd/bucket-replication.go:61`、`:1326-1329`、`:1612-1615` |
| 复制 worker 池容量 | (与 L1 联动) | 见 `api replication_priority` / `replication_max_workers` | 50/100/500 | — | 常量 `WorkerMinLimit/AutoDefault/MaxLimit` | `cmd/bucket-replication.go:1873-1888` |
| MRF worker 池容量 | (与 L1 联动) | 由 `api replication_priority` 决定 | 2/4/8 | — | `MRFWorkerMin/Auto/MaxLimit` | `cmd/bucket-replication.go:1882-1888` |
| 大对象 worker 池容量 | (与 L1 联动) | 由 `api replication_max_lrg_workers` 决定 | 10 | — | `LargeWorkerCount` | `cmd/bucket-replication.go:1891` |

### 关键约束
- **带宽下限 100 MB/s**：handler 中硬校验，`<100*1000*1000` 直接拒（`cmd/admin-bucket-handlers.go:246-249`，错误 `ErrReplicationBandwidthLimitError`）。
- **限速值是集群总和**：`SetBandwidthLimit` 内部 `limit / NodeCount`（`internal/bucket/bandwidth/monitor.go:199`）。10 节点集群设 1Gbps 后单节点 100Mbps。
- **per-(bucket, ARN) 独立桶**：同 bucket 配 N 个 ARN 即 N 个独立桶（`monitor.go:43,196-206`）。

### 常用 mc 命令示例
```bash
mc admin bucket remote add ALIAS/mybucket https://target/bucket --service replication --bandwidth 500MB
mc admin bucket remote edit ALIAS/mybucket --arn arn:minio:replication::xxx:bucket --bandwidth 1G
mc admin bucket remote rm ALIAS/mybucket --arn arn:minio:replication::xxx:bucket
mc admin replicate update SITE --default-bandwidth 2G
mc admin bucket bandwidth ALIAS mybucket
```

---

## L4 容量配额 (Bucket Quota)

最简单的一层 —— 仅一个旋钮：**桶级硬配额**。MinIO 已经移除了 fifo（软）配额，遇到旧配置直接拒绝并提示用 ILM 替代 (`cmd/bucket-quota.go:94-96`)。配额检查依赖 scanner 写入的 `data-usage.bin`，最差有 `10s + scanner_cycle` 的滞后。

### 配置项汇总

| 控制项 | 类型 | 名称 | 默认值 | 取值范围 | 作用 | 源码引用 |
|---|---|---|---|---|---|---|
| 桶硬配额 | mc-cmd | `mc admin bucket quota ALIAS/BUCKET --hard SIZE` | 无（不限） | 字节大小（如 `100GB`、`1TB`） | 写入路径在 `enforceQuotaHard` 拦截：单文件超额或 `用量+大小≥配额` 时返回 `BucketQuotaExceeded` | `cmd/admin-bucket-handlers.go:52-105`、路由 `cmd/admin-router.go:326-328`、`cmd/bucket-quota.go:103-133` |
| 查询桶配额 | mc-cmd | `mc admin bucket quota ALIAS/BUCKET` | — | — | 读取持久化的 `quota.json` | `cmd/admin-bucket-handlers.go:108-139`、`cmd/admin-router.go:323-325` |
| 清除桶配额 | mc-cmd | `mc admin bucket quota ALIAS/BUCKET --clear` | — | — | 同 PUT 接口，传空配置（`Size==0 && Quota==0`） | `cmd/admin-bucket-handlers.go:97-105` |
| 配额缓存 TTL | (硬编码常量) | `bucketStorageCache` TTL | `10 * time.Second` | — | 配额检查的用量数据缓存窗口；`ReturnLastGood` 容错 | `cmd/bucket-quota.go:46-62` |
| 配额检查触发点 | (硬编码) | `enforceBucketQuotaHard` | — | — | 仅在 PUT/CompleteMultipartUpload 等写入路径调用，GET/HEAD/LIST 不触发 | `cmd/bucket-quota.go:135-140` |
| 已废弃软配额 | (拒绝接受) | quota type `fifo` | — | — | 解析时直接报错并提示 `mc quota clear` + `mc ilm add` | `cmd/bucket-quota.go:94-96` |

### 常用 mc 命令示例
```bash
mc admin bucket quota ALIAS/mybucket --hard 100GB        # 设 100 GB 硬配额
mc admin bucket quota ALIAS/mybucket                     # 查询当前配额
mc admin bucket quota ALIAS/mybucket --clear             # 清除（恢复无限）
```

### 注意事项
- **永远预留 5-10% buffer**：因为 `10s + scanner_cycle`（`scanner speed=default` 时 1 分钟，`slowest` 时 30 分钟）的滞后窗口，配额过紧会出现"用量已经回落但仍被拒"或"瞬时超额"。
- **scanner 挂时配额停止生效**：`cmd/bucket-quota.go:72-75` 会打 once-warning `unable to retrieve usage information for bucket: ..., quota will not be enforced`。

---

## 综合调优场景

### 场景 A：高吞吐 PUT 工作负载（NVMe + 万兆网，bulk-load）
目标：所有资源给前台 PUT，scanner/heal 让到最低；配额是唯一硬墙。
```bash
mc admin config set ALIAS api requests_max=8000
mc admin config set ALIAS scanner speed=slowest
mc admin config set ALIAS heal max_io=10 max_sleep=1s
mc admin config set ALIAS heal bitrotscan=off
mc admin config set ALIAS api replication_priority=fast
mc admin bucket quota ALIAS/data --hard 9TB
```
关键文件：`internal/config/api/api.go:36`、`internal/config/scanner/scanner.go:158`、`internal/config/heal/heal.go:40-41`。

### 场景 B：Healing 优先集群（刚扩容、有降级对象需要修复）
目标：尽快修完降级对象，前台流量短期降级可接受。
```bash
mc admin config set ALIAS scanner speed=fast
mc admin config set ALIAS heal drive_workers=8 max_io=500 max_sleep=10ms
mc admin heal ALIAS --recursive
mc admin config set ALIAS api requests_max=200
```
关键文件：`cmd/data-scanner.go:59`（healObjectSelectProb=1024）、`internal/config/heal/heal.go:42`（drive_workers）。

### 场景 C：带宽受限的 DR 异地复制
目标：不让 replication 占满有限的跨地域专线，前台读写带宽优先。
```bash
mc admin bucket remote edit ALIAS/critical --arn arn:minio:replication::abc:dr-bucket --bandwidth 200MB
mc admin config set ALIAS api replication_priority=slow
mc admin bucket bandwidth ALIAS critical
```
关键文件：`cmd/admin-bucket-handlers.go:236-249`、`internal/bucket/bandwidth/monitor.go:196-207`、`cmd/bucket-replication.go:1873-1915`。

### 场景 D：多租户共享集群（无 noisy neighbor 网关）
MinIO 不内置租户级限流，通用做法：
```bash
mc admin config set ALIAS api requests_max=400
mc admin bucket quota ALIAS/tenant-a --hard 5TB
mc admin bucket quota ALIAS/tenant-b --hard 5TB
mc admin config set ALIAS api cluster_deadline=5s
```
注：真正的 per-tenant rate limit 必须在前置网关（nginx `limit_req` / Envoy 限流过滤器）实现 —— MinIO `maxClients` 是节点级总池，无租户维度。

---

## 附录：env 与 mc-config 的关系（"哪个赢"）

源码统一模式（以 `requests_max` 为例，`internal/config/api/api.go:239`）：
```go
requestsMax, err := strconv.Atoi(env.Get(EnvAPIRequestsMax, kvs.GetWithDefault(apiRequestsMax, DefaultKVS)))
```

`env.Get(EnvX, fallback)` 的语义是：**env 设了就用 env，否则用 fallback**。优先级：
1. 进程启动时的环境变量（最高）
2. `mc admin config set` 持久化的值
3. `DefaultKVS` 内置默认值（最低）

含义：
- env 设置的值会**屏蔽**任何后续 `mc admin config set`。`mc admin config get ALIAS api` 会显示 `(env)` 标记。
- 容器化部署（K8s）建议**不在 env 里设动态参数**，全部通过 `mc admin config set` 管理，避免重启容器才能改值。
- "必须用 env"的场景：root 凭据、TLS 证书路径、`MINIO_ROOTDRIVE_THRESHOLD_SIZE` 等启动期生效的 bootstrap 参数。
