# MinIO 限流操作引數手冊

> **配套文件**：`06-module-rate-control.md`（機制原理）。本手冊按 4 層結構列舉每層的可調引數 —— 環境變數、`mc admin config` 配置鍵、`mc admin` 命令 —— 每條都給出原始碼引用 (`path:line`)。
> **程式碼版本**：`/home/vscode/repo-analyses/minio-20260503/repo`（約 2026-05 截面）。
>
> **使用約定**：
> - "型別" 列：`env` = 程序啟動環境變數；`mc-config` = 透過 `mc admin config set` 持久化到叢集配置；`mc-cmd` = 專用 `mc admin` 子命令。
> - 大多數 API/Scanner/Heal 類引數同時支援 env 與 mc-config 兩種入口；env 在 `LookupConfig` 裡作為 override（典型模式：`env.Get(EnvX, kvs.GetWithDefault(X, DefaultKVS))`，見 `internal/config/api/api.go:239`）。
> - "(no env override, config-only)" 表示只能透過 `mc admin config set` 設定，沒有對應 env。

---

## L1 入站 API 限流 (Incoming Request Limit)

操作員在此層主要回答兩個問題：(1) 單節點最多接多少併發 S3 請求？(2) 複製/transition/cleanup 等"前臺觸發的後臺動作"分多少 worker？這層全部歸在 `api` 子系統下，熱更新友好（改完即生效，正在執行的請求按舊配置完成）。

### 配置項彙總

| 控制項 | 型別 | 名稱 | 預設值 | 取值範圍 | 作用 | 原始碼引用 |
|---|---|---|---|---|---|---|
| 最大併發請求數（每節點） | env | `MINIO_API_REQUESTS_MAX` | `0`（自動按 RAM 算） | `≥0` 整數 | 顯式設值時是**叢集總數**，執行時再除以節點數 | `internal/config/api/api.go:56`、`cmd/handler-api.go:127-150` |
| 最大併發請求數 | mc-config | `api requests_max` | `0` | 同上 | 同上 | `internal/config/api/api.go:36`、`:93-95` |
| 叢集健康超時 | env | `MINIO_API_CLUSTER_DEADLINE` | `10s` | duration | 節點間健康檢查/分散式呼叫最長等待 | `internal/config/api/api.go:58`、`:97-99` |
| 叢集健康超時 | mc-config | `api cluster_deadline` | `10s` | duration | 同上 | `internal/config/api/api.go:37`、`cmd/handler-api.go:115-119` |
| CORS 允許的源 | env | `MINIO_API_CORS_ALLOW_ORIGIN` | `*` | 逗號分隔 | 瀏覽器跨域白名單 | `internal/config/api/api.go:59`、`:101-103` |
| CORS 允許的源 | mc-config | `api cors_allow_origin` | `*` | 同上 | 同上 | `internal/config/api/api.go:38` |
| 遠端傳輸超時 | env | `MINIO_API_REMOTE_TRANSPORT_DEADLINE` | `2h` | duration | 聯邦/代理 transport 上限 | `internal/config/api/api.go:60`、`:105-107` |
| 遠端傳輸超時 | mc-config | `api remote_transport_deadline` | `2h` | duration | 同上 | `internal/config/api/api.go:39` |
| LIST quorum 策略 | env | `MINIO_API_LIST_QUORUM` | `strict` | `strict`/`optimal`/`reduced`/`disk`/`auto` | LIST 時的 quorum 策略 | `internal/config/api/api.go:62`、`:262-266` |
| LIST quorum 策略 | mc-config | `api list_quorum` | `strict` | 同上 | 同上 | `internal/config/api/api.go:40` |
| 複製優先順序 | env | `MINIO_API_REPLICATION_PRIORITY` | `auto` | `slow`/`fast`/`auto` | 決定 worker 數預設檔位 | `internal/config/api/api.go:64`、`:269-275` |
| 複製優先順序 | mc-config | `api replication_priority` | `auto` | 同上 | `slow`→50/`auto`→100/`fast`→500 worker | `internal/config/api/api.go:41`、`cmd/bucket-replication.go:1905-1915` |
| 複製 worker 上限 | env | `MINIO_API_REPLICATION_MAX_WORKERS` | `500` | 1–500 | priority=fast 時的硬上限 | `internal/config/api/api.go:65`、`:280-282` |
| 複製 worker 上限 | mc-config | `api replication_max_workers` | `500` | 1–500 | 同上 | `internal/config/api/api.go:42`、`:117-119` |
| 大物件複製 worker 上限 | env | `MINIO_API_REPLICATION_MAX_LRG_WORKERS` | `10` | 1–10 | 處理 ≥128MiB 物件的獨立池 | `internal/config/api/api.go:66`、`:289-291` |
| 大物件複製 worker 上限 | mc-config | `api replication_max_lrg_workers` | `10` | 1–10 | 同上 | `internal/config/api/api.go:43`、`:121-123` |
| Transition worker 數 | env | `MINIO_API_TRANSITION_WORKERS` | `100` | 整數 | 生命週期 transition 到冷層的 worker 數 | `internal/config/api/api.go:61`、`:295-298` |
| Transition worker 數 | mc-config | `api transition_workers` | `100` | 整數 | 同上 | `internal/config/api/api.go:45`、`:125-127` |
| 過期 multipart 清理週期 | env | `MINIO_API_STALE_UPLOADS_CLEANUP_INTERVAL` | `6h` | duration | 多久觸發一次清理掃描 | `internal/config/api/api.go:68`、`:312-315` |
| 過期 multipart 清理週期 | mc-config | `api stale_uploads_cleanup_interval` | `6h` | duration | 同上 | `internal/config/api/api.go:46`、`:129-131` |
| 過期 multipart 閾值 | env | `MINIO_API_STALE_UPLOADS_EXPIRY` | `24h` | duration | 多久未完成的 multipart 視為過期 | `internal/config/api/api.go:69`、`:318-321` |
| 過期 multipart 閾值 | mc-config | `api stale_uploads_expiry` | `24h` | duration | 同上 | `internal/config/api/api.go:47`、`:133-135` |
| 刪除清理週期 | env | `MINIO_API_DELETE_CLEANUP_INTERVAL`（相容舊 `MINIO_DELETE_CLEANUP_INTERVAL`） | `5m` | duration | 永久刪除 trash 中檔案的週期 | `internal/config/api/api.go:70-71`、`:301-309` |
| 刪除清理週期 | mc-config | `api delete_cleanup_interval` | `5m` | duration | 同上 | `internal/config/api/api.go:48`、`:137-139` |
| O_DIRECT 寫 | env | `MINIO_API_ODIRECT` | `on` | `on`/`off` | 是否對大物件寫啟用 O_DIRECT | `internal/config/api/api.go:72`、`:212` |
| O_DIRECT 寫 | mc-config | `api odirect` | `on` | `on`/`off` | 同上 | `internal/config/api/api.go:50`、`:146-148` |
| 服務端 gzip | env | `MINIO_API_GZIP_OBJECTS` | `off` | `on`/`off` | 是否對響應做 gzip | `internal/config/api/api.go:74`、`:213` |
| 服務端 gzip | mc-config | `api gzip_objects` | `off` | `on`/`off` | 同上 | `internal/config/api/api.go:51`、`:150-152` |
| Root 憑據訪問 | env | `MINIO_API_ROOT_ACCESS` | `on` | `on`/`off` | 是否允許 root 憑據走 S3 介面 | `internal/config/api/api.go:75`、`:214` |
| Root 憑據訪問 | mc-config | `api root_access` | `on` | `on`/`off` | 同上 | `internal/config/api/api.go:52`、`:154-156` |
| Bucket 通知同步傳送 | env | `MINIO_API_SYNC_EVENTS` | `off` | `on`/`off` | 通知同步傳送（吞吐降低，丟失風險降低） | `internal/config/api/api.go:76`、`:324` |
| Bucket 通知同步傳送 | mc-config | `api sync_events` | `off` | `on`/`off` | 同上 | `internal/config/api/api.go:53`、`:158-160` |
| 單物件最大版本數 | env | `MINIO_API_OBJECT_MAX_VERSIONS`（相容舊 `_MINIO_OBJECT_MAX_VERSIONS`） | `MaxInt64`（`9223372036854775807`） | 正整數 | 同 key 累計版本超過即拒 PUT | `internal/config/api/api.go:77-78`、`:326-340` |
| 單物件最大版本數 | mc-config | `api object_max_versions` | `9223372036854775807` | 正整數 | 同上 | `internal/config/api/api.go:54`、`:162-164` |
| Root drive 閾值 | env | `MINIO_ROOTDRIVE_THRESHOLD_SIZE`（舊 `MINIO_ROOTDISK_THRESHOLD_SIZE`） | 未設 | 容量 | 小於此閾值的盤視為系統盤並跳過 | `internal/config/constants.go:67-68`、`cmd/common-main.go:750-752` |
| 服務凍結 | mc-cmd | `mc admin service freeze ALIAS` / `unfreeze ALIAS` | 關 | — | `globalServiceFreeze` 原子位，`maxClients` 中介軟體入口處阻塞所有請求 | `cmd/admin-handlers.go:480-518`、`cmd/handler-api.go:315` |

### 已廢棄（仍可識別但會被忽略）
- `requests_deadline` / `MINIO_API_REQUESTS_DEADLINE`（早期"等待 X 秒拿不到訊號量就 503"，現在改為非阻塞 `default` 立即拒）：`internal/config/api/api.go:84`。
- `replication_workers` / `replication_failed_workers` / `expiry_workers`：被 `replication_priority` + `replication_max_workers` 取代：`internal/config/api/api.go:85-86`、`:202-208`。

### 常用 mc 命令示例
```bash
mc admin config get ALIAS api                                     # 檢視當前 api 配置（含 env override 標記）
mc admin config set ALIAS api requests_max=8000                   # 調大併發上限（值為叢集總數，自動除以節點數）
mc admin config set ALIAS api replication_priority=fast           # 切換複製為 fast（500 worker / 8 MRF）
mc admin service freeze ALIAS                                     # 臨時凍結整個服務
mc admin service unfreeze ALIAS                                   # 解除凍結
```

---

## L2 後臺任務節流 (Background Task Throttling)

這一層調的是 scanner、healing、delete-cleanup 等後臺子系統的"佔多少時間片"。Scanner 和 healing 各有獨立的配置子系統 (`scanner` / `heal`)。Sleeper 例項的 `factor`/`maxWait` 由 scanner 子系統的預設檔位 (`speed`) 一次性配齊；healing 用一組獨立的 IO/sleep 上限。

### 配置項彙總

| 控制項 | 型別 | 名稱 | 預設值 | 取值範圍 | 作用 | 原始碼引用 |
|---|---|---|---|---|---|---|
| Scanner 速度檔位 | env | `MINIO_SCANNER_SPEED` | `default` | `fastest`/`fast`/`default`/`slow`/`slowest` | 一次性設定 `Delay`/`MaxWait`/`Cycle`：`fastest` 0/0/1s、`fast` 1/100ms/1m、`default` 2/1s/1m、`slow` 10/15s/1m、`slowest` 100/15s/30m | `internal/config/scanner/scanner.go:32`、`:158-170` |
| Scanner 速度檔位 | mc-config | `scanner speed` | `default` | 同上 | 同上 | `internal/config/scanner/scanner.go:31`、`:78-80` |
| Scanner 空閒行為 | env | `MINIO_SCANNER_IDLE_SPEED` | `on`（繼續 sleep） | `on`/`off` | `off` 時繞過 `dynamicSleeper`，scanner 永遠全速跑 | `internal/config/scanner/scanner.go:35`、`:139-146` |
| Scanner 空閒行為 | mc-config | `scanner idle_speed` | `""`（按 on 處理） | `on`/`off` | 同上 | `internal/config/scanner/scanner.go:34`、`:82-85` |
| 版本數告警閾值 | env | `MINIO_SCANNER_ALERT_EXCESS_VERSIONS` | `100` | 整數 | 單物件版本超過此數會進 audit 日誌 | `internal/config/scanner/scanner.go:38`、`:127-130` |
| 版本數告警閾值 | mc-config | `scanner alert_excess_versions` | `100` | 整數 | 同上 | `internal/config/scanner/scanner.go:37`、`:87-89` |
| 子目錄數告警閾值 | env | `MINIO_SCANNER_ALERT_EXCESS_FOLDERS` | `50000` | 整數 | 單 erasure set 單資料夾下子目錄超過此數告警 | `internal/config/scanner/scanner.go:41`、`:133-136` |
| 子目錄數告警閾值 | mc-config | `scanner alert_excess_folders` | `50000` | 整數 | 同上 | `internal/config/scanner/scanner.go:40`、`:90-92` |
| Bitrot 掃描週期 | env | `MINIO_HEAL_BITROTSCAN` | `off` | `on`（連續）/`off`（關）/`Nm`（N≥1 月） | scanner 抽樣時是否做 bitrot 校驗 | `internal/config/heal/heal.go:39`、`:159-164` |
| Bitrot 掃描週期 | mc-config | `heal bitrotscan` | `off` | 同上 | 同上 | `internal/config/heal/heal.go:34`、`:104-107` |
| Healing 單物件 sleep 上限 | env | `MINIO_HEAL_MAX_SLEEP` | `250ms` | duration | healSleeper 的 `maxWait` | `internal/config/heal/heal.go:40`、`:166-168` |
| Healing 單物件 sleep 上限 | mc-config | `heal max_sleep` | `250ms` | duration | 同上 | `internal/config/heal/heal.go:35`、`:108-111` |
| Healing 每秒 IO 上限 | env | `MINIO_HEAL_MAX_IO` | `100` | 正整數 | dynamicSleeper 速率換算上限 | `internal/config/heal/heal.go:41`、`:170-172` |
| Healing 每秒 IO 上限 | mc-config | `heal max_io` | `100` | 正整數 | 同上 | `internal/config/heal/heal.go:36`、`:112-115` |
| Healing 單盤 worker 數 | env | `MINIO_HEAL_DRIVE_WORKERS` | 自動（按盤數） | `≥1` 整數 | 每磁碟併發 healer 數，覆蓋預設 | `internal/config/heal/heal.go:42`、`:174-185` |
| Healing 單盤 worker 數 | mc-config | `heal drive_workers` | `""`（自動） | `≥1` 整數 | 同上 | `internal/config/heal/heal.go:37`、`:116-119` |
| Healing 全域性 worker 數 | env | `_MINIO_HEAL_WORKERS`（內部，下劃線字首） | `GOMAXPROCS/2` | 正整數 | 覆蓋 `newHealRoutine` 預設值 | `cmd/background-heal-ops.go:157-165` |
| Healing 全域性 worker 數 | (no env override, config-only via 內部 env) | — | `GOMAXPROCS/2` | — | 無 mc-config 入口；非生產推薦 | 同上 |
| MRF healing factor | (硬編碼常量) | — | `factor=5, maxWait=1s` | — | mrf.go 的 `healSleeper` | `cmd/mrf.go:213` 附近、實現見 `cmd/data-scanner.go:1365-1468` |
| Trash 清理 sleeper | (硬編碼常量) | — | `factor=5, maxWait=25ms` | — | `deleteCleanupSleeper` | `cmd/globals.go:441` |
| 過期 multipart 清理 sleeper | (硬編碼常量) | — | `factor=5, maxWait=25ms` | — | `deleteMultipartCleanupSleeper` | `cmd/globals.go:444` |
| 抽樣 heal 機率 | (硬編碼常量) | `healObjectSelectProb` | `1024`（即 1/1024） | — | scanner 每物件觸發"shouldHeal"的機率 | `cmd/data-scanner.go:59` |
| Scanner sleeper 預設例項 | (硬編碼) | `scannerSleeper` | `factor=2, maxWait=1s` | — | 啟動預設；執行時被 `scanner speed` 覆蓋 | `cmd/data-scanner.go:66` |
| 主動 heal | mc-cmd | `mc admin heal ALIAS[/BUCKET[/PREFIX]] --recursive` | — | — | 即時 healing；走 admin `/heal/{bucket}` 路由 | `cmd/admin-router.go`（healHandler 路由） |

### 已廢棄（仍可解析但被 `speed` 檔位覆蓋）
- `MINIO_SCANNER_DELAY` / `MINIO_CRAWLER_DELAY`：`internal/config/scanner/scanner.go:48,50`、`:176-184`。
- `MINIO_SCANNER_MAX_WAIT` / `MINIO_CRAWLER_MAX_WAIT`：`:51-52`、`:185-192`。
- `MINIO_SCANNER_CYCLE`：`:49`、`:193-201`。

### 常用 mc 命令示例
```bash
mc admin config set ALIAS scanner speed=slowest                              # 讓 99% 時間給前臺
mc admin config set ALIAS scanner speed=fastest idle_speed=off               # 冷資料叢集全速掃
mc admin config set ALIAS heal bitrotscan=1m                                 # 每月一次 bitrot
mc admin config set ALIAS heal max_io=50 max_sleep=500ms                     # 抑制 healing IO
mc admin heal ALIAS/mybucket --recursive                                     # 主動修某 bucket
```

---

## L3 跨叢集頻寬 (Replication Bandwidth)

這層是真正"上限式"的速率限制，基於 `golang.org/x/time/rate.Limiter` 令牌桶，per-`(bucket, ARN)` 一個獨立桶。**沒有 env / mc-config 直接調它** —— 限速值隨 bucket-target 一起配置（`mc admin bucket remote add/edit --bandwidth=...`），或站點複製場景由 `mc admin replicate update --default-bandwidth=...` 下發。

### 配置項彙總

| 控制項 | 型別 | 名稱 | 預設值 | 取值範圍 | 作用 | 原始碼引用 |
|---|---|---|---|---|---|---|
| 單 bucket-target 頻寬限速 | mc-cmd | `mc admin bucket remote add ALIAS/BUCKET URL --bandwidth=N[MGT]` | 無（不限速） | `≥100MB/s`（最小硬下限） | 設定 `target.BandwidthLimit`，叢集總頻寬，執行時除以節點數 | `cmd/admin-bucket-handlers.go:236-249`（`BandwidthLimitUpdateType` + 100MB/s 校驗）、`cmd/bucket-targets.go:402-413`、`internal/bucket/bandwidth/monitor.go:196-207` |
| 更新已存在 target 的頻寬限速 | mc-cmd | `mc admin bucket remote edit ALIAS/BUCKET --arn=ARN --bandwidth=N` | 同上 | 同上 | 同 handler，傳 `update=true` | `cmd/admin-bucket-handlers.go:147,186-241`、路由 `cmd/admin-router.go:334-336` |
| 刪除 target（同時清掉限速） | mc-cmd | `mc admin bucket remote rm ALIAS/BUCKET --arn=ARN` | — | — | `RemoveTarget` 呼叫 `updateBandwidthLimit(..., 0)` | `cmd/bucket-targets.go:465`、`cmd/admin-router.go:337-339` |
| Site-replication 預設頻寬 | mc-cmd | `mc admin replicate update SITE --default-bandwidth=N` | 不限速 | 整數（`bandwidth.IsSet` 決定是否啟用） | 設到 `peer.DefaultBandwidth.Limit`，對每個 target 應用 | `cmd/site-replication.go:999-1019,4042,4171`，handler `cmd/admin-handlers-site-replication.go:407` |
| 監控頻寬（不是控制項） | mc-cmd | `mc admin bucket bandwidth ALIAS [BUCKETS...]` | — | — | 讀 EWMA 報告 | `cmd/peer-rest-server.go:80,1034-1037`、`cmd/admin-handlers.go:1564,1581` |
| 限速在 reader 上的應用 | (無運維入口) | `bandwidth.NewMonitoredReader` | — | — | replication 讀取時掛上令牌桶，`WaitN` 阻塞 | `internal/bucket/bandwidth/reader.go:49-93`、`cmd/bucket-replication.go:1318-1331,1604-1617` |
| 複製佇列容量 | (硬編碼常量) | worker 池 channel | `100000`（按 worker 數分攤） | — | 上層入站緩衝；溢位轉 MRF | `cmd/bucket-replication.go:1855-1864`、`:1929-1932` |
| 大物件閾值 | (硬編碼常量) | `minLargeObjSize` | `128 * humanize.MiByte`（128MiB） | — | 超過此大小走獨立 large worker 池 | `cmd/bucket-replication.go:2176`、`:2197` |
| 小物件 throttle 超時 | (硬編碼常量) | `throttleDeadline` | `1 * time.Hour` | — | 小物件在限速佇列等令牌的最長時間 | `cmd/bucket-replication.go:61`、`:1326-1329`、`:1612-1615` |
| 複製 worker 池容量 | (與 L1 聯動) | 見 `api replication_priority` / `replication_max_workers` | 50/100/500 | — | 常量 `WorkerMinLimit/AutoDefault/MaxLimit` | `cmd/bucket-replication.go:1873-1888` |
| MRF worker 池容量 | (與 L1 聯動) | 由 `api replication_priority` 決定 | 2/4/8 | — | `MRFWorkerMin/Auto/MaxLimit` | `cmd/bucket-replication.go:1882-1888` |
| 大物件 worker 池容量 | (與 L1 聯動) | 由 `api replication_max_lrg_workers` 決定 | 10 | — | `LargeWorkerCount` | `cmd/bucket-replication.go:1891` |

### 關鍵約束
- **頻寬下限 100 MB/s**：handler 中硬校驗，`<100*1000*1000` 直接拒（`cmd/admin-bucket-handlers.go:246-249`，錯誤 `ErrReplicationBandwidthLimitError`）。
- **限速值是叢集總和**：`SetBandwidthLimit` 內部 `limit / NodeCount`（`internal/bucket/bandwidth/monitor.go:199`）。10 節點叢集設 1Gbps 後單節點 100Mbps。
- **per-(bucket, ARN) 獨立桶**：同 bucket 配 N 個 ARN 即 N 個獨立桶（`monitor.go:43,196-206`）。

### 常用 mc 命令示例
```bash
mc admin bucket remote add ALIAS/mybucket https://target/bucket --service replication --bandwidth 500MB
mc admin bucket remote edit ALIAS/mybucket --arn arn:minio:replication::xxx:bucket --bandwidth 1G
mc admin bucket remote rm ALIAS/mybucket --arn arn:minio:replication::xxx:bucket
mc admin replicate update SITE --default-bandwidth 2G
mc admin bucket bandwidth ALIAS mybucket
```

---

## L4 容量配額 (Bucket Quota)

最簡單的一層 —— 僅一個旋鈕：**桶級硬配額**。MinIO 已經移除了 fifo（軟）配額，遇到舊配置直接拒絕並提示用 ILM 替代 (`cmd/bucket-quota.go:94-96`)。配額檢查依賴 scanner 寫入的 `data-usage.bin`，最差有 `10s + scanner_cycle` 的滯後。

### 配置項彙總

| 控制項 | 型別 | 名稱 | 預設值 | 取值範圍 | 作用 | 原始碼引用 |
|---|---|---|---|---|---|---|
| 桶硬配額 | mc-cmd | `mc admin bucket quota ALIAS/BUCKET --hard SIZE` | 無（不限） | 位元組大小（如 `100GB`、`1TB`） | 寫入路徑在 `enforceQuotaHard` 攔截：單檔案超額或 `用量+大小≥配額` 時返回 `BucketQuotaExceeded` | `cmd/admin-bucket-handlers.go:52-105`、路由 `cmd/admin-router.go:326-328`、`cmd/bucket-quota.go:103-133` |
| 查詢桶配額 | mc-cmd | `mc admin bucket quota ALIAS/BUCKET` | — | — | 讀取持久化的 `quota.json` | `cmd/admin-bucket-handlers.go:108-139`、`cmd/admin-router.go:323-325` |
| 清除桶配額 | mc-cmd | `mc admin bucket quota ALIAS/BUCKET --clear` | — | — | 同 PUT 介面，傳空配置（`Size==0 && Quota==0`） | `cmd/admin-bucket-handlers.go:97-105` |
| 配額快取 TTL | (硬編碼常量) | `bucketStorageCache` TTL | `10 * time.Second` | — | 配額檢查的用量資料快取視窗；`ReturnLastGood` 容錯 | `cmd/bucket-quota.go:46-62` |
| 配額檢查觸發點 | (硬編碼) | `enforceBucketQuotaHard` | — | — | 僅在 PUT/CompleteMultipartUpload 等寫入路徑呼叫，GET/HEAD/LIST 不觸發 | `cmd/bucket-quota.go:135-140` |
| 已廢棄軟配額 | (拒絕接受) | quota type `fifo` | — | — | 解析時直接報錯並提示 `mc quota clear` + `mc ilm add` | `cmd/bucket-quota.go:94-96` |

### 常用 mc 命令示例
```bash
mc admin bucket quota ALIAS/mybucket --hard 100GB        # 設 100 GB 硬配額
mc admin bucket quota ALIAS/mybucket                     # 查詢當前配額
mc admin bucket quota ALIAS/mybucket --clear             # 清除（恢復無限）
```

### 注意事項
- **永遠預留 5-10% buffer**：因為 `10s + scanner_cycle`（`scanner speed=default` 時 1 分鐘，`slowest` 時 30 分鐘）的滯後視窗，配額過緊會出現"用量已經回落但仍被拒"或"瞬時超額"。
- **scanner 掛時配額停止生效**：`cmd/bucket-quota.go:72-75` 會打 once-warning `unable to retrieve usage information for bucket: ..., quota will not be enforced`。

---

## 綜合調優場景

### 場景 A：高吞吐 PUT 工作負載（NVMe + 萬兆網，bulk-load）
目標：所有資源給前臺 PUT，scanner/heal 讓到最低；配額是唯一硬牆。
```bash
mc admin config set ALIAS api requests_max=8000
mc admin config set ALIAS scanner speed=slowest
mc admin config set ALIAS heal max_io=10 max_sleep=1s
mc admin config set ALIAS heal bitrotscan=off
mc admin config set ALIAS api replication_priority=fast
mc admin bucket quota ALIAS/data --hard 9TB
```
關鍵檔案：`internal/config/api/api.go:36`、`internal/config/scanner/scanner.go:158`、`internal/config/heal/heal.go:40-41`。

### 場景 B：Healing 優先叢集（剛擴容、有降級物件需要修復）
目標：儘快修完降級物件，前臺流量短期降級可接受。
```bash
mc admin config set ALIAS scanner speed=fast
mc admin config set ALIAS heal drive_workers=8 max_io=500 max_sleep=10ms
mc admin heal ALIAS --recursive
mc admin config set ALIAS api requests_max=200
```
關鍵檔案：`cmd/data-scanner.go:59`（healObjectSelectProb=1024）、`internal/config/heal/heal.go:42`（drive_workers）。

### 場景 C：頻寬受限的 DR 異地複製
目標：不讓 replication 佔滿有限的跨地域專線，前臺讀寫頻寬優先。
```bash
mc admin bucket remote edit ALIAS/critical --arn arn:minio:replication::abc:dr-bucket --bandwidth 200MB
mc admin config set ALIAS api replication_priority=slow
mc admin bucket bandwidth ALIAS critical
```
關鍵檔案：`cmd/admin-bucket-handlers.go:236-249`、`internal/bucket/bandwidth/monitor.go:196-207`、`cmd/bucket-replication.go:1873-1915`。

### 場景 D：多租戶共享叢集（無 noisy neighbor 閘道器）
MinIO 不內建租戶級限流，通用做法：
```bash
mc admin config set ALIAS api requests_max=400
mc admin bucket quota ALIAS/tenant-a --hard 5TB
mc admin bucket quota ALIAS/tenant-b --hard 5TB
mc admin config set ALIAS api cluster_deadline=5s
```
注：真正的 per-tenant rate limit 必須在前置閘道器（nginx `limit_req` / Envoy 限流過濾器）實現 —— MinIO `maxClients` 是節點級總池，無租戶維度。

---

## 附錄：env 與 mc-config 的關係（"哪個贏"）

原始碼統一模式（以 `requests_max` 為例，`internal/config/api/api.go:239`）：
```go
requestsMax, err := strconv.Atoi(env.Get(EnvAPIRequestsMax, kvs.GetWithDefault(apiRequestsMax, DefaultKVS)))
```

`env.Get(EnvX, fallback)` 的語義是：**env 設了就用 env，否則用 fallback**。優先順序：
1. 程序啟動時的環境變數（最高）
2. `mc admin config set` 持久化的值
3. `DefaultKVS` 內建預設值（最低）

含義：
- env 設定的值會**遮蔽**任何後續 `mc admin config set`。`mc admin config get ALIAS api` 會顯示 `(env)` 標記。
- 容器化部署（K8s）建議**不在 env 裡設動態引數**，全部透過 `mc admin config set` 管理，避免重啟容器才能改值。
- "必須用 env"的場景：root 憑據、TLS 證書路徑、`MINIO_ROOTDRIVE_THRESHOLD_SIZE` 等啟動期生效的 bootstrap 引數。
