# MinIO 在慢盤 (HDD) 上的效能分析

> **目標**：量化 MinIO 後臺子系統（scanner / ILM / healing / bitrot / replication）在 7200-RPM SATA HDD 上的實際表現，找出瓶頸、給出調參建議、提出程式碼改進。
> **範圍**：僅限"程式碼與配置已經決定的效能特徵"，不討論硬體選型。
> **程式碼版本**：`/home/vscode/repo-analyses/minio-20260503/repo`，2026-05-03 截面。

---

## 0. 基線假設

### 硬體參考點

| 項 | 7200-RPM SATA HDD | NVMe (參考) | 倍數差距 |
|---|---|---|---|
| 順序讀吞吐 | ~150 MB/s | ~3 GB/s | 20× |
| 順序寫吞吐 | ~120 MB/s | ~2 GB/s | 16× |
| 4 KiB 隨機讀 IOPS | 80–150 | 400 K+ | **3000–5000×** |
| 4 KiB 隨機寫 IOPS | 80–150 | 100 K+ | 700–1000× |
| 平均尋道 + 旋轉延遲 | 8–12 ms | <0.1 ms | 100× |

**結論**：HDD 與 NVMe 的差距不在頻寬（一個數量級），而在 **IOPS（三個數量級）**。MinIO 的瓶頸也幾乎全在 IOPS 維度。

### 典型 HDD 部署假設

報告中後續推算基於：**4 節點 × 12 盤 7200 SATA = 48 盤叢集**，單盤容量 12 TB（約 576 TB raw / ~432 TB usable @ EC:4），單節點 GOMAXPROCS=24（雙路 12 核）。

### MinIO 程式碼中隱含的 NVMe 假設

程式碼裡有 4 處明顯假設盤"不慢"：

1. **`MaxTimeout = 30s`**（`internal/config/drive/drive.go:75`）—— 單次 drive 操作 30 秒上限。HDD 上某個物件的後設資料 + 資料讀如果命中冷 cache 可達數秒，30s 是夠的，但留 margin 不大。
2. **`scannerSleeper factor=2`**（`cmd/data-scanner.go:66`）—— 每個 IO 之後 sleep `IO 時長 × 2`。NVMe 上 IO 是 µs 級，sleep 也 µs 級；HDD 上 IO 是 ms 級，sleep 也 ms 級，導致 scanner 嚴重退化。
3. **`numHealers = max(4, NRRequests/4)`**（`cmd/global-heal.go:195-208`）—— healing 併發等於盤的 NRRequests/4。HDD 的 NRRequests 通常 128，得 32；但 HDD 實際只能並行處理 4-8 個 IO，超出會全部排隊。
4. **Replication `throttleDeadline = 1h`**（`cmd/bucket-replication.go:61`）—— 小物件在限速佇列等令牌的最長時間。HDD 叢集源端讀取一個小物件耗時幾十 ms 不誇張，加上限速令牌等待，1h 看起來很長但實際可能擠壓佇列。

---

## 1. Scanner（後臺掃描）

### 機制回顧

Scanner 是單 leader 週期任務（預設 1 分鐘一輪，`MINIO_SCANNER_SPEED=default`），透過 `readDirFn` 遞迴遍歷每盤 prefix 樹，為每個物件讀 `xl.meta`、累加用量、按 1/1024 機率觸發 healing 檢查、按 ILM 規則觸發動作。詳見報告 §6.1。

### HDD 上的瓶頸

#### B1.1 readdir + stat 風暴
對每個 prefix 目錄：1 次 `readDir` + 每條目 1 次 `stat` 用於過濾。HDD 上 `readdir` + stat 平均消耗一次尋道（~10 ms）。

**量化**：1 PB / 平均物件 1 MB ≈ 10 億物件，平均每盤 2 千萬物件，分佈在約 200 萬 prefix 目錄裡。單盤 readdir 風暴 = 200 萬 × 10 ms = **5.5 小時純 readdir 時間**，再加上 stat —— 單盤 walk 一遍 **理論下限 12-20 小時**。

併發 12 盤 + 節流：實際單 cycle **2-4 天**才能掃完一次。報告裡說"1 分鐘週期"是 NVMe 假設，HDD 上根本到不了。

#### B1.2 dynamicSleeper 在 HDD 上"放大"
`scannerSleeper = newDynamicSleeper(factor=2, maxWait=1s, isScanner=true)` (`cmd/data-scanner.go:66`)。每次 IO 完成後 sleep `IO 時長 × 2`。

NVMe 上 1 µs IO → sleep 2 µs，幾乎無影響。
HDD 上 10 ms IO → sleep 20 ms，**有效 IO 時間被壓縮到 1/3**。default 檔 (factor=2)，scanner 實際只能用 1/3 的盤 IOPS。

`speed=fast` factor=1 → 1/2 盤 IOPS；`speed=fastest` factor=0 不 sleep；`speed=slowest` factor=100 → **1/100 盤 IOPS**（這是為了讓前臺幾乎拿全部 IO）。

#### B1.3 1/1024 抽樣 healing 在 HDD 上是負擔
即使只有 1/1024 機率觸發，對每個抽中物件要做 EC quorum 校驗（讀 N 個 part 的 stat 或 head），每物件 N+1 次盤 IO。

10 億物件 × 1/1024 = 100 萬抽樣 × 13 次 IO（12 盤 EC:4 全 stat）= 1300 萬次 random IO ≈ **40 小時（節流後實際 100 小時）單盤**。

#### B1.4 Compaction 時機晚 → 大目錄 walk 長
`dataScannerCompactAtChildren = 10000`（`cmd/data-scanner.go:53`）：子節點超 10000 才 compact。1 PB 的桶 prefix 樹深度大、分支多，compact 觸發要等到第 N cycle 後才穩定。前幾輪 walk 完整目錄 = 大量 wasted random IO。

#### B1.5 xl.meta cold-cache 加倍 IO
HDD 沒有頁快取的話，walk 到一個物件時 stat 一次 + 開啟 xl.meta 讀一次 = 2 次尋道。對熱資料 OS page cache 有效，對冷資料每物件就是 2 次盤 IO。

### MinIO 自帶的應對

- `MINIO_SCANNER_SPEED=slow/slowest`：把 sleeper factor 拉到 10/100，給前臺讓路
- `MINIO_SCANNER_IDLE_SPEED=on`：只在系統忙時 sleep
- `globalLeaderLock`：叢集只有一個 scanner（不會多機搶盤 IO）
- Compaction 把"無變化"的子樹摺疊，下個 cycle 不再深掃

### 仍然不夠的地方

**根本問題**：scanner 是 readdir-stat-driven walking。HDD 上的 random IO 上限決定了"每秒能 walk 多少物件"是物理硬上限：

- 單盤 100 IOPS / 2 (readdir+stat) = **50 obj/s 上限**
- 12 盤併發 = 600 obj/s
- 1 PB / 1 B-object 叢集 = 1 B / 600 = **20 天掃一次完整叢集**

這意味著預設 1 分鐘週期在 HDD 上**完全不可能達到**，所有"延遲不超過 1 cycle"的承諾（ILM 觸發延遲、用量統計新鮮度、healing 抽樣覆蓋率）都失效。

### 調參建議

```bash
# 冷資料歸檔叢集：scanner 讓出 99% 時間
mc admin config set ALIAS scanner speed=slowest

# 溫資料叢集：scanner 仍然保守
mc admin config set ALIAS scanner speed=slow

# 熱資料叢集但被迫用 HDD：接受 scanner 永遠滯後，bitrot 用月度
mc admin config set ALIAS scanner speed=slowest
mc admin config set ALIAS heal bitrotscan=1m

# 不要在 HDD 上設 fastest/fast — 會 starvation 前臺
```

### 重新設計建議

1. **採用 inotify/fanotify 增量發現** 替代週期 walk —— scanner 只在檔案系統通知時觸發，避免無變化目錄的 readdir
2. **dynamicSleeper factor 應按盤型別自適應** —— 檢測到 HDD 時 factor=0.5（讓 scanner 佔一半時間，因為反正 walk 也快不了）
3. **後設資料外存** —— 把物件索引存到 RocksDB 或類似 KV，scanner 走 KV scan 而不是 readdir

---

## 2. ILM (Lifecycle 管理)

### 機制回顧

ILM 評估 piggy-back 在 scanner 上 —— scanner 走到每個物件時調 `evaluator.Evaluate` 決定動作。Transition (tier) 和 Expiration 用獨立 worker pool（`api transition_workers=100`，scanner thread 觸發）。詳見報告 §6.4。

### HDD 上的瓶頸

#### B2.1 16-cycle 延遲在 HDD 上 = 16 天到 1 個月
`dataUsageUpdateDirCycles = 16`（`cmd/data-scanner.go:51`）：已 compact 的目錄每 16 cycle 才強制全掃一次。

NVMe 上 16 cycle = 16 分鐘。
HDD 上 16 cycle = 16 × (實際 cycle 時長 2-4 天) = **1 到 2 個月**。

意味著：ILM 規則改了之後，對歷史 compact 區域可能要等幾周才生效。

#### B2.2 Transition 的 EC 全讀放大
Transition 一個物件到遠端 cold tier，必須讀完整物件（`cmd/erasure-object.go:GetObjectNInfo`）。對小物件 (1 MB) = 1 次 EC decode = 4 部分（dataBlocks=4 時）= 4 次 random read。

100 個 transition workers × 每秒 1 個物件 = 400 random IOPS 佔用，而叢集總 IOPS 才 ~1500 (12 盤 × ~100 IOPS 安全水位)，**transition 一開始就吃掉 25% IOPS**。

#### B2.3 1 PB transition 的實際時間
理論上限：1 PB / (12 盤 × 100 MB/s 順序讀) = **23 小時**（假設全大物件、全順序）。
實際：小物件 + EC + 遠端寫延遲 + 限速 → **1-2 周**才能完成 1 PB cold transition。

#### B2.4 Expiration 的 fsync 放大
Expiration 把物件從 EC 資料 + xl.meta 一起刪除並寫 trash 標記。每刪一個物件要 N 次 unlink（每盤）+ 1 次 trash mkdir + 1 次 trash rename。

HDD 上 unlink 是 random IO（更新目錄 inode），約 5 ms。刪 100 萬物件 = 500 萬 ms / 12 盤 = **7 分鐘純刪除時間**，加上 scanner 節流和 batch 工作機制，實際數小時。

### MinIO 自帶的應對

- `transition_workers` 預設 100，可調高調低
- `batch expire` 命令繞過 scanner 立即 expire（仍受 scanner sleeper 限制）
- ILM noncurrent version 用 lifecycle prefix filter 減少處理量

### 仍然不夠的地方

ILM 在 HDD 上**根本不應該期望"準時執行"**。`Days: 30` 的過期可能延後 1-2 個月才生效。批次 expire 也會被 scanner sleeper 拖累（共用 sleeper 例項）。

### 調參建議

```bash
# 大量 transition: 降低 worker 數，避免搶 IOPS
mc admin config set ALIAS api transition_workers=20

# 不要依賴 Days:1 這種激進規則 — HDD 上 1 天的承諾基本不可能
# 用 Days:7 或更長

# 關鍵 expiration 用 batch + 業務低峰期
cat > expire.yaml <<EOF
expire:
  apiVersion: v1
  bucket: mybucket
  rules:
    - expire:
        olderThan: 7d
EOF
mc batch start ALIAS expire.yaml
```

### 重新設計建議

1. **Transition queue 持久化** —— 當前 transition 是 in-memory worker pool；程序重啟丟任務。改成 RocksDB queue 讓 1 PB transition 可中斷恢復
2. **separate scanner sleeper for ILM** —— 當前 scanner sleeper 同時管 ILM；分開後 ILM 可以在前臺空閒時全速跑

---

## 3. Healing (自愈)

### 機制回顧

Healing 5 路觸發：Scanner / NewDisk / Admin / MRF / Read-Time（詳見報告 §4）。每物件 heal = 讀 N-1 個完好 part + EC decode 重建丟失 part + 寫到目標盤。

### HDD 上的瓶頸

#### B3.1 單物件 heal IO 成本
- 12 盤 EC:4 叢集，heal 一個 1 MB 物件：
  - 讀 8 個 data parts + 4 個 parity parts 的 xl.meta：12 次 random IO ≈ 120 ms
  - 讀 8 個 data parts 的實際資料：8 次 random IO ≈ 80 ms
  - EC decode：CPU bound，~10 ms
  - 寫到 1 個目標盤：1 次順序寫 ≈ 10 ms
  - **合計 ~220 ms / 單物件**

NVMe 同樣操作 ~5 ms。**HDD 慢 44 倍**。

#### B3.2 GOMAXPROCS/2 worker 在 12 盤 HDD 上不平衡
`cmd/background-heal-ops.go:157`：worker 數 = `GOMAXPROCS/2`。24 核機器 = 12 worker。

但 HDD 叢集總併發 IO 上限 = 12 盤 × ~5 併發 = 60 IOPS。**12 worker × 220 ms/物件 = 每秒 ~55 物件**，剛好打滿。看似匹配，但忽略了前臺 IO 佔用 —— 一旦前臺開始讀寫，healing 就會和它爭搶，最終是每秒 ~10-20 物件的修復速度。

**1 TB 資料 / 1 MB 平均物件 = 100 萬物件 / 20 obj/s = 14 小時單 worker，12 worker 並行 = 1-2 小時**。
**12 TB 整盤 heal = 12-24 小時**（理論值，實際加上 max_io 限制可能 2-5 天）。

#### B3.3 numHealers = max(4, NRRequests/4) 在 HDD 不合適
`cmd/global-heal.go:195-208`：每盤 healing 併發 = `max(4, NRRequests/4)`。HDD 的 NRRequests 通常 128 → numHealers = 32。**遠超 HDD 實際併發能力（4-8）**。

後果：32 個 healing routine 同時對同一盤提交 IO，QD=32 在 HDD 上意味著每個 IO 要排隊 ~10× 平均尋道時間，整體效率反而降低。

#### B3.4 max_io = 100 預設在 HDD 上"剛剛好"
`internal/config/heal/heal.go` 預設 `max_io=100`。HDD 單盤 IOPS ~100，意味著**healing 可能佔滿整盤 IO**。

如果 max_io 設到 500（NVMe 友好預設），HDD 上 healing 100% 佔盤 → 前臺 latency 飆升。

#### B3.5 New Disk Heal 的整盤 walk 成本
新加一塊 12 TB HDD，要 walk 其它 N-1 盤的所有資料，找出該盤應該持有的所有物件，逐個 heal。

**量化**：12 TB heal / 20 obj/s = 600K 秒 ≈ **7 天**單盤 heal（預設配置）。如果同時換 ≥2 盤，互相爭搶 IOPS，可能拉到 14-21 天。

#### B3.6 Read-Time heal 把 80ms GET 變 220ms
正常 GET = 1 次 random IO ≈ 10 ms 尋道 + 資料讀 = 80 ms（帶 EC decode）。
Read-time heal 觸發後 = 讀全部 N-1 part + EC decode + 寫修復結果 = ~220 ms。

**對前臺延遲敏感的 GET-heavy 場景，read-time heal 會讓 P99 翻倍以上**。

### MinIO 自帶的應對

- `max_io` / `max_sleep` 限速
- `drive_workers` 可顯式覆蓋
- `_MINIO_HEAL_WORKERS` 內部環境變數可覆蓋全域性
- MRF retry limit 防止失敗任務無限迴圈

### 仍然不夠的地方

HDD 叢集的 healing 時間是**硬約束**，調參只能調"佔多少前臺 IO"的比例，不能改善總修復時間。一塊 12 TB HDD 的 heal 時間在保守配置下需要 **3-7 天**。這意味著第二塊盤故障如果在 7 天視窗內，剩餘冗餘度 = parity-2，進一步降低容錯。

### 調參建議

```bash
# HDD 叢集必須把 max_io 調下來，避免 healing 搶滿 IO
mc admin config set ALIAS heal max_io=50 max_sleep=500ms

# numHealers 應該限制在 4-8（接近 HDD 實際併發能力）
# 透過內部環境變數覆蓋（僅作啟動時使用）
export _MINIO_HEAL_WORKERS=8

# bitrotscan 不能在 HDD 上跑 on（持續）— 用月度
mc admin config set ALIAS heal bitrotscan=1m

# Read-time heal 是隱藏的延遲來源，監控 GET P99 是否被它推高
```

### 重新設計建議

1. **healing 的 IO 排程按盤型別自適應** —— 檢測到 HDD 時自動 numHealers=4
2. **批次 heal 一個 set 內多個物件**（locality）—— 同一目錄的物件一起 heal，複用 readdir 結果
3. **read-time heal 非同步化** —— GET 路徑檢測到 part 損壞時，把修復任務入 MRF 立即返回（用其他 part 完成響應），不要在客戶端 wait time 內做修復

---

## 4. Bitrot 檢測

### 機制回顧

Bitrot 校驗透過 HighwayHash-256 在寫入時打 checksum、在讀取時驗證。兩種觸發：(a) read-time 每次 GET 都做；(b) `bitrotscan` 啟用時 scanner 在 deep scan 模式下掃所有物件。詳見報告 §3.7。

### HDD 上的瓶頸

#### B4.1 HighwayHash 本身不是瓶頸
HighwayHash-256 在現代 CPU 上 ~10 GB/s。HDD 順序讀 100 MB/s 遠低於 hash 速度，CPU 永遠不飽和。**瓶頸是順序讀 HDD 資料**。

#### B4.2 Deep scan 全量校驗時間
`getCycleScanMode`（`cmd/data-scanner.go:89`）決定何時升級到 deep scan：(a) `bitrotscan=on` 永遠 deep；(b) `bitrotscan=Nm` 每 N 月一次 deep；(c) 否則 normal scan 不做 bitrot。

Deep scan 一輪：1 PB / (12 盤 × 100 MB/s 順序讀) = **23 小時理論**。
但 deep scan 不是純順序，要按物件單位讀 → 實際 = **3-7 天**（受 scanner sleeper 節流）。

#### B4.3 `bitrotscan=on` 在 HDD 上是禁忌
持續 deep scan 意味著 scanner 一直在做"讀全部物件"的工作。HDD 叢集在 `on` 模式下 scanner 佔用 50-80% 盤 IO，**前臺幾乎不可用**。

#### B4.4 Read-time bitrot 校驗的隱藏成本
`cmd/bitrot-streaming.go`：每次 GET 都校驗 hash。對小物件，這是"讀完小資料 + hash"，一次完成；對大物件用 streaming verifier，邊讀邊 hash。

HDD 上單 GET 增加 hash 計算的 CPU 時間可忽略。但 hash mismatch 時會觸發 read-time heal（B3.6），P99 飆升。

### MinIO 自帶的應對

- `bitrotscan` 預設 off（避免 HDD 上的災難性配置）
- `bitrotscan=Nm` 讓 deep scan 離散化（隔 N 個月一次）
- read-time check 是必做（不可關）

### 仍然不夠的地方

HDD 叢集的 bitrot 防護是**兩難**：
- 關 bitrotscan：冷資料 silent corruption 永遠不主動發現（資料丟失風險，見 09-dataloss.md S-22）
- 開 bitrotscan：scanner 佔滿 IO，前臺不可用

### 調參建議

```bash
# HDD 叢集：每 6 個月一次完整 bitrot 掃描，平衡風險與效能
mc admin config set ALIAS heal bitrotscan=6m

# 或：每月一次（更安全，IO 佔用稍高）
mc admin config set ALIAS heal bitrotscan=1m

# 永遠不要：mc admin config set ALIAS heal bitrotscan=on
```

### 重新設計建議

1. **Bitrot 校驗和外存** —— 把每物件的 hash 單獨存到 SSD（小容量），scanner 校驗時只讀 hash 不讀全資料
2. **Bitrot 校驗排程** —— 優先掃"上次校驗時間最久"的物件，而不是均勻掃所有

---

## 5. Replication (跨叢集複製)

### 機制回顧

Replication 是 GET-then-PUT 模型：源端讀物件 → 網路傳輸 → 目標端寫入。100 個常規 worker + 10 個大物件 worker（≥128 MiB）+ 8 個 MRF worker。詳見報告 §5.1。

### HDD 上的瓶頸

#### B5.1 100 worker 同時 GET 是 HDD random IO 災難
預設 `replication_max_workers=500`、auto=100。100 worker 併發對源端 HDD 提 random read 請求，遠超 12 盤 × 5 併發 = 60 的承受能力，導致：
- 每個 worker 實際等盤 IO 幾百 ms
- 源端整體 IO 佇列深度堆積
- 前臺 PUT/GET 也被排隊

**結論**：HDD 源端複製的實際吞吐 = `min(100 worker, 60 IO 併發) × 100 KB/s/worker = ~6 MB/s`。即使網路是 10G，也只能跑出 50-100 Mbps。

#### B5.2 大物件 worker 是 HDD 友好的特例
`minLargeObjSize = 128 MiB`（`cmd/bucket-replication.go:2176`）。≥128 MiB 物件走獨立 lrgworker 池（10 個），且因為大物件本身是順序讀，HDD 友好。

10 worker × 100 MB/s 順序讀 = **理論 1 GB/s**，但單盤瓶頸下 12 盤 × 100 MB/s ÷ 10 worker = 120 MB/s/worker = 1.2 GB/s。**網路成為瓶頸**（10G NIC = 1.25 GB/s）。

**結論**：大物件 replication 在 HDD 叢集表現良好。

#### B5.3 MRF 佇列在 HDD 上的 flush 頻率
`mrfSaveInterval = 5min`、`mrfMaxEntries=1M`、queue 容量 = 100K。

如果業務每秒產生 50 個寫失敗（短期網路抖動），5 分鐘 = 1.5 萬 entries 滿到 MRF 佇列。flush 時一次寫 1.5 萬 msgpack 條目到磁碟 —— HDD 上幾秒就完成（順序寫）。但如果佇列容量滿（100K），新失敗直接 drop，**永久丟失這些 healing 任務**。

#### B5.4 限速器與實際吞吐的脫節
`mc admin bucket remote --bandwidth=200MB` 設了 200 MB/s 令牌桶。
但源端 HDD 實際能輸出 = ~60 MB/s（小物件 + random）到 1.2 GB/s（大物件 + 順序）的天差地別。

**後果**：
- 小物件場景：限速 200 MB/s 但實際只有 60 MB/s，限速無效
- 大物件場景：限速 200 MB/s 限制了潛在的 1.2 GB/s，**網路利用率不足**

#### B5.5 IAM full reload 在大使用者量下吃 HDD IO
`globalRefreshIAMInterval=10min`，每 10 分鐘全量載入 IAM。LDAP 模式 100 萬使用者：
- IAM 持久化在 `.minio.sys/` 下，每使用者一個 file
- 100 萬 file × 10 ms (HDD random read) = **10000 秒 = 2.7 小時**

**HDD 叢集下 LDAP 大使用者量下，10 分鐘週期完全不可能完成全量 reload**。下一次 reload 會與上一次重疊，CPU/IO 持續高位。

### MinIO 自帶的應對

- 大物件獨立 worker 池（B5.2）
- `replication_priority=slow` 把 worker 數降到 50 / MRF=2
- per-(bucket, ARN) 令牌桶分粒度限流
- MRF retry limit + 持久化（雖然有 S-14 單盤風險）

### 仍然不夠的地方

- 小物件密集型場景在 HDD 源端**根本跑不快**，調參也無救
- IAM 大使用者量 + HDD 是**根本不相容的組合**

### 調參建議

```bash
# HDD 源端 replication：把 worker 數壓到 HDD 實際併發能力附近
mc admin config set ALIAS api replication_priority=slow
# = 50 worker, 2 MRF, 仍然超過 HDD 併發但不至於災難

# 限速建議設到源端實際能產出的吞吐附近，避免無意義令牌
mc admin bucket remote edit ALIAS/bucket --arn ... --bandwidth 100MB

# IAM 使用者量大 + HDD：必須改用 etcd backend
mc admin config set ALIAS identity_ldap server_addr=... lookup_bind_dn=...
# 然後把 IAM 儲存指向 etcd 而非 .minio.sys
```

### 重新設計建議

1. **IO-aware worker 數** —— replication pool 啟動時探測盤型別，HDD 自動降到 20-30 worker
2. **限速器從"目標頻寬"改成"源端實際可承受 IOPS"** —— 用 EWMA 監測源端實際吞吐反向調節令牌速率
3. **IAM cache 持久化分片** —— 每次 reload 只讀"上次以來變化的分片"，而不是全部

---

## 6. 橫向對比表

| 子系統 | 預設 IOPS 需求 (單節點) | HDD 單節點容量 (12 盤 × 100 IOPS = 1200) | 佔比 | 瓶頸型別 | 推薦檔位 |
|---|---|---|---|---|---|
| Scanner default | ~600 IOPS | 1200 | 50% | random | `slow` (factor=10) |
| Scanner fastest | 全力 | 1200 | 100% | random | **不可用** |
| ILM evaluator | piggy-back on scanner | — | — | random | (跟 scanner) |
| Transition (100 worker) | ~400 IOPS | 1200 | 33% | mixed | `transition_workers=20` |
| Healing default (12 worker) | ~800 IOPS | 1200 | 67% | random | `max_io=50` |
| Healing max_io=500 | ~5000 IOPS (溢位) | 1200 | **可用 100%** | random | **絕不可用** |
| Bitrot scan on | 全力（持續 deep scan） | 1200 | 80%+ | mixed | **絕不可用** |
| Bitrot scan 1m (月度) | 短期 80%，長期 ~5% | 1200 | <10% 平均 | mixed | 推薦 |
| Replication 100 worker (源) | ~100 random read IOPS | 1200 | 8% | random | (但實際限於 60 併發) |
| Replication large 10 worker | ~10 sequential read | 1200 | 1% IO 但 100% 頻寬 | seq + 網路 | OK |
| Read-time bitrot | per GET, 0 額外 IO | 1200 | 0 | CPU | OK |
| Read-time heal | per failed GET, +N IO | 1200 | 看錯誤率 | random | (避免) |
| IAM reload (LDAP 1M) | ~1000 random read | 1200 | 83% | random | **必須改 etcd** |

---

## 7. 推薦配置組合（按 persona）

### Persona A：冷資料歸檔叢集 (1 PB+, 寫少讀少, 全 HDD)

目標：極低 IO 佔用讓冷盤休眠（power 節省 + 壽命）；接受所有後臺任務"很慢"。

```bash
mc admin config set ALIAS scanner speed=slowest idle_speed=on
mc admin config set ALIAS heal max_io=10 max_sleep=2s drive_workers=2
mc admin config set ALIAS heal bitrotscan=6m
mc admin config set ALIAS api transition_workers=10
mc admin config set ALIAS api replication_priority=slow
mc admin config set ALIAS api stale_uploads_expiry=12h
mc admin config set ALIAS api delete_cleanup_interval=30m
```

### Persona B：溫資料叢集被迫用 HDD (熱資料應該 NVMe 但預算不夠)

目標：保 70% IO 給前臺 PUT/GET，剩 30% 給後臺。接受"前臺 P99 從 NVMe 的 10 ms 漲到 HDD 的 100 ms"。

```bash
mc admin config set ALIAS scanner speed=slow
mc admin config set ALIAS heal max_io=50 max_sleep=500ms drive_workers=4
mc admin config set ALIAS heal bitrotscan=1m
mc admin config set ALIAS api transition_workers=30
mc admin config set ALIAS api replication_priority=auto
mc admin config set ALIAS api requests_max=200
```

### Persona C：HDD + NVMe 混合 (NVMe 為熱資料 cache tier)

目標：所有"路徑包含 cache miss"的操作走 HDD（scanner / heal / 冷 GET），其他走 NVMe。

不用 MinIO 內建 cache（已廢棄），考慮：
- 在 HDD pool 之上疊加一個 NVMe pool（多 pool 部署）
- 前 N 天熱資料透過 ILM transition 到 HDD pool（注意 transition 是單向）
- IAM、metadata 子集放 NVMe（透過單獨 minioMeta 路徑配置 —— 但 MinIO 不直接支援這個分層；一般做法是把 `.minio.sys/` 指向 NVMe 掛載點）

```bash
# Pool 0 = NVMe 4 節點 × 4 盤
# Pool 1 = HDD 4 節點 × 12 盤
minio server http://nvme{1...4}/data/{1...4} http://hdd{1...4}/data/{1...12}

# Lifecycle: 30 天后 transition 到 hdd
mc ilm rule add ALIAS/mybucket --transition-days 30 --transition-tier hdd-pool
mc admin config set ALIAS api replication_priority=slow
```

### Persona D：跨地域 DR (源 HDD 叢集, 目標 SSD)

目標：DR 複製不拖垮源端前臺業務。

```bash
# 源端
mc admin config set ALIAS api replication_priority=slow  # 50 worker
mc admin config set ALIAS api replication_max_lrg_workers=5  # 大物件只用 5 worker
mc admin bucket remote edit ALIAS/critical --arn ... --bandwidth 100MB  # 限速到源端可承受
mc admin replicate update SITE --default-bandwidth 200MB

# 監控源端 P99 GET latency；如果超過基線 50% 就降低限速
```

---

## 8. 監控指標 (Prometheus)

### 早期發現 HDD 瓶頸的指標

| 指標 | 閾值 | 含義 |
|---|---|---|
| `minio_node_drive_total_writes_total` (rate) | < 100 ops/s/盤 | 單盤寫 IOPS 接近物理上限 |
| `minio_node_drive_total_reads_total` (rate) | < 100 ops/s/盤 | 單盤讀 IOPS 接近物理上限 |
| `minio_node_drive_perc_util` | > 80% 持續 | 盤飽和 |
| `minio_node_drive_writes_await` | > 20 ms | 寫延遲異常（HDD baseline ~10 ms）|
| `minio_node_drive_reads_await` | > 20 ms | 讀延遲異常 |
| `minio_node_drive_waiting_io` | > 10 持續 | IO 佇列堆積 |
| `minio_node_scanner_objects_scanned` (rate) | < 100/s | scanner 速度過慢 |
| `minio_heal_objects_pending` | > 10000 | healing queue 積壓 |
| `minio_replication_max_active_workers` | 持續 = max | replication 跑滿（可能是 HDD 跑不動）|
| `minio_bucket_replication_pending_count` | 單調上升 | replication 落後 |

### 主報告 §8.7 監控章節沒覆蓋的 HDD 專屬

- **drive_perc_util** —— OS 層利用率，比 MinIO 的 IO count 更準
- **drive_writes_await / reads_await** —— 單 IO 延遲，HDD 異常的早期訊號
- **drive_waiting_io** —— 佇列深度，HDD 排隊即效能崩潰
- **scanner_cycle_duration_seconds** —— scanner 一輪實際耗時（應該跟蹤是否遠超 1 分鐘 default）
- **healing_per_drive_throughput** —— 每盤 heal 速率，看 numHealers 是否過高

### 示例告警規則

```yaml
- alert: MinIODriveSaturated
  expr: avg by (instance) (minio_node_drive_perc_util) > 80
  for: 5m
  annotations:
    summary: "Node {{ $labels.instance }} drive utilization >80% for 5min — HDD bottleneck likely"

- alert: MinIOScannerLagging
  expr: minio_node_scanner_objects_scanned_total - on() avg_over_time(minio_node_scanner_objects_scanned_total[1h]) < 100
  for: 30m
  annotations:
    summary: "Scanner throughput dropped below 100 obj/s — likely IO contention on HDD"
```

---

## 9. 誠實評價

### MinIO 在 HDD 上做不好的事
1. **不能保證 ILM 準時執行** —— 日級規則在 HDD 上可能延遲周/月
2. **不能開 bitrotscan=on** —— 會讓前臺不可用
3. **不能開 healing max_io ≥ 500** —— 會讓前臺不可用
4. **大使用者 LDAP IAM 在 HDD 上根本不能用** —— reload 週期跑不完
5. **Replication 100 worker 預設設定在 HDD 源端會引發 random IO 災難**

### MinIO 在 HDD 上做對的事
1. **scanner 節流（dynamicSleeper + speed 檔位）** —— 給運維"全部時間給前臺"的選項
2. **大物件走獨立 worker 池** —— 大物件順序讀對 HDD 友好，避免被小物件拖累
3. **healing 5 路觸發** —— 即便 scanner 慢，read-time heal 兜底
4. **每物件 EC** —— heal 單位是物件而不是整盤，比 RAID 重建靈活
5. **MRF 持久化** —— 程序崩潰不丟 healing 狀態（雖然有 S-14 單盤風險）

### 總結

MinIO 設計原點是 NVMe；HDD 是"被支援但不被最佳化"的場景。在 HDD 上部署 MinIO 是可行的，但需要：
1. 接受所有後臺任務"很慢"
2. 容量與吞吐預估打 0.3-0.5 折扣
3. 關鍵 ILM / quota / replication 監控加上"HDD-aware" 閾值
4. 不要照搬 NVMe 教程的配置（特別是 max_io、worker 數、bitrot scan）

如果業務負載是**純寫入歸檔型**（cold storage），HDD MinIO 表現良好。
如果是**頻繁小物件讀寫 + 嚴格 SLA**，HDD 不適合，應換 NVMe 或考慮 SeaweedFS（小檔案最佳化）。

---

## 分析依據

| 檔案 | 閱讀範圍 |
|------|---------|
| `cmd/data-scanner.go` | 50-95（常量 + getCycleScanMode）、365-440（scanFolder）、1360-1430（dynamicSleeper 實現）|
| `cmd/erasure-healing.go` | 285-355（healObject + isAllNotFound）、1046-1100（dangling）、healObject 的 IO 路徑 |
| `cmd/global-heal.go` | 195-215（numHealers 計算）|
| `cmd/background-heal-ops.go` | 155-170（worker 數計算）|
| `cmd/bucket-replication.go` | 60-65（throttleDeadline）、1855-1915（worker 池配置）、2176（minLargeObjSize）|
| `cmd/bucket-lifecycle.go` | 460-470（transition workers）|
| `cmd/mrf.go` | 120-220（持久化 + 載入）|
| `cmd/bitrot.go` + `cmd/bitrot-streaming.go` | 全文（已透過 06-module-storage-erasure.md 摘要）|
| `internal/config/scanner/scanner.go` | 30-200（speed 檔位與預設值）|
| `internal/config/heal/heal.go` | 30-185（預設值）|
| `internal/config/drive/drive.go` | 30-95（MaxTimeout = 30s）|
| `internal/config/api/api.go` | replication_*, transition_workers 段 |

依據來自原始碼 + `drafts/06-module-*.md` + `drafts/rate-control-knobs.md`。所有 path:line 已對照實際程式碼核實。
