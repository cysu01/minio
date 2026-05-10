# MinIO 資料丟失場景全景

> **目標**：窮舉從程式碼層面可推匯出的所有資料丟失風險，按"觸發原因"分 5 類，每條給出程式碼引用、量化的最差情況、MinIO 自帶防護、殘留風險、緩解動作。
> **範圍**：scanner / replication / multipart / S3 API / healing 交叉路徑。
> **程式碼版本**：`/home/vscode/repo-analyses/minio-20260503/repo`，2026-05-03 截面。
> **不重複**報告 §10.2 已有的 6 個簡略問題；本文是其 5 倍深度的擴充套件。

---

## 場景模板

每條場景按下面 6 欄位呈現：

```
S-NN. 一句話描述
- 觸發: 具體動作 / 事件
- 程式碼路徑: function (`path:line`) → ...
- 後果: 資料丟失 / 靜默損壞 / 臨時不可用 / 可恢復
- MinIO 防護: 已有的兜底機制
- 殘留風險: 量化的最差情況（多少資料 / 多大視窗 / 多大機率）
- 緩解: 可調引數 / 運維動作 / 客戶端策略
```

---

## 1. 客戶端取消 / 網路中斷

### S-01. PUT 中途客戶端斷開 — 臨時檔案殘留
- **觸發**: 客戶端在傳送 body 過程中 close TCP，或 keep-alive 超時
- **程式碼路徑**: `cmd/object-handlers.go:PutObjectHandler` → `cmd/erasure-object.go:putObject:1330` → 寫入 `minioMetaTmpBucket` → `RenameData` 未到達
- **後果**: 臨時不可用（寫入未提交，對客戶端是失敗）；磁碟空間暫時佔用
- **MinIO 防護**: 臨時檔案存在 `tmp/` 子目錄；`cleanupStaleUploads` 週期清理（`cmd/erasure-multipart.go:230-249`），過期閾值 `STALE_UPLOADS_EXPIRY` (24h) + 清理週期 `STALE_UPLOADS_CLEANUP_INTERVAL` (6h)
- **殘留風險**: 最差 24h + 6h = **30 小時**孤兒臨時檔案佔盤空間；不會變成"半成品物件"
- **緩解**: `mc admin config set ALIAS api stale_uploads_expiry=2h stale_uploads_cleanup_interval=1h`；監控 `.minio.sys/tmp/` 大小

### S-02. Multipart Upload 啟動後被遺忘
- **觸發**: 客戶端 `CreateMultipartUpload` 成功後崩潰；忘了調 `CompleteMultipartUpload` 或 `AbortMultipartUpload`
- **程式碼路徑**: `cmd/erasure-multipart.go:cleanupStaleUploadsOnDisk:168-228` 掃描每個 multipart UUID 目錄，若 `time.Since(modTime) >= staleUploadsExpiry` 則 `renameAll` 到 `.trash/`
- **後果**: 臨時不可用（part 資料佔盤）
- **MinIO 防護**: 上傳 ID 編碼了納秒時間戳（`base64(UUID + "x" + UnixNano)`，line 175-182），免去 stat IO；掃描週期 6h
- **殘留風險**: 最差 24h+6h = 30 小時孤兒 parts；如果一次性上傳 100 個 5GB part 後崩潰，30 小時內佔用 500GB
- **緩解**: 縮短 `stale_uploads_expiry`；客戶端實現"啟動時清理孤兒 multipart"

### S-03. CompleteMultipartUpload 中途 client 斷開
- **觸發**: 客戶端發起 `CompleteMultipartUpload` 後超時（一個大物件的 merge 可能很慢），但服務端已經在 merge
- **程式碼路徑**: `cmd/object-multipart-handlers.go:CompleteMultipartUploadHandler` → `cmd/erasure-multipart.go:CompleteMultipartUpload` → `RenameData` 拼接 parts
- **後果**: 部分情況下 **客戶端以為失敗但服務端實際成功**（500 響應或超時，但 RenameData 已完成）
- **MinIO 防護**: 無；S3 API 語義本身允許客戶端透過重試 `CompleteMultipartUpload` 或 `HeadObject` 驗證
- **殘留風險**: 客戶端發起重試 → 第二次 Complete 失敗（parts 已被清理）→ 客戶端可能誤認為物件不存在 → 誤刪源資料
- **緩解**: 客戶端在重試前必須用 `HeadObject(versionID=...)` 校驗

### S-04. GET 中途客戶端斷開
- **觸發**: 客戶端讀取物件時關閉連線
- **程式碼路徑**: `cmd/object-handlers.go:GetObjectHandler` → `getObjectNInfo` → 流式 read
- **後果**: 無；讀路徑無副作用
- **MinIO 防護**: 不適用
- **殘留風險**: 無
- **緩解**: 不適用

### S-05. PUT 期間客戶端在 sigV4 流式簽名校驗失敗
- **觸發**: 客戶端傳送 `aws-chunked` 格式資料，某 chunk 簽名算錯
- **程式碼路徑**: `cmd/streaming-signature-v4.go:Read` 在每 chunk 計算簽名並比對
- **後果**: 服務端拒絕寫入（4xx）；不會落盤
- **MinIO 防護**: 簽名錯誤立即終止 reader，上層 `putObject` 返回錯誤，臨時檔案由 `cleanupStaleUploads` 清掉
- **殘留風險**: 與 S-01 相同（孤兒臨時檔案 30h 視窗）
- **緩解**: 同 S-01

### S-06. SelectObject 流式響應中斷
- **觸發**: 客戶端在 SQL 查詢中途斷開
- **程式碼路徑**: `cmd/object-handlers.go:SelectObjectContentHandler`
- **後果**: 服務端繼續掃描完整物件（不可中止）；無副作用
- **MinIO 防護**: 不適用
- **殘留風險**: CPU/IO 浪費但不丟資料
- **緩解**: 不適用

---

## 2. 服務端故障 / 程序崩潰

### S-07. `kill -9` 在 Encode 中段
- **觸發**: 程序被 SIGKILL，此時 `erasure.Encode` 已經寫入部分 part 檔案但未生成 xl.meta
- **程式碼路徑**: `cmd/erasure-object.go:1183` (Encode) → `cmd/erasure-object.go:1564` (RenameData，未到達)
- **後果**: 不可見到客戶端（PUT 整體失敗，沒回 200）；臨時目錄有殘骸
- **MinIO 防護**: 未 RenameData 的物件在 `tmp/` 內，被 `cleanupStaleUploads` 清理
- **殘留風險**: 同 S-01；30h 內孤兒資料
- **緩解**: 同 S-01

### S-08. `kill -9` 在 RenameData 中間
- **觸發**: 程序被殺，N 個磁碟中只有 K (K<writeQuorum) 個完成 RenameData
- **程式碼路徑**: `cmd/erasure-object.go:renameData:1019-1100` 用 `writeQuorum` 校驗
- **後果**: 看是否過 quorum：(a) K ≥ writeQuorum → 客戶端可能已收到 200，物件有效；(b) K < writeQuorum → 物件被視為"部分寫入失敗"
- **MinIO 防護**: `renameData` 返回錯誤時，呼叫方觸發 `addPartialOp` (見 `erasure-object.go:1607`) 入 MRF 佇列等待 healing；同時未完成 rename 的盤上殘留 `tmp/` 目錄由 `cleanupStaleUploads` 清理
- **殘留風險**: 如果 K = writeQuorum-1 且故障前 200 已發回客戶端，**理論上客戶端以為成功但服務端只有少數副本** —— 但實際程式碼必須 ≥writeQuorum 才返回 200，所以這種情況實際不會發生。**真正的風險**：剩餘 (writeQuorum) 副本中如果再丟失 (writeQuorum - dataBlocks) 個之前 healing 還沒完成，物件不可讀
- **緩解**: 監控 `minio_heal_objects_pending`；縮短 `MRF queueInterval` (預設 6 分鐘)；保證 `defaultParityCount ≥ EC:4`

### S-09. `kill -9` 在 MRF flush 之間 — 記憶體佇列丟失
- **觸發**: MRF entries 累積在 `mrfState.opCh` (容量 100K)，程序在 5 分鐘 flush 週期之間被殺
- **程式碼路徑**: `cmd/mrf.go:saveMRFEntries:113-153` —— 注意 `for _, localDrive := range localDrives { ... if err == nil { break } }` **只寫到第一塊成功的本地盤**（line 145-152）
- **後果**: **可能丟失到 100K 條 healing 任務**；這些物件將依靠 Scanner 路徑或 Read-Time 路徑被發現，延遲可達數小時到天
- **MinIO 防護**: Scanner 1/1024 抽樣最終會發現；Read-Time healing 在 GET 時觸發；NewDisk 啟動時全盤掃描
- **殘留風險**: **量化**：平均 `mrfSaveInterval=5min` flush 一次（`bucket-replication.go:3486`），最差丟 5 分鐘內的 MRF entries。如果業務每秒產生 100 個寫失敗，最差丟 30000 條；這些物件 EC 可恢復但延遲修復
- **緩解**: B-2（見末尾程式碼改進）：MRF 寫 quorum 多盤；監控 `minio_node_drive_total_writes_total{api="MRF"}`

### S-10. `kill -9` 在 healing 中段
- **觸發**: 程序在 heal 單物件的 RenameData 之前被殺
- **程式碼路徑**: `cmd/erasure-healing.go:healObject` → `RenameData` 提交修復結果
- **後果**: 臨時修復結果在 `tmp/` 殘留；下次 heal 重新做
- **MinIO 防護**: heal 是冪等的；`.healing.bin` 跟蹤 set 級別進度；啟動時 `globalBackgroundHealState.pushHealLocalDisks` 重新發現
- **殘留風險**: 30h 內 `tmp/` 佔盤；進度回退到本批次起點（一個 set 可能要重掃幾小時）
- **緩解**: 監控 `tmp/` 大小；監控 healing pending count

### S-11. 單磁碟整盤掉線 (link down) 但程序存活
- **觸發**: SAS/SATA 控制器報 IO error；某盤 fd 全部 EIO
- **程式碼路徑**: `cmd/xl-storage-disk-id-check.go:84-101` 裝飾器在每次 IO 前校驗 diskID
- **後果**: 進入 quorum 寫路徑，N-1 盤可寫則成功；**剩餘物件修復依賴 NewDisk monitor 10s 心跳重連**
- **MinIO 防護**: 裝飾器攔截 `errDiskNotFound`；物件級 healing 自動排程
- **殘留風險**: 假設 EC:4，單盤掉時降級為 EC:3 但仍可寫；連續 4 盤掉則叢集停擺
- **緩解**: 部署 ≥6 節點 + EC:4；監控 `minio_cluster_drive_offline_count`

### S-12. 全 set 節點全部掉線（機櫃斷電、交換機故障）
- **觸發**: 某 erasure set 的所有節點同時不可達
- **程式碼路徑**: `cmd/erasure-sets.go` 路由層；`cmd/erasure-object.go` 寫時返回 `errErasureWriteQuorum`
- **後果**: 該 set 上所有物件**臨時不可用**；當時正在寫入的物件失敗
- **MinIO 防護**: 節點恢復後自動重連；正在寫入的物件客戶端層面失敗（未回 200）
- **殘留風險**: 故障期間內的 in-flight writes 全部失敗；已存物件重連後即可讀
- **緩解**: 跨機櫃 set 拓撲（節點編號 % set_size 應分散）；多 pool 部署

### S-13. 啟動時 `format.json` 讀不出（損壞 / 誤刪）
- **觸發**: 單盤 `format.json` 損壞或丟失
- **程式碼路徑**: `cmd/format-erasure.go` + `cmd/prepare-storage.go`
- **後果**: 該盤被識別為"未格式化"，進入 NewDisk heal 流程
- **MinIO 防護**: 其它盤的 `format.json` 互相校驗（quorum）；缺失盤的格式從其它盤恢復
- **殘留風險**: 如果 (parityBlocks + 1) 個盤的 `format.json` 同時丟失，叢集無法啟動
- **緩解**: 監控 format.json 完整性；不要手動操作 `.minio.sys/`

### S-14. MRF 持久化是"first-success-then-break" 單盤寫
- **觸發**: MRF 佇列在程序退出前被持久化到磁碟（`saveMRFEntries`），但只寫到**第一塊成功的本地盤**
- **程式碼路徑**: `cmd/mrf.go:145-152`：`for _, localDrive := range localDrives { ... if err == nil { break } }`
- **後果**: 如果該盤事後損壞，整個 100K 條 MRF 佇列丟失
- **MinIO 防護**: 啟動時 `startMRFPersistence` 也是"first-success" 讀（`mrf.go:194-210`）；如果第一盤讀不出會試下一塊；但只要第一塊沒壞就讀成功；如果**所有盤都沒儲存**（第一次 save 完所有盤都失敗的機率極小但存在），完全丟
- **殘留風險**: 單盤損壞 → 100K 個 healing 任務丟失 → 必須依賴 Scanner 抽樣（1/1024）補救，可能需要數天才能發現遺漏
- **緩解**: B-2（寫 quorum 數盤以增強冗餘）

---

## 3. 配置錯誤 / 誤操作

### S-15. parity 設得太低（EC:1 在 4 節點）
- **觸發**: `mc admin config set ALIAS storage_class standard=EC:1` 或 `MINIO_STORAGE_CLASS_STANDARD=EC:1`
- **程式碼路徑**: `internal/config/storageclass/storage-class.go:354-368` 的 `DefaultParityBlocks`
- **後果**: 同時 2 盤故障即資料丟失
- **MinIO 防護**: 啟動時校驗 `parity ≥ 2`（預設值約束）；但允許顯式設 EC:1
- **殘留風險**: 直接的資料丟失等價於 RAID 0；**任何一對盤同時故障即永久丟失**
- **緩解**: 始終保持 `parity ≥ EC:4`（預設）；評估盤故障率與 MTBF

### S-16. 磁碟空間不均（pool 1 滿了，pool 2 空著）
- **觸發**: 多 pool 部署，pool 1 長期熱、pool 2 冷；新寫入仍可能落到幾乎滿的 pool
- **程式碼路徑**: `cmd/erasure-server-pool.go:390-411,417-480` —— 路由按 free space 加權
- **後果**: 寫入失敗；不是資料丟失
- **MinIO 防護**: free space 路由演算法；用滿前會偏向其他 pool
- **殘留風險**: free space 計算依賴 scanner 寫入的 `data-usage.bin`，最差有 10s + scanner_cycle 滯後；瞬時全滿可能拒寫
- **緩解**: 監控每 pool 用量；擴容前不要讓任何 pool 超 80%

### S-17. ILM 規則寫 `Expiration: Days: 1` 應用到舊桶
- **觸發**: 運維誤把"1 天后過期"應用到已存在的桶（其中 90% 物件已經超過 1 天）
- **程式碼路徑**: Scanner 下次掃到時 `cmd/bucket-lifecycle.go:applyAction` 觸發刪除
- **後果**: **大批物件被刪除**（按物件級 EC 處理，物件的 part 檔案 + xl.meta 都進入 `.trash/`）
- **MinIO 防護**: 預設有 `.trash/` 延遲刪除；scanner 節流（預設 1 分鐘週期）讓刪除速率"自動慢"
- **殘留風險**: `.trash/` 持有期 = `MINIO_API_DELETE_CLEANUP_INTERVAL`（預設 5 分鐘），**5 分鐘後真正物理刪除**；規模 100 萬物件一夜全沒
- **緩解**: 在生產應用任何 ILM 規則前先用 `mc ilm rule run --dry-run`；為重要桶啟用 `Object Lock`

### S-18. ILM 規則被覆蓋（put-bucket-lifecycle 是全替換）
- **觸發**: 第二條 lifecycle PUT 沒有攜帶原有規則，導致舊規則被清空
- **程式碼路徑**: `cmd/bucket-lifecycle-handlers.go:PutBucketLifecycleHandler`（PUT 全替換語義遵循 S3 標準）
- **後果**: 關鍵 retention 規則消失；之後寫入的資料失去保護
- **MinIO 防護**: S3 API 本身就是 PUT-replace 語義；ILM 配置不進 audit log
- **殘留風險**: 配置變更無版本歷史；恢復需要重新寫 XML
- **緩解**: 把 lifecycle XML 納入 IaC 管理（Terraform/Pulumi）

### S-19. Bucket Quota 設得低於當前用量
- **觸發**: `mc admin bucket quota --hard 100GB` 但實際用量 200GB
- **程式碼路徑**: `cmd/bucket-quota.go:enforceBucketQuotaHard:135-140`
- **後果**: 拒絕**所有**新 PUT；GET/DELETE 不受影響
- **MinIO 防護**: 僅 PUT/CompleteMultipart 路徑檢查；不刪除已有資料
- **殘留風險**: 業務可能誤以為"滿了，需要刪資料"，觸發誤刪
- **緩解**: 設 quota 時監控當前用量；預留 5-10% buffer

### S-20. Replication target 刪除後佇列堆積
- **觸發**: `mc admin bucket remote rm` 移除 target，但源端仍在產生待複製物件
- **程式碼路徑**: `cmd/bucket-targets.go:RemoveTarget:465`；下游 `replicateObject` 在找不到 target 時返回失敗 → MRF
- **後果**: MRF 佇列被無效條目填滿，擠佔其他正常 healing
- **MinIO 防護**: MRF 有 retry limit (`mrfRetryLimit=3`, `bucket-replication.go:3489`)；3 次後丟棄
- **殘留風險**: 重試 3 次期間額外 IO 浪費；如果同時多 target 頻繁更替，正常修復進度被拖慢
- **緩解**: 刪 target 前先 `mc replicate suspend`

### S-21. Site replication 加入到已有資料的叢集
- **觸發**: 在已有資料的桶上新增 site replication 站點
- **程式碼路徑**: `cmd/site-replication.go` —— 不會自動同步存量
- **後果**: 舊資料**永遠不會**複製到新站點（除非顯式 `mc batch start`）
- **MinIO 防護**: 文件警告；新寫入物件正常複製
- **殘留風險**: 跨站點容災以為已生效，實際只覆蓋增量；災難來時丟歷史
- **緩解**: 加入站點後立即跑 `mc batch generate replicate ALIAS/bucket | mc batch start`

### S-22. `MINIO_HEAL_BITROTSCAN=off` 是預設值
- **觸發**: 預設部署不開 bitrot 掃描
- **程式碼路徑**: `internal/config/heal/heal.go:103-107`：`Bitrot = "bitrotscan"; ... DefaultKVS` 中 bitrotscan 預設 `off`
- **後果**: **冷資料 silent corruption 永遠不會被主動發現**；只有當客戶端 GET 觸發 read-time 校驗或 scanner 1/1024 抽樣命中時才發現
- **MinIO 防護**: Read-time bitrot check（每次 GET 強制做）；HighwayHash 寫時記錄
- **殘留風險**: **量化**：1 PB 資料，每年 silent corruption 機率約 ~3% per drive (業界 SLA)，單盤 4 TB 即年化 120 GB 錯誤。冷資料如果一年不被讀，錯誤物件在第一次 GET 時只能從 EC 恢復（仍能恢復，只要損壞的 part ≤ parity）。**最差**：連續 (parity+1) 個 part 都 silent corrupt 且未被讀取，則永久丟失
- **緩解**: **強烈建議生產**：`mc admin config set ALIAS heal bitrotscan=1m`（每月一輪主動掃描）

### S-23. Object Lock 未對 versionID 啟用 → "刪除標記"覆蓋
- **觸發**: bucket 沒開 versioning + object lock；DELETE 請求覆蓋了物件
- **程式碼路徑**: `cmd/object-handlers.go:DeleteObjectHandler`
- **後果**: 物件進 `.trash/`，5 分鐘後物理刪除
- **MinIO 防護**: 僅當 versioning + Object Lock 都啟用時才有 retention 保護
- **殘留風險**: 誤刪後 5 分鐘視窗內可恢復；過後永久丟失
- **緩解**: 關鍵桶必須開 versioning + Object Lock + Compliance retention

---

## 4. 時序競爭 / 併發問題

### S-24. Scanner 把"正在寫入的多副本物件"當作 dangling
- **觸發**: 客戶端 PUT 大物件（耗時 30s+），scanner 同時掃到該物件的臨時目錄
- **程式碼路徑**: `cmd/erasure-healing.go:isObjectDangling:989-1054`；`cmd/data-scanner.go` 掃描 `tmp/` 子樹
- **後果**: 理論上 scanner 可能誤判，但實際 scanner **不掃 `tmp/`**，只掃已 commit 的物件目錄
- **MinIO 防護**: 路徑隔離（`tmp/` 不在 scan 範圍）；寫完成後 `RenameData` 原子改名
- **殘留風險**: 極低
- **緩解**: 不需要

### S-25. **Dangling 刪除在多盤故障期間誤刪可恢復物件**
- **觸發**: N 盤叢集中 K 盤暫時離線（重啟、網路抖動），剩餘 (N-K) 盤上 xl.meta 數 < (N-K)；scanner / heal 觸發 `deleteIfDangling`
- **程式碼路徑**: `cmd/erasure-healing.go:1046-1054`：`if notFoundMetaErrs > 0 && notFoundMetaErrs > validMeta.Erasure.ParityBlocks { return validMeta, true }`
- **後果**: **永久資料丟失** —— `deleteIfDangling` 在所有可見盤上刪除該物件（包括健康盤上的有效副本）
- **MinIO 防護**: (1) 只在 `errFileNotFound` 之類的"明確丟失"錯誤才計入 `notFoundMetaErrs`，其他 IO 錯誤進 `nonActionableMetaErrs` 阻止刪除；(2) 必須滿足 `notFoundMetaErrs > parityBlocks` 而不是 `>= dataBlocks`，要求"明確丟失數 > 容錯數"
- **殘留風險**: **場景**：12 盤 EC:4，5 盤同時被換出（運維誤操作）。Scanner 看到 5 盤缺 xl.meta，5 > 4 (parity)，**觸發刪除剩餘 7 盤上的有效資料**。報告 §10.2(6) 提及的就是此 bug。**量化**：1 PB / 1 B-object 叢集在 24h 健康視窗內若有 1% 物件被 scanner 命中且當時正處於上述狀態，約 1000 萬物件有風險
- **緩解**: B-1（見末尾）；運維嚴禁同時換 ≥ parity 塊盤；換盤前 `mc admin heal ALIAS --recursive`

### S-26. Site replication LWW 時鐘偏移
- **觸發**: 跨 region 節點 NTP 失步，時鐘漂移 5+ 秒；兩站點幾乎同時改同一使用者
- **程式碼路徑**: `cmd/site-replication.go:healIAMSystem` 用 `updatedAt` 比較
- **後果**: "時鐘領先"的站點的更改勝出，"操作發生晚"的站點的更改丟失
- **MinIO 防護**: 無；純 wall-clock LWW
- **殘留風險**: IAM 策略丟失 → 使用者失去許可權或獲得錯誤許可權；最差**安全事故**
- **緩解**: 嚴格 NTP（chrony）；避免同一使用者在多站點併發改

### S-27. 同 key 併發 PUT (無 versioning)
- **觸發**: 兩個客戶端同時 PUT 到 `mybucket/file.bin`
- **程式碼路徑**: `cmd/erasure-object.go:putObject` —— 用 namespace lock (`internal/dsync`) 序列化
- **後果**: 後到的 PUT 覆蓋前者；S3 標準行為
- **MinIO 防護**: 分散式鎖保證不會"撕裂寫入"
- **殘留風險**: 業務層期望"先寫 wins"或"先寫後讀 wins"會失望
- **緩解**: 開 versioning；顯式 `If-None-Match: *`

### S-28. DELETE + PUT 同 key 併發
- **觸發**: 客戶端 1 PUT，客戶端 2 DELETE，幾乎同時到達
- **程式碼路徑**: namespace lock 序列
- **後果**: 看到達順序：PUT-then-DELETE 留 delete marker；DELETE-then-PUT 物件存活
- **MinIO 防護**: 鎖序列
- **殘留風險**: 業務設計假設的順序可能與實際不符
- **緩解**: 用 versioning 保留每個 op 的版本 ID

### S-29. Healing 完成後 RenameData 與新寫入衝突
- **觸發**: heal 完成後 RenameData 時，剛好客戶端 PUT 同物件
- **程式碼路徑**: `cmd/erasure-healing.go:healObject` → `RenameData`；同時 `cmd/erasure-object.go:putObject`
- **後果**: namespace lock 序列；後者等前者完成
- **MinIO 防護**: 鎖
- **殘留風險**: heal 持鎖期間正常 IO 等待，可能超時
- **緩解**: heal 用更短的批次

### S-30. Scanner walks 時物件被併發刪除
- **觸發**: scanner 列出物件 → 準備 stat → 客戶端刪除 → stat 返回 ENOENT
- **程式碼路徑**: `cmd/data-scanner.go:scanFolder:399-440` 處理 `errFileNotFound`
- **後果**: scanner 略過該物件；不影響資料
- **MinIO 防護**: 錯誤吞掉，統計跳過
- **殘留風險**: 用量統計可能短暫不準
- **緩解**: 不需要

### S-31. NewerNoncurrentVersions 與併發 PUT 競爭
- **觸發**: ILM 規則 `NewerNoncurrentVersions: 5` 保留最新 5 個版本；客戶端高頻 PUT，scanner 滯後
- **程式碼路徑**: `cmd/bucket-lifecycle.go` + `internal/bucket/lifecycle/noncurrentversion.go`
- **後果**: 真實保留數可能多於 5（scanner 還沒掃到）；滯後約 1 分鐘
- **MinIO 防護**: 最終一致；過期但未清的版本不影響功能
- **殘留風險**: 容量預估偏差
- **緩解**: 不需要

### S-32. Replication retry 與源 DELETE 競爭
- **觸發**: 源端 PUT → 入複製佇列 → 失敗 → 進入 MRF retry → 期間客戶端 DELETE → MRF 重試時仍按"複製 PUT"操作
- **程式碼路徑**: `cmd/bucket-replication.go:replicateObject:1184` 重試時用 versionID 拉源物件
- **後果**: 如果源物件已被永久刪除（非版本桶），重試拉不到 → 失敗丟棄；如果是版本桶，按 versionID 仍能拉到 → 複製成功
- **MinIO 防護**: 複製是按 versionID 不變性進行的
- **殘留風險**: 非版本桶下"短命物件"可能不被複制
- **緩解**: 複製源桶必須開 versioning（見 `cmd/bucket-replication.go:SetTarget` 校驗）

---

## 5. 已知 Bug / 邊界情況

### S-33. **Dangling deletion race during disk replacement**
- **觸發**: 同時換 ≥ parity 塊盤；換上的盤還沒 NewDisk-heal 完成，scanner 已經掃到該 set
- **程式碼路徑**: `cmd/erasure-healing.go:isObjectDangling:1046-1054` + `cmd/background-newdisks-heal-ops.go:559`
- **後果**: 報告 §10.2(6) 提及；**真實場景**：12 盤 EC:4 叢集同時換 5 塊盤（運維以為冗餘夠用），換盤後 NewDisk-heal 心跳是 10 秒，全盤 heal 需要小時級；這視窗內 scanner 掃到的物件會觸發 dangling 刪除
- **MinIO 防護**: NewDisk monitor 每 10 秒檢查；要求換盤後等 heal 完成再換下一批
- **殘留風險**: **量化**：1 PB / 1 B-object，scanner 預設 cycle = 1 分鐘，每分鐘可掃 ~10 萬物件；24 小時 heal 視窗內若 5 盤均處於 healing-in-progress，約 1.4 億物件處於"理論可被誤刪"狀態。MinIO 多重檢查（IsValid + nonActionable + parity 校驗）使實際觸發率極低，但非零
- **緩解**: B-1 改進；運維 SOP：單批次換盤 ≤ parity-1；換盤後 `mc admin heal --recursive` 強制即時 heal

### S-34. dataScannerForceCompactAtFolders 死程式碼
- **觸發**: 極端情況下 prefix 樹極度膨脹
- **程式碼路徑**: `cmd/data-scanner.go:55` 定義了 `dataScannerForceCompactAtFolders = 250000` 但**只在定義處出現**，沒有任何地方呼叫它
- **後果**: 非"壓扁"路徑下可能 OOM（>250K 子目錄）；實際由 `dataScannerCompactAtChildren=10000` (`data-scanner.go:373`) 兜底
- **MinIO 防護**: 實際 compaction 用 10000 閾值，遠小於 250K；不會觸發
- **殘留風險**: 死程式碼本身無害，但反映了 review 不夠（cross-validation §3 說"未找到"，更準確地應該是"已定義但未引用"）
- **緩解**: 刪除死程式碼（B-3）

### S-35. ILM 16-cycle delay + Expiration: Days:1
- **觸發**: ILM 配置很激進的過期；scanner 是 lazy scan
- **程式碼路徑**: `cmd/data-scanner.go:dataUsageUpdateDirCycles=16`：每 16 cycle 才強制全量掃一次 compacted 目錄
- **後果**: 已經 compact 的目錄可能 15 cycle 都不被掃；ILM 規則可能延遲到 16 分鐘（default speed）才生效
- **MinIO 防護**: `batch expire` 命令繞過 scanner 立即生效；新物件（未 compact）每 cycle 都掃
- **殘留風險**: "1 天過期"實際可能延遲到 1 天 + 16 分鐘；速度 slowest 時延遲到 1 天 + 8 小時
- **緩解**: 關鍵過期場景用 `mc batch start expire-job.yaml`；不依賴 scanner timing

### S-36. Decommission pool 期間被寫入
- **觸發**: `mc admin decommission start` 期間客戶端繼續寫入
- **程式碼路徑**: `cmd/erasure-server-pool-decom.go`
- **後果**: 被 decom 的 pool 不再接收新寫入；正在遷移的物件不可寫
- **MinIO 防護**: pool 遷移用 source/destination 鎖
- **殘留風險**: 大物件 in-flight write 在 decom 期間可能失敗
- **緩解**: 業務低峰期 decom

### S-37. IAM cache spike 期間的鑑權延遲
- **觸發**: `LoadIAMCache` 週期觸發（預設 10 分鐘）；LDAP 模式 100 萬使用者全量載入
- **程式碼路徑**: `cmd/iam-store.go:LoadIAMCache:643` 構建 `newCache` 再 swap
- **後果**: 載入期間記憶體翻倍；交換期間請求繼續走舊 cache（無中斷）
- **MinIO 防護**: 雙緩衝 swap 保證讀路徑無鎖等待
- **殘留風險**: 記憶體高峰可能 OOM；新建使用者在載入完成前不可見
- **緩解**: 增量同步（B-4）；監控記憶體

### S-38. Site replication "split brain"
- **觸發**: 站點 A 和 B 網路分割槽超過 IAM 同步週期；分割槽期間各自接受寫入
- **程式碼路徑**: `cmd/site-replication.go` LWW 協議
- **後果**: 網路恢復後 LWW 決出勝負；落選方的修改丟失
- **MinIO 防護**: 透過 wall-clock 時間排序
- **殘留風險**: 跨站點同時改同一資源時，落選者修改不可恢復
- **緩解**: 業務路由層面隔離（同一資源只允許在一個 region 改）；啟用 `mc admin replicate status` 監控分歧

### S-39. Quorum 寫"剛好滿足" + 一盤是 stale 的
- **觸發**: writeQuorum = 5（dataBlocks=4 + 1）；其中 1 塊剛才掉線又上線，但還沒完成 heal；它持有該物件的舊版本但響應"成功寫"
- **程式碼路徑**: `cmd/erasure-object.go:1059` 的 `reduceWriteQuorumErrs` 檢查
- **後果**: 寫入似乎成功，但回收的"5 個成功"中包含 1 個並不真實新寫入的盤；理論上下次讀會讀不出（quorum 不夠新）
- **MinIO 防護**: `xl-storage-disk-id-check.go` 攔截 stale disk（diskID 不匹配則拒）；`addPartialOp` 把這種盤加到 MRF
- **殘留風險**: diskID 校驗如果透過（盤沒有 reformat 只是斷電），仍可能"看似寫入"
- **緩解**: 監控 partial writes；diskID 嚴格檢查不要禁

### S-40. Healing 期間該 set 被 decom
- **觸發**: 一個 set 正在 NewDisk-heal，運維同時啟動 pool decommission
- **程式碼路徑**: `erasure-server-pool-decom.go` + `cmd/global-heal.go`
- **後果**: heal 寫入"即將被刪除"的 pool；浪費 IO 但無資料風險
- **MinIO 防護**: decom 完成時整 pool 資料遷出
- **殘留風險**: IO 浪費幾小時
- **緩解**: 完成 heal 再開 decom

### S-41. KMS 不可達期間已加密物件 GET
- **觸發**: KMS 服務掛掉；客戶端 GET 加密物件
- **程式碼路徑**: `internal/kms/kms.go` Decrypt 路徑
- **後果**: 該物件**臨時不可讀**；不丟失資料
- **MinIO 防護**: KMS 重試 + 多 KMS 節點配置
- **殘留風險**: KMS 長期不可達 = 資料"加密鎖死"，理論上仍是資料丟失
- **緩解**: KMS 高可用部署；定期備份 KMS master key

### S-42. Trash 5 分鐘視窗內叢集異常
- **觸發**: 誤刪後立即想 undelete；但 `MINIO_API_DELETE_CLEANUP_INTERVAL=5m` 已經過去
- **程式碼路徑**: `cmd/erasure.go` deleteCleanupSleeper；trash 物理刪除路徑
- **後果**: 無法 undelete
- **MinIO 防護**: 5 分鐘視窗；可調長
- **殘留風險**: 誤刪 5 分鐘內未發現 = 永久丟失
- **緩解**: 調長清理週期：`mc admin config set ALIAS api delete_cleanup_interval=2h`；關鍵桶啟用 versioning

### S-43. xl.meta v2 inline data 損壞 → 整物件損壞
- **觸發**: 小物件 (≤128 KiB) 的資料嵌入 xl.meta；該 xl.meta 損壞
- **程式碼路徑**: `cmd/xl-storage-format-v2.go`、`cmd/xl-storage-meta-inline.go`
- **後果**: 物件後設資料 + 資料同時損壞；EC 仍可恢復（其他盤的 xl.meta 包含相同 inline data）
- **MinIO 防護**: 物件級 EC 也覆蓋 inline data
- **殘留風險**: 與 non-inline 物件一致，無額外風險
- **緩解**: 不需要

### S-44. 寫入瞬間所有節點 reboot（電源故障）
- **觸發**: 資料中心斷電；所有節點同時重啟
- **程式碼路徑**: 各種 in-flight 狀態
- **後果**: 與 S-08 + S-12 組合；in-flight write 失敗；已 commit 的物件透過 EC 恢復
- **MinIO 防護**: 啟動時全盤 fsck-like 路徑；NewDisk heal 自動修復
- **殘留風險**: 啟動期間叢集不可用（數十秒到數分鐘）
- **緩解**: UPS；分散電源故障域

### S-45. Bitrot 觸發的 healing 過程中 part 檔案被併發 GET
- **觸發**: read-time bitrot 檢測發現 part 損壞 → 觸發 inline 修復 → 同時另一客戶端 GET 同物件
- **程式碼路徑**: `cmd/erasure-healing.go` + `cmd/erasure-object.go:GetObjectNInfo`
- **後果**: 第二客戶端可能在修復完成前讀到同一損壞 part；MinIO 在 EC decode 時跳過損壞 part
- **MinIO 防護**: EC decode 自動用其他 part 恢復
- **殘留風險**: 僅當損壞 part 數 > parity 時不可恢復
- **緩解**: 啟用 bitrotscan 減少損壞堆積

---

## 風險等級矩陣

按"機率 × 嚴重度"二維表分類：

| | **臨時不可用** | **可恢復** | **靜默損壞** | **永久丟失** |
|---|---|---|---|---|
| **常見** | S-01, S-04, S-05, S-12, S-37 | S-02, S-07, S-10, S-30, S-31 | — | — |
| **偶發** | S-11, S-16, S-19, S-36, S-40, S-41 | S-03, S-08, S-09, S-20, S-27, S-28, S-29, S-32, S-44, S-45 | S-26 | S-17, S-18, S-22, S-23, S-42 |
| **極少** | — | S-13, S-39, S-43 | S-22 (冷資料) | **S-15, S-25, S-33**, S-38 |

**最危險三角**（極少 × 永久丟失）：S-15（誤配 EC:1）、S-25 / S-33（dangling 誤刪）、S-38（站點腦裂）。

---

## 運維 checklist（按風險等級倒序）

### 必須（生產環境強制）
1. **storage class parity ≥ EC:4** — 防 S-15
2. **bitrotscan = 1m 或更短** — 防 S-22；命令：`mc admin config set ALIAS heal bitrotscan=1m`
3. **關鍵桶啟用 versioning + Object Lock + Retention** — 防 S-17, S-23, S-42
4. **NTP/chrony 嚴格時鐘同步** — 防 S-26, S-38
5. **運維 SOP：單批次換盤 ≤ parity-1，換盤後強制 heal** — 防 S-25, S-33
6. **跨機櫃部署節點拓撲（節點編號 % set_size 分散）** — 防 S-12

### 建議
7. 縮短 `STALE_UPLOADS_EXPIRY` 到 2-6h — 防 S-01, S-02 佔盤
8. 調長 `DELETE_CLEANUP_INTERVAL` 到 2h — 防 S-42
9. ILM 應用前先 `mc ilm rule run --dry-run` — 防 S-17
10. lifecycle 配置納入 IaC — 防 S-18
11. KMS 高可用部署 + 備份 master key — 防 S-41
12. 加 site replication 後立即跑 batch — 防 S-21
13. UPS 防全叢集同時斷電 — 防 S-44
14. 監控 `minio_heal_objects_pending`、`minio_node_drive_total_writes_total{api="MRF"}`、`minio_cluster_drive_offline_count` — 早期發現 S-09, S-14

### 可選
15. 縮短 `MRF queueInterval` 到 2-3 分鐘 — 減小 S-09 丟失視窗
16. 啟用 `mc admin replicate status` 自動告警分歧 — 早期發現 S-38
17. 業務路由層隔離同一資源到單 region — 進一步防 S-38
18. 監控每 pool 用量 + 80% 告警 — 防 S-16

---

## 程式碼層面建議（B-N）

### B-1. 加強 dangling 刪除的安全檢查
- **位置**: `cmd/erasure-healing.go:isObjectDangling:1046-1054`
- **問題**: `notFoundMetaErrs > parityBlocks` 不足以保證"物件真的不可恢復"，特別是大批次換盤場景
- **建議**: 引入"近期 healing 狀態"檢查 —— 如果該 set 正在 NewDisk-heal 中，禁止 deleteIfDangling；同時記錄每次 dangling 刪除到 audit log（帶觸發原因）
- **實施**: 增加 `if globalBackgroundHealState.isAnyDriveHealing(set) { return validMeta, false }` 檢查

### B-2. MRF 持久化寫 quorum 多盤
- **位置**: `cmd/mrf.go:saveMRFEntries:145-152`
- **問題**: `for _, localDrive ... if err == nil { break }` 只寫第一塊成功盤
- **建議**: 改為寫到 `min(N, 3)` 塊本地盤並需 quorum 成功；啟動讀時挑最新版本
- **實施**: 用類似 `xl.meta` 的多副本 + 時間戳排序

### B-3. 刪除 dataScannerForceCompactAtFolders 死程式碼
- **位置**: `cmd/data-scanner.go:55`
- **問題**: 定義了 `dataScannerForceCompactAtFolders = 250000` 但無任何呼叫
- **建議**: 直接刪除，或在 `forceCompact` 路徑加上"如果 children > 250000 強制無條件壓扁"邏輯（防超大目錄 OOM）

### B-4. IAM 增量同步替代全量過載
- **位置**: `cmd/iam-store.go:LoadIAMCache:643`
- **問題**: 週期全量重建有 IO/CPU spike；LDAP 大使用者量下耗時長
- **建議**: 引入"自上次同步以來的變更日誌"概念（參考 etcd watch）；重建只對變更 entry 操作

### B-5. Site replication 引入 HLC 替代 wall-clock LWW
- **位置**: `cmd/site-replication.go:healIAMSystem`
- **問題**: 時鐘漂移導致 LWW 不可靠
- **建議**: 引入 hybrid logical clock（物理時間 + 邏輯計數器），保證因果序

### B-6. CompleteMultipartUpload 冪等化
- **位置**: `cmd/object-multipart-handlers.go:CompleteMultipartUploadHandler`
- **問題**: 客戶端重試可能誤判"物件不存在"
- **建議**: 第二次相同 (uploadId, parts) 呼叫直接返回 200 + 已有 ETag（如果 parts 還能找到）

### B-7. Quota 檢查提前到 ILM expiration 路徑
- **位置**: `cmd/bucket-lifecycle.go:applyAction`
- **問題**: ILM 觸發的 batch 刪除不經過 quota 檢查（雖然這是減少用量），但應該有審計
- **建議**: 所有 ILM 刪除寫入獨立 audit log，包含觸發規則 ID

### B-8. cleanupStaleUploads 加上"未達 quorum 的臨時資料" 路徑
- **位置**: `cmd/erasure-multipart.go:cleanupStaleUploads:134`
- **問題**: 當前清理每盤獨立掃描；如果僅一盤有殘留（其他盤清完），仍要等到 30h
- **建議**: 啟動時跨盤清理：找出所有未達 quorum 的臨時資料，立即清理（不等 expiry）

---

## 覆蓋率（本分析依據）

| 檔案 | 閱讀範圍 |
|------|---------|
| `cmd/erasure-healing.go` | 320-475（dangling caller）、985-1060（isObjectDangling 實現）|
| `cmd/erasure-object.go` | 1019-1100 (renameData)、1183-1340 (write quorum)、1564-1607 (commit + addPartial) |
| `cmd/erasure-multipart.go` | 130-250 (cleanupStaleUploads + Stale paths) |
| `cmd/mrf.go` | 110-220（saveMRFEntries + startMRFPersistence）|
| `cmd/bucket-replication.go` | 460-560、1080-1200、1780-1900（MRF 入隊 + retry）|
| `cmd/site-replication.go` | 已透過 06-module-replication.md 摘要核對 |
| `cmd/data-scanner.go` | 50-95（compaction 常量 + getCycleScanMode）、365-440（scanFolder）|
| `cmd/bucket-lifecycle.go` | 460-470（transition workers）|
| `cmd/bucket-quota.go` | 60-140（enforceBucketQuotaHard 全文）|
| `cmd/iam-store.go` | LoadIAMCache 已透過 06-module-api-iam.md 摘要核對 |
| `internal/config/heal/heal.go` | 30-185（全文，含 bitrotscan 預設值）|
| `internal/config/api/api.go` | stale_uploads_*、delete_cleanup_interval 段 |
| `internal/config/storageclass/storage-class.go` | DefaultParityBlocks 已核對 |

依據來自原始碼 + `drafts/06-module-*.md` + `drafts/07-cross-validation.md`。所有 path:line 已對照實際程式碼核實。
