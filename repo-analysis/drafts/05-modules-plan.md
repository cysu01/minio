# 模組分析計劃與敘事線

## 報告結構

1. 簡短背景（300字）：MinIO 定位 + 歸檔背景
2. Repo 目錄樹（兩層）+ 各子目錄功能說明
3. 整體元件架構圖（Mermaid）
4. **核心模組 1**：儲存引擎 + Erasure Coding + Quorum
5. **核心模組 2**：Healing 自愈機制（最高優先順序）
6. **核心模組 3**：Replication（Bucket 複製 + Site 複製）
7. **核心模組 4**：Scanner + Life Cycle Manager
8. **核心模組 5**：S3 API 層 + IAM + 鑑權
9. 內部基礎設施（Grid 通訊、Config、Event）
10. Design Patterns 彙總表
11. 評價與啟發

## 敘事線

儲存引擎是基礎（資料如何寫入磁碟）
→ [寫入的資料如果磁碟故障怎麼辦？] →
Healing 自愈機制（確保資料完整性）
→ [單叢集保證了資料安全，跨地域怎麼做？] →
Replication（資料跨節點/跨站點複製）
→ [資料長期堆積，怎麼自動管理生命週期？] →
Scanner + Lifecycle（資料治理）
→ [所有這些功能透過什麼介面暴露給使用者？] →
S3 API 層 + IAM（對外介面與安全）

## 模組清單

| 模組 | 型別 | 主要檔案 | 預估行數 |
|------|------|---------|---------|
| 儲存引擎 + Erasure Coding | 核心 | erasure-*.go, xl-storage*.go | ~25000 |
| Healing 自愈機制 | 核心 | erasure-healing*.go, global-heal.go, background-*.go | ~5000 |
| Bucket Replication | 核心 | bucket-replication*.go, batch-replicate.go | ~6000 |
| Site Replication | 核心 | site-replication*.go | ~7000 |
| Scanner + ILM | 核心 | data-scanner.go, bucket-lifecycle.go, ilm-config.go | ~3000 |
| S3 API + IAM | 核心 | object-handlers.go, api-router.go, auth-handler.go, iam*.go | ~9000 |
| 內部基礎設施 | 次要 | internal/grid, internal/dsync, internal/event | ~15000 |

## 分析模式

深度分析（≥90%覆蓋率）
