# 模块分析计划与叙事线

## 报告结构

1. 简短背景（300字）：MinIO 定位 + 归档背景
2. Repo 目录树（两层）+ 各子目录功能说明
3. 整体组件架构图（Mermaid）
4. **核心模块 1**：存储引擎 + Erasure Coding + Quorum
5. **核心模块 2**：Healing 自愈机制（最高优先级）
6. **核心模块 3**：Replication（Bucket 复制 + Site 复制）
7. **核心模块 4**：Scanner + Life Cycle Manager
8. **核心模块 5**：S3 API 层 + IAM + 鉴权
9. 内部基础设施（Grid 通信、Config、Event）
10. Design Patterns 汇总表
11. 评价与启发

## 叙事线

存储引擎是基础（数据如何写入磁盘）
→ [写入的数据如果磁盘故障怎么办？] →
Healing 自愈机制（确保数据完整性）
→ [单集群保证了数据安全，跨地域怎么做？] →
Replication（数据跨节点/跨站点复制）
→ [数据长期堆积，怎么自动管理生命周期？] →
Scanner + Lifecycle（数据治理）
→ [所有这些功能通过什么接口暴露给用户？] →
S3 API 层 + IAM（对外接口与安全）

## 模块清单

| 模块 | 类型 | 主要文件 | 预估行数 |
|------|------|---------|---------|
| 存储引擎 + Erasure Coding | 核心 | erasure-*.go, xl-storage*.go | ~25000 |
| Healing 自愈机制 | 核心 | erasure-healing*.go, global-heal.go, background-*.go | ~5000 |
| Bucket Replication | 核心 | bucket-replication*.go, batch-replicate.go | ~6000 |
| Site Replication | 核心 | site-replication*.go | ~7000 |
| Scanner + ILM | 核心 | data-scanner.go, bucket-lifecycle.go, ilm-config.go | ~3000 |
| S3 API + IAM | 核心 | object-handlers.go, api-router.go, auth-handler.go, iam*.go | ~9000 |
| 内部基础设施 | 次要 | internal/grid, internal/dsync, internal/event | ~15000 |

## 分析模式

深度分析（≥90%覆盖率）
