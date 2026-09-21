# 检查点实验台（Checkpoint Lab）

一个不依赖 Kafka / 云服务 / 外置数据库的本地流处理检查点实验台，用 TypeScript 实现。

## 安装与运行

```bash
corepack enable
pnpm install --frozen-lockfile
pnpm test -- --run
pnpm dev -- --host 127.0.0.1 --port 5219
```

浏览器打开 <http://127.0.0.1:5219>，页面标题为“检查点实验台”。

## 语义模型

- 事件按分区追加，`offset` 由分区高水位（HWM）分配；同一分区严格有序，分区间没有全局顺序。
- 聚合处理器对 `key` 求和/计数；v2 额外维护平方和 `sumSq`。
- 每条事件走两阶段本地事务日志（WAL）：先追加 `prepared`（状态写入），再追加 `committed`
  （offset 提交）。“一次事件的状态更新与 offset 提交”共享同一事务序号 `seq`，构成原子边界。
- 重复投递会再次经过处理器（`deliveries` 增加），但幂等键（默认 `分区:offset`，可自定义）
  保证 `effectiveChanges` 不重复增加。
- 检查点包含：分区进度、聚合状态（含幂等集合）、处理器版本、输入日志高水位、统计与校验和。
  检查点先写临时文件再 `rename` 原子发布；恢复时校验 checksum，写了一半 / 损坏的检查点被忽略。
- 重启后从**最后一个完整检查点**恢复，再重放其后已提交的 WAL 事务；界面显示的内存游标
  不构成提交语义。
- 处理器版本变化后，旧检查点进入 `needsMigration`，只有显式迁移才可恢复；迁移以
  “写新检查点、不改旧检查点”的方式完成，失败只追加一条失败迁移记录，原检查点保持不变。
- 所有事件、WAL、检查点、迁移记录持久化在本地 `.lab-data/`；可导出实验 JSON 包并导入空实例，
  重放出相同的状态哈希。

## 预设故障点

| 故障点 | 含义 |
| --- | --- |
| `beforeStateWrite` | 写入聚合状态之前终止 |
| `afterStateWriteBeforeCommit` | 状态已改、WAL prepared 之后、提交 offset 之前终止 |
| `beforeOffsetCommit` | committed 记录落盘之前终止 |
| `beforeCheckpointRename` | 检查点临时文件写好、原子 rename 之前终止 |
| `afterCheckpointRename` | 新检查点已发布、WAL 轮转之前终止 |

## 目录结构

- `src/shared/types.ts`：共享类型与实验导出格式。
- `src/server/aggregator.ts`：聚合状态、幂等应用、版本迁移、稳定哈希。
- `src/server/storage.ts`：fsync、原子文件与 JSONL 原语。
- `src/server/engine.ts`：分区、两阶段 WAL、检查点、故障注入、恢复、迁移、导出。
- `src/server/api.ts`：Vite 中间件提供的本地 HTTP API。
- `src/web/`：原生 TypeScript 页面。
- `test/`：vitest 覆盖故障注入恢复、重复幂等、分区独立推进、版本迁移回滚、导出重放。
