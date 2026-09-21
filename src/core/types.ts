export const PROCESSOR_VERSIONS = [1, 2, 3] as const;
export type ProcessorVersion = (typeof PROCESSOR_VERSIONS)[number];
export const LATEST_VERSION: ProcessorVersion = 3;

export const PROCESSOR_INFO: Record<ProcessorVersion, { name: string; desc: string }> = {
  1: { name: 'count@v1', desc: '按 key 统计事件条数（value 被忽略）' },
  2: { name: 'sum@v2', desc: '按 key 对数值 value 求和' },
  3: { name: 'avg@v3', desc: '按 key 计算平均值（sum/count）' },
};

export interface EventRecord {
  offset: number;
  key: string;
  value: number;
  enqueuedAt: number;
}

export interface PartitionInfo {
  partition: string;
  events: EventRecord[];
}

/** 游标统一为“下一个待处理 offset”。hwm 是输入日志高水位（= 已追加事件数）。 */
export interface Progress {
  offset: number;
  hwm: number;
}

/** 幂等键 -> 最近一次出现位置（集合本身用于去重判定） */
export type IdempotencySeen = Record<string, string>;

export interface AggV1 {
  count: number;
}
export interface AggV2 {
  count: number;
  sum: number;
}
export interface AggV3 {
  count: number;
  sum: number;
  avg: number;
}
export type AggState = AggV1 | AggV2 | AggV3;

export interface KeyedAgg {
  key: string;
  state: AggState;
}

export interface ProcessorMeta {
  name: string;
  version: ProcessorVersion;
}

export interface Counters {
  /** 至少一次：经过处理器的次数（含重复投递） */
  delivered: number;
  /** 有效状态变更次数（被幂等键挡下的重复不计） */
  effective: number;
  duplicates: number;
}

export interface CheckpointData {
  kind: 'checkpoint';
  seq: number;
  createdAt: number;
  reason: 'manual' | 'periodic' | 'batch' | 'migrate';
  processor: ProcessorMeta;
  progress: Record<string, Progress>;
  aggregates: Record<string, AggState>;
  seen: IdempotencySeen;
  counters: Counters;
  /** 本检查点生成后，被一次崩溃丢弃的处理尝试数（教学用） */
  replayedAttempts: number;
}

export interface CheckpointFile {
  name: string;
  seq: number;
  data: CheckpointData;
}

export type FaultPoint =
  | 'beforeStateWrite'
  | 'afterStateWrite'
  | 'beforeOffsetCommit'
  | 'beforeCheckpointRename'
  | 'afterCheckpointRename'
  | 'beforeMigrationCommit';

export type EngineStatus = 'ready' | 'running' | 'paused' | 'crashed' | 'awaiting_migration';

export interface MigrationRecord {
  id: string;
  seq: number;
  timestamp: number;
  fromVersion: ProcessorVersion;
  toVersion: ProcessorVersion;
  success: boolean;
  reason: string;
  sourceCheckpoint: string | null;
  resultCheckpoint: string | null;
  /** 迁移发生时的分区进度快照，供空实例确定性重放定位迁移边界 */
  progress?: Record<string, Progress>;
}

export interface CrashRecord {
  timestamp: number;
  fault: FaultPoint;
  phase: string;
  detail: string;
  /** 崩溃瞬间的内存游标；恢复时以检查点为准，而非这些位置 */
  liveProgress: Record<string, Progress>;
  checkpointProgress: Record<string, Progress>;
}

export interface ExportBundle {
  format: 'checkpoint-bench-export';
  version: 1;
  exportedAt: number;
  deployedVersion: ProcessorVersion;
  partitions: PartitionInfo[];
  migrations: MigrationRecord[];
  finalCheckpoint: CheckpointData | null;
  finalStateHash: string;
}

export interface ReplayResult {
  matches: boolean;
  exportedHash: string;
  replayHash: string;
}
