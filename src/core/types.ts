export type ProcessorVersion = 1 | 2;

export type CrashPoint =
  | 'beforeStateWrite'
  | 'afterStateWrite'
  | 'beforeOffsetCommit'
  | 'beforeCheckpointWrite'
  | 'beforeCheckpointRename'
  | 'afterCheckpointRename';

export interface InputEvent {
  id: string;
  partition: string;
  offset: number;
  key: string;
  value: number;
  idempotencyKey?: string;
  appendedAt: number;
}

export interface AggregateRecord {
  key: string;
  count: number;
  sum: number;
  average?: number;
  lastPartition?: string;
  lastOffset?: number;
}

export interface ProcessorState {
  version: ProcessorVersion;
  records: Record<string, AggregateRecord>;
  seenIdempotencyKeys: string[];
}

export type PartitionProgress = Record<string, number>;
export type HighWatermarks = Record<string, number>;

export interface ProcessorMetrics {
  processAttempts: number;
  effectiveStateChanges: number;
  duplicateDeliveries: number;
  checkpointAttempts: number;
  completedCheckpoints: number;
  crashes: number;
}

export interface CheckpointData {
  id: string;
  createdAt: number;
  processorVersion: ProcessorVersion;
  progress: PartitionProgress;
  highWatermarks: HighWatermarks;
  state: ProcessorState;
  metrics: ProcessorMetrics;
}

export interface CheckpointEnvelope {
  kind: 'pairwise-gsb-checkpoint';
  formatVersion: 1;
  checkpoint: CheckpointData;
  semanticHash: string;
}

export type MigrationStatus = 'succeeded' | 'failed';

export interface MigrationRecord {
  id: string;
  fromVersion: ProcessorVersion;
  toVersion: ProcessorVersion;
  sourceCheckpointId: string;
  targetCheckpointId?: string;
  status: MigrationStatus;
  error?: string;
  startedAt: number;
  finishedAt: number;
}

export interface ExperimentBundle {
  kind: 'pairwise-gsb-experiment';
  formatVersion: 1;
  exportedAt: number;
  partitions: string[];
  events: InputEvent[];
  checkpoints: CheckpointEnvelope[];
  migrations: MigrationRecord[];
  finalProcessorVersion: ProcessorVersion;
  finalProgress: PartitionProgress;
  finalHighWatermarks: HighWatermarks;
  expectedSemanticHash: string;
}

export interface RuntimeSnapshot {
  status: 'stopped' | 'paused' | 'running' | 'crashed';
  processorVersion: ProcessorVersion;
  partitions: string[];
  progress: PartitionProgress;
  highWatermarks: HighWatermarks;
  state: ProcessorState;
  metrics: ProcessorMetrics;
  latestCheckpointId?: string;
  armedCrashPoint?: CrashPoint;
  statusMessage: string;
}

export class SimulatedCrash extends Error {
  constructor(public readonly crashPoint: CrashPoint) {
    super(`模拟进程崩溃：${crashPoint}`);
    this.name = 'SimulatedCrash';
  }
}

export class MigrationRequiredError extends Error {
  constructor(
    public readonly storedVersion: ProcessorVersion,
    public readonly requestedVersion: ProcessorVersion,
    public readonly checkpointId: string
  ) {
    super(`检查点处理器版本为 v${storedVersion}，请求 v${requestedVersion}；必须显式迁移`);
    this.name = 'MigrationRequiredError';
  }
}

export function emptyMetrics(): ProcessorMetrics {
  return {
    processAttempts: 0,
    effectiveStateChanges: 0,
    duplicateDeliveries: 0,
    checkpointAttempts: 0,
    completedCheckpoints: 0,
    crashes: 0
  };
}

export function emptyState(version: ProcessorVersion): ProcessorState {
  return { version, records: {}, seenIdempotencyKeys: [] };
}
