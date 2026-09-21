export type ProcessorVersion = 1 | 2;

export interface LabEvent {
  id: string;
  partition: number;
  offset: number;
  key: string;
  value: number;
  idemKey: string;
}

export interface PartitionInfo {
  id: number;
  hwm: number;
}

export interface Aggregate {
  key: string;
  sum: number;
  count: number;
  sumSq: number;
}

export interface AggregatorState {
  version: ProcessorVersion;
  aggregates: Record<string, Aggregate>;
  seenIdem: string[];
}

export interface Stats {
  deliveries: number;
  effectiveChanges: number;
}

export type WalStage = "prepared" | "committed";

export interface WalRecord {
  seq: number;
  stage: WalStage;
  partition: number;
  offset: number;
  eventId: string;
  key: string;
  value: number;
  idemKey: string;
  delta?: Aggregate;
}

export interface PartitionProgress {
  committed: number;
  hwm: number;
}

export interface CheckpointData {
  kind: "checkpoint";
  seq: number;
  walBaseSeq: number;
  processorVersion: ProcessorVersion;
  progress: Record<number, PartitionProgress>;
  state: AggregatorState;
  stats: Stats;
  createdAt: string;
  checksum: string;
}

export interface MigrationRecord {
  id: string;
  fromVersion: ProcessorVersion;
  toVersion: ProcessorVersion;
  sourceSeq: number;
  targetSeq: number;
  ok: boolean;
  reason?: string;
  at: string;
}

export interface LabConfig {
  processorVersion: ProcessorVersion;
  checkpointEvery: number;
  checkpointIntervalMs: number;
}

export type CrashPoint =
  | "beforeStateWrite"
  | "afterStateWriteBeforeCommit"
  | "beforeOffsetCommit"
  | "beforeCheckpointRename"
  | "afterCheckpointRename";

export type RunState = "running" | "paused" | "crashed" | "needsMigration";

export interface LabStatus {
  runState: RunState;
  processorVersion: ProcessorVersion;
  partitions: Array<{
    id: number;
    hwm: number;
    displayedCursor: number;
    committedOffset: number;
  }>;
  aggregates: Aggregate[];
  stateHash: string;
  stats: Stats;
  config: LabConfig;
  checkpoints: Array<{
    seq: number;
    file: string;
    processorVersion: ProcessorVersion;
    createdAt: string;
    valid: boolean;
  }>;
  migrations: MigrationRecord[];
  lastCrash?: { point: CrashPoint; at: string } | null;
  recoveredFromSeq: number | null;
  replayedCommits: number;
  pendingCrashPoint: CrashPoint | null;
}

export interface ExportBundle {
  format: "checkpoint-lab/v1";
  exportedAt: string;
  config: LabConfig;
  partitions: number;
  events: LabEvent[];
  checkpoints: { file: string; data: CheckpointData }[];
  wal: WalRecord[];
  migrations: MigrationRecord[];
  finalStateHash: string;
}
