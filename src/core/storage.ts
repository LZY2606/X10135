import type {
  CheckpointEnvelope,
  InputEvent,
  MigrationRecord
} from './types';

export interface CheckpointHooks {
  beforeWrite?: () => void;
  beforeRename?: () => void;
  afterRename?: () => void;
}

export interface LabStorage {
  ensurePartition(partition: string): Promise<void>;
  listPartitions(): Promise<string[]>;
  appendEvent(event: InputEvent): Promise<void>;
  readEvents(partition?: string): Promise<InputEvent[]>;
  appendMigration(record: MigrationRecord): Promise<void>;
  listMigrations(): Promise<MigrationRecord[]>;
  commitCheckpoint(envelope: CheckpointEnvelope, hooks?: CheckpointHooks): Promise<void>;
  listCheckpoints(): Promise<CheckpointEnvelope[]>;
  clearAll(): Promise<void>;
}
