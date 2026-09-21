import { isValidEnvelope } from '../core/checkpoint';
import { canonicalJson } from '../core/hash';
import type { CheckpointHooks, LabStorage } from '../core/storage';
import type {
  CheckpointEnvelope,
  InputEvent,
  MigrationRecord
} from '../core/types';

const prefix = 'checkpoint-lab:';

export class LocalStorageLabStorage implements LabStorage {
  constructor(private readonly store: Storage = window.localStorage) {}

  async ensurePartition(partition: string): Promise<void> {
    const partitions = new Set(await this.listPartitions());
    partitions.add(partition);
    this.set('partitions', [...partitions].sort((left, right) => left.localeCompare(right)));
  }

  async listPartitions(): Promise<string[]> {
    return this.get<string[]>('partitions') ?? [];
  }

  async appendEvent(event: InputEvent): Promise<void> {
    await this.ensurePartition(event.partition);
    const events = await this.readEvents();
    this.set('events', [...events, event]);
  }

  async readEvents(partition?: string): Promise<InputEvent[]> {
    const events = this.get<InputEvent[]>('events') ?? [];
    const filtered = partition ? events.filter((event) => event.partition === partition) : events;
    return [...filtered].sort((left, right) =>
      left.partition.localeCompare(right.partition) ||
      left.offset - right.offset ||
      left.appendedAt - right.appendedAt
    );
  }

  async appendMigration(record: MigrationRecord): Promise<void> {
    const records = await this.listMigrations();
    this.set('migrations', [...records, record]);
  }

  async listMigrations(): Promise<MigrationRecord[]> {
    return this.get<MigrationRecord[]>('migrations') ?? [];
  }

  async commitCheckpoint(envelope: CheckpointEnvelope, hooks?: CheckpointHooks): Promise<void> {
    hooks?.beforeWrite?.();
    const tempKey = this.key(`checkpoints-tmp:${envelope.checkpoint.id}`);
    const finalKey = this.key(`checkpoints:${envelope.checkpoint.id}`);
    this.store.removeItem(tempKey);
    this.store.setItem(tempKey, canonicalJson({ ...envelope, complete: false }));
    hooks?.beforeRename?.();
    this.store.removeItem(finalKey);
    this.store.setItem(finalKey, canonicalJson(envelope));
    this.store.removeItem(tempKey);
    hooks?.afterRename?.();
  }

  async listCheckpoints(): Promise<CheckpointEnvelope[]> {
    const result: CheckpointEnvelope[] = [];
    for (let index = 0; index < this.store.length; index += 1) {
      const key = this.store.key(index);
      if (!key?.startsWith(`${prefix}checkpoints:`)) {
        continue;
      }
      try {
        const envelope = JSON.parse(this.store.getItem(key) ?? 'null') as CheckpointEnvelope;
        if (await isValidEnvelope(envelope)) {
          result.push(envelope);
        }
      } catch {
        // Ignore corrupted checkpoint.
      }
    }
    return result.sort((left, right) => left.checkpoint.createdAt - right.checkpoint.createdAt);
  }

  async clearAll(): Promise<void> {
    const removable: string[] = [];
    for (let index = 0; index < this.store.length; index += 1) {
      const key = this.store.key(index);
      if (key?.startsWith(prefix)) {
        removable.push(key);
      }
    }
    removable.forEach((key) => this.store.removeItem(key));
  }

  private get<T>(name: string): T | null {
    const raw = this.store.getItem(this.key(name));
    return raw === null ? null : (JSON.parse(raw) as T);
  }

  private set(name: string, value: unknown): void {
    this.store.setItem(this.key(name), canonicalJson(value));
  }

  private key(name: string): string {
    return `${prefix}${name}`;
  }
}
