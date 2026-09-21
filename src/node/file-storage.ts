import { mkdir, readdir, readFile, rename, rm, writeFile, appendFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { buildEnvelope, isValidEnvelope } from '../core/checkpoint';
import { canonicalJson } from '../core/hash';
import type { CheckpointHooks, LabStorage } from '../core/storage';
import type {
  CheckpointData,
  CheckpointEnvelope,
  InputEvent,
  MigrationRecord
} from '../core/types';

export class FileStorage implements LabStorage {
  private readonly eventsDir: string;
  private readonly checkpointsDir: string;
  private readonly migrationsPath: string;
  private readonly partitionsPath: string;

  constructor(private readonly root: string) {
    this.eventsDir = join(root, 'events');
    this.checkpointsDir = join(root, 'checkpoints');
    this.migrationsPath = join(root, 'migrations.jsonl');
    this.partitionsPath = join(root, 'partitions.json');
  }

  async ensurePartition(partition: string): Promise<void> {
    await mkdir(this.eventsDir, { recursive: true });
    await mkdir(this.checkpointsDir, { recursive: true });
    const partitions = await this.listPartitions();
    if (!partitions.includes(partition)) {
      partitions.push(partition);
      partitions.sort((left, right) => left.localeCompare(right));
      await writeFile(this.partitionsPath, canonicalJson(partitions), 'utf8');
    }
    await mkdir(join(this.eventsDir, this.safe(partition)), { recursive: true });
  }

  async listPartitions(): Promise<string[]> {
    try {
      return JSON.parse(await readFile(this.partitionsPath, 'utf8')) as string[];
    } catch {
      return [];
    }
  }

  async appendEvent(event: InputEvent): Promise<void> {
    await this.ensurePartition(event.partition);
    await appendFile(join(this.eventsDir, this.safe(event.partition), 'log.jsonl'), `${JSON.stringify(event)}\n`, 'utf8');
  }

  async readEvents(partition?: string): Promise<InputEvent[]> {
    const partitions = partition ? [partition] : await this.listPartitions();
    const events: InputEvent[] = [];
    for (const name of partitions) {
      try {
        const content = await readFile(join(this.eventsDir, this.safe(name), 'log.jsonl'), 'utf8');
        for (const line of content.split('\n')) {
          if (line.trim()) {
            events.push(JSON.parse(line) as InputEvent);
          }
        }
      } catch {
        // Empty partition has no event file.
      }
    }
    return partition
      ? events
      : events.sort((left, right) =>
          left.partition.localeCompare(right.partition) ||
          left.offset - right.offset ||
          left.appendedAt - right.appendedAt
        );
  }

  async appendMigration(record: MigrationRecord): Promise<void> {
    await mkdir(dirname(this.migrationsPath), { recursive: true });
    await appendFile(this.migrationsPath, `${JSON.stringify(record)}\n`, 'utf8');
  }

  async listMigrations(): Promise<MigrationRecord[]> {
    try {
      const content = await readFile(this.migrationsPath, 'utf8');
      return content
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line) as MigrationRecord);
    } catch {
      return [];
    }
  }

  async commitCheckpoint(envelope: CheckpointEnvelope, hooks?: CheckpointHooks): Promise<void> {
    hooks?.beforeWrite?.();
    const id = envelope.checkpoint.id;
    const temp = join(this.checkpointsDir, `.tmp-${this.safe(id)}`);
    const finalPath = join(this.checkpointsDir, this.safe(id));
    await rm(temp, { recursive: true, force: true });
    await mkdir(temp, { recursive: true });

    const checkpoint: CheckpointData = envelope.checkpoint;
    await writeFile(join(temp, 'metadata.json'), canonicalJson({
      kind: envelope.kind,
      formatVersion: envelope.formatVersion,
      id: checkpoint.id,
      createdAt: checkpoint.createdAt,
      processorVersion: checkpoint.processorVersion
    }));
    await writeFile(join(temp, 'progress.json'), canonicalJson(checkpoint.progress));
    await writeFile(join(temp, 'high-watermarks.json'), canonicalJson(checkpoint.highWatermarks));
    await writeFile(join(temp, 'state.json'), canonicalJson(checkpoint.state));
    await writeFile(join(temp, 'metrics.json'), canonicalJson(checkpoint.metrics));
    await writeFile(join(temp, 'semantic-hash.txt'), envelope.semanticHash, 'utf8');
    await writeFile(join(temp, 'COMPLETE'), envelope.semanticHash, 'utf8');

    hooks?.beforeRename?.();
    await rm(finalPath, { recursive: true, force: true });
    await rename(temp, finalPath);
    hooks?.afterRename?.();
  }

  async listCheckpoints(): Promise<CheckpointEnvelope[]> {
    let names: string[];
    try {
      names = await readdir(this.checkpointsDir);
    } catch {
      return [];
    }

    const checkpoints: CheckpointEnvelope[] = [];
    for (const name of names) {
      if (name.startsWith('.tmp-')) {
        continue;
      }
      const directory = join(this.checkpointsDir, name);
      try {
        await readFile(join(directory, 'COMPLETE'), 'utf8');
        const metadata = JSON.parse(await readFile(join(directory, 'metadata.json'), 'utf8'));
        const checkpoint: CheckpointData = {
          id: metadata.id,
          createdAt: metadata.createdAt,
          processorVersion: metadata.processorVersion,
          progress: JSON.parse(await readFile(join(directory, 'progress.json'), 'utf8')),
          highWatermarks: JSON.parse(await readFile(join(directory, 'high-watermarks.json'), 'utf8')),
          state: JSON.parse(await readFile(join(directory, 'state.json'), 'utf8')),
          metrics: JSON.parse(await readFile(join(directory, 'metrics.json'), 'utf8'))
        };
        const envelope = {
          kind: metadata.kind,
          formatVersion: metadata.formatVersion,
          checkpoint,
          semanticHash: await readFile(join(directory, 'semantic-hash.txt'), 'utf8')
        } satisfies CheckpointEnvelope;
        if (await isValidEnvelope(envelope)) {
          checkpoints.push(envelope);
        }
      } catch {
        // Half-written or corrupted checkpoint is intentionally ignored.
      }
    }
    return checkpoints.sort((left, right) => left.checkpoint.createdAt - right.checkpoint.createdAt);
  }

  async clearAll(): Promise<void> {
    await rm(this.root, { recursive: true, force: true });
    await mkdir(this.root, { recursive: true });
  }

  private safe(value: string): string {
    return encodeURIComponent(value).replace(/%2F/gi, '_');
  }
}

export async function writeCheckpoint(storage: LabStorage, checkpoint: CheckpointData, hooks?: CheckpointHooks) {
  await storage.commitCheckpoint(await buildEnvelope(checkpoint), hooks);
}
