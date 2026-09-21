import {
  createHash,
} from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import type { CheckpointData, CrashRecord, EventRecord, MigrationRecord } from './types';

type CrashRecordShape = CrashRecord;

export interface StoredCheckpoint {
  name: string;
  seq: number;
  data: CheckpointData;
}

export interface IgnoredCheckpoint {
  name: string;
  reason: string;
}

interface CheckpointEnvelope {
  checksum: string;
  data: CheckpointData;
}

/** 存储布局：events/<p>.log（每行一个 JSON）；checkpoints/；migrations.jsonl；meta.json */
export class FileStorage {
  readonly root: string;
  readonly eventsDir: string;
  readonly ckptDir: string;
  private readonly metaPath: string;
  private readonly migrationPath: string;
  private readonly crashPath: string;

  constructor(root: string) {
    this.root = root;
    this.eventsDir = join(root, 'events');
    this.ckptDir = join(root, 'checkpoints');
    this.metaPath = join(root, 'meta.json');
    this.migrationPath = join(root, 'migrations.jsonl');
    this.crashPath = join(root, 'crash.json');
    mkdirSync(this.eventsDir, { recursive: true });
    mkdirSync(this.ckptDir, { recursive: true });
  }

  private static checksum(data: unknown): string {
    return createHash('sha256').update(JSON.stringify(data)).digest('hex');
  }

  private partitionFile(partition: string): string {
    return join(this.eventsDir, `${encodeURIComponent(partition)}.log`);
  }

  listPartitions(): string[] {
    return readdirSync(this.eventsDir)
      .filter((f) => f.endsWith('.log'))
      .map((f) => decodeURIComponent(f.slice(0, -'.log'.length)))
      .sort();
  }

  partitionExists(partition: string): boolean {
    return existsSync(this.partitionFile(partition));
  }

  ensurePartition(partition: string): void {
    const path = this.partitionFile(partition);
    if (!existsSync(path)) writeFileSync(path, '');
  }

  appendEvent(partition: string, event: EventRecord): void {
    const path = this.partitionFile(partition);
    writeFileSync(path, JSON.stringify(event) + '\n', { flag: 'a' });
  }

  readEvents(partition: string): EventRecord[] {
    const path = this.partitionFile(partition);
    if (!existsSync(path)) return [];
    const raw = readFileSync(path, 'utf8');
    const out: EventRecord[] = [];
    for (const line of raw.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      out.push(JSON.parse(trimmed) as EventRecord);
    }
    return out;
  }

  readAllPartitions(): { partition: string; events: EventRecord[] }[] {
    return this.listPartitions().map((partition) => ({
      partition,
      events: this.readEvents(partition),
    }));
  }

  readMeta(): { deployedVersion: number } | null {
    if (!existsSync(this.metaPath)) return null;
    return JSON.parse(readFileSync(this.metaPath, 'utf8')) as { deployedVersion: number };
  }

  writeMeta(deployedVersion: number): void {
    this.atomicWrite(this.metaPath, { deployedVersion });
  }

  /**
   * 原子检查点：先写 .partial，再 rename 到最终名。
   * hooks 用于在 rename 前后注入“进程崩溃”。
   */
  writeCheckpoint(
    data: CheckpointData,
    hooks?: {
      beforeRename?: (partialPath: string) => void;
      afterRename?: (finalPath: string) => void;
    },
  ): string {
    const name = `checkpoint-${String(data.seq).padStart(6, '0')}.json`;
    const partialName = `${name}.partial`;
    const partialPath = join(this.ckptDir, partialName);
    const finalPath = join(this.ckptDir, name);
    const envelope: CheckpointEnvelope = {
      checksum: FileStorage.checksum(data),
      data,
    };
    writeFileSync(partialPath, JSON.stringify(envelope, null, 2));
    hooks?.beforeRename?.(partialPath);
    renameSync(partialPath, finalPath);
    hooks?.afterRename?.(finalPath);
    return name;
  }

  /** 扫描检查点目录：完成且校验通过的按 seq 升序返回；半成品 / 损坏文件列入 ignored。 */
  listCheckpoints(): { ok: StoredCheckpoint[]; ignored: IgnoredCheckpoint[] } {
    const ok: StoredCheckpoint[] = [];
    const ignored: IgnoredCheckpoint[] = [];
    for (const name of readdirSync(this.ckptDir).sort()) {
      if (name.startsWith('.') || !name.endsWith('.json')) {
        if (name.endsWith('.partial') || name.startsWith('checkpoint-')) {
          ignored.push({ name, reason: '写到一半的检查点（.partial）' });
        }
        continue;
      }
      try {
        const envelope = JSON.parse(
          readFileSync(join(this.ckptDir, name), 'utf8'),
        ) as CheckpointEnvelope;
        if (!envelope.data || envelope.data.kind !== 'checkpoint') {
          ignored.push({ name, reason: '内容不是合法检查点' });
          continue;
        }
        if (FileStorage.checksum(envelope.data) !== envelope.checksum) {
          ignored.push({ name, reason: '校验和不匹配（损坏 / 截断）' });
          continue;
        }
        ok.push({ name, seq: envelope.data.seq, data: envelope.data });
      } catch {
        ignored.push({ name, reason: 'JSON 无法解析（损坏）' });
      }
    }
    ok.sort((a, b) => a.seq - b.seq);
    return { ok, ignored };
  }

  latestCheckpoint(): StoredCheckpoint | null {
    const { ok } = this.listCheckpoints();
    return ok.length ? ok[ok.length - 1] : null;
  }

  removePartialCheckpoints(): number {
    let removed = 0;
    for (const name of readdirSync(this.ckptDir)) {
      if (name.endsWith('.partial')) {
        rmSync(join(this.ckptDir, name), { force: true });
        removed += 1;
      }
    }
    return removed;
  }

  appendMigration(record: MigrationRecord): void {
    writeFileSync(this.migrationPath, JSON.stringify(record) + '\n', { flag: 'a' });
  }

  readMigrations(): MigrationRecord[] {
    if (!existsSync(this.migrationPath)) return [];
    const raw = readFileSync(this.migrationPath, 'utf8');
    const out: MigrationRecord[] = [];
    for (const line of raw.split('\n')) {
      const trimmed = line.trim();
      if (trimmed) out.push(JSON.parse(trimmed) as MigrationRecord);
    }
    return out;
  }

  saveCrash(record: unknown): void {
    writeFileSync(this.crashPath, JSON.stringify(record, null, 2));
  }

  readCrash(): CrashRecordShape | null {
    if (!existsSync(this.crashPath)) return null;
    try {
      return JSON.parse(readFileSync(this.crashPath, 'utf8')) as CrashRecordShape;
    } catch {
      return null;
    }
  }

  clearCrash(): void {
    rmSync(this.crashPath, { force: true });
  }

  /** 保留 .partial 文件以便恢复时可见并报告；可显式清理。 */
  clearPartialCheckpoints(): number {
    return this.removePartialCheckpoints();
  }

  /** 清空全部实验数据（分区、事件、检查点、迁移、崩溃标记、meta）。 */
  wipe(): void {
    rmSync(this.eventsDir, { recursive: true, force: true });
    rmSync(this.ckptDir, { recursive: true, force: true });
    rmSync(this.migrationPath, { force: true });
    rmSync(this.crashPath, { force: true });
    rmSync(this.metaPath, { force: true });
    mkdirSync(this.eventsDir, { recursive: true });
    mkdirSync(this.ckptDir, { recursive: true });
  }

  /** meta 等小文件也走 tmp+rename，避免半写状态。 */
  private atomicWrite(path: string, data: unknown): void {
    const tmp = `${path}.tmp`;
    writeFileSync(tmp, JSON.stringify(data, null, 2));
    renameSync(tmp, path);
  }
}
