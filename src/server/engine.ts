import { randomUUID } from "node:crypto";
import type {
  AggregatorState,
  CheckpointData,
  CrashPoint,
  ExportBundle,
  LabConfig,
  LabEvent,
  LabStatus,
  MigrationRecord,
  PartitionInfo,
  PartitionProgress,
  ProcessorVersion,
  RunState,
  Stats,
  WalRecord,
} from "../shared/types.js";
import {
  applyEvent,
  emptyState,
  listAggregates,
  migrateState,
  stableStringify,
  stateHash,
} from "./aggregator.js";
import {
  appendJsonl,
  ensureDir,
  fsyncDir,
  joinPath,
  listFiles,
  pathExists,
  readJson,
  readJsonLax,
  readJsonl,
  readJsonlTolerant,
  removeFile,
  rmSync,
  writeFileSync,
  renameAtomic,
  sha256,
  writeJson,
} from "./storage.js";

export class CrashInjectedError extends Error {
  constructor(readonly point: CrashPoint) {
    super(`在预设故障点终止: ${point}`);
    this.name = "CrashInjectedError";
  }
}

const CP_PREFIX = "checkpoint-";
const KEEP_CHECKPOINTS = 5;

export interface EngineOptions {
  dataDir: string;
}

export class LabEngine {
  private readonly dirs: {
    root: string;
    partitions: string;
    checkpoints: string;
    wal: string;
  };
  private config: LabConfig;
  private partitions = new Map<number, PartitionInfo>();
  private events: LabEvent[] = [];
  private state: AggregatorState;
  private progress = new Map<number, PartitionProgress>();
  private stats: Stats = { deliveries: 0, effectiveChanges: 0 };
  private cursor = new Map<number, number>();
  private lastServedPartition = -1;
  private walSeq = 0;
  private cpSeq = 0;
  private recoveredFromSeq: number | null = null;
  private replayedCommits = 0;
  private runState: RunState = "paused";
  private pendingCrashPoint: CrashPoint | null = null;
  private lastCrash: { point: CrashPoint; at: string } | null = null;
  private timer: NodeJS.Timeout | null = null;
  private lockChain: Promise<void> = Promise.resolve();

  private constructor(opts: EngineOptions) {
    this.dirs = {
      root: opts.dataDir,
      partitions: joinPath(opts.dataDir, "partitions"),
      checkpoints: joinPath(opts.dataDir, "checkpoints"),
      wal: joinPath(opts.dataDir, "wal"),
    };
    ensureDir(this.dirs.partitions);
    ensureDir(this.dirs.checkpoints);
    ensureDir(this.dirs.wal);
    this.config = {
      processorVersion: 1,
      checkpointEvery: 3,
      checkpointIntervalMs: 0,
    };
    this.state = emptyState(1);
  }

  static async open(opts: EngineOptions): Promise<LabEngine> {
    const engine = new LabEngine(opts);
    await engine.load();
    return engine;
  }

  private locked<T>(fn: () => Promise<T> | T): Promise<T> {
    const run = this.lockChain.then(fn, fn);
    this.lockChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private get configPath(): string {
    return joinPath(this.dirs.root, "config.json");
  }
  private get migrationsPath(): string {
    return joinPath(this.dirs.root, "migrations.jsonl");
  }
  private get walPath(): string {
    return joinPath(this.dirs.wal, "wal.jsonl");
  }
  private crashPath(point: CrashPoint): string {
    return joinPath(this.dirs.root, `crash-${point}.json`);
  }
  private partitionPath(id: number): string {
    return joinPath(this.dirs.partitions, `p${id}.jsonl`);
  }
  private checkpointPath(seq: number): string {
    return joinPath(this.dirs.checkpoints, `${CP_PREFIX}${String(seq).padStart(6, "0")}.json`);
  }
  private checkpointTmpPath(seq: number): string {
    return joinPath(this.dirs.checkpoints, `.tmp-${CP_PREFIX}${String(seq).padStart(6, "0")}.json`);
  }

  private persistConfig(): void {
    writeJson(this.configPath, this.config);
  }

  private async load(): Promise<void> {
    const savedConfig = readJsonLax<LabConfig>(this.configPath);
    if (savedConfig) this.config = savedConfig;

    const crashMarker = listFiles(this.dirs.root)
      .filter((f) => f.startsWith("crash-") && f.endsWith(".json"))[0];
    if (crashMarker) {
      const marker = readJsonLax<{ point: CrashPoint; at: string }>(
        joinPath(this.dirs.root, crashMarker),
      );
      if (marker) this.lastCrash = marker;
    }

    for (const file of listFiles(this.dirs.partitions).filter((f) => f.endsWith(".jsonl"))) {
      const id = Number(file.slice(1, -".jsonl".length));
      if (!Number.isInteger(id)) continue;
      const records = readJsonl<LabEvent>(this.partitionPath(id));
      const hwm = records.length === 0 ? 0 : records[records.length - 1]!.offset + 1;
      this.partitions.set(id, { id, hwm });
      this.events.push(...records);
      this.progress.set(id, { committed: 0, hwm });
      this.cursor.set(id, 0);
    }

    const valid: CheckpointData[] = [];
    for (const file of listFiles(this.dirs.checkpoints)) {
      if (!file.startsWith(CP_PREFIX)) continue;
      const parsed = this.readCheckpointFile(joinPath(this.dirs.checkpoints, file));
      if (parsed) valid.push(parsed);
    }
    valid.sort((a, b) => a.seq - b.seq);
    const latest = valid[valid.length - 1] ?? null;

    const walOutcome = readJsonlTolerant<WalRecord>(this.walPath);
    const rawWal = walOutcome.records;

    if (latest) {
      this.recoveredFromSeq = latest.seq;
      this.cpSeq = latest.seq;
      this.state = latest.state;
      this.stats = { ...latest.stats };
      for (const [id, p] of Object.entries(latest.progress)) {
        this.progress.set(Number(id), { ...p });
        this.cursor.set(Number(id), p.committed);
      }
      if (latest.processorVersion !== this.config.processorVersion) {
        this.runState = "needsMigration";
      }
    } else {
      this.state = emptyState(this.config.processorVersion);
    }
    this.replayWal(rawWal, latest);

    for (const [id, info] of this.partitions) {
      const p = this.progress.get(id);
      if (p) p.hwm = info.hwm;
      else this.progress.set(id, { committed: 0, hwm: info.hwm });
      if (!this.cursor.has(id)) this.cursor.set(id, this.progress.get(id)!.committed);
    }

    for (const marker of listFiles(this.dirs.root).filter(
      (f) => f.startsWith("crash-") && f.endsWith(".json"),
    )) {
      removeFile(joinPath(this.dirs.root, marker));
    }
    fsyncDir(this.dirs.root);
  }

  private readCheckpointFile(path: string): CheckpointData | null {
    try {
      const data = readJson<CheckpointData>(path);
      if (data.kind !== "checkpoint") return null;
      const { checksum, ...body } = data;
      if (sha256(stableStringify(body)) !== checksum) return null;
      if (!data.state || !data.progress || !data.stats) return null;
      return data;
    } catch {
      return null;
    }
  }

  private replayWal(rawWal: WalRecord[], checkpoint: CheckpointData | null): void {
    const base = checkpoint ? checkpoint.walBaseSeq : 0;
    const bySeq = new Map<number, WalRecord>();
    let maxSeq = checkpoint ? checkpoint.walBaseSeq : 0;
    const surviving: WalRecord[] = [];

    for (const rec of rawWal) {
      if (rec.seq <= base) continue;
      if (rec.stage === "prepared") {
        maxSeq = Math.max(maxSeq, rec.seq);
        continue;
      }
      bySeq.set(rec.seq, rec);
      maxSeq = Math.max(maxSeq, rec.seq);
    }

    const committed = [...bySeq.values()].sort((a, b) => a.seq - b.seq);
    for (const rec of committed) {
      this.applyCommitted(rec);
      surviving.push(rec);
    }
    this.commitsSinceCheckpoint = committed.length;

    this.replayedCommits = committed.length;
    this.walSeq = maxSeq;

    const hasDangling = rawWal.some(
      (r) => r.stage === "prepared" && r.seq > base,
    );
    if (hasDangling || surviving.length !== rawWal.length) {
      this.rewriteWal(surviving);
    }
  }

  private rewriteWal(records: WalRecord[]): void {
    const tmp = `${this.walPath}.rewrite`;
    const body =
      records.map((r) => JSON.stringify(r)).join("\n") +
      (records.length ? "\n" : "");
    writeFileSync(tmp, body);
    renameAtomic(tmp, this.walPath);
    fsyncDir(this.dirs.wal);
  }

  private rotateWal(): void {
    writeFileSync(this.walPath, "");
    fsyncDir(this.dirs.wal);
  }

  private applyCommitted(rec: WalRecord): void {
    const result = applyEvent(this.state, rec.key, rec.value, rec.idemKey);
    this.stats.deliveries += 1;
    if (result.changed) this.stats.effectiveChanges += 1;
    const p = this.progress.get(rec.partition) ?? {
      committed: rec.offset,
      hwm: rec.offset + 1,
    };
    p.committed = rec.offset + 1;
    this.progress.set(rec.partition, p);
    this.cursor.set(rec.partition, rec.offset + 1);
  }

  async addPartition(): Promise<{ id: number }> {
    return this.locked(() => {
      const used = [...this.partitions.keys()];
      const id = used.length === 0 ? 0 : Math.max(...used) + 1;
      ensureDir(this.dirs.partitions);
      writeFileSync(this.partitionPath(id), "");
      this.partitions.set(id, { id, hwm: 0 });
      this.progress.set(id, { committed: 0, hwm: 0 });
      this.cursor.set(id, 0);
      fsyncDir(this.dirs.partitions);
      return { id };
    });
  }

  async appendEvent(input: {
    partition: number;
    key: string;
    value: number;
    idemKey?: string;
  }): Promise<LabEvent> {
    return this.locked(() => {
      const info = this.partitions.get(input.partition);
      if (!info) throw new Error(`分区不存在: ${input.partition}`);
      const event: LabEvent = {
        id: randomUUID(),
        partition: input.partition,
        offset: info.hwm,
        key: input.key,
        value: input.value,
        idemKey: input.idemKey ?? `p${input.partition}:${info.hwm}`,
      };
      appendJsonl(this.partitionPath(input.partition), event);
      info.hwm += 1;
      this.events.push(event);
      const p = this.progress.get(input.partition)!;
      p.hwm = info.hwm;
      return event;
    });
  }

  private eventAt(partitionId: number, offset: number): LabEvent | null {
    return this.events.find(
      (e) => e.partition === partitionId && e.offset === offset,
    ) ?? null;
  }

  private nextEvent(): LabEvent | null {
    const ready = [...this.partitions.values()]
      .filter((info) => (this.cursor.get(info.id) ?? 0) < info.hwm)
      .sort((a, b) => a.id - b.id);
    if (ready.length === 0) return null;
    let chosen = ready[0]!;
    let best = this.lastServedPartition;
    for (const info of ready) {
      if (info.id > best) {
        chosen = info;
        break;
      }
    }
    const offset = this.cursor.get(chosen.id) ?? 0;
    this.lastServedPartition = chosen.id;
    return this.eventAt(chosen.id, offset);
  }

  async step(partition?: number): Promise<{ processed: boolean }> {
    return this.locked(() => {
      this.ensureRunnable();
      let event = this.nextEvent();
      if (partition !== undefined) {
        const info = this.partitions.get(partition);
        if (!info) throw new Error(`分区不存在: ${partition}`);
        const pos = this.cursor.get(partition) ?? 0;
        event = pos < info.hwm ? this.eventAt(partition, pos) : null;
      }
      if (!event) return { processed: false };
      this.processEvent(event);
      return { processed: true };
    });
  }

  async processBatch(count: number): Promise<{ processed: number }> {
    return this.locked(() => {
      this.ensureRunnable();
      let processed = 0;
      for (let i = 0; i < count; i++) {
        const event = this.nextEvent();
        if (!event) break;
        this.processEvent(event);
        processed += 1;
      }
      return { processed };
    });
  }

  private ensureRunnable(): void {
    if (this.runState === "crashed") throw new Error("进程已崩溃，请先恢复（重启）");
    if (this.runState === "needsMigration") {
      throw new Error("处理器版本不匹配，需要显式迁移旧检查点");
    }
  }

  private processEvent(event: LabEvent): void {
    const seq = this.walSeq + 1;
    this.walSeq = seq;

    this.fireCrashIfArmed("beforeStateWrite");

    const preview = applyEvent(
      cloneState(this.state),
      event.key,
      event.value,
      event.idemKey,
    );
    const prepared: WalRecord = {
      seq,
      stage: "prepared",
      partition: event.partition,
      offset: event.offset,
      eventId: event.id,
      key: event.key,
      value: event.value,
      idemKey: event.idemKey,
      delta: preview.delta ?? undefined,
    };
    appendJsonl(this.walPath, prepared);

    applyEvent(this.state, event.key, event.value, event.idemKey);
    this.stats.deliveries += 1;
    if (preview.changed) this.stats.effectiveChanges += 1;

    this.fireCrashIfArmed("afterStateWriteBeforeCommit");

    const committed: WalRecord = { ...prepared, stage: "committed" };
    this.fireCrashIfArmed("beforeOffsetCommit");
    appendJsonl(this.walPath, committed);

    const p = this.progress.get(event.partition) ?? {
      committed: event.offset,
      hwm: event.offset + 1,
    };
    p.committed = event.offset + 1;
    this.progress.set(event.partition, p);
    this.cursor.set(event.partition, event.offset + 1);

    this.commitsSinceCheckpoint += 1;
    if (
      this.config.checkpointEvery > 0 &&
      this.commitsSinceCheckpoint >= this.config.checkpointEvery
    ) {
      this.makeCheckpoint();
    }
  }

  async armCrash(point: CrashPoint): Promise<{ armed: true }> {
    return this.locked(() => {
      this.pendingCrashPoint = point;
      return { armed: true as const };
    });
  }

  private fireCrashIfArmed(point: CrashPoint): void {
    if (this.pendingCrashPoint !== point) return;
    this.pendingCrashPoint = null;
    const at = new Date().toISOString();
    this.lastCrash = { point, at };
    writeJson(this.crashPath(point), { point, at });
    fsyncDir(this.dirs.root);
    this.stopTimer();
    this.runState = "crashed";
    throw new CrashInjectedError(point);
  }

  private commitsSinceCheckpoint = 0;

  async checkpoint(): Promise<{ seq: number }> {
    return this.locked(() => {
      this.ensureRunnable();
      return this.makeCheckpoint();
    });
  }

  private makeCheckpoint(): { seq: number } {
    const seq = this.cpSeq + 1;
    this.cpSeq = seq;
    const data = {
      kind: "checkpoint" as const,
      seq,
      walBaseSeq: this.walSeq,
      processorVersion: this.config.processorVersion,
      progress: Object.fromEntries(
        [...this.progress.entries()].map(([id, p]) => [id, { ...p }]),
      ) as Record<number, PartitionProgress>,
      state: cloneState(this.state),
      stats: { ...this.stats },
      createdAt: new Date().toISOString(),
    };
    const checksum = sha256(stableStringify(data));
    const complete: CheckpointData = { ...data, checksum };

    const tmp = this.checkpointTmpPath(seq);
    const final = this.checkpointPath(seq);
    writeJson(tmp, complete);
    fsyncDir(this.dirs.checkpoints);

    this.fireCrashIfArmed("beforeCheckpointRename");
    renameAtomic(tmp, final);
    fsyncDir(this.dirs.checkpoints);
    this.fireCrashIfArmed("afterCheckpointRename");

    this.rotateWal();
    this.pruneCheckpoints();
    this.commitsSinceCheckpoint = 0;
    return { seq };
  }

  private pruneCheckpoints(): void {
    const files = listFiles(this.dirs.checkpoints)
      .filter((f) => f.startsWith(CP_PREFIX) && f.endsWith(".json"))
      .sort();
    while (files.length > KEEP_CHECKPOINTS) {
      const old = files.shift()!;
      removeFile(joinPath(this.dirs.checkpoints, old));
    }
  }

  async start(): Promise<{ state: RunState }> {
    return this.locked(() => {
      this.ensureRunnable();
      this.runState = "running";
      this.startTimer();
      return { state: this.runState };
    });
  }

  async pause(): Promise<{ state: RunState }> {
    return this.locked(() => {
      this.stopTimer();
      if (this.runState === "running") this.runState = "paused";
      return { state: this.runState };
    });
  }

  private startTimer(): void {
    this.stopTimer();
    const interval = this.config.checkpointIntervalMs;
    if (interval > 0) {
      this.timer = setInterval(() => {
        this.locked(() => {
          if (this.runState !== "running") return;
          if (this.commitsSinceCheckpoint > 0) this.makeCheckpoint();
        }).catch(() => undefined);
      }, interval);
      this.timer.unref?.();
    }
  }

  private stopTimer(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async close(): Promise<void> {
    this.stopTimer();
    await this.lockChain;
  }

  async setConfig(patch: Partial<LabConfig>): Promise<LabConfig> {
    return this.locked(() => {
      if (patch.checkpointEvery !== undefined) {
        this.config.checkpointEvery = Math.max(0, Math.floor(patch.checkpointEvery));
      }
      if (patch.checkpointIntervalMs !== undefined) {
        this.config.checkpointIntervalMs = Math.max(
          0,
          Math.floor(patch.checkpointIntervalMs),
        );
      }
      if (
        patch.processorVersion !== undefined &&
        patch.processorVersion !== this.config.processorVersion
      ) {
        this.config.processorVersion = patch.processorVersion;
        this.persistConfig();
      }
      this.persistConfig();
      this.startTimer();
      return { ...this.config };
    });
  }

  async migrate(opts: { toVersion: ProcessorVersion }): Promise<MigrationRecord> {
    return this.locked(() => {
      const recordBase = {
        id: randomUUID(),
        toVersion: opts.toVersion,
        at: new Date().toISOString(),
      };
      try {
        if (this.runState !== "needsMigration") {
          throw new Error("当前没有待迁移的旧检查点");
        }
        const files = listFiles(this.dirs.checkpoints)
          .filter((f) => f.startsWith(CP_PREFIX) && f.endsWith(".json"))
          .sort();
        if (files.length === 0) throw new Error("没有可迁移的检查点");
        const latestFile = files[files.length - 1]!;
        const source = this.readCheckpointFile(
          joinPath(this.dirs.checkpoints, latestFile),
        );
        if (!source) throw new Error("最新检查点损坏，无法迁移");
        const fromVersion = source.processorVersion;

        const migratedState = migrateState(source.state, fromVersion, opts.toVersion);
        const seq = this.cpSeq + 1;
        const data = {
          kind: "checkpoint" as const,
          seq,
          walBaseSeq: this.walSeq,
          processorVersion: opts.toVersion,
          progress: source.progress,
          state: migratedState,
          stats: source.stats,
          createdAt: new Date().toISOString(),
        };
        const complete: CheckpointData = {
          ...data,
          checksum: sha256(stableStringify(data)),
        };
        const tmp = this.checkpointTmpPath(seq);
        writeJson(tmp, complete);
        fsyncDir(this.dirs.checkpoints);
        renameAtomic(tmp, this.checkpointPath(seq));
        fsyncDir(this.dirs.checkpoints);

        const record: MigrationRecord = {
          ...recordBase,
          fromVersion,
          sourceSeq: source.seq,
          targetSeq: seq,
          ok: true,
        };
        appendJsonl(this.migrationsPath, record);

        this.state = migratedState;
        this.cpSeq = seq;
        this.recoveredFromSeq = seq;
        this.stats = { ...source.stats };
        for (const [id, p] of Object.entries(source.progress)) {
          this.progress.set(Number(id), { ...p });
          this.cursor.set(Number(id), p.committed);
        }
        this.config.processorVersion = opts.toVersion;
        this.persistConfig();
        this.runState = "paused";
        this.rotateWal();
        this.pruneCheckpoints();
        return record;
      } catch (err) {
        const record: MigrationRecord = {
          ...recordBase,
          fromVersion: this.config.processorVersion,
          sourceSeq: this.cpSeq,
          targetSeq: this.cpSeq,
          ok: false,
          reason: err instanceof Error ? err.message : String(err),
        };
        appendJsonl(this.migrationsPath, record);
        throw err;
      }
    });
  }

  async forceVersion(version: ProcessorVersion): Promise<{ version: ProcessorVersion }> {
    return this.locked(() => {
      if (this.runState === "crashed") throw new Error("进程已崩溃，请先恢复");
      this.config.processorVersion = version;
      this.persistConfig();
      return { version };
    });
  }

  status(): LabStatus {
    return this.buildStatus();
  }

  private buildStatus(): LabStatus {
    const checkpointFiles = listFiles(this.dirs.checkpoints)
      .filter((f) => f.endsWith(".json") && !f.startsWith(".tmp"))
      .sort();
    const checkpoints = checkpointFiles.map((file) => {
      const parsed = this.readCheckpointFile(
        joinPath(this.dirs.checkpoints, file),
      );
      return {
        seq: parsed ? parsed.seq : -1,
        file,
        processorVersion: (parsed?.processorVersion ?? 0) as ProcessorVersion,
        createdAt: parsed?.createdAt ?? "(损坏/未完成)",
        valid: parsed !== null,
      };
    }).sort((a, b) => a.seq - b.seq);

    return {
      runState: this.runState,
      processorVersion: this.config.processorVersion,
      partitions: [...this.partitions.values()]
        .sort((a, b) => a.id - b.id)
        .map((info) => ({
          id: info.id,
          hwm: info.hwm,
          displayedCursor: this.cursor.get(info.id) ?? 0,
          committedOffset: this.progress.get(info.id)?.committed ?? 0,
        })),
      aggregates: listAggregates(this.state),
      stateHash: stateHash(this.state),
      stats: { ...this.stats },
      config: { ...this.config },
      checkpoints,
      migrations: readJsonl<MigrationRecord>(this.migrationsPath),
      lastCrash: this.lastCrash,
      recoveredFromSeq: this.recoveredFromSeq,
      replayedCommits: this.replayedCommits,
      pendingCrashPoint: this.pendingCrashPoint,
    };
  }

  async exportBundle(): Promise<ExportBundle> {
    return this.locked(() => {
      const events = [...this.events].sort((a, b) =>
        a.partition === b.partition
          ? a.offset - b.offset
          : a.partition - b.partition,
      );
      const checkpoints = listFiles(this.dirs.checkpoints)
        .filter((f) => f.startsWith(CP_PREFIX) && f.endsWith(".json"))
        .sort()
        .flatMap((file) => {
          const data = this.readCheckpointFile(
            joinPath(this.dirs.checkpoints, file),
          );
          return data ? [{ file, data }] : [];
        });
      return {
        format: "checkpoint-lab/v1",
        exportedAt: new Date().toISOString(),
        config: { ...this.config },
        partitions: this.partitions.size,
        events,
        checkpoints,
        wal: readJsonl<WalRecord>(this.walPath),
        migrations: readJsonl<MigrationRecord>(this.migrationsPath),
        finalStateHash: stateHash(this.state),
      };
    });
  }

  static async importInto(
    dataDir: string,
    bundle: ExportBundle,
  ): Promise<{ stateHash: string }> {
    ensureDir(dataDir);
    const occupied = listFiles(dataDir).filter((f) => f !== ".gitkeep");
    if (occupied.length > 0) {
      throw new Error("导入目标目录必须为空实例");
    }
    const engine = new LabEngine({ dataDir });
    ensureDir(engine.dirs.partitions);
    ensureDir(engine.dirs.checkpoints);
    ensureDir(engine.dirs.wal);
    writeJson(engine.configPath, bundle.config);
    const byPartition = new Map<number, LabEvent[]>();
    for (const ev of bundle.events) {
      const list = byPartition.get(ev.partition) ?? [];
      list.push(ev);
      byPartition.set(ev.partition, list);
    }
    for (const [pid, list] of byPartition) {
      writeFileSync(engine.partitionPath(pid), "");
      for (const ev of list.sort((a, b) => a.offset - b.offset)) {
        appendJsonl(engine.partitionPath(pid), ev);
      }
    }
    for (const cp of bundle.checkpoints) {
      writeJson(joinPath(engine.dirs.checkpoints, cp.file), cp.data);
    }
    for (const rec of bundle.migrations) appendJsonl(engine.migrationsPath, rec);
    writeFileSync(
      engine.walPath,
      bundle.wal.map((r) => JSON.stringify(r)).join("\n") +
        (bundle.wal.length ? "\n" : ""),
    );
    fsyncDir(dataDir);
    await engine.close();

    const reopened = await LabEngine.open({ dataDir });
    const hash = reopened.status().stateHash;
    await reopened.close();
    return { stateHash: hash };
  }

  static async reset(dataDir: string): Promise<void> {
    rmSync(dataDir, { recursive: true, force: true });
    ensureDir(dataDir);
  }
}

function cloneState(state: AggregatorState): AggregatorState {
  return {
    version: state.version,
    aggregates: Object.fromEntries(
      Object.values(state.aggregates).map((agg) => [agg.key, { ...agg }]),
    ),
    seenIdem: [...state.seenIdem],
  };
}
