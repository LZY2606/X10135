import { FileStorage } from './storage';
import {
  applyEvent,
  canMigrate,
  migrateState,
  processorName,
  semanticStateHash,
  MigrationError,
} from './processor';
import {
  LATEST_VERSION,
  type AggState,
  type CheckpointData,
  type Counters,
  type CrashRecord,
  type EngineStatus,
  type EventRecord,
  type FaultPoint,
  type IdempotencySeen,
  type MigrationRecord,
  type PartitionInfo,
  type ProcessorVersion,
  type Progress,
} from './types';

export class CrashSimulation extends Error {
  constructor(
    readonly fault: FaultPoint,
    readonly detail: string,
  ) {
    super(`模拟进程崩溃：${fault}`);
    this.name = 'CrashSimulation';
  }
}

export class EngineError extends Error {}

interface RuntimeState {
  status: EngineStatus;
  version: ProcessorVersion;
  progress: Record<string, Progress>;
  aggregates: Record<string, AggState>;
  seen: IdempotencySeen;
  counters: Counters;
  replayedAttempts: number;
  lastCheckpointSeq: number;
  rrIndex: number;
  lastCrash: CrashRecord | null;
  pendingIgnored: number;
}

export interface EngineSnapshot {
  status: EngineStatus;
  deployedVersion: ProcessorVersion;
  progress: Record<string, Progress>;
  aggregates: Record<string, AggState>;
  counters: Counters;
  replayedAttempts: number;
  lastCrash: CrashRecord | null;
  events: Record<string, EventRecord[]>;
  checkpoints: { name: string; seq: number; data: CheckpointData }[];
  ignoredCheckpoints: { name: string; reason: string }[];
  migrations: MigrationRecord[];
  checkpointProgress: Record<string, Progress>;
  checkpointCounters: Counters | null;
  liveStateHash: string;
  checkpointStateHash: string | null;
  rrPartition: string | null;
  lastCheckpointName: string | null;
  lastCheckpointReason: string | null;
  lastRecoveryIgnored: number;
}

const ZERO_COUNTERS: Counters = { delivered: 0, effective: 0, duplicates: 0 };

function cloneProgress(p: Record<string, Progress>): Record<string, Progress> {
  return JSON.parse(JSON.stringify(p)) as Record<string, Progress>;
}

export class BenchEngine {
  private rt: RuntimeState;
  private faults: Map<FaultPoint, boolean> = new Map();
  readonly storage: FileStorage;
  private readonly clock: () => number;

  constructor(storage: FileStorage, clock: () => number = () => Date.now()) {
    this.storage = storage;
    this.clock = clock;
    this.rt = this.bootstrap();
  }

  // ---------- 初始化 / 恢复 ----------

  private bootstrap(): RuntimeState {
    const meta = this.storage.readMeta();
    const deployed = (meta?.deployedVersion ?? LATEST_VERSION) as ProcessorVersion;
    if (!meta) this.storage.writeMeta(deployed);
    const { ignored } = this.storage.listCheckpoints();
    const latest = this.storage.latestCheckpoint();
    const persistedCrash = this.storage.readCrash();

    if (latest) {
      const replayed = this.computeReplayedAttempts(persistedCrash, latest.data);
      return {
        status: latest.data.processor.version !== deployed ? 'awaiting_migration' : 'paused',
        version:
          latest.data.processor.version !== deployed
            ? latest.data.processor.version
            : deployed,
        progress: cloneProgress(latest.data.progress),
        aggregates: structuredClone(latest.data.aggregates),
        seen: structuredClone(latest.data.seen),
        counters: structuredClone(latest.data.counters),
        replayedAttempts: replayed,
        lastCheckpointSeq: latest.data.seq,
        rrIndex: 0,
        lastCrash: persistedCrash ? structuredClone(persistedCrash) : null,
        pendingIgnored: ignored.length,
      };
    }

    return {
      status: 'ready',
      version: deployed,
      progress: {},
      aggregates: {},
      seen: {},
      counters: { ...ZERO_COUNTERS },
      replayedAttempts: 0,
      lastCheckpointSeq: 0,
      rrIndex: 0,
      lastCrash: persistedCrash ? structuredClone(persistedCrash) : null,
      pendingIgnored: ignored.length,
    };
  }

  get status(): EngineStatus {
    return this.rt.status;
  }

  private computeReplayedAttempts(
    crash: CrashRecord | null,
    ckpt: CheckpointData,
  ): number {
    if (!crash) return ckpt.replayedAttempts ?? 0;
    let dropped = 0;
    for (const [partition, live] of Object.entries(crash.liveProgress)) {
      const committed = ckpt.progress[partition]?.offset ?? 0;
      dropped += Math.max(0, live.offset - committed);
    }
    return (ckpt.replayedAttempts ?? 0) + dropped;
  }

  /** 重新从磁盘引导（模拟重启）。重放用：切版本后让引擎进入 awaiting_migration。 */
  reloadFromDisk(): void {
    this.rt = this.bootstrap();
  }

  /** 清空全部持久化数据并回到空实例（存储目录保持不变）。 */
  resetStorage(): void {
    this.storage.wipe();
    this.faults.clear();
    this.rt = this.bootstrap();
  }

  markRunning(): void {
    if (this.rt.status === 'ready' || this.rt.status === 'paused') {
      this.rt.status = 'running';
    }
  }

  markPaused(): void {
    if (this.rt.status === 'running') this.rt.status = 'paused';
  }

  // ---------- 故障注入 ----------

  armFault(point: FaultPoint): void {
    this.faults.set(point, true);
  }

  disarmFault(point: FaultPoint): void {
    this.faults.delete(point);
  }

  listFaults(): FaultPoint[] {
    return [...this.faults.keys()];
  }

  private trigger(
    point: FaultPoint,
    phase: string,
    detail: string,
    displayProgress?: Record<string, Progress>,
  ): never {
    this.faults.delete(point);
    const checkpoint = this.storage.latestCheckpoint();
    const crash: CrashRecord = {
      timestamp: this.clock(),
      fault: point,
      phase,
      detail,
      liveProgress: cloneProgress(displayProgress ?? this.rt.progress),
      checkpointProgress: checkpoint ? cloneProgress(checkpoint.data.progress) : {},
    };
    this.rt.status = 'crashed';
    this.rt.lastCrash = crash;
    this.storage.saveCrash(crash);
    throw new CrashSimulation(point, detail);
  }

  private assertMutable(): void {
    if (this.rt.status === 'crashed') throw new EngineError('进程已崩溃，请先重启恢复');
    if (this.rt.status === 'awaiting_migration') {
      throw new EngineError('检查点版本过旧，需要显式迁移或回滚部署版本');
    }
  }

  // ---------- 分区与输入日志 ----------

  createPartition(partition: string): void {
    if (!/^[A-Za-z0-9_-]+$/.test(partition)) {
      throw new EngineError('分区名只允许字母、数字、下划线与短横线');
    }
    if (this.storage.partitionExists(partition)) {
      throw new EngineError(`分区 ${partition} 已存在`);
    }
    this.storage.ensurePartition(partition);
    if (!this.rt.progress[partition]) {
      this.rt.progress[partition] = { offset: 0, hwm: 0 };
    }
  }

  appendEvent(partition: string, key: string, value: number): EventRecord {
    if (!this.storage.partitionExists(partition)) {
      throw new EngineError(`分区 ${partition} 不存在`);
    }
    if (!key.trim()) throw new EngineError('key 不能为空');
    const events = this.storage.readEvents(partition);
    const event: EventRecord = {
      offset: events.length,
      key: key.trim(),
      value: Number.isFinite(value) ? value : 0,
      enqueuedAt: this.clock(),
    };
    this.storage.appendEvent(partition, event);
    const prog = this.rt.progress[partition] ?? { offset: 0, hwm: 0 };
    prog.hwm = events.length + 1;
    this.rt.progress[partition] = prog;
    return event;
  }

  // ---------- 消费与原子边界 ----------

  private partitionsWithLag(): { partition: string; events: EventRecord[] }[] {
    const out: { partition: string; events: EventRecord[] }[] = [];
    for (const partition of this.storage.listPartitions()) {
      const prog = this.rt.progress[partition] ?? { offset: 0, hwm: 0 };
      if (prog.offset < prog.hwm) {
        out.push({ partition, events: this.storage.readEvents(partition) });
      }
    }
    return out;
  }

  private pickPartition(
    lagging: { partition: string; events: EventRecord[] }[],
    requested?: string,
  ): { partition: string; events: EventRecord[] } {
    if (requested) {
      const hit = lagging.find((l) => l.partition === requested);
      if (!hit) throw new EngineError(`分区 ${requested} 没有待处理事件`);
      return hit;
    }
    if (lagging.length === 0) throw new EngineError('所有分区都已追平，没有可消费事件');
    const names = lagging.map((l) => l.partition).sort();
    const idx = this.rt.rrIndex % names.length;
    const name = names[idx];
    this.rt.rrIndex = (idx + 1) % names.length;
    return lagging.find((l) => l.partition === name)!;
  }

  /**
   * 处理单条事件。状态更新与 offset 提交共享同一原子边界：
   * 在任何预设崩溃点“死亡”，恢复时统一回退到最后一个完整检查点，
   * 重复投递由幂等键（partition:offset）约束，不会二次改变状态。
   */
  consumeOne(requestedPartition?: string): { idempotencyKey: string; duplicate: boolean } {
    this.assertMutable();
    const lagging = this.partitionsWithLag();
    const { partition, events } = this.pickPartition(lagging, requestedPartition);
    const prog = this.rt.progress[partition];
    const event = events[prog.offset];
    const idempotencyKey = `${partition}:${event.offset}`;
    const duplicate = idempotencyKey in this.rt.seen;

    this.rt.counters.delivered += 1;
    if (duplicate) {
      this.rt.counters.duplicates += 1;
      // 重复事件可以再次经过处理器，但结果被丢弃，最终状态不变。
      applyEvent(this.rt.version, this.rt.aggregates[event.key], event);
      prog.offset += 1;
      return { idempotencyKey, duplicate: true };
    }

    // —— 原子边界开始 ——
    const inFlightProgress = (): Record<string, Progress> => {
      const projected = cloneProgress(this.rt.progress);
      projected[partition] = { ...prog, offset: prog.offset + 1 };
      return projected;
    };
    this.faultArmed('beforeStateWrite') &&
      this.trigger(
        'beforeStateWrite',
        '写状态前',
        `准备把 key=${event.key}（${idempotencyKey}）写入聚合状态`,
        inFlightProgress(),
      );

    const { state: nextState } = applyEvent(
      this.rt.version,
      this.rt.aggregates[event.key],
      event,
    );

    this.faultArmed('afterStateWrite') &&
      this.trigger(
        'afterStateWrite',
        '写状态后、提交 offset 前',
        `key=${event.key} 已计算，分区 ${partition} 的 offset 尚未提交`,
        inFlightProgress(),
      );
    this.faultArmed('beforeOffsetCommit') &&
      this.trigger(
        'beforeOffsetCommit',
        '提交 offset 前',
        `即将提交 ${idempotencyKey}，崩溃后该事件会被重复投递`,
        inFlightProgress(),
      );

    this.rt.aggregates[event.key] = nextState;
    this.rt.seen[idempotencyKey] = event.key;
    prog.offset += 1;
    this.rt.counters.effective += 1;
    // —— 原子边界结束 ——

    return { idempotencyKey, duplicate: false };
  }

  private faultArmed(point: FaultPoint): boolean {
    return this.faults.get(point) === true;
  }

  consumeBatch(count: number, requestedPartition?: string): number {
    this.assertMutable();
    let done = 0;
    for (let i = 0; i < count; i += 1) {
      try {
        this.consumeOne(requestedPartition);
        done += 1;
      } catch (err) {
        if (err instanceof CrashSimulation) throw err;
        if (done > 0) return done;
        throw err;
      }
    }
    return done;
  }

  /** 按分区内顺序消费到指定进度（重放用）。分区间顺序只影响合并展示，不影响最终哈希。 */
  consumeUpTo(targets: Record<string, { offset: number; hwm?: number }>): number {
    if (this.rt.status === 'crashed') throw new EngineError('进程已崩溃');
    if (this.rt.status === 'awaiting_migration') {
      throw new EngineError('重放内部错误：迁移点不应有未处理事件');
    }
    let guard = 0;
    let done = 0;
    for (;;) {
      const lagging: { partition: string }[] = [];
      for (const [partition, target] of Object.entries(targets)) {
        const cur = this.rt.progress[partition]?.offset ?? 0;
        if (cur < target.offset) lagging.push({ partition });
      }
      if (lagging.length === 0) return done;
      const { partition } = lagging[guard % lagging.length];
      this.consumeOne(partition);
      done += 1;
      guard += 1;
      if (guard > 1_000_000) throw new EngineError('consumeUpTo 超过安全上限');
    }
  }

  /** 显式重放某分区已经提交过的事件（演示“至少一次”下的重复投递）。 */
  redeliver(partition: string, offset: number): void {
    this.assertMutable();
    const events = this.storage.readEvents(partition);
    const event = events[offset];
    if (!event) throw new EngineError('该 offset 不存在');
    const prog = this.rt.progress[partition];
    if (!prog || offset >= prog.offset) {
      throw new EngineError('只能重复投递已经处理过的事件');
    }
    this.rt.counters.delivered += 1;
    this.rt.counters.duplicates += 1;
    applyEvent(this.rt.version, this.rt.aggregates[event.key], event);
  }

  // ---------- 检查点 ----------

  private nextCheckpointSeq(): number {
    const { ok } = this.storage.listCheckpoints();
    return ok.reduce((m, c) => Math.max(m, c.seq), this.rt.lastCheckpointSeq) + 1;
  }

  checkpoint(reason: CheckpointData['reason'] = 'manual'): string {
    this.assertMutable();
    const seq = this.nextCheckpointSeq();
    const data: CheckpointData = {
      kind: 'checkpoint',
      seq,
      createdAt: this.clock(),
      reason,
      processor: { name: processorName(this.rt.version), version: this.rt.version },
      progress: cloneProgress(this.rt.progress),
      aggregates: structuredClone(this.rt.aggregates),
      seen: structuredClone(this.rt.seen),
      counters: structuredClone(this.rt.counters),
      replayedAttempts: this.rt.replayedAttempts,
    };
    const name = this.storage.writeCheckpoint(data, {
      beforeRename: () => {
        if (this.faultArmed('beforeCheckpointRename')) {
          this.trigger(
            'beforeCheckpointRename',
            '检查点 rename 前',
            `检查点 #${seq} 已写入临时文件但尚未 rename，恢复时必须被忽略`,
          );
        }
      },
      afterRename: () => {
        if (this.faultArmed('afterCheckpointRename')) {
          // rename 已完成，检查点是完整的；崩溃后必须从它恢复。
          this.rt.lastCheckpointSeq = seq;
          this.trigger(
            'afterCheckpointRename',
            '检查点 rename 后',
            `检查点 #${seq} 已原子发布，崩溃后应从它恢复`,
          );
        }
      },
    });
    this.rt.lastCheckpointSeq = seq;
    this.rt.replayedAttempts = 0;
    return name;
  }

  /** 模拟进程崩溃后的重启：从最后一个完整检查点重建内存状态。 */
  restart(): void {
    if (this.rt.status !== 'crashed') {
      throw new EngineError('只有崩溃状态才需要重启恢复');
    }
    const ignoredOnRecovery = this.storage.listCheckpoints().ignored;
    const droppedPartials = this.storage.clearPartialCheckpoints();
    const fresh = this.bootstrap();
    fresh.lastCrash = this.rt.lastCrash;
    fresh.pendingIgnored = Math.max(ignoredOnRecovery.length, droppedPartials);
    this.storage.clearCrash();
    this.rt = fresh;
  }

  // ---------- 处理器版本与迁移 ----------

  deployVersion(version: ProcessorVersion): void {
    if (version === this.rt.version) return;
    if (this.rt.status === 'awaiting_migration') {
      throw new EngineError('旧检查点需显式迁移；也可把部署版本切回旧版本再启动');
    }
    this.assertMutable();
    const latest = this.storage.latestCheckpoint();
    if (latest && version < latest.data.processor.version) {
      throw new EngineError(
        `不支持降级部署（最新检查点 v${latest.data.processor.version} → v${version}）；旧检查点只能显式迁移`,
      );
    }
    const lag = this.currentLag();
    if (lag > 0) {
      throw new EngineError(`仍有 ${lag} 条事件已处理但未进检查点，请先建立检查点再切换版本`);
    }
    this.storage.writeMeta(version);
    this.rt.version = version;
  }

  private currentLag(): number {
    const latest = this.storage.latestCheckpoint();
    const base = latest?.data.progress ?? {};
    let lag = 0;
    for (const [p, prog] of Object.entries(this.rt.progress)) {
      lag += prog.offset - (base[p]?.offset ?? 0);
    }
    return lag;
  }

  /**
   * 显式迁移：基于旧检查点逐版本生成新检查点。
   * 迁移失败只记录一条失败日志，原检查点保持不变。
   */
  migrate(target?: ProcessorVersion): MigrationRecord {
    if (this.rt.status !== 'awaiting_migration') {
      throw new EngineError('当前没有等待迁移的旧检查点');
    }
    const to = target ?? (this.storage.readMeta()?.deployedVersion as ProcessorVersion);
    const from = this.rt.version;
    const latest = this.storage.latestCheckpoint()!;
    const timestamp = this.clock();

    if (!canMigrate(from, to)) {
      return this.recordMigrationFailure(from, to, timestamp, `不允许的迁移路径 ${from} -> ${to}`);
    }

    try {
      if (this.faultArmed('beforeMigrationCommit')) {
        this.faults.delete('beforeMigrationCommit');
        throw new MigrationError('注入故障：迁移结果提交前失败');
      }
      let version = from;
      let aggregates = structuredClone(this.rt.aggregates);
      while (version < to) {
        const next = (version + 1) as ProcessorVersion;
        const migrated: Record<string, AggState> = {};
        for (const [key, state] of Object.entries(aggregates)) {
          migrated[key] = migrateState(version, next, state);
        }
        aggregates = migrated;
        version = next;
      }
      const data: CheckpointData = {
        kind: 'checkpoint',
        seq: this.nextCheckpointSeq(),
        createdAt: timestamp,
        reason: 'migrate',
        processor: { name: processorName(to), version: to },
        progress: cloneProgress(this.rt.progress),
        aggregates,
        seen: structuredClone(this.rt.seen),
        counters: structuredClone(this.rt.counters),
        replayedAttempts: this.rt.replayedAttempts,
      };
      const name = this.storage.writeCheckpoint(data);
      const record: MigrationRecord = {
        id: `m-${timestamp}-${from}-${to}`,
        seq: data.seq,
        timestamp,
        fromVersion: from,
        toVersion: to,
        success: true,
        reason: `显式迁移成功，生成检查点 ${name}`,
        sourceCheckpoint: latest.name,
        resultCheckpoint: name,
        progress: cloneProgress(this.rt.progress),
      };
      this.storage.appendMigration(record);
      this.rt = {
        ...this.rt,
        status: 'paused',
        version: to,
        aggregates,
        lastCheckpointSeq: data.seq,
      };
      return record;
    } catch (err) {
      return this.recordMigrationFailure(
        from,
        to,
        timestamp,
        err instanceof Error ? err.message : String(err),
      );
    }
  }

  private recordMigrationFailure(
    from: ProcessorVersion,
    to: ProcessorVersion,
    timestamp: number,
    reason: string,
  ): MigrationRecord {
    const record: MigrationRecord = {
      id: `m-${timestamp}-${from}-${to}-failed`,
      seq: this.rt.lastCheckpointSeq,
      timestamp,
      fromVersion: from,
      toVersion: to,
      success: false,
      reason,
      sourceCheckpoint: this.storage.latestCheckpoint()?.name ?? null,
      resultCheckpoint: null,
    };
    this.storage.appendMigration(record);
    return record;
  }

  // ---------- 快照、导出与重放 ----------

  snapshot(): EngineSnapshot {
    const { ok, ignored } = this.storage.listCheckpoints();
    const latest = ok[ok.length - 1] ?? null;
    const events: Record<string, EventRecord[]> = {};
    for (const partition of this.storage.listPartitions()) {
      events[partition] = this.storage.readEvents(partition);
    }
    const deployed = (this.storage.readMeta()?.deployedVersion ?? this.rt.version) as ProcessorVersion;
    const lagging = this.partitionsWithLag().map((l) => l.partition).sort();
    return {
      status: this.rt.status,
      deployedVersion: deployed,
      progress: cloneProgress(this.rt.progress),
      aggregates: structuredClone(this.rt.aggregates),
      counters: structuredClone(this.rt.counters),
      replayedAttempts: this.rt.replayedAttempts,
      lastCrash: this.rt.lastCrash ? structuredClone(this.rt.lastCrash) : null,
      events,
      checkpoints: ok,
      ignoredCheckpoints: ignored,
      migrations: this.storage.readMigrations(),
      checkpointProgress: latest ? cloneProgress(latest.data.progress) : {},
      checkpointCounters: latest ? structuredClone(latest.data.counters) : null,
      liveStateHash: semanticStateHash({
        version: this.rt.version,
        progress: this.rt.progress,
        aggregates: this.rt.aggregates,
      }),
      checkpointStateHash: latest
        ? semanticStateHash({
            version: latest.data.processor.version,
            progress: latest.data.progress,
            aggregates: latest.data.aggregates,
          })
        : null,
      rrPartition: lagging.length
        ? lagging[(this.rt.rrIndex - 1 + lagging.length) % lagging.length]
        : null,
      lastCheckpointName: latest?.name ?? null,
      lastCheckpointReason: latest ? latest.data.reason : null,
      lastRecoveryIgnored: this.rt.pendingIgnored,
    };
  }

  exportExperiment() {
    const latest = this.storage.latestCheckpoint();
    return {
      format: 'checkpoint-bench-export',
      version: 1,
      exportedAt: this.clock(),
      deployedVersion: this.storage.readMeta()?.deployedVersion ?? this.rt.version,
      partitions: this.storage.readAllPartitions(),
      migrations: this.storage.readMigrations(),
      finalCheckpoint: latest?.data ?? null,
      finalStateHash: latest
        ? semanticStateHash({
            version: latest.data.processor.version,
            progress: latest.data.progress,
            aggregates: latest.data.aggregates,
          })
        : semanticStateHash({
            version: this.rt.version,
            progress: this.rt.progress,
            aggregates: this.rt.aggregates,
          }),
    };
  }
}

/**
 * 在空实例（空目录）上重放一次导出：
 * 按分区内有序消费到每个迁移边界，再按记录顺序执行成功迁移，
 * 最后消费到最终检查点并比较语义状态哈希。
 */
export function replayExport(
  storage: FileStorage,
  bundle: {
    partitions: PartitionInfo[];
    migrations: MigrationRecord[];
    finalCheckpoint: CheckpointData | null;
    finalStateHash: string;
  },
  clock: () => number = () => 0,
): { matches: boolean; exportedHash: string; replayHash: string } {
  const startVersion = bundle.migrations.find((m) => m.success)?.fromVersion
    ?? (bundle.finalCheckpoint?.processor.version ?? LATEST_VERSION);
  storage.writeMeta(startVersion);
  const engine = new BenchEngine(storage, clock);
  engine.storage.writeMeta(startVersion);
  for (const part of bundle.partitions) {
    engine.createPartition(part.partition);
    for (const event of part.events) {
      engine.appendEvent(part.partition, event.key, event.value);
    }
  }

  for (const migration of bundle.migrations.filter((m) => m.success)) {
    engine.consumeUpTo(migration.progress ?? {});
    engine.checkpoint('manual');
    engine.storage.writeMeta(migration.toVersion);
    engine.reloadFromDisk();
    engine.migrate(migration.toVersion);
  }

  const finalTargets = bundle.finalCheckpoint?.progress ?? {};
  engine.consumeUpTo(finalTargets);
  engine.checkpoint('manual');
  const ckpt = engine.storage.latestCheckpoint()!;
  const replayHash = semanticStateHash({
    version: ckpt.data.processor.version,
    progress: ckpt.data.progress,
    aggregates: ckpt.data.aggregates,
  });
  return {
    matches: replayHash === bundle.finalStateHash,
    exportedHash: bundle.finalStateHash,
    replayHash,
  };
}
