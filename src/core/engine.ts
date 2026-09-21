import { buildEnvelope, semanticHash } from './checkpoint';
import { canonicalJson } from './hash';
import { migrateV1ToV2, processEvent } from './processors';
import type { LabStorage } from './storage';
import type {
  CheckpointData,
  CheckpointEnvelope,
  CrashPoint,
  ExperimentBundle,
  HighWatermarks,
  InputEvent,
  MigrationRecord,
  PartitionProgress,
  ProcessorMetrics,
  ProcessorState,
  ProcessorVersion,
  RuntimeSnapshot
} from './types';
import { MigrationRequiredError, SimulatedCrash, emptyMetrics, emptyState } from './types';

export interface EngineOptions {
  processorVersion?: ProcessorVersion;
  autoCheckpointEvery?: number;
  highWatermarks?: HighWatermarks;
}

export interface BatchResult {
  processed: number;
  duplicates: number;
  effectiveChanges: number;
  crashedAt?: CrashPoint;
  checkpointId?: string;
}

interface Runtime {
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
  lastScheduledPartition?: string;
  deliveriesSinceCheckpoint: number;
}

let runtimeCounter = 0;

export class CheckpointLabEngine {
  private runtime!: Runtime;
  private readonly autoCheckpointEvery: number;

  constructor(private readonly storage: LabStorage, options: EngineOptions = {}) {
    this.autoCheckpointEvery = options.autoCheckpointEvery ?? 3;
    this.runtime = {
      status: 'stopped',
      processorVersion: options.processorVersion ?? 1,
      partitions: [],
      progress: {},
      highWatermarks: {},
      state: emptyState(options.processorVersion ?? 1),
      metrics: emptyMetrics(),
      statusMessage: '尚未启动',
      deliveriesSinceCheckpoint: 0
    };
    if (options.highWatermarks) {
      this.runtime.highWatermarks = structuredClone(options.highWatermarks);
    }
  }

  async start(): Promise<RuntimeSnapshot> {
    const partitions = await this.storage.listPartitions();
    const checkpoints = await this.storage.listCheckpoints();
    const latest = checkpoints.at(-1);
    const requestedVersion = this.runtime.processorVersion;

    if (latest) {
      if (latest.checkpoint.processorVersion !== requestedVersion) {
        this.runtime = {
          ...this.runtime,
          status: 'stopped',
          statusMessage: `需要从 v${latest.checkpoint.processorVersion} 显式迁移到 v${requestedVersion}`
        };
        throw new MigrationRequiredError(
          latest.checkpoint.processorVersion,
          requestedVersion,
          latest.checkpoint.id
        );
      }
      this.restoreFrom(latest);
      this.runtime.status = 'paused';
      this.runtime.statusMessage = '已从最后一个完整检查点恢复';
      await this.refreshPartitions(partitions);
    } else {
      this.runtime = {
        ...this.runtime,
        status: 'paused',
        partitions,
        progress: Object.fromEntries(partitions.map((partition) => [partition, -1])),
        highWatermarks: await this.readHighWatermarks(partitions),
        statusMessage: '空检查点启动'
      };
      await this.refreshPartitions(partitions);
    }
    return this.snapshot();
  }

  pause(): RuntimeSnapshot {
    if (this.runtime.status === 'crashed') {
      throw new Error('进程已崩溃，必须重新启动');
    }
    this.runtime.status = 'paused';
    this.runtime.statusMessage = '已暂停';
    return this.snapshot();
  }

  resume(): RuntimeSnapshot {
    if (this.runtime.status === 'crashed') {
      throw new Error('进程已崩溃，必须重新启动');
    }
    this.runtime.status = 'running';
    this.runtime.statusMessage = '运行中';
    return this.snapshot();
  }

  getSnapshot(): RuntimeSnapshot {
    return this.snapshot();
  }

  async createPartition(name: string): Promise<RuntimeSnapshot> {
    assertPartitionName(name);
    await this.storage.ensurePartition(name);
    if (!this.runtime.partitions.includes(name)) {
      this.runtime.partitions = [...this.runtime.partitions, name].sort((left, right) => left.localeCompare(right));
    }
    this.runtime.progress[name] ??= -1;
    const hwm = await this.partitionHighWatermark(name);
    this.runtime.highWatermarks[name] = hwm;
    this.runtime.statusMessage = `已创建分区 ${name}`;
    return this.snapshot();
  }

  async appendEvent(input: {
    partition: string;
    key: string;
    value: number;
    idempotencyKey?: string;
  }): Promise<InputEvent> {
    assertPartitionName(input.partition);
    await this.createPartition(input.partition);
    const events = await this.storage.readEvents(input.partition);
    const offset = events.length === 0 ? 0 : events[events.length - 1]!.offset + 1;
    if (!Number.isFinite(input.value)) {
      throw new Error('事件值必须是数字');
    }
    const event: InputEvent = {
      id: `evt-${Date.now()}-${runtimeCounter++}`,
      partition: input.partition,
      offset,
      key: input.key,
      value: input.value,
      appendedAt: Date.now()
    };
    if (input.idempotencyKey) {
      event.idempotencyKey = input.idempotencyKey;
    }
    await this.storage.appendEvent(event);
    this.runtime.highWatermarks[input.partition] = offset;
    this.runtime.statusMessage = `已追加 offset=${offset}`;
    return event;
  }

  async step(): Promise<BatchResult> {
    return this.consume(1);
  }

  async batch(maxEvents: number): Promise<BatchResult> {
    return this.consume(maxEvents);
  }

  async redeliver(partition: string, offset: number): Promise<BatchResult> {
    this.ensureLive();
    const event = (await this.storage.readEvents(partition)).find((candidate) => candidate.offset === offset);
    if (!event) {
      throw new Error(`找不到 ${partition}:${offset}`);
    }
    const result = await this.deliver(event);
    return {
      processed: 1,
      duplicates: result.duplicate ? 1 : 0,
      effectiveChanges: result.effectiveChange ? 1 : 0
    };
  }

  armCrash(point: CrashPoint): RuntimeSnapshot {
    this.runtime.armedCrashPoint = point;
    this.runtime.statusMessage = `已预置崩溃点：${point}`;
    return this.snapshot();
  }

  clearArmedCrash(): RuntimeSnapshot {
    this.runtime.armedCrashPoint = undefined;
    this.runtime.statusMessage = '已取消故障注入';
    return this.snapshot();
  }

  async consumeAll(): Promise<BatchResult> {
    return this.consume(Number.MAX_SAFE_INTEGER);
  }

  async replayToProgress(target: PartitionProgress): Promise<BatchResult> {
    const totals = Object.entries(target).map(([partition, offset]) => Math.max(0, offset - (this.runtime.progress[partition] ?? -1)));
    const maxEvents = totals.reduce((sum, count) => sum + count, 0);
    return this.consume(maxEvents);
  }

  async checkpoint(): Promise<CheckpointEnvelope> {
    this.ensureLive();
    this.runtime.metrics.checkpointAttempts += 1;
    this.runtime.metrics.completedCheckpoints += 1;

    const checkpoint: CheckpointData = {
      id: `cp-${Date.now()}-${runtimeCounter++}`,
      createdAt: Date.now(),
      processorVersion: this.runtime.processorVersion,
      progress: structuredClone(this.runtime.progress),
      highWatermarks: structuredClone(this.runtime.highWatermarks),
      state: structuredClone(this.runtime.state),
      metrics: structuredClone(this.runtime.metrics)
    };
    const envelope = await buildEnvelope(checkpoint);

    try {
      await this.storage.commitCheckpoint(envelope, {
        beforeWrite: () => this.crashIfArmed('beforeCheckpointWrite'),
        beforeRename: () => this.crashIfArmed('beforeCheckpointRename'),
        afterRename: () => this.crashIfArmed('afterCheckpointRename')
      });
    } catch (error) {
      if (error instanceof SimulatedCrash) {
        this.runtime.status = 'crashed';
        throw error;
      }
      throw error;
    }

    this.runtime.latestCheckpointId = checkpoint.id;
    this.runtime.deliveriesSinceCheckpoint = 0;
    this.runtime.statusMessage = `检查点 ${checkpoint.id} 已完整提交`;
    return envelope;
  }

  async migrateLatest(
    targetVersion: ProcessorVersion,
    options: { simulateFailure?: boolean } = {}
  ): Promise<MigrationRecord> {
    const checkpoints = await this.storage.listCheckpoints();
    const latest = checkpoints.at(-1);
    if (!latest) {
      throw new Error('没有可迁移的检查点');
    }

    const startedAt = Date.now();
    const baseRecord = {
      id: `mig-${Date.now()}-${runtimeCounter++}`,
      fromVersion: latest.checkpoint.processorVersion,
      toVersion: targetVersion,
      sourceCheckpointId: latest.checkpoint.id,
      startedAt
    };

    if (options.simulateFailure || latest.checkpoint.processorVersion !== 1 || targetVersion !== 2) {
      const errorMessage = options.simulateFailure
        ? '模拟迁移转换器失败'
        : `不支持 v${latest.checkpoint.processorVersion} -> v${targetVersion}`;
      const failed: MigrationRecord = {
        ...baseRecord,
        status: 'failed',
        error: errorMessage,
        finishedAt: Date.now()
      };
      await this.storage.appendMigration(failed);
      this.runtime.statusMessage = '迁移失败，原检查点未修改';
      throw new Error(errorMessage);
    }

    const migratedState = migrateV1ToV2(latest.checkpoint.state);
    const target: CheckpointData = {
      ...latest.checkpoint,
      id: `cp-${Date.now()}-${runtimeCounter++}`,
      createdAt: Date.now(),
      processorVersion: targetVersion,
      state: migratedState
    };
    const envelope = await buildEnvelope(target);
    await this.storage.commitCheckpoint(envelope);

    const record: MigrationRecord = {
      ...baseRecord,
      targetCheckpointId: target.id,
      status: 'succeeded',
      finishedAt: Date.now()
    };
    await this.storage.appendMigration(record);
    this.restoreFrom(envelope);
    this.runtime.status = 'paused';
    this.runtime.statusMessage = '显式迁移成功，已从新检查点恢复';
    return record;
  }

  async exportExperiment(): Promise<ExperimentBundle> {
    const [partitions, events, checkpoints, migrations] = await Promise.all([
      this.storage.listPartitions(),
      this.storage.readEvents(),
      this.storage.listCheckpoints(),
      this.storage.listMigrations()
    ]);
    const expectedSemanticHash = await semanticHash({
      processorVersion: this.runtime.processorVersion,
      progress: this.runtime.progress,
      highWatermarks: this.runtime.highWatermarks,
      state: this.runtime.state
    });
    return {
      kind: 'pairwise-gsb-experiment',
      formatVersion: 1,
      exportedAt: Date.now(),
      partitions,
      events,
      checkpoints,
      migrations,
      finalProcessorVersion: this.runtime.processorVersion,
      finalProgress: structuredClone(this.runtime.progress),
      finalHighWatermarks: structuredClone(this.runtime.highWatermarks),
      expectedSemanticHash
    };
  }

  static async importInto(storage: LabStorage, bundle: ExperimentBundle): Promise<void> {
    validateBundle(bundle);
    await storage.clearAll();
    for (const partition of bundle.partitions) {
      await storage.ensurePartition(partition);
    }
    for (const event of bundle.events) {
      if (event.offset > (bundle.finalProgress[event.partition] ?? -1)) {
        continue;
      }
      await storage.appendEvent(event);
    }
    for (const migration of bundle.migrations) {
      await storage.appendMigration(migration);
    }
    for (const checkpoint of bundle.checkpoints) {
      await storage.commitCheckpoint(checkpoint);
    }
  }

  static async replayInEmptyStorage(storage: LabStorage, bundle: ExperimentBundle): Promise<{
    semanticHash: string;
    expectedSemanticHash: string;
    matches: boolean;
  }> {
    validateBundle(bundle);
    await storage.clearAll();
    for (const partition of bundle.partitions) {
      await storage.ensurePartition(partition);
    }
    for (const event of bundle.events) {
      await storage.appendEvent(event);
    }

    const engine = new CheckpointLabEngine(storage, {
      processorVersion: bundle.finalProcessorVersion,
      autoCheckpointEvery: Number.MAX_SAFE_INTEGER,
      highWatermarks: bundle.finalHighWatermarks
    });
    await engine.start();
    await engine.consumeAll();
    const actual = await semanticHash({
      processorVersion: engine.runtime.processorVersion,
      progress: engine.runtime.progress,
      highWatermarks: engine.runtime.highWatermarks,
      state: engine.runtime.state
    });
    return {
      semanticHash: actual,
      expectedSemanticHash: bundle.expectedSemanticHash,
      matches:
        actual === bundle.expectedSemanticHash &&
        canonicalEqual(engine.runtime.highWatermarks, bundle.finalHighWatermarks) &&
        canonicalEqual(engine.runtime.progress, bundle.finalProgress)
    };
  }

  private async consume(maxEvents: number): Promise<BatchResult> {
    this.ensureLive();
    const result: BatchResult = { processed: 0, duplicates: 0, effectiveChanges: 0 };

    while (result.processed < maxEvents) {
      const event = await this.pickNextEvent();
      if (!event) {
        this.runtime.statusMessage = result.processed > 0 ? `本批处理 ${result.processed} 条` : '没有待处理事件';
        break;
      }
      try {
        const delivery = await this.deliver(event);
        result.processed += 1;
        result.duplicates += delivery.duplicate ? 1 : 0;
        result.effectiveChanges += delivery.effectiveChange ? 1 : 0;
      } catch (error) {
        result.crashedAt = error instanceof SimulatedCrash ? error.crashPoint : undefined;
        throw error;
      }

      if (this.runtime.deliveriesSinceCheckpoint >= this.autoCheckpointEvery) {
        const envelope = await this.checkpoint();
        result.checkpointId = envelope.checkpoint.id;
      }
    }
    return result;
  }

  private async deliver(event: InputEvent): Promise<ReturnType<typeof processEvent>> {
    this.ensureLive();
    this.runtime.metrics.processAttempts += 1;
    this.crashIfArmed('beforeStateWrite');

    const staged = processEvent(this.runtime.state, event);
    this.crashIfArmed('afterStateWrite');
    this.crashIfArmed('beforeOffsetCommit');

    this.runtime.state = staged.state;
    this.runtime.progress[event.partition] = event.offset;
    if (staged.duplicate) {
      this.runtime.metrics.duplicateDeliveries += 1;
    } else {
      this.runtime.metrics.effectiveStateChanges += 1;
    }
    this.runtime.deliveriesSinceCheckpoint += 1;
    this.runtime.lastScheduledPartition = event.partition;
    this.runtime.statusMessage = `已提交 ${event.partition}:${event.offset}${staged.duplicate ? '（重复幂等）' : ''}`;
    return staged;
  }

  private async pickNextEvent(): Promise<InputEvent | undefined> {
    const partitions = this.runtime.partitions;
    if (partitions.length === 0) {
      return undefined;
    }
    const length = this.runtime.partitions.length;
    const lastIndex = this.runtime.partitions.indexOf(this.runtime.lastScheduledPartition ?? '');
    const start = lastIndex === -1 ? 0 : (lastIndex + 1) % length;
    for (let step = 0; step < length; step += 1) {
      const index = (start + step) % length;
      const partition = partitions[index]!;
      const committedOffset = this.runtime.progress[partition] ?? -1;
      const event = (await this.storage.readEvents(partition)).find((candidate) => candidate.offset > committedOffset);
      if (event) {
        return event;
      }
    }
    return undefined;
  }

  private crashIfArmed(point: CrashPoint): void {
    if (this.runtime.armedCrashPoint !== point) {
      return;
    }
    this.runtime.armedCrashPoint = undefined;
    this.runtime.status = 'crashed';
    this.runtime.statusMessage = `进程在 ${point} 崩溃；内存进度丢弃`;
    this.runtime.metrics.crashes += 1;
    throw new SimulatedCrash(point);
  }

  private ensureLive(): void {
    if (this.runtime.status === 'crashed') {
      throw new Error('进程已崩溃；请从最后一个完整检查点重启');
    }
    if (this.runtime.status === 'stopped') {
      throw new Error('请先启动处理器');
    }
  }

  private restoreFrom(envelope: CheckpointEnvelope): void {
    this.runtime = {
      ...this.runtime,
      status: 'paused',
      processorVersion: envelope.checkpoint.processorVersion,
      partitions: [],
      progress: structuredClone(envelope.checkpoint.progress),
      highWatermarks: structuredClone(envelope.checkpoint.highWatermarks),
      state: structuredClone(envelope.checkpoint.state),
      metrics: structuredClone(envelope.checkpoint.metrics),
      latestCheckpointId: envelope.checkpoint.id,
      armedCrashPoint: undefined,
      lastScheduledPartition: undefined,
      deliveriesSinceCheckpoint: 0
    };
  }

  private async refreshPartitions(storagePartitions: string[]): Promise<void> {
    const merged = new Set([...storagePartitions, ...Object.keys(this.runtime.progress)]);
    this.runtime.partitions = [...merged].sort((left, right) => left.localeCompare(right));
    for (const partition of this.runtime.partitions) {
      this.runtime.progress[partition] ??= -1;
      this.runtime.highWatermarks[partition] ??= await this.partitionHighWatermark(partition);
    }
  }

  private async partitionHighWatermark(partition: string): Promise<number> {
    const events = await this.storage.readEvents(partition);
    return events.length === 0 ? -1 : events[events.length - 1]!.offset;
  }

  private async readHighWatermarks(partitions: string[]): Promise<HighWatermarks> {
    return Object.fromEntries(
      await Promise.all(partitions.map(async (partition) => [partition, await this.partitionHighWatermark(partition)]))
    );
  }

  private snapshot(): RuntimeSnapshot {
    const { status, processorVersion, partitions, progress, highWatermarks, state, metrics, latestCheckpointId, armedCrashPoint, statusMessage } = this.runtime;
    return {
      status,
      processorVersion,
      partitions,
      progress: structuredClone(progress),
      highWatermarks: structuredClone(highWatermarks),
      state: structuredClone(state),
      metrics: structuredClone(metrics),
      latestCheckpointId,
      ...(armedCrashPoint ? { armedCrashPoint } : {}),
      statusMessage
    };
  }
}

function assertPartitionName(name: string): void {
  if (!name || name.trim().length === 0 || name.includes('/') || name.includes('\\')) {
    throw new Error('分区名不能为空，也不能包含路径分隔符');
  }
}

function validateBundle(bundle: ExperimentBundle): void {
  if (bundle.kind !== 'pairwise-gsb-experiment' || bundle.formatVersion !== 1) {
    throw new Error('无法识别的实验导出格式');
  }
  if (!bundle.expectedSemanticHash || !Array.isArray(bundle.events) || !Array.isArray(bundle.checkpoints)) {
    throw new Error('实验导出内容不完整');
  }
}

function canonicalEqual(left: unknown, right: unknown): boolean {
  return canonicalJson(left) === canonicalJson(right);
}
