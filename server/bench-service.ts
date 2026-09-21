import { BenchEngine, CrashSimulation, EngineError } from '../src/core/engine';
import { FileStorage } from '../src/core/storage';
import type { EngineSnapshot } from '../src/core/engine';
import type { FaultPoint } from '../src/core/types';

export interface RunConfig {
  intervalMs: number;
  batchSize: number;
  checkpointEvery: number;
  autoCheckpoint: boolean;
}

/**
 * 持有一个引擎实例与自动消费循环。
 * “崩溃”终结运行时（内存状态作废），循环停止；故障注入配置与磁盘数据保留，
 * restart() 后由 BenchEngine 从最后一个完整检查点恢复。
 */
export class BenchService {
  engine: BenchEngine;
  runConfig: RunConfig = {
    intervalMs: 400,
    batchSize: 1,
    checkpointEvery: 5,
    autoCheckpoint: false,
  };
  private timer: NodeJS.Timeout | null = null;
  private sinceCheckpoint = 0;

  constructor(dataDir: string) {
    this.engine = new BenchEngine(new FileStorage(dataDir));
  }

  reset(): EngineSnapshot {
    this.stopLoop();
    this.engine.resetStorage();
    this.sinceCheckpoint = 0;
    return this.engine.snapshot();
  }

  start(): EngineSnapshot {
    if (this.engine.status === 'crashed') throw new EngineError('已崩溃，请先重启恢复');
    if (this.engine.status === 'awaiting_migration') {
      throw new EngineError('旧检查点需要显式迁移');
    }
    this.engine.markRunning();
    this.ensureLoop();
    return this.engine.snapshot();
  }

  pause(): EngineSnapshot {
    this.stopLoop();
    this.engine.markPaused();
    return this.engine.snapshot();
  }

  updateRunConfig(patch: Partial<RunConfig>): RunConfig {
    const wasRunning = this.timer !== null;
    this.stopLoop();
    this.runConfig = { ...this.runConfig, ...patch };
    if (wasRunning && this.engine.status === 'running') this.ensureLoop();
    return this.runConfig;
  }

  armFault(point: FaultPoint): void {
    this.engine.armFault(point);
  }

  disarmFault(point: FaultPoint): void {
    this.engine.disarmFault(point);
  }

  shutdown(): void {
    this.stopLoop();
  }

  private ensureLoop(): void {
    if (this.timer) return;
    this.timer = setInterval(() => this.tick(), this.runConfig.intervalMs);
  }

  private stopLoop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private tick(): void {
    if (this.engine.status !== 'running') return;
    try {
      const processed = this.engine.consumeBatch(this.runConfig.batchSize);
      this.sinceCheckpoint += processed;
      if (this.runConfig.autoCheckpoint && this.sinceCheckpoint >= this.runConfig.checkpointEvery) {
        this.engine.checkpoint('periodic');
        this.sinceCheckpoint = 0;
      }
    } catch (err) {
      this.stopLoop();
      if (!(err instanceof CrashSimulation)) throw err;
    }
  }
}
