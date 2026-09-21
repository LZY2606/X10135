import { describe, expect, it, beforeEach } from 'vitest';
import { rmSync } from 'node:fs';
import { BenchEngine, CrashSimulation } from '../src/core/engine';
import { FileStorage } from '../src/core/storage';
import { semanticStateHash } from '../src/core/processor';
import { makeEngine, seed } from './helpers';

let dirs: string[] = [];

beforeEach(() => {
  dirs = [];
});

function cleanup(...extra: string[]): void {
  for (const d of [...dirs, ...extra]) rmSync(d, { recursive: true, force: true });
}

describe('基本处理与原子检查点', () => {
  it('分区内有序消费，聚合与游标同步推进', () => {
    const { dir, engine } = makeEngine();
    dirs.push(dir);
    seed(engine, [
      [
        { key: 'a', value: 1 },
        { key: 'b', value: 2 },
        { key: 'a', value: 3 },
      ],
    ]);
    engine.consumeBatch(3);
    const s = engine.snapshot();
    expect(s.progress.p0).toEqual({ offset: 3, hwm: 3 });
    expect(s.aggregates.a).toMatchObject({ count: 2 });
    expect(s.counters).toMatchObject({ delivered: 3, effective: 3, duplicates: 0 });
    cleanup(dir);
  });

  it('检查点包含分区进度、聚合状态、处理器版本和输入日志高水位', () => {
    const { dir, engine } = makeEngine();
    dirs.push(dir);
    seed(engine, [[{ key: 'a' }], [{ key: 'b' }]]);
    engine.deployVersion(1);
    engine.consumeBatch(2);
    engine.checkpoint('manual');
    const latest = engine.storage.latestCheckpoint()!;
    expect(latest.data.processor.version).toBe(1);
    expect(latest.data.progress.p0).toEqual({ offset: 1, hwm: 1 });
    expect(latest.data.progress.p1).toEqual({ offset: 1, hwm: 1 });
    expect(latest.data.aggregates.a.count).toBe(1);
    expect(latest.data.seen['p0:0']).toBe('a');
    cleanup(dir);
  });
});

describe('offset 三处崩溃点：写状态前 / 写状态后 / 提交前', () => {
  for (const fault of ['beforeStateWrite', 'afterStateWrite', 'beforeOffsetCommit'] as const) {
    it(`${fault} 崩溃后从最后完整检查点恢复，而非界面最后位置`, () => {
      const { dir, engine, storage } = makeEngine();
      dirs.push(dir);
      seed(engine, [
        [
          { key: 'a' },
          { key: 'a' },
          { key: 'a' },
          { key: 'a' },
        ],
      ]);
      engine.deployVersion(1);
      engine.consumeBatch(2);
      engine.checkpoint('manual'); // 提交到 offset=2

      engine.armFault(fault);
      expect(() => engine.consumeOne()).toThrow(CrashSimulation);
      expect(engine.status).toBe('crashed');

      // 崩溃记录记下“险些显示到 3”的位置（教学演示用），但该位置从未成为恢复点
      const crashedSnapshot = engine.snapshot();
      expect(crashedSnapshot.lastCrash!.liveProgress.p0.offset).toBe(3);
      expect(crashedSnapshot.lastCrash!.checkpointProgress.p0.offset).toBe(2);

      // 用一个全新的引擎实例模拟进程重启
      const restarted = new BenchEngine(new FileStorage(dir));
      const rs = restarted.snapshot();
      expect(rs.progress.p0.offset).toBe(2);
      expect(rs.aggregates.a.count).toBe(2);
      expect(rs.status).toBe('paused');
      void storage;

      // 重放“丢失”的事件：再次经过处理器，但幂等键保证状态不错乱
      restarted.consumeBatch(4);
      const final = restarted.snapshot();
      expect(final.aggregates.a.count).toBe(4);
      expect(final.counters.delivered).toBeGreaterThanOrEqual(2 + 2);
      cleanup(dir);
    });
  }

  it('崩溃瞬间的重放尝试数被记录，下一检查点清零', () => {
    const { dir, engine } = makeEngine();
    dirs.push(dir);
    seed(engine, [[{ key: 'a' }, { key: 'a' }, { key: 'a' }]]);
    engine.consumeOne();
    engine.checkpoint('manual');
    engine.armFault('beforeOffsetCommit');
    expect(() => engine.consumeBatch(3)).toThrow(CrashSimulation);
    const restarted = new BenchEngine(new FileStorage(dir));
    expect(restarted.snapshot().replayedAttempts).toBe(1);
    restarted.consumeBatch(5);
    restarted.checkpoint('manual');
    expect(restarted.snapshot().replayedAttempts).toBe(0);
    cleanup(dir);
  });
});
