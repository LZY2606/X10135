import { describe, expect, it } from 'vitest';
import { rmSync } from 'node:fs';
import { join } from 'node:path';
import { BenchEngine, CrashSimulation, EngineError } from '../src/core/engine';
import { FileStorage } from '../src/core/storage';
import { makeEngine, seed } from './helpers';

describe('检查点 rename 前后的崩溃恢复', () => {
  it('rename 前崩溃：只留下 .partial，恢复时被识别并忽略', () => {
    const { dir, engine, storage } = makeEngine();
    seed(engine, [[{ key: 'a' }], [{ key: 'a' }]]);
    engine.consumeOne();
    engine.checkpoint('manual'); // checkpoint #1: offset p0=1

    engine.consumeBatch(3);
    engine.armFault('beforeCheckpointRename');
    expect(() => engine.checkpoint('manual')).toThrow(CrashSimulation);

    const partials = storage
      .listCheckpoints()
      .ignored.filter((i) => i.name.endsWith('.partial'));
    expect(partials.length).toBe(1);

    const restarted = new BenchEngine(new FileStorage(dir));
    const s = restarted.snapshot();
    expect(s.checkpoints.length).toBe(1); // 半成品被忽略
    expect(s.progress.p0.offset).toBe(1);
    expect(s.aggregates.a.count).toBe(1);
    // 全新进程引导时就已识别到 .partial；重启恢复动作会将其清理并计入恢复报告
    expect(s.lastRecoveryIgnored + s.ignoredCheckpoints.length).toBeGreaterThanOrEqual(1);
    expect(s.lastCrash?.fault).toBe('beforeCheckpointRename');
    rmSync(dir, { recursive: true, force: true });
  });

  it('rename 后崩溃：检查点已完整发布，恢复必须采用它', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [[{ key: 'a' }, { key: 'a' }, { key: 'a' }]]);
    engine.consumeBatch(3);
    engine.armFault('afterCheckpointRename');
    expect(() => engine.checkpoint('manual')).toThrow(CrashSimulation);

    const restarted = new BenchEngine(new FileStorage(dir));
    const s = restarted.snapshot();
    expect(s.checkpoints.length).toBe(1);
    expect(s.progress.p0.offset).toBe(3);
    expect(s.aggregates.a.count).toBe(3);
    rmSync(dir, { recursive: true, force: true });
  });

  it('损坏（截断）和校验和错误的检查点被忽略，仍能恢复到上一个完好的', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [[{ key: 'a' }]]);
    engine.consumeOne();
    engine.checkpoint('manual');
    const { writeFileSync, readdirSync } = require('node:fs') as typeof import('node:fs');
    const ckptDir = join(dir, 'checkpoints');
    writeFileSync(join(ckptDir, 'checkpoint-000002.json'), '{"checksum":"deadbeef","data":{}}');
    writeFileSync(join(ckptDir, 'checkpoint-000003.json'), '{"broken json');
    const restarted = new BenchEngine(new FileStorage(dir));
    const s = restarted.snapshot();
    expect(s.checkpoints.length).toBe(1);
    expect(s.ignoredCheckpoints.length).toBe(2);
    expect(readdirSync(ckptDir).some((f) => f.endsWith('.json'))).toBe(true);
    rmSync(dir, { recursive: true, force: true });
  });
});

describe('重复投递与幂等', () => {
  it('同一幂等键（partition:offset）的重复可以再过处理器，但不改变最终状态', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [
      [
        { key: 'a', value: 10 },
        { key: 'a', value: 20 },
      ],
    ]);
    engine.consumeBatch(2);
    expect(engine.snapshot().aggregates.a).toMatchObject({ count: 2, sum: 30, avg: 15 });

    engine.redeliver('p0', 0);
    engine.redeliver('p0', 1);
    const s = engine.snapshot();
    expect(s.aggregates.a).toMatchObject({ count: 2, sum: 30, avg: 15 });
    expect(s.counters.delivered).toBe(4);
    expect(s.counters.duplicates).toBe(2);
    expect(s.counters.effective).toBe(2);
    rmSync(dir, { recursive: true, force: true });
  });

  it('崩溃重放后 delivered 可能增加，但有效状态与哈希保持一致', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [[{ key: 'a', value: 5 }, { key: 'a', value: 7 }]]);
    engine.consumeOne();
    engine.checkpoint('manual');
    const expectedHash = engine.snapshot().liveStateHash;
    engine.armFault('afterStateWrite');
    expect(() => engine.consumeOne()).toThrow(CrashSimulation);

    const restarted = new BenchEngine(new FileStorage(dir));
    expect(restarted.snapshot().liveStateHash).toBe(expectedHash);
    restarted.consumeBatch(5);
    const s = restarted.snapshot();
    expect(s.aggregates.a).toMatchObject({ count: 2, sum: 12, avg: 6 });
    expect(s.counters.delivered).toBeGreaterThanOrEqual(2);
    expect(s.counters.effective).toBe(2);
    rmSync(dir, { recursive: true, force: true });
  });
});

describe('分区独立推进与合并展示', () => {
  it('分区间无全局顺序：按轮询交替消费，各分区游标独立', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [
      [{ key: 'a' }, { key: 'a' }],
      [{ key: 'b' }, { key: 'b' }, { key: 'b' }],
    ]);
    engine.consumeOne(); // p0
    engine.consumeOne(); // p1
    engine.consumeOne(); // p0
    const s = engine.snapshot();
    expect(s.progress.p0.offset).toBe(2);
    expect(s.progress.p1.offset).toBe(1);
    expect(s.aggregates.a.count).toBe(2);
    expect(s.aggregates.b.count).toBe(1);
    rmSync(dir, { recursive: true, force: true });
  });

  it('只处理单分区时不影响其他分区的游标与状态', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [
      [{ key: 'a' }, { key: 'a' }],
      [{ key: 'b' }],
    ]);
    engine.consumeBatch(2, 'p1');
    expect(() => engine.consumeBatch(1, 'p1')).toThrow(EngineError);
    const s = engine.snapshot();
    expect(s.progress.p0.offset).toBe(0);
    expect(s.progress.p1.offset).toBe(1);
    rmSync(dir, { recursive: true, force: true });
  });
});
