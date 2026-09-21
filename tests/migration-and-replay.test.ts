import { describe, expect, it } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BenchEngine, EngineError, replayExport } from '../src/core/engine';
import { FileStorage } from '../src/core/storage';
import { makeEngine, seed } from './helpers';

describe('处理器版本迁移', () => {
  it('版本不一致的旧检查点不能直接恢复，必须显式迁移', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [[{ key: 'a', value: 3 }], [{ key: 'b', value: 4 }]]);
    engine.deployVersion(1);
    engine.consumeBatch(2);
    engine.checkpoint('manual');

    // “部署新版本的代码”后重启
    engine.storage.writeMeta(2);
    const restarted = new BenchEngine(new FileStorage(dir));
    expect(restarted.status).toBe('awaiting_migration');
    expect(() => restarted.consumeOne()).toThrow(EngineError);

    const record = restarted.migrate();
    expect(record.success).toBe(true);
    const s = restarted.snapshot();
    expect(s.status).toBe('paused');
    expect(s.aggregates.a).toMatchObject({ count: 1, sum: 0 });
    expect(s.checkpoints.at(-1)!.data.processor.version).toBe(2);
    // 原检查点仍在
    expect(s.checkpoints.length).toBe(2);
    rmSync(dir, { recursive: true, force: true });
  });

  it('迁移失败不会污染原检查点，可在修复后重试成功', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [[{ key: 'a', value: 3 }, { key: 'a', value: 5 }]]);
    engine.deployVersion(1);
    engine.consumeBatch(2);
    engine.checkpoint('manual');
    engine.storage.writeMeta(2);

    let restarted = new BenchEngine(new FileStorage(dir));
    restarted.armFault('beforeMigrationCommit');
    const failed = restarted.migrate();
    expect(failed.success).toBe(false);
    expect(failed.resultCheckpoint).toBeNull();
    expect(restarted.status).toBe('awaiting_migration');

    // 原检查点完好无损
    const listing = restarted.storage.listCheckpoints();
    expect(listing.ok.length).toBe(1);
    expect(listing.ok[0].data.aggregates.a.count).toBe(2);

    // 重新执行迁移（模拟重启后再次尝试）
    restarted = new BenchEngine(new FileStorage(dir));
    const ok = restarted.migrate();
    expect(ok.success).toBe(true);
    expect(restarted.snapshot().aggregates.a).toMatchObject({ count: 2, sum: 0 });

    const records = restarted.storage.readMigrations();
    expect(records.map((r) => r.success)).toEqual([false, true]);
    rmSync(dir, { recursive: true, force: true });
  });

  it('链式迁移 v2 -> v3 补出 avg', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [[{ key: 'a', value: 2 }, { key: 'a', value: 4 }]]);
    engine.deployVersion(2);
    engine.consumeBatch(2);
    engine.checkpoint('manual');
    engine.storage.writeMeta(3);
    const restarted = new BenchEngine(new FileStorage(dir));
    restarted.migrate();
    expect(restarted.snapshot().aggregates.a).toMatchObject({ count: 2, sum: 6, avg: 3 });
    rmSync(dir, { recursive: true, force: true });
  });

  it('不允许降级；有未检查点处理量时禁止切换部署版本', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [[{ key: 'a' }]]);
    engine.deployVersion(2);
    engine.consumeOne();
    expect(() => engine.deployVersion(1)).toThrow(EngineError);
    engine.checkpoint('manual');
    expect(() => engine.deployVersion(1)).toThrow(/不支持降级|旧检查点/);
    rmSync(dir, { recursive: true, force: true });
  });
});

describe('导出与空实例重放', () => {
  it('导出后在全新空目录重放，最终状态哈希相同', () => {
    const { dir, engine } = makeEngine(2_000_000);
    seed(engine, [
      [
        { key: 'a', value: 1 },
        { key: 'b', value: 2 },
        { key: 'a', value: 3 },
        { key: 'b', value: 4 },
      ],
      [{ key: 'a', value: 5 }, { key: 'c', value: 6 }],
    ]);
    engine.deployVersion(1);
    engine.consumeBatch(3);
    engine.checkpoint('manual');
    engine.consumeBatch(10);
    engine.checkpoint('manual');
    const bundle = engine.exportExperiment();

    const replayDir = mkdtempSync(join(tmpdir(), 'cp-bench-replay-'));
    const result = replayExport(new FileStorage(replayDir), bundle);
    expect(result.matches).toBe(true);
    expect(result.replayHash).toBe(result.exportedHash);
    rmSync(dir, { recursive: true, force: true });
    rmSync(replayDir, { recursive: true, force: true });
  });

  it('含版本迁移的实验也能确定性重放出相同哈希', () => {
    const { dir, engine } = makeEngine(3_000_000);
    seed(engine, [
      [
        { key: 'a', value: 2 },
        { key: 'a', value: 4 },
        { key: 'b', value: 10 },
      ],
    ]);
    engine.deployVersion(1);
    engine.consumeBatch(2);
    engine.checkpoint('manual');
    engine.storage.writeMeta(2);
    let restarted = new BenchEngine(new FileStorage(dir));
    restarted.migrate();
    restarted.consumeOne(); // b=10
    restarted.checkpoint('manual');

    const bundle = restarted.exportExperiment();
    const replayDir = mkdtempSync(join(tmpdir(), 'cp-bench-replay-'));
    const result = replayExport(new FileStorage(replayDir), bundle);
    expect(result.matches).toBe(true);
    rmSync(dir, { recursive: true, force: true });
    rmSync(replayDir, { recursive: true, force: true });
  });

  it('跨分区不同消费顺序不改变最终语义哈希', () => {
    const { dir, engine } = makeEngine();
    seed(engine, [
      [{ key: 'a', value: 1 }, { key: 'a', value: 2 }],
      [{ key: 'a', value: 3 }, { key: 'b', value: 1 }],
    ]);
    engine.deployVersion(2);
    engine.consumeBatch(2, 'p0');
    engine.consumeBatch(2, 'p1');
    engine.checkpoint('manual');
    const hashA = engine.snapshot().checkpointStateHash;

    const { dir: dir2, engine: engine2 } = makeEngine();
    engine2.createPartition('p0');
    engine2.createPartition('p1');
    engine2.deployVersion(2);
    engine2.appendEvent('p0', 'a', 1);
    engine2.appendEvent('p1', 'a', 3);
    engine2.appendEvent('p0', 'a', 2);
    engine2.appendEvent('p1', 'b', 1);
    engine2.consumeBatch(10);
    engine2.checkpoint('manual');
    expect(engine2.snapshot().checkpointStateHash).toBe(hashA);
    rmSync(dir, { recursive: true, force: true });
    rmSync(dir2, { recursive: true, force: true });
  });
});
