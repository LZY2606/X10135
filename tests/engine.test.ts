import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, test } from 'vitest';
import { CheckpointLabEngine } from '../src/core/engine';
import { SimulatedCrash } from '../src/core/types';
import { FileStorage } from '../src/node/file-storage';

async function freshRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'checkpoint-lab-'));
  testRoots.push(root);
  return root;
}

const testRoots: string[] = [];

async function boot(root: string, version: 1 | 2 = 1): Promise<{
  engine: CheckpointLabEngine;
  storage: FileStorage;
}> {
  const storage = new FileStorage(root);
  const engine = new CheckpointLabEngine(storage, { processorVersion: version });
  await engine.start();
  return { engine, storage };
}

async function restart(root: string, version: 1 | 2 = 1): Promise<CheckpointLabEngine> {
  const engine = new CheckpointLabEngine(new FileStorage(root), { processorVersion: version });
  await engine.start();
  return engine;
}

afterEach(async (context) => {
  for (const root of testRoots.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

test('同一事件状态更新与 offset 提交保持原子边界', async () => {
  const root = await freshRoot();
  const { engine } = await boot(root);
  await engine.createPartition('orders');
  await engine.appendEvent({ partition: 'orders', key: 'user-1', value: 10 });

  const snapshot = engine.getSnapshot();
  expect(snapshot.progress.orders).toBe(-1);
  expect(snapshot.state.records).toEqual({});

  await engine.step();
  const committed = engine.getSnapshot();
  expect(committed.progress.orders).toBe(0);
  expect(committed.state.records['user-1']?.sum).toBe(10);
});

test.each([
  ['beforeStateWrite', 'beforeStateWrite'],
  ['afterStateWrite', 'afterStateWrite'],
  ['beforeOffsetCommit', 'beforeOffsetCommit']
] as const)('%s 崩溃后丢弃未提交事务', async (_name, point) => {
  const root = await freshRoot();
  let { engine } = await boot(root);
  await engine.appendEvent({ partition: 'p0', key: 'k', value: 1 });
  engine.armCrash(point);
  await expect(engine.step()).rejects.toBeInstanceOf(SimulatedCrash);

  engine = await restart(root);
  expect(engine.getSnapshot().progress.p0).toBe(-1);
  expect(engine.getSnapshot().state.records).toEqual({});

  await engine.step();
  expect(engine.getSnapshot().progress.p0).toBe(0);
  expect(engine.getSnapshot().state.records.k?.sum).toBe(1);
});

test('rename 前的半写检查点会被忽略', async () => {
  const root = await freshRoot();
  let { engine, storage } = await boot(root);
  await engine.appendEvent({ partition: 'p0', key: 'k', value: 1 });
  engine.armCrash('beforeCheckpointRename');
  await expect(engine.checkpoint()).rejects.toBeInstanceOf(SimulatedCrash);

  expect((await storage.listCheckpoints())).toHaveLength(0);
  expect((await readdir(join(root, 'checkpoints'))).some((name) => name.startsWith('.tmp-'))).toBe(true);

  engine = await restart(root);
  expect(engine.getSnapshot().progress.p0).toBe(-1);
});

test('rename 后的完整检查点可在崩溃后恢复', async () => {
  const root = await freshRoot();
  let { engine } = await boot(root);
  await engine.appendEvent({ partition: 'p0', key: 'k', value: 3 });
  await engine.step();
  engine.armCrash('afterCheckpointRename');
  await expect(engine.checkpoint()).rejects.toBeInstanceOf(SimulatedCrash);

  engine = await restart(root);
  const snapshot = engine.getSnapshot();
  expect(snapshot.progress.p0).toBe(0);
  expect(snapshot.state.records.k?.sum).toBe(3);
  expect(snapshot.latestCheckpointId).toBeTypeOf('string');
});

test('重复投递会再次执行处理，但幂等键只允许一次有效状态变更', async () => {
  const root = await freshRoot();
  const { engine } = await boot(root);
  await engine.appendEvent({ partition: 'p0', key: 'k', value: 5 });
  await engine.step();
  await engine.redeliver('p0', 0);

  const snapshot = engine.getSnapshot();
  expect(snapshot.metrics.processAttempts).toBe(2);
  expect(snapshot.metrics.duplicateDeliveries).toBe(1);
  expect(snapshot.metrics.effectiveStateChanges).toBe(1);
  expect(snapshot.state.records.k).toMatchObject({ count: 1, sum: 5 });
});

test('各分区独立推进，展示采用稳定轮转但不引入全局顺序', async () => {
  const root = await freshRoot();
  const { engine } = await boot(root);
  await engine.appendEvent({ partition: 'a', key: 'a1', value: 1 });
  await engine.appendEvent({ partition: 'a', key: 'a2', value: 2 });
  await engine.appendEvent({ partition: 'b', key: 'b1', value: 4 });

  await engine.batch(3);
  const snapshot = engine.getSnapshot();
  expect(snapshot.progress.a).toBe(1);
  expect(snapshot.progress.b).toBe(0);
  expect(snapshot.state.records.a1?.lastOffset).toBe(0);
  expect(snapshot.state.records.a2?.lastOffset).toBe(1);
  expect(snapshot.state.records.b1?.lastOffset).toBe(0);

  await engine.checkpoint();
  const recovered = await restart(root);
  expect(recovered.getSnapshot().progress).toEqual({ a: 1, b: 0 });
});

test('处理器版本必须显式迁移，失败回滚且不污染原检查点', async () => {
  const root = await freshRoot();
  const { engine: v1, storage } = await boot(root, 1);
  await v1.appendEvent({ partition: 'p0', key: 'k', value: 8 });
  await v1.step();
  await v1.checkpoint();

  const before = await storage.listCheckpoints();
  expect(before).toHaveLength(1);

  const newV2 = new CheckpointLabEngine(storage, { processorVersion: 2 });
  await expect(newV2.start()).rejects.toThrow(/必须显式迁移/);

  await expect(v1.migrateLatest(2, { simulateFailure: true })).rejects.toThrow('模拟迁移转换器失败');
  const afterFailure = await storage.listCheckpoints();
  expect(afterFailure).toHaveLength(1);
  expect(afterFailure[0]?.checkpoint.state.records.k?.average).toBeUndefined();

  await v1.migrateLatest(2);
  const migrated = await restart(root, 2);
  expect(migrated.getSnapshot().state.records.k?.average).toBe(8);
  expect((await storage.listMigrations()).map((record) => record.status)).toEqual(['failed', 'succeeded']);
});

test('导出的实验可在空实例重放出相同语义哈希', async () => {
  const root = await freshRoot();
  const { engine, storage } = await boot(root);
  await engine.appendEvent({ partition: 'a', key: 'k', value: 2 });
  await engine.appendEvent({ partition: 'b', key: 'k', value: 5 });
  await engine.consumeAll();
  const bundle = await engine.exportExperiment();

  const replayRoot = await freshRoot();
  const replayStorage = new FileStorage(replayRoot);
  const replay = await CheckpointLabEngine.replayInEmptyStorage(replayStorage, bundle);
  expect(replay.matches).toBe(true);

  const importRoot = await freshRoot();
  const importStorage = new FileStorage(importRoot);
  await CheckpointLabEngine.importInto(importStorage, bundle);
  const imported = await restart(importRoot);
  await imported.consumeAll();
  expect(imported.getSnapshot().state.records.k?.sum).toBe(7);
});

test('检查点包含高水位，即使事件尚未处理', async () => {
  const root = await freshRoot();
  const { engine, storage } = await boot(root);
  await engine.appendEvent({ partition: 'p0', key: 'k', value: 1 });
  await engine.appendEvent({ partition: 'p0', key: 'k2', value: 2 });
  await engine.step();
  await engine.checkpoint();
  const checkpoints = await storage.listCheckpoints();
  expect(checkpoints.at(-1)?.checkpoint.highWatermarks.p0).toBe(1);
  expect(checkpoints.at(-1)?.checkpoint.progress.p0).toBe(0);
});
