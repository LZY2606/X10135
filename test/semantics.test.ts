import { describe, expect, it } from "vitest";
import { readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  cloneBundle,
  expectCrash,
  freshEngine,
  reopen,
  setupPartitions,
} from "./helpers.js";
import { readJsonl } from "../src/server/storage.js";

describe("幂等与重复投递", () => {
  it("崩溃重投会再次经过处理器，但有效变更只发生一次", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 1);
    await engine.checkpoint();
    await engine.armCrash("beforeOffsetCommit");
    await expectCrash(() => engine.step(), "beforeOffsetCommit");

    const r1 = await reopen(dir);
    expect(r1.status().stats).toMatchObject({ deliveries: 0, effectiveChanges: 0 });
    await r1.step();
    expect(r1.status().stats).toMatchObject({ deliveries: 1, effectiveChanges: 1 });
    await cleanup();
  });

  it("显式幂等键相同的两条事件只产生一次有效状态变更", async () => {
    const { engine, cleanup } = await freshEngine();
    await engine.addPartition();
    await engine.appendEvent({ partition: 0, key: "k", value: 5, idemKey: "dedup-1" });
    await engine.appendEvent({ partition: 0, key: "k", value: 7, idemKey: "dedup-1" });
    await engine.processBatch(2);
    const s = engine.status();
    expect(s.stats.deliveries).toBe(2);
    expect(s.stats.effectiveChanges).toBe(1);
    expect(s.aggregates[0]).toMatchObject({ key: "k", sum: 5, count: 1 });
    await cleanup();
  });

  it("prepared/committed 交错出现时悬挂的 prepared 不产生聚合，恢复后再次投递仍收敛", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 1);
    const walPath = join(dir, "wal", "wal.jsonl");
    writeFileSync(
      walPath,
      JSON.stringify({
        seq: 1,
        stage: "prepared",
        partition: 0,
        offset: 0,
        eventId: "x",
        key: "alpha",
        value: 1,
        idemKey: "p0:0",
      }) + "\n",
    );

    const restarted = await reopen(dir);
    expect(restarted.status().stats.effectiveChanges).toBe(0);
    expect(readFileSync(walPath, "utf8").trim()).toBe("");
    await restarted.step();
    const s = restarted.status();
    expect(s.stats.deliveries).toBe(1);
    expect(s.stats.effectiveChanges).toBe(1);
    await cleanup();
  });
});

describe("分区独立性", () => {
  it("分区各自维护游标，一个分区不阻塞另一个分区", async () => {
    const { engine, cleanup } = await freshEngine();
    await setupPartitions(engine, 2, 2);
    await engine.step(0);
    await engine.step(0);
    const s = engine.status();
    expect(s.partitions.find((p) => p.id === 0)?.displayedCursor).toBe(2);
    expect(s.partitions.find((p) => p.id === 1)?.displayedCursor).toBe(0);
    await engine.step(1);
    expect(engine.status().partitions.find((p) => p.id === 1)?.displayedCursor).toBe(1);
    await cleanup();
  });

  it("单分区崩溃恢复只回滚该分区未提交进度", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 2, 2);
    await engine.processBatch(2);
    await engine.checkpoint();
    await engine.armCrash("beforeStateWrite");
    await expectCrash(() => engine.step(0), "beforeStateWrite");

    const restarted = await reopen(dir);
    const p0 = restarted.status().partitions.find((p) => p.id === 0)!;
    const p1 = restarted.status().partitions.find((p) => p.id === 1)!;
    expect(p0.committedOffset).toBe(1);
    expect(p1.committedOffset).toBe(1);
    await cleanup();
  });

  it("跨分区聚合按 key 稳定合并，展示顺序不改变提交状态", async () => {
    const { engine, cleanup } = await freshEngine();
    await setupPartitions(engine, 3, 1);
    await engine.processBatch(3);
    const keys = engine.status().aggregates.map((a) => a.key);
    expect(keys).toEqual([...keys].sort());
    await cleanup();
  });
});
