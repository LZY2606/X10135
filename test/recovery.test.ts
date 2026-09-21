import { describe, expect, it } from "vitest";
import { readFileSync, existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { expectCrash, freshEngine, reopen, setupPartitions } from "./helpers.js";

describe("崩溃注入与检查点恢复", () => {
  it("写状态前崩溃：事件完全回滚，恢复后重投并由幂等键收敛", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 2);
    await engine.setConfig({ checkpointEvery: 0 });
    await engine.processBatch(1);
    await engine.checkpoint();
    const hashBefore = engine.status().stateHash;

    await engine.armCrash("beforeStateWrite");
    await expectCrash(() => engine.step(), "beforeStateWrite");

    const restarted = await reopen(dir);
    expect(restarted.status().stateHash).toBe(hashBefore);
    expect(restarted.status().partitions[0]!.displayedCursor).toBe(1);
    await restarted.step();
    expect(restarted.status().partitions[0]!.displayedCursor).toBe(2);
    await cleanup();
  });

  it("写状态后/提交 offset 前崩溃：prepared 事务悬挂，恢复时忽略，重投不重复计数", async () => {
    for (const point of [
      "afterStateWriteBeforeCommit",
      "beforeOffsetCommit",
    ] as const) {
      const { engine, dir, cleanup } = await freshEngine();
      await setupPartitions(engine, 1, 2);
      await engine.setConfig({ checkpointEvery: 0 });
      await engine.processBatch(1);
      await engine.checkpoint();
      const before = engine.status();
      const deliveriesBefore = before.stats.deliveries;
      const effectiveBefore = before.stats.effectiveChanges;

      await engine.armCrash(point);
      await expectCrash(() => engine.step(), point);

      const walRaw = readFileSync(join(dir, "wal", "wal.jsonl"), "utf8")
        .split("\n")
        .filter(Boolean)
        .map((l) => JSON.parse(l) as { stage: string });
      expect(walRaw.some((r) => r.stage === "prepared")).toBe(true);

      const restarted = await reopen(dir);
      const after = restarted.status();
      expect(after.stats.deliveries).toBe(deliveriesBefore);
      expect(after.stats.effectiveChanges).toBe(effectiveBefore);
      expect(after.partitions[0]!.displayedCursor).toBe(1);

      await restarted.step();
      const redelivered = restarted.status();
      expect(redelivered.stats.deliveries).toBe(deliveriesBefore + 1);
      expect(redelivered.stats.effectiveChanges).toBe(effectiveBefore + 1);

      const walLines = readFileSync(join(dir, "wal", "wal.jsonl"), "utf8")
        .split("\n").filter(Boolean).map((l) => JSON.parse(l) as { seq: number; stage: string });
      const preparedSeqs = walLines.filter((r) => r.stage === "prepared").map((r) => r.seq);
      const committedSeqs = new Set(walLines.filter((r) => r.stage === "committed").map((r) => r.seq));
      for (const seq of preparedSeqs) expect(committedSeqs.has(seq)).toBe(true);
      await cleanup();
    }
  });

  it("检查点 rename 前崩溃：临时文件被忽略，仍恢复上一个完整检查点", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 5);
    await engine.setConfig({ checkpointEvery: 0 });
    await engine.processBatch(3);
    await engine.checkpoint();
    const firstCpHash = engine.status().stateHash;
    void firstCpHash;
    await engine.processBatch(2);
    const hashAfterTwoMore = engine.status().stateHash;

    await engine.armCrash("beforeCheckpointRename");
    await expectCrash(() => engine.checkpoint(), "beforeCheckpointRename");

    const cpDir = join(dir, "checkpoints");
    const tmps = readdirSync(cpDir).filter((f) => f.startsWith(".tmp"));
    expect(tmps.length).toBe(1);

    const restarted = await reopen(dir);
    const status = restarted.status();
    expect(status.recoveredFromSeq).toBe(1);
    expect(status.stateHash).toBe(hashAfterTwoMore);
    expect(status.partitions[0]!.displayedCursor).toBe(5);
    expect(status.replayedCommits).toBe(2);
    await cleanup();
  });

  it("检查点 rename 后崩溃：新检查点已生效，从新检查点恢复且 WAL 被轮转", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 5);
    await engine.setConfig({ checkpointEvery: 0 });
    await engine.processBatch(3);
    await engine.checkpoint();
    await engine.processBatch(2);
    const hashAtSecond = engine.status().stateHash;

    await engine.armCrash("afterCheckpointRename");
    await expectCrash(() => engine.checkpoint(), "afterCheckpointRename");

    const walRaw = readFileSync(join(dir, "wal", "wal.jsonl"), "utf8").trim();
    expect(walRaw).not.toBe("");

    const restarted = await reopen(dir);
    const status = restarted.status();
    expect(status.recoveredFromSeq).toBe(2);
    expect(status.stateHash).toBe(hashAtSecond);
    expect(status.partitions[0]!.displayedCursor).toBe(5);
    expect(readFileSync(join(dir, "wal", "wal.jsonl"), "utf8").trim()).toBe("");
    await cleanup();
  });

  it("写了一半（损坏）的检查点文件在恢复时被识别并忽略", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 3);
    await engine.setConfig({ checkpointEvery: 0 });
    await engine.processBatch(3);
    await engine.checkpoint();
    const hash1 = engine.status().stateHash;

    const broken = join(dir, "checkpoints", "checkpoint-000002.json");
    const { writeFileSync } = await import("node:fs");
    writeFileSync(broken, '{"kind":"checkpoint","seq":2,"walBaseSeq":99,"sta');

    const restarted = await reopen(dir);
    const status = restarted.status();
    expect(status.recoveredFromSeq).toBe(1);
    expect(status.stateHash).toBe(hash1);
    expect(status.checkpoints.find((c) => c.seq === -1)?.valid).toBe(false);
    await cleanup();
  });

  it("恢复从最后完整检查点开始，而不是界面最后显示的位置", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 6);
    await engine.setConfig({ checkpointEvery: 0 });
    await engine.processBatch(2);
    await engine.checkpoint();
    await engine.processBatch(3);
    expect(engine.status().partitions[0]!.displayedCursor).toBe(5);
    expect(engine.status().partitions[0]!.committedOffset).toBe(5);

    const restarted = await reopen(dir);
    const status = restarted.status();
    expect(status.partitions[0]!.committedOffset).toBe(5);
    expect(status.recoveredFromSeq).toBe(1);
    expect(status.partitions[0]!.displayedCursor).toBe(5);
    await cleanup();
  });
});
