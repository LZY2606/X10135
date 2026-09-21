import { describe, expect, it } from "vitest";
import {
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { freshEngine, reopen, setupPartitions } from "./helpers.js";
import { LabEngine } from "../src/server/engine.js";
import { readJsonl } from "../src/server/storage.js";
import type { ExportBundle, MigrationRecord } from "../src/shared/types.js";

function checkpointFingerprints(dir: string): string[] {
  return readdirSync(dir)
    .filter((f) => f.startsWith("checkpoint-") && f.endsWith(".json"))
    .sort()
    .map((f) => `${f}:${readFileSync(join(dir, f), "utf8")}`);
}

describe("处理器版本迁移", () => {
  it("版本变化后旧检查点不可直接恢复，必须显式迁移", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 2);
    await engine.processBatch(2);
    await engine.checkpoint();
    await engine.forceVersion(2);
    await engine.close();

    const blocked = await reopen(dir);
    expect(blocked.status().runState).toBe("needsMigration");
    await expect(blocked.step()).rejects.toThrow(/迁移/);
    await blocked.close();
    await cleanup();
  });

  it("显式迁移 v1 -> v2 生成新检查点，v2 字段可用", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 2);
    await engine.processBatch(2);
    await engine.checkpoint();
    await engine.forceVersion(2);
    await engine.close();

    const blocked = await reopen(dir);
    const rec = await blocked.migrate({ toVersion: 2 });
    expect(rec.ok).toBe(true);
    const s = blocked.status();
    expect(s.runState).toBe("paused");
    expect(s.aggregates.find((a) => a.key === "alpha")?.sumSq).toBe(0);
    await blocked.appendEvent({ partition: 0, key: "alpha", value: 4 });
    await blocked.step();
    expect(blocked.status().aggregates.find((a) => a.key === "alpha")?.sumSq).toBe(16);
    await blocked.close();
    await cleanup();
  });

  it("迁移失败不会污染原检查点（回滚）", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 2);
    await engine.processBatch(2);
    await engine.checkpoint();
    const originals = checkpointFingerprints(join(dir, "checkpoints"));
    const originalHash = engine.status().stateHash;
    await engine.forceVersion(2);
    await engine.close();

    const blocked = await reopen(dir);
    await expect(blocked.migrate({ toVersion: 99 as 2 })).rejects.toThrow(/迁移/);

    expect(checkpointFingerprints(join(dir, "checkpoints"))).toEqual(originals);
    const migrations = readJsonl<MigrationRecord>(join(dir, "migrations.jsonl"));
    expect(migrations[0]?.ok).toBe(false);

    const stillBlocked = await reopen(dir);
    expect(stillBlocked.status().runState).toBe("needsMigration");
    expect(stillBlocked.status().stateHash).toBe(originalHash);
    await stillBlocked.close();
    await cleanup();
  });

  it("v2 -> v1 不支持迁移，原检查点保持不变", async () => {
    const { engine, dir, cleanup } = await freshEngine();
    await setupPartitions(engine, 1, 1);
    await engine.forceVersion(2);
    await engine.processBatch(1);
    await engine.checkpoint();
    const originals = checkpointFingerprints(join(dir, "checkpoints"));
    await engine.forceVersion(1);
    await engine.close();

    const blocked = await reopen(dir);
    await expect(blocked.migrate({ toVersion: 1 })).rejects.toThrow(/迁移/);
    expect(checkpointFingerprints(join(dir, "checkpoints"))).toEqual(originals);
    await blocked.close();
    await cleanup();
  });
});

describe("导出与空实例重放", () => {
  it("导出包导入空实例后状态哈希与统计一致", async () => {
    const ctx = await freshEngine();
    await setupPartitions(ctx.engine, 2, 4);
    await ctx.engine.processBatch(5);
    await ctx.engine.checkpoint();
    await ctx.engine.processBatch(3);
    const bundle = await ctx.engine.exportBundle();
    const originalHash = ctx.engine.status().stateHash;
    const originalStats = { ...ctx.engine.status().stats };
    await ctx.cleanup();

    const target = mkdtempSync(join(tmpdir(), "cplab-import-"));
    const result = await LabEngine.importInto(target, bundle);
    expect(result.stateHash).toBe(originalHash);
    const imported = await LabEngine.open({ dataDir: target });
    const s = imported.status();
    expect(s.stateHash).toBe(originalHash);
    expect(s.stats).toEqual(originalStats);
    await imported.close();
    rmSync(target, { recursive: true, force: true });
  });

  it("导入两次得到相同状态哈希（确定性重放）", async () => {
    const ctx = await freshEngine();
    await setupPartitions(ctx.engine, 2, 3);
    await ctx.engine.processBatch(6);
    const bundle = await ctx.engine.exportBundle();
    await ctx.cleanup();

    const dirs = [
      mkdtempSync(join(tmpdir(), "cplab-r1-")),
      mkdtempSync(join(tmpdir(), "cplab-r2-")),
    ];
    const hashes: string[] = [];
    for (const dir of dirs) {
      const copy: ExportBundle = JSON.parse(JSON.stringify(bundle)) as ExportBundle;
      const r = await LabEngine.importInto(dir, copy);
      hashes.push(r.stateHash);
      rmSync(dir, { recursive: true, force: true });
    }
    expect(hashes[0]).toBe(hashes[1]);
  });

  it("非空目录拒绝导入", async () => {
    const ctx = await freshEngine();
    await setupPartitions(ctx.engine, 1, 1);
    const bundle = await ctx.engine.exportBundle();
    await ctx.cleanup();
    const target = mkdtempSync(join(tmpdir(), "cplab-bad-"));
    writeFileSync(join(target, "existing.txt"), "x");
    await expect(LabEngine.importInto(target, bundle)).rejects.toThrow(/空实例/);
    rmSync(target, { recursive: true, force: true });
  });
});
