import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CrashInjectedError, LabEngine } from "../src/server/engine.js";
import type { CrashPoint, ExportBundle } from "../src/shared/types.js";

export async function freshEngine(): Promise<{
  engine: LabEngine;
  dir: string;
  cleanup: () => Promise<void>;
}> {
  const dir = mkdtempSync(join(tmpdir(), "cplab-"));
  const engine = await LabEngine.open({ dataDir: dir });
  return {
    engine,
    dir,
    cleanup: async () => {
      await engine.close();
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

export async function reopen(dir: string): Promise<LabEngine> {
  return LabEngine.open({ dataDir: dir });
}

export async function expectCrash(
  fn: () => Promise<unknown> | unknown,
  point: CrashPoint,
): Promise<void> {
  let crashed = false;
  try {
    await fn();
  } catch (err) {
    if (err instanceof CrashInjectedError && err.point === point) crashed = true;
    else throw err;
  }
  if (!crashed) throw new Error(`期望在 ${point} 崩溃，但没有发生`);
}

export async function setupPartitions(
  engine: LabEngine,
  partitionCount = 2,
  eventsPerPartition = 3,
): Promise<void> {
  for (let i = 0; i < partitionCount; i++) await engine.addPartition();
  for (let p = 0; p < partitionCount; p++) {
    for (let i = 0; i < eventsPerPartition; i++) {
      await engine.appendEvent({
        partition: p,
        key: ["alpha", "beta", "gamma"][i % 3]!,
        value: i + 1,
      });
    }
  }
}

export function cloneBundle(b: ExportBundle): ExportBundle {
  return JSON.parse(JSON.stringify(b)) as ExportBundle;
}
