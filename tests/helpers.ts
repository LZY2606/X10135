import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BenchEngine } from '../src/core/engine';
import { FileStorage } from '../src/core/storage';

export function makeEngine(clockStart = 1_000_000): {
  dir: string;
  engine: BenchEngine;
  storage: FileStorage;
  advance: (ms: number) => void;
} {
  const dir = mkdtempSync(join(tmpdir(), 'cp-bench-test-'));
  let now = clockStart;
  const storage = new FileStorage(dir);
  const engine = new BenchEngine(storage, () => now);
  return {
    dir,
    engine,
    storage,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

export function seed(
  engine: BenchEngine,
  spec: { key: string; value?: number }[][],
): void {
  spec.forEach((events, i) => {
    const name = `p${i}`;
    engine.createPartition(name);
    for (const e of events) engine.appendEvent(name, e.key, e.value ?? 1);
  });
}
