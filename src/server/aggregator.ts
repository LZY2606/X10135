import { createHash } from "node:crypto";
import type { Aggregate, AggregatorState, ProcessorVersion } from "../shared/types.js";

export function emptyState(version: ProcessorVersion): AggregatorState {
  return { version, aggregates: {}, seenIdem: [] };
}

function stableClone(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableClone);
  if (value && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      out[key] = stableClone((value as Record<string, unknown>)[key]);
    }
    return out;
  }
  return value;
}

export function stableStringify(value: unknown): string {
  return JSON.stringify(stableClone(value));
}

export function stateHash(state: AggregatorState): string {
  return createHash("sha256").update(stableStringify(state)).digest("hex");
}

export interface ApplyResult {
  changed: boolean;
  delta: Aggregate | null;
}

export function applyEvent(
  state: AggregatorState,
  key: string,
  value: number,
  idemKey: string,
): ApplyResult {
  const seen = new Set(state.seenIdem);
  if (seen.has(idemKey)) return { changed: false, delta: null };

  const prev = state.aggregates[key] ?? { key, sum: 0, count: 0, sumSq: 0 };
  const next: Aggregate = {
    key,
    sum: prev.sum + value,
    count: prev.count + 1,
    sumSq: state.version === 2 ? prev.sumSq + value * value : 0,
  };
  state.aggregates[key] = next;
  state.seenIdem = [...state.seenIdem, idemKey].sort();
  return { changed: true, delta: next };
}

export function listAggregates(state: AggregatorState): Aggregate[] {
  return Object.values(state.aggregates).sort((a, b) => a.key.localeCompare(b.key));
}

export function migrateState(
  state: AggregatorState,
  from: ProcessorVersion,
  to: ProcessorVersion,
): AggregatorState {
  if (from === to) return state;
  if (from === 1 && to === 2) {
    return {
      version: 2,
      aggregates: Object.fromEntries(
        Object.values(state.aggregates).map((agg) => [agg.key, { ...agg, sumSq: 0 }]),
      ),
      seenIdem: [...state.seenIdem].sort(),
    };
  }
  throw new Error(`不支持的处理器迁移: v${from} -> v${to}`);
}
