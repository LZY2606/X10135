import { createHash } from 'node:crypto';
import {
  LATEST_VERSION,
  PROCESSOR_INFO,
  PROCESSOR_VERSIONS,
  type AggState,
  type AggV1,
  type AggV2,
  type AggV3,
  type EventRecord,
  type ProcessorVersion,
} from './types';

export class MigrationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'MigrationError';
  }
}

export interface ApplyResult {
  state: AggState;
  /** true 表示幂等检查之后真正改变了状态 */
  changed: boolean;
}

function freshState(version: ProcessorVersion): AggState {
  if (version === 1) return { count: 0 };
  if (version === 2) return { count: 0, sum: 0 };
  return { count: 0, sum: 0, avg: 0 };
}

/** 把一个事件作用于 key 的聚合状态。 */
export function applyEvent(
  version: ProcessorVersion,
  prev: AggState | undefined,
  event: EventRecord,
): ApplyResult {
  const state: AggState = prev ? structuredClone(prev) : freshState(version);
  if (version === 1) {
    const v = state as AggV1;
    v.count += 1;
    return { state: v, changed: true };
  }
  if (version === 2) {
    const v = state as AggV2;
    v.count += 1;
    v.sum = round2(v.sum + event.value);
    return { state: v, changed: true };
  }
  const v = state as AggV3;
  v.count += 1;
  v.sum = round2(v.sum + event.value);
  v.avg = round2(v.sum / v.count);
  return { state: v, changed: true };
}

/** v1 -> v2：v1 期间没有记录数值，sum 只能从 0 起步。 */
export function migrateState(
  from: ProcessorVersion,
  to: ProcessorVersion,
  prev: AggState | undefined,
): AggState {
  if (from === to) return prev ? structuredClone(prev) : freshState(to);
  if (from > to) {
    throw new MigrationError(`不支持降级：${from} -> ${to}`);
  }
  if (to !== from + 1) {
    throw new MigrationError(`只能迁移到相邻版本，收到 ${from} -> ${to}`);
  }
  if (from === 1 && to === 2) {
    const v1 = (prev ?? { count: 0 }) as AggV1;
    return { count: v1.count, sum: 0 } satisfies AggV2;
  }
  if (from === 2 && to === 3) {
    const v2 = (prev ?? { count: 0, sum: 0 }) as AggV2;
    return {
      count: v2.count,
      sum: v2.sum,
      avg: v2.count > 0 ? round2(v2.sum / v2.count) : 0,
    } satisfies AggV3;
  }
  throw new MigrationError(`未知迁移路径 ${from} -> ${to}`);
}

export function canMigrate(from: ProcessorVersion, to: ProcessorVersion): boolean {
  return PROCESSOR_VERSIONS.includes(from) && PROCESSOR_VERSIONS.includes(to) && from < to;
}

export function processorName(version: ProcessorVersion): string {
  return PROCESSOR_INFO[version].name;
}

export { LATEST_VERSION };

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}

/** 仅由“语义状态”决定的哈希：已提交 offset + 聚合，不含 hwm/计数器/seen/seq。
 *  不包含输入日志高水位，保证“处理到同一提交位置”时哈希与事件追加时机无关。 */
export function semanticStateHash(input: {
  version: ProcessorVersion;
  progress: Record<string, { offset: number; hwm?: number }>;
  aggregates: Record<string, AggState>;
}): string {
  const progress = Object.keys(input.progress)
    .sort()
    .map((p) => [p, input.progress[p].offset]);
  const aggregates = Object.keys(input.aggregates)
    .sort()
    .map((k) => [k, input.aggregates[k]]);
  const canonical = JSON.stringify({
    version: input.version,
    progress,
    aggregates,
  });
  return createHash('sha256').update(canonical).digest('hex').slice(0, 16);
}
