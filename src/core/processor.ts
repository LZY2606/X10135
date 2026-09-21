import type { EventRecord } from './types';

export interface AggregateV1 {
  total: number;
  count: number;
  perKey: Record<string, number>;
}

export interface AggregateV2 extends AggregateV1 {
  distinctKeys: string[];
  maxPerKey: Record<string, number>;
}

/**
 * 聚合处理器。apply 必须是纯函数，且对同一批事件满足交换律/结合律：
 * 分区间没有全局到达顺序，任何合并顺序都必须得到相同的最终状态。
 */
export interface Processor {
  readonly version: string;
  initial(): unknown;
  apply(state: unknown, event: EventRecord): unknown;
}

function applyV1(state: AggregateV1, event: EventRecord): AggregateV1 {
  return {
    total: state.total + event.value,
    count: state.count + 1,
    perKey: {
      ...state.perKey,
      [event.key]: (state.perKey[event.key] ?? 0) + event.value,
    },
  };
}

export const processorV1: Processor = {
  version: 'v1',
  initial(): AggregateV1 {
    return { total: 0, count: 0, perKey: {} };
  },
  apply(state: unknown, event: EventRecord): AggregateV1 {
    return applyV1(state as AggregateV1, event);
  },
};

export const processorV2: Processor = {
  version: 'v2',
  initial(): AggregateV2 {
    return { total: 0, count: 0, perKey: {}, distinctKeys: [], maxPerKey: {} };
  },
  apply(state: unknown, event: EventRecord): AggregateV2 {
    const current = state as AggregateV2;
    const base = applyV1(current, event);
    const seen = current.distinctKeys.includes(event.key);
    return {
      ...base,
      distinctKeys: seen
        ? current.distinctKeys
        : [...current.distinctKeys, event.key].sort(),
      maxPerKey: {
        ...current.maxPerKey,
        [event.key]: Math.max(current.maxPerKey[event.key] ?? -Infinity, event.value),
      },
    };
  },
};

export const processors: Record<string, Processor> = {
  v1: processorV1,
  v2: processorV2,
};

/** v1 → v2 显式迁移：纯函数，失败（抛错）时不得污染原检查点。 */
export function migrateV1toV2(state: unknown): AggregateV2 {
  const old = state as AggregateV1;
  if (typeof old.total !== 'number' || typeof old.perKey !== 'object') {
    throw new Error('v1 状态结构不完整，无法迁移');
  }
  const distinctKeys = Object.keys(old.perKey).sort();
  const maxPerKey: Record<string, number> = {};
  // 历史逐事件最大值已不可考，以各 key 的累计值作为迁移基线。
  for (const key of distinctKeys) maxPerKey[key] = old.perKey[key];
  return { ...old, distinctKeys, maxPerKey };
}

export const migrations: Record<string, (state: unknown) => unknown> = {
  'v1->v2': migrateV1toV2,
};
