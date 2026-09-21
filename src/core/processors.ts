import type { AggregateRecord, InputEvent, ProcessorState } from './types';
import { emptyState } from './types';

export interface ProcessResult {
  state: ProcessorState;
  effectiveChange: boolean;
  duplicate: boolean;
}

export function eventIdempotencyKey(event: InputEvent): string {
  return event.idempotencyKey ?? `${event.partition}:${event.offset}`;
}

function cloneState(state: ProcessorState): ProcessorState {
  return structuredClone(state);
}

export function processEvent(state: ProcessorState, event: InputEvent): ProcessResult {
  const next = cloneState(state);
  const dedupeKey = eventIdempotencyKey(event);
  const duplicate = next.seenIdempotencyKeys.includes(dedupeKey);

  if (duplicate) {
    return { state: next, effectiveChange: false, duplicate: true };
  }

  const previous = next.records[event.key] ?? { key: event.key, count: 0, sum: 0 };
  const record: AggregateRecord = {
    ...previous,
    count: previous.count + 1,
    sum: previous.sum + event.value,
    lastPartition: event.partition,
    lastOffset: event.offset
  };
  if (next.version === 2) {
    record.average = record.sum / record.count;
  }

  next.records[event.key] = record;
  next.seenIdempotencyKeys = [...next.seenIdempotencyKeys, dedupeKey].sort();
  return { state: next, effectiveChange: true, duplicate: false };
}

export function migrateV1ToV2(state: ProcessorState): ProcessorState {
  if (state.version !== 1) {
    throw new Error(`只能从 v1 迁移，当前为 v${state.version}`);
  }
  const next = cloneState(state);
  next.version = 2;
  for (const record of Object.values(next.records)) {
    record.average = record.count === 0 ? 0 : record.sum / record.count;
  }
  return next;
}

export function initialState(version: 1 | 2 = 1): ProcessorState {
  return emptyState(version);
}
