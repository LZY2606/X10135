import { canonicalJson, sha256Hex } from './hash';
import type {
  CheckpointData,
  CheckpointEnvelope,
  HighWatermarks,
  PartitionProgress,
  ProcessorState
} from './types';

export function semanticInput(input: {
  processorVersion: 1 | 2;
  progress: PartitionProgress;
  highWatermarks: HighWatermarks;
  state: ProcessorState;
}) {
  return {
    processorVersion: input.processorVersion,
    progress: sortRecord(input.progress),
    highWatermarks: sortRecord(input.highWatermarks),
    state: {
      version: input.state.version,
      records: Object.fromEntries(
        Object.entries(input.state.records).sort(([left], [right]) => left.localeCompare(right))
      ),
      seenIdempotencyKeys: [...input.state.seenIdempotencyKeys].sort()
    }
  };
}

function sortRecord<T>(record: Record<string, T>): Record<string, T> {
  return Object.fromEntries(Object.entries(record).sort(([left], [right]) => left.localeCompare(right)));
}

export async function semanticHash(input: Parameters<typeof semanticInput>[0]): Promise<string> {
  return sha256Hex(canonicalJson(semanticInput(input)));
}

export async function buildEnvelope(checkpoint: CheckpointData): Promise<CheckpointEnvelope> {
  const hash = await semanticHash(checkpoint);
  return {
    kind: 'pairwise-gsb-checkpoint',
    formatVersion: 1,
    checkpoint,
    semanticHash: hash
  };
}

export async function isValidEnvelope(envelope: unknown): Promise<boolean> {
  if (!envelope || typeof envelope !== 'object') {
    return false;
  }
  const candidate = envelope as CheckpointEnvelope;
  if (candidate.kind !== 'pairwise-gsb-checkpoint' || candidate.formatVersion !== 1) {
    return false;
  }
  if (!candidate.checkpoint || !candidate.checkpoint.state) {
    return false;
  }
  const expected = await semanticHash(candidate.checkpoint);
  return expected === candidate.semanticHash;
}
