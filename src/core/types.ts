/** 输入日志中的一条事件。同一分区内 offset 严格递增，分区间无全局顺序。 */
export interface EventRecord {
  partition: number;
  offset: number;
  key: string;
  value: number;
  /** 幂等键：重复投递（同一幂等键）只允许影响最终状态一次。 */
  idempotencyKey: string;
  appendedAt: number;
}

/** 运行指标：processed 为“至少一次处理次数”，effective 为“有效状态变更次数”。 */
export interface Metrics {
  processed: number;
  effective: number;
}

/**
 * 已提交状态。状态更新与 offset 提交（cursors 前进）在同一个原子边界内完成：
 * 它们作为一条提交记录整体落盘（tmp + rename），要么同时可见，要么同时不可见。
 */
export interface CommittedState {
  txnSeq: number;
  /** 每个分区下一个待消费的 offset。 */
  cursors: Record<string, number>;
  aggregate: unknown;
  /** 已生效的幂等键（排序存储，保证哈希确定）。 */
  appliedKeys: string[];
  metrics: Metrics;
  processorVersion: string;
}

/** 检查点 = 分区进度 + 聚合状态 + 处理器版本 + 输入日志高水位。 */
export interface Checkpoint {
  seq: number;
  txnSeq: number;
  cursors: Record<string, number>;
  aggregate: unknown;
  appliedKeys: string[];
  metrics: Metrics;
  processorVersion: string;
  /** 建立检查点时每个分区输入日志的末尾 offset（高水位）。 */
  highWater: Record<string, number>;
  createdAt: number;
  /** 若由迁移产生，记录来源检查点 seq。 */
  migratedFrom: number | null;
  /** 对以上全部内容的校验和；写入一半或内容损坏的检查点会被识别并忽略。 */
  checksum: string;
}

export interface MigrationRecord {
  seq: number;
  fromCheckpoint: number;
  fromVersion: string;
  toVersion: string;
  /** 迁移产生的新检查点 seq；原检查点不被修改。 */
  checkpointSeq: number;
  at: number;
}
