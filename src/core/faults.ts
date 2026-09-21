/** 可注入故障点：覆盖状态写入、offset 提交、检查点 rename 与迁移等关键路径。 */
export type FaultPoint =
  | 'beforeStateWrite'
  | 'afterStateWrite'
  | 'beforeOffsetCommit'
  | 'afterOffsetCommit'
  | 'beforeCheckpointWrite'
  | 'beforeCheckpointRename'
  | 'afterCheckpointRename'
  | 'duringMigration';

export const FAULT_POINTS: FaultPoint[] = [
  'beforeStateWrite',
  'afterStateWrite',
  'beforeOffsetCommit',
  'afterOffsetCommit',
  'beforeCheckpointWrite',
  'beforeCheckpointRename',
  'afterCheckpointRename',
  'duringMigration',
];

export const FAULT_POINT_LABELS: Record<FaultPoint, string> = {
  beforeStateWrite: '写状态前',
  afterStateWrite: '写状态后（tmp 已落盘）',
  beforeOffsetCommit: '提交 offset 前',
  afterOffsetCommit: '提交 offset 后',
  beforeCheckpointWrite: '写检查点前',
  beforeCheckpointRename: '检查点 rename 前',
  afterCheckpointRename: '检查点 rename 后',
  duringMigration: '版本迁移过程中',
};

/** 模拟进程崩溃：引擎实例收到该错误后必须废弃，从存储重新恢复。 */
export class CrashError extends Error {
  constructor(public readonly point: FaultPoint | 'manual') {
    super(`模拟进程崩溃 @ ${point}`);
    this.name = 'CrashError';
  }
}

/**
 * 故障注入器。武装某个故障点后，下一次（或前 times 次）经过该点时抛出
 * CrashError，模拟进程在该精确位置崩溃。
 */
export class FaultInjector {
  private armed = new Map<FaultPoint, number>();

  arm(point: FaultPoint, times = 1): void {
    this.armed.set(point, (this.armed.get(point) ?? 0) + times);
  }

  disarm(): void {
    this.armed.clear();
  }

  check(point: FaultPoint): void {
    const remaining = this.armed.get(point);
    if (remaining === undefined) return;
    if (remaining <= 1) this.armed.delete(point);
    else this.armed.set(point, remaining - 1);
    throw new CrashError(point);
  }
}
