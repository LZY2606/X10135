import './style.css';
import { LocalStorageLabStorage } from './browser/local-storage';
import { CheckpointLabEngine } from './core/engine';
import type { CrashPoint, ExperimentBundle, ProcessorVersion, RuntimeSnapshot } from './core/types';

const storage = new LocalStorageLabStorage();
let engine = new CheckpointLabEngine(storage, { autoCheckpointEvery: 5 });
let timer: number | undefined;
let latestSnapshot: RuntimeSnapshot | undefined;
let events: Awaited<ReturnType<LocalStorageLabStorage['readEvents']>> = [];
let checkpoints: Awaited<ReturnType<LocalStorageLabStorage['listCheckpoints']>> = [];
let migrations: Awaited<ReturnType<LocalStorageLabStorage['listMigrations']>> = [];

const $ = <T extends HTMLElement>(selector: string): T => {
  const element = document.querySelector<T>(selector);
  if (!element) {
    throw new Error(`Missing element: ${selector}`);
  }
  return element;
};

const elements = {
  statusMessage: $('#statusMessage'),
  runtimeStatus: $('#runtimeStatus'),
  latestCheckpoint: $('#latestCheckpoint'),
  partitionRows: $('#partitionRows'),
  aggregateRows: $('#aggregateRows'),
  attempts: $('#attempts'),
  effectiveChanges: $('#effectiveChanges'),
  duplicates: $('#duplicates'),
  checkpointCount: $('#checkpointCount'),
  crashes: $('#crashes'),
  eventRows: $('#eventRows'),
  checkpointList: $('#checkpointList'),
  migrationList: $('#migrationList'),
  crashPoint: $<HTMLSelectElement>('#crashPoint'),
  processorVersion: $<HTMLSelectElement>('#processorVersion'),
  batchSize: $<HTMLInputElement>('#batchSize'),
  simulateMigrationFailure: $<HTMLInputElement>('#simulateMigrationFailure'),
  importFile: $<HTMLInputElement>('#importFile')
};

async function refresh(): Promise<void> {
  latestSnapshot = engine.getSnapshot();
  [events, checkpoints, migrations] = await Promise.all([
    storage.readEvents(),
    storage.listCheckpoints(),
    storage.listMigrations()
  ]);
  render(latestSnapshot);
}

function render(snapshot: RuntimeSnapshot): void {
  elements.statusMessage.textContent = snapshot.statusMessage;
  elements.runtimeStatus.textContent =
    snapshot.status === 'running' ? '运行中' :
    snapshot.status === 'paused' ? '已暂停' :
    snapshot.status === 'crashed' ? '已崩溃' : '已停止';
  elements.latestCheckpoint.textContent = snapshot.latestCheckpointId
    ? `最新检查点：${snapshot.latestCheckpointId}`
    : '无完整检查点';
  elements.processorVersion.value = String(snapshot.processorVersion);

  elements.partitionRows.innerHTML = snapshot.partitions.map((partition) => {
    const committed = snapshot.progress[partition] ?? -1;
    const hwm = snapshot.highWatermarks[partition] ?? -1;
    return `<tr><td>${escapeHtml(partition)}</td><td>${committed}</td><td>${hwm}</td><td>${Math.max(0, hwm - committed)}</td></tr>`;
  }).join('') || '<tr><td colspan="4">暂无分区</td></tr>';

  const records = Object.values(snapshot.state.records).sort((left, right) => left.key.localeCompare(right.key));
  elements.aggregateRows.innerHTML = records.map((record) => `<tr>
    <td>${escapeHtml(record.key)}</td><td>${record.count}</td><td>${record.sum}</td><td>${record.average?.toFixed(2) ?? '—'}</td>
  </tr>`).join('') || '<tr><td colspan="4">暂无聚合状态</td></tr>';

  elements.attempts.textContent = String(snapshot.metrics.processAttempts);
  elements.effectiveChanges.textContent = String(snapshot.metrics.effectiveStateChanges);
  elements.duplicates.textContent = String(snapshot.metrics.duplicateDeliveries);
  elements.checkpointCount.textContent = String(snapshot.metrics.completedCheckpoints);
  elements.crashes.textContent = String(snapshot.metrics.crashes);

  elements.eventRows.innerHTML = events.map((event) => {
    const committed = event.offset <= (snapshot.progress[event.partition] ?? -1);
    return `<tr>
      <td>${escapeHtml(event.partition)}</td><td>${event.offset}</td><td>${escapeHtml(event.key)}</td><td>${event.value}</td>
      <td>${escapeHtml(event.idempotencyKey ?? `${event.partition}:${event.offset}`)} ${committed ? '✅' : '⏳'}</td>
    </tr>`;
  }).join('') || '<tr><td colspan="5">暂无事件</td></tr>';

  elements.checkpointList.innerHTML = [...checkpoints].reverse().map((envelope) => `<li>
    <strong>${escapeHtml(envelope.checkpoint.id)}</strong>
    v${envelope.checkpoint.processorVersion} · ${new Date(envelope.checkpoint.createdAt).toLocaleString()}<br />
    offset=${formatRecord(envelope.checkpoint.progress)} · hwm=${formatRecord(envelope.checkpoint.highWatermarks)}
  </li>`).join('') || '<li>尚无完整检查点</li>';

  elements.migrationList.innerHTML = [...migrations].reverse().map((record) => `<li>
    <strong>v${record.fromVersion} → v${record.toVersion} <span class="badge ${record.status === 'succeeded' ? 'ok' : 'fail'}">${record.status === 'succeeded' ? '成功' : '失败'}</span></strong>
    ${escapeHtml(record.sourceCheckpointId)}${record.error ? `<br />${escapeHtml(record.error)}` : ''}
  </li>`).join('') || '<li>暂无迁移记录</li>';
}

function formatRecord(record: Record<string, number>): string {
  return Object.entries(record).sort(([left], [right]) => left.localeCompare(right)).map(([key, value]) => `${key}:${value}`).join(', ') || '{}';
}
function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char] ?? char);
}

async function runAction(action: () => Promise<unknown>): Promise<void> {
  try {
    await action();
    await refresh();
  } catch (error) {
    if (latestSnapshot) {
      latestSnapshot.statusMessage = error instanceof Error ? error.message : String(error);
      render({ ...latestSnapshot, status: engine.getSnapshot().status, statusMessage: latestSnapshot.statusMessage });
    }
  }
}

async function startEngine(versionOverride?: ProcessorVersion): Promise<void> {
  const version = versionOverride ?? Number(elements.processorVersion.value) as ProcessorVersion;
  engine = new CheckpointLabEngine(storage, { processorVersion: version, autoCheckpointEvery: 5 });
  await engine.start();
  await refresh();
}

function stopTimer(): void {
  if (timer !== undefined) {
    window.clearInterval(timer);
    timer = undefined;
  }
}

$('#partitionForm').addEventListener('submit', (event) => {
  event.preventDefault();
  const form = event.currentTarget as HTMLFormElement;
  const data = new FormData(form);
  void runAction(async () => {
    await engine.createPartition(String(data.get('partition')));
    form.reset();
  });
});

$('#eventForm').addEventListener('submit', (event) => {
  event.preventDefault();
  const form = event.currentTarget as HTMLFormElement;
  const data = new FormData(form);
  const idempotencyKey = String(data.get('idempotencyKey') ?? '').trim();
  void runAction(async () => {
    await engine.appendEvent({
      partition: String(data.get('partition')),
      key: String(data.get('key')),
      value: Number(data.get('value')),
      ...(idempotencyKey ? { idempotencyKey } : {})
    });
    form.reset();
  });
});

$('#restartBtn').addEventListener('click', () => void runAction(() => startEngine()));
$('#pauseBtn').addEventListener('click', () => void runAction(async () => {
  stopTimer();
  engine.pause();
}));
$('#resumeBtn').addEventListener('click', () => void runAction(async () => {
  engine.resume();
  stopTimer();
  timer = window.setInterval(() => void runAction(() => engine.step()), 650);
}));
$('#stepBtn').addEventListener('click', () => void runAction(() => engine.step()));
$('#batchBtn').addEventListener('click', () => void runAction(() => engine.batch(Math.max(1, Number(elements.batchSize.value)))));
$('#consumeAllBtn').addEventListener('click', () => void runAction(() => engine.consumeAll()));
$('#checkpointBtn').addEventListener('click', () => void runAction(() => engine.checkpoint()));
$('#armCrashBtn').addEventListener('click', () => void runAction(async () => {
  engine.armCrash(elements.crashPoint.value as CrashPoint);
}));
$('#clearCrashBtn').addEventListener('click', () => void runAction(async () => {
  engine.clearArmedCrash();
}));
$('#migrateBtn').addEventListener('click', () => void runAction(async () => {
  const target = Number(elements.processorVersion.value) as ProcessorVersion;
  await engine.migrateLatest(target, { simulateFailure: elements.simulateMigrationFailure.checked });
}));

$('#exportBtn').addEventListener('click', () => void runAction(async () => {
  await engine.consumeAll();
  const bundle = await engine.exportExperiment();
  const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `checkpoint-experiment-${new Date().toISOString().replaceAll(':', '-')}.json`;
  anchor.click();
  URL.revokeObjectURL(url);
}));

elements.importFile.addEventListener('change', () => void runAction(async () => {
  const file = elements.importFile.files?.[0];
  if (!file) {
    return;
  }
  const bundle = JSON.parse(await file.text()) as ExperimentBundle;
  stopTimer();
  await CheckpointLabEngine.importInto(storage, bundle);
  await startEngine(bundle.finalProcessorVersion);
  await engine.replayToProgress(bundle.finalProgress);
  elements.importFile.value = '';
}));

$('#clearBtn').addEventListener('click', () => void runAction(async () => {
  stopTimer();
  await storage.clearAll();
  await startEngine(1);
}));

void (async () => {
  try {
    const stored = await storage.listCheckpoints();
    const latest = stored.at(-1);
    await startEngine(latest?.checkpoint.processorVersion ?? 1);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes('v1') && message.includes('v2')) {
      elements.processorVersion.value = '2';
    }
    elements.statusMessage.textContent = `启动失败：${message}。请先显式迁移。`;
    elements.runtimeStatus.textContent = '已停止';
  }
})();
