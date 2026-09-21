import './style.css';
import { api } from './client';
import type { EngineSnapshot } from '../core/engine';
import {
  PROCESSOR_INFO,
  PROCESSOR_VERSIONS,
  type FaultPoint,
} from '../core/types';

interface FullState {
  snapshot: EngineSnapshot;
  faults: FaultPoint[];
  runConfig: {
    intervalMs: number;
    batchSize: number;
    checkpointEvery: number;
    autoCheckpoint: boolean;
  };
}

const FAULT_LABELS: Record<FaultPoint, string> = {
  beforeStateWrite: '写状态前',
  afterStateWrite: '写状态后 / 提交 offset 前',
  beforeOffsetCommit: '提交 offset 前',
  beforeCheckpointRename: '检查点 rename 前',
  afterCheckpointRename: '检查点 rename 后',
  beforeMigrationCommit: '迁移提交前',
};

const STATUS_LABELS: Record<EngineSnapshot['status'], string> = {
  ready: '就绪（空）',
  running: '运行中',
  paused: '已暂停',
  crashed: '已崩溃',
  awaiting_migration: '等待迁移',
};

let state: FullState | null = null;
let toastTimer: ReturnType<typeof setTimeout> | null = null;
let mergeByKey = true;

function toast(msg: string, kind: 'ok' | 'error' = 'ok'): void {
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.textContent = msg;
  document.body.appendChild(el);
  if (toastTimer) clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.remove(), 4000);
  setTimeout(() => el.remove(), 4200);
}

function esc(s: string): string {
  return s.replace(/[&<>"']/g, (c) =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]!,
  );
}

async function refresh(): Promise<void> {
  try {
    state = await api<FullState>('/state');
    render();
  } catch (err) {
    toast((err as Error).message, 'error');
  }
}

async function act(path: string, body?: unknown, message?: string): Promise<void> {
  try {
    await api(path, body ?? {});
    if (message) toast(message);
    await refresh();
  } catch (err) {
    toast((err as Error).message, 'error');
    await refresh();
  }
}

function headerHtml(): string {
  const s = state!.snapshot;
  const crashed = s.lastCrash;
  const banner = crashed
    ? `<div class="crash-banner">
         <strong>进程已终止：${esc(FAULT_LABELS[crashed.fault as FaultPoint])}</strong>
         <div class="muted">${esc(crashed.detail)}</div>
         <div class="muted">界面最后显示的位置（offset=${progressSummary(crashed.liveProgress)}）
         不等于恢复点；恢复以最后完整检查点为准（offset=${progressSummary(crashed.checkpointProgress)}）。</div>
         <div class="row" style="margin-top:8px">
           <button class="green" onclick="window.__bench.restart()">重启并从检查点恢复</button>
         </div>
       </div>`
    : '';
  const migration = s.status === 'awaiting_migration'
    ? `<div class="migration-banner">
         <strong>旧检查点处理器版本与当前部署不一致</strong>，需要显式迁移（或把部署版本切回旧版本）。
         <div class="row" style="margin-top:8px">
           <button class="primary" onclick="window.__bench.migrate()">执行迁移到 v${s.deployedVersion}</button>
         </div>
       </div>`
    : '';
  return `
    <h1>检查点实验台</h1>
    <p class="subtitle">本地流处理演示：同一分区内有序、分区间无全局顺序；状态更新与 offset 提交共享原子边界，检查点原子发布。</p>
    <div class="row">
      <span class="badge ${s.status}">${STATUS_LABELS[s.status]}</span>
      <span class="pill">部署处理器：${PROCESSOR_INFO[s.deployedVersion as 1 | 2 | 3].name}</span>
      <span class="pill">运行时版本：v${runtimeVersion()}</span>
      <span class="pill hash">内存状态哈希 ${s.liveStateHash}</span>
      <span class="pill ${s.checkpointStateHash && s.checkpointStateHash !== s.liveStateHash ? 'warn' : ''}">
        检查点哈希 ${s.checkpointStateHash ?? '—'}
      </span>
      ${s.ignoredCheckpoints.length ? `<span class="pill bad">已忽略半成品/损坏检查点 ${s.ignoredCheckpoints.length} 个</span>` : ''}
      ${s.lastRecoveryIgnored > 0 ? `<span class="pill warn">上次恢复忽略了 ${s.lastRecoveryIgnored} 个未完成检查点</span>` : ''}
    </div>
    <div style="height:10px"></div>
    ${banner}${migration}`;
}

function runtimeVersion(): number {
  const latest = state!.snapshot.checkpoints.at(-1);
  return latest ? latest.data.processor.version : state!.snapshot.deployedVersion;
}

function progressSummary(p: Record<string, { offset: number; hwm: number }>): string {
  const parts = Object.entries(p)
    .map(([k, v]) => `${k}:${v.offset}/${v.hwm}`)
    .join(' ');
  return parts || '空';
}

function controlPanelHtml(): string {
  const s = state!.snapshot;
  const status = s.status;
  const canConsume = status === 'ready' || status === 'paused';
  return `
  <div class="panel">
    <h2>运行控制</h2>
    <div class="row">
      <button class="green" ${status === 'running' ? 'disabled' : ''} onclick="window.__bench.start()">启动</button>
      <button class="amber" ${status !== 'running' ? 'disabled' : ''} onclick="window.__bench.pause()">暂停</button>
      <button class="primary" ${!canConsume ? 'disabled' : ''} onclick="window.__bench.step()">单步消费一条</button>
      <input id="batch-count" type="number" value="5" min="1" max="200" style="width:70px" />
      <button ${!canConsume ? 'disabled' : ''} onclick="window.__bench.batch()">批量消费</button>
      <button onclick="window.__bench.checkpoint()">建立检查点</button>
    </div>
    <div class="row">
      <label class="inline">
        <input type="checkbox" ${state!.runConfig.autoCheckpoint ? 'checked' : ''}
          onchange="window.__bench.setAuto(this.checked)" />
        自动检查点
      </label>
      <span class="muted">每</span>
      <input id="ckpt-every" type="number" min="1" max="100" value="${state!.runConfig.checkpointEvery}" style="width:64px" />
      <span class="muted">条一次；轮询间隔</span>
      <input id="interval" type="number" min="50" max="5000" step="50" value="${state!.runConfig.intervalMs}" style="width:80px" />
      <span class="muted">ms；每拍</span>
      <input id="tick-batch" type="number" min="1" max="50" value="${state!.runConfig.batchSize}" style="width:60px" />
      <span class="muted">条</span>
      <button class="small ghost" onclick="window.__bench.applyRunConfig()">应用</button>
    </div>
    <div class="legend">单步按分区轮询（round-robin）选择下一个有待处理事件的分区；仅保证同一分区内有序，跨分区顺序不是提交语义。</div>
  </div>`;
}

function partitionPanelHtml(): string {
  const s = state!.snapshot;
  const partitionNames = Object.keys(s.events);
  const rows = partitionNames
    .map((p) => {
      const prog = s.progress[p] ?? { offset: 0, hwm: 0 };
      const ckpt = s.checkpointProgress[p]?.offset ?? 0;
      const lag = prog.hwm - prog.offset;
      return `<tr>
        <td class="mono">${esc(p)}</td>
        <td class="num">${prog.hwm}</td>
        <td class="num">${prog.offset}</td>
        <td class="num">${ckpt}</td>
        <td class="num ${lag ? 'warn' : 'ok'}">${lag}</td>
      </tr>`;
    })
    .join('');
  const options = partitionNames
    .map((p) => `<option value="${esc(p)}">${esc(p)}</option>`)
    .join('');
  return `
  <div class="panel">
    <h2>分区与输入日志</h2>
    <div class="row">
      <input id="new-partition" placeholder="新分区名，如 orders" />
      <button onclick="window.__bench.createPartition()">创建分区</button>
    </div>
    <div class="row">
      <select id="ev-partition">${options}</select>
      <input id="ev-key" placeholder="key，如 user-1" />
      <input id="ev-value" type="number" value="1" style="width:90px" placeholder="value" />
      <button class="primary" onclick="window.__bench.appendEvent()">追加事件</button>
    </div>
    <table>
      <thead><tr><th>分区</th><th class="num">高水位(hwm)</th><th class="num">运行时游标</th><th class="num">检查点游标</th><th class="num">未处理</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function aggregatePanelHtml(): string {
  const s = state!.snapshot;
  const entries = Object.entries(s.aggregates);
  const order = mergeByKey
    ? entries.sort((a, b) => a[0].localeCompare(b[0]))
    : entries;
  const rows = entries.length
    ? order.map(([key, agg]) => `<tr>
        <td class="mono">${esc(key)}</td>
        <td class="num">${agg.count}</td>
        <td class="num">${'sum' in agg ? agg.sum : '—'}</td>
        <td class="num">${'avg' in agg ? agg.avg : '—'}</td>
      </tr>`).join('')
    : `<tr><td colspan="4" class="muted">还没有聚合状态</td></tr>`;
  return `
  <div class="panel">
    <h2>聚合状态（合并展示）</h2>
    <div class="row">
      <label class="inline">
        <input type="radio" name="merge" ${mergeByKey ? 'checked' : ''}
          onchange="window.__bench.setMerge(true)" /> 按 key 稳定排序
      </label>
      <label class="inline">
        <input type="radio" name="merge" ${!mergeByKey ? 'checked' : ''}
          onchange="window.__bench.setMerge(false)" /> 按首次出现顺序
      </label>
      <span class="muted">展示顺序只用于阅读，不代表提交语义</span>
    </div>
    <table>
      <thead><tr><th>key</th><th class="num">count</th><th class="num">sum</th><th class="num">avg</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function countersPanelHtml(): string {
  const s = state!.snapshot;
  const c = s.counters;
  const cc = s.checkpointCounters;
  return `
  <div class="panel">
    <h2>处理次数对比</h2>
    <table>
      <thead><tr><th></th><th class="num">运行时（可能丢失）</th><th class="num">最近检查点（恢复基准）</th></tr></thead>
      <tbody>
        <tr><td>至少一次处理次数</td><td class="num">${c.delivered}</td><td class="num">${cc ? cc.delivered : '—'}</td></tr>
        <tr><td class="ok">有效状态变更次数</td><td class="num ok">${c.effective}</td><td class="num ok">${cc ? cc.effective : '—'}</td></tr>
        <tr><td class="warn">被幂等键挡下的重复</td><td class="num warn">${c.duplicates}</td><td class="num warn">${cc ? cc.duplicates : '—'}</td></tr>
      </tbody>
    </table>
    <div class="legend">
      delivered = 经过处理器的总次数；effective = 真正改变状态的次数。
      崩溃后运行时列回退到检查点；重启后若事件被重新投递，delivered 会大于 effective，而聚合值不变。
      ${s.replayedAttempts > 0 ? `<span class="bad">上次崩溃丢弃的未检查点尝试：${s.replayedAttempts} 条（将在下一检查点后清零）</span>` : ''}
    </div>
    <div class="row" style="margin-top:8px">
      <span class="muted">手动重复投递：</span>
      <select id="redeliver-p">${Object.keys(s.events).map((p) => `<option>${esc(p)}</option>`).join('')}</select>
      <input id="redeliver-off" type="number" min="0" value="0" style="width:80px" placeholder="offset" />
      <button class="small amber" onclick="window.__bench.redeliver()">让该事件再过一次处理器</button>
    </div>
  </div>`;
}

function processorPanelHtml(): string {
  const s = state!.snapshot;
  const latest = s.checkpoints.at(-1);
  const ckptVersion = latest ? latest.data.processor.version : null;
  const radios = PROCESSOR_VERSIONS.map((v) => `
    <label class="inline" style="display:flex;gap:6px;align-items:flex-start;margin-bottom:4px">
      <input type="radio" name="deploy" value="${v}" ${s.deployedVersion === v ? 'checked' : ''}
        onchange="window.__bench.deploy(${v})" />
      <span><strong>v${v} ${esc(PROCESSOR_INFO[v].name)}</strong> — ${esc(PROCESSOR_INFO[v].desc)}</span>
    </label>`).join('');
  return `
  <div class="panel">
    <h2>处理器版本</h2>
    ${radios}
    <div class="legend">
      部署版本即“当前代码版本”。最近检查点版本：${ckptVersion ? `v${ckptVersion}` : '尚无检查点'}。
      已处理但未检查点时禁止切换；版本不一致的旧检查点必须显式迁移，v1→v2 时历史 value 无法恢复，sum 从 0 起算。
    </div>
  </div>`;
}

function faultsPanelHtml(): string {
  const faults = state!.faults;
  const points: FaultPoint[] = [
    'beforeStateWrite',
    'afterStateWrite',
    'beforeOffsetCommit',
    'beforeCheckpointRename',
    'afterCheckpointRename',
    'beforeMigrationCommit',
  ];
  const rows = points.map((p) => `
    <tr>
      <td>${esc(FAULT_LABELS[p])}</td>
      <td class="mono muted">${p}</td>
      <td>${faults.includes(p) ? '<span class="ok">已布防（一次性）</span>' : '<span class="muted">—</span>'}</td>
      <td>
        <button class="small ${faults.includes(p) ? 'ghost' : 'red'}"
          onclick="window.__bench.armFault('${p}')">布防</button>
        <button class="small ghost" onclick="window.__bench.disarmFault('${p}')">解除</button>
      </td>
    </tr>`).join('');
  return `
  <div class="panel">
    <h2>故障注入（预设崩溃点）</h2>
    <table>
      <thead><tr><th>崩溃点</th><th>标识</th><th>状态</th><th></th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="legend">所有崩溃点均为一次性触发。offset 三处崩溃点恢复后效果一致（回退到检查点 + 幂等去重）；rename 前的检查点会被忽略，rename 后的会被采用。</div>
  </div>`;
}

function checkpointsPanelHtml(): string {
  const s = state!.snapshot;
  const rows = s.checkpoints.slice().reverse().map((c) => `
    <tr>
      <td class="num">#${c.data.seq}</td>
      <td class="mono">${esc(c.name)}</td>
      <td>${esc(c.data.processor.name)}</td>
      <td>${esc(c.data.reason)}</td>
      <td class="num">${c.data.counters.effective}</td>
      <td class="num">${new Date(c.data.createdAt).toLocaleTimeString('zh-CN')}</td>
    </tr>`).join('');
  const ignored = s.ignoredCheckpoints
    .map((i) => `<div><span class="mono">${esc(i.name)}</span> — <span class="warn">${esc(i.reason)}</span></div>`)
    .join('');
  return `
  <div class="panel">
    <h2>检查点与迁移记录（全部持久化）</h2>
    <div class="scroll">
      <table>
        <thead><tr><th class="num">seq</th><th>文件</th><th>处理器</th><th>原因</th><th class="num">effective</th><th>时间</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="6" class="muted">尚无检查点</td></tr>'}</tbody>
      </table>
    </div>
    ${ignored ? `<div style="margin-top:8px"><strong class="warn">恢复时忽略：</strong>${ignored}</div>` : ''}
    ${migrationsHtml()}
  </div>`;
}

function migrationsHtml(): string {
  const migrations = state!.snapshot.migrations;
  if (!migrations.length) return '';
  const rows = migrations.slice().reverse().map((m) => `
    <tr>
      <td>v${m.fromVersion} → v${m.toVersion}</td>
      <td>${m.success ? '<span class="ok">成功</span>' : '<span class="bad">失败（原检查点未改动）</span>'}</td>
      <td class="muted">${esc(m.reason)}</td>
      <td class="num">${new Date(m.timestamp).toLocaleTimeString('zh-CN')}</td>
    </tr>`).join('');
  return `<h2 style="margin-top:12px">迁移记录</h2>
    <table>
      <thead><tr><th>路径</th><th>结果</th><th>说明</th><th class="num">时间</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

function eventsPanelHtml(): string {
  const s = state!.snapshot;
  const blocks = Object.entries(s.events).map(([p, events]) => {
    const cursor = s.progress[p]?.offset ?? 0;
    const rows = events.map((e) => {
      const processed = e.offset < cursor;
      const key = `${p}:${e.offset}`;
      return `<tr class="${processed ? '' : 'muted'}">
        <td class="num mono">${e.offset}</td>
        <td class="mono">${esc(e.key)}</td>
        <td class="num">${e.value}</td>
        <td>${processed ? '<span class="ok">已提交</span>' : '<span class="warn">未提交</span>'}</td>
        <td class="mono muted">${key}</td>
      </tr>`;
    }).join('');
    return `<div style="margin-bottom:10px">
      <strong class="mono">${esc(p)}</strong>（hwm=${events.length}，游标=${cursor}）
      <div class="scroll" style="max-height:170px">
        <table>
          <thead><tr><th class="num">offset</th><th>key</th><th class="num">value</th><th>状态</th><th>幂等键</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </div>`;
  }).join('');
  return `<div class="panel"><h2>输入日志（分区内严格有序）</h2>${blocks || '<p class="muted">先创建分区</p>'}</div>`;
}

function exportPanelHtml(): string {
  return `
  <div class="panel">
    <h2>实验导出与空实例重放</h2>
    <div class="row">
      <button class="primary" onclick="window.__bench.exportBundle()">导出实验 JSON</button>
      <span class="muted">或粘贴导出内容：</span>
      <button onclick="window.__bench.replayFile()">选择导出文件，在空实例重放</button>
    </div>
    <div id="replay-result" class="legend">重放会在一个全新的空存储目录上重放全部事件与迁移，并比较最终状态哈希。</div>
  </div>
  <div class="panel">
    <h2>危险操作</h2>
    <button class="red" onclick="window.__bench.reset()">清空全部数据（空实例）</button>
  </div>`;
}

function render(): void {
  if (!state) return;
  document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
    ${headerHtml()}
    ${controlPanelHtml()}
    <div class="grid">
      ${partitionPanelHtml()}
      ${countersPanelHtml()}
    </div>
    <div class="grid">
      ${aggregatePanelHtml()}
      ${processorPanelHtml()}
    </div>
    ${faultsPanelHtml()}
    ${checkpointsPanelHtml()}
    ${eventsPanelHtml()}
    ${exportPanelHtml()}`;
}

function val(id: string): string {
  return (document.getElementById(id) as HTMLInputElement | null)?.value ?? '';
}
function checked(id: string): boolean {
  return (document.getElementById(id) as HTMLInputElement | null)?.checked ?? false;
}

declare global {
  interface Window {
    __bench: Record<string, (...args: never[]) => void>;
  }
}

window.__bench = {
  start: () => void act('/start', {}, '已启动自动消费'),
  pause: () => void act('/pause', {}, '已暂停'),
  step: () => void act('/consume/one', {}),
  batch: () => void act('/consume/batch', { count: Number(val('batch-count')) || 5 }),
  checkpoint: () => void act('/checkpoint', {}, '检查点已原子发布'),
  restart: () => void act('/crash/restart', {}, '已重启：状态来自最后一个完整检查点'),
  createPartition: () => {
    const name = val('new-partition').trim();
    if (!name) return toast('分区名不能为空', 'error');
    void act('/partitions', { partition: name }, `分区 ${name} 已创建`);
  },
  appendEvent: () => {
    const partition = val('ev-partition');
    const key = val('ev-key').trim();
    if (!partition) return toast('请先创建并选择分区', 'error');
    if (!key) return toast('key 不能为空', 'error');
    void act('/events', { partition, key, value: Number(val('ev-value')) || 0 }, '事件已追加到输入日志');
  },
  redeliver: () =>
    void act(
      '/redeliver',
      { partition: val('redeliver-p'), offset: Number(val('redeliver-off')) },
      '事件被再次投递给处理器（被幂等键挡下）',
    ),
  deploy: (version: number) => void act('/processor/deploy', { version }),
  migrate: () => void act('/processor/migrate', {}, '迁移完成'),
  armFault: (fault: string) => void act('/faults/arm', { fault }, '故障点已布防，下一次触发即“崩溃”'),
  disarmFault: (fault: string) => void act('/faults/disarm', { fault }, '故障点已解除'),
  setAuto: (on: boolean) => void act('/run-config', { autoCheckpoint: on }),
  applyRunConfig: () =>
    void act('/run-config', {
      intervalMs: Number(val('interval')) || 400,
      batchSize: Number(val('tick-batch')) || 1,
      checkpointEvery: Number(val('ckpt-every')) || 5,
    }, '运行参数已应用'),
  setMerge: (byKey: boolean) => {
    mergeByKey = byKey;
    render();
  },
  reset: () => {
    if (!window.confirm('确认清空所有分区、事件、检查点与迁移记录？')) return;
    void act('/reset', {}, '已恢复为空实例');
  },
  exportBundle: async () => {
    const bundle = await api('/export');
    const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `checkpoint-bench-${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
    a.click();
    URL.revokeObjectURL(a.href);
    toast('已导出');
  },
  replayFile: () => {},
};

void refresh();
setInterval(() => {
  if (state?.snapshot.status === 'running') void refresh();
}, 700);

const fileInput = document.createElement('input');
fileInput.type = 'file';
fileInput.accept = 'application/json';
fileInput.style.display = 'none';
fileInput.addEventListener('change', async () => {
  const file = fileInput.files?.[0];
  if (!file) return;
  try {
    const bundle = JSON.parse(await file.text());
    const result = await api<{ matches: boolean; exportedHash: string; replayHash: string }>(
      '/replay',
      bundle,
    );
    const el = document.getElementById('replay-result');
    if (el) {
      el.innerHTML = result.matches
        ? `<span class="ok">✓ 空实例重放状态哈希一致</span>：导出 ${result.exportedHash} == 重放 ${result.replayHash}`
        : `<span class="bad">✗ 哈希不一致</span>：导出 ${result.exportedHash} ≠ 重放 ${result.replayHash}`;
    }
    toast(result.matches ? '重放哈希一致' : '重放哈希不一致', result.matches ? 'ok' : 'error');
  } catch (err) {
    toast((err as Error).message, 'error');
  } finally {
    fileInput.value = '';
  }
});
document.body.appendChild(fileInput);
window.__bench.replayFile = () => fileInput.click();
